import json
import difflib
import os
from tqdm import tqdm

# Configuration
IDENTITIES_FILE = "metadata/identities.json"
REPORT_FILE = "metadata/audit_potential_matches.json"

def is_initial_match(name1, name2):
    """
    Checks if 'S. Nakamoto' matches 'Satoshi Nakamoto' or vice versa.
    Simplified: first initial + same last name.
    """
    n1 = name1.lower().split()
    n2 = name2.lower().split()
    
    if len(n1) < 2 or len(n2) < 2:
        return False
        
    # Check if last names match exactly
    if n1[-1] == n2[-1]:
        # Check if first initial matches
        if n1[0][0] == n2[0][0]:
            return True
            
    # Check for reversal "Nakamoto, Satoshi" vs "Satoshi Nakamoto"
    n1_clean = [x.strip(",") for x in n1]
    n2_clean = [x.strip(",") for x in n2]
    if set(n1_clean) == set(n2_clean):
        return True
        
    return False

def audit_l4():
    if not os.path.exists(IDENTITIES_FILE):
        print(f"Error: {IDENTITIES_FILE} not found. Please run build_identities.py first.")
        return

    print(f"Loading {IDENTITIES_FILE}...")
    with open(IDENTITIES_FILE, "r") as f:
        data = json.load(f)
        identities = data.get("identities", [])

    # Pre-process records to speed up comparisons
    records = []
    for iden in identities:
        names = set([iden["display_name"]])
        if "git_signatures" in iden:
            names.update(iden["git_signatures"].get("names", []))
        if iden.get("platforms", {}).get("github"):
            names.add(iden["platforms"]["github"])
        
        # Filter out very short strings that cause noise
        clean_names = [n for n in names if n and len(n) > 2]
        if clean_names:
            records.append({
                "uuid": iden["uuid"],
                "names": clean_names,
                "display_name": iden["display_name"]
            })

    print(f"Auditing {len(records)} identities for Level 4 (Fuzzy) potential matches...")
    potential_matches = []
    
    # O(N^2) comparison with optimizations
    for i in tqdm(range(len(records))):
        r1 = records[i]
        for j in range(i + 1, len(records)):
            r2 = records[j]
            
            match_found = False
            for n1 in r1["names"]:
                for n2 in r2["names"]:
                    n1_l, n2_l = n1.lower(), n2.lower()
                    
                    # Skip exact same string - should have been merged by Level 2
                    if n1_l == n2_l:
                        score = 0.99 
                    # Level 4 Rule: Structural/Initial match
                    elif is_initial_match(n1_l, n2_l):
                        score = 0.95
                    # Level 4 Rule: Fuzzy similarity
                    else:
                        # Quick optimization: if length difference is too large, fuzzy ratio won't be high enough
                        if abs(len(n1_l) - len(n2_l)) > 6:
                            continue
                        
                        score = difflib.SequenceMatcher(None, n1_l, n2_l).ratio()
                    
                    if score > 0.88: # High confidence threshold for report
                        potential_matches.append({
                            "score": round(score, 3),
                            "id1": {
                                "uuid": r1["uuid"], 
                                "display": r1["display_name"], 
                                "match_string": n1
                            },
                            "id2": {
                                "uuid": r2["uuid"], 
                                "display": r2["display_name"], 
                                "match_string": n2
                            }
                        })
                        match_found = True
                        break
                if match_found: break

    # Sort by score descending
    potential_matches.sort(key=lambda x: x["score"], reverse=True)
    
    # Save the report
    os.makedirs(os.path.dirname(REPORT_FILE), exist_ok=True)
    with open(REPORT_FILE, "w") as f:
        json.dump({
            "audit_metadata": {
                "total_comparisons": len(records),
                "potential_matches_found": len(potential_matches),
                "threshold": 0.88
            },
            "matches": potential_matches
        }, f, indent=2)
    
    print(f"\n✨ Identity Auditor L4 Finished.")
    print(f"   Found {len(potential_matches)} matches for review.")
    print(f"   Report saved to: {REPORT_FILE}")

if __name__ == "__main__":
    audit_l4()
