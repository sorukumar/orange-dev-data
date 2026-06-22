import sys
import json
import os
import time
from pathlib import Path

# Add project root to path to allow importing utils
root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.append(str(root_dir))

from scripts.utils.identity import IdentityResolver

# --- Centralized Paths ---
REGISTRY_PATH = "metadata/contributors.json"
ALIASES_PATH = "metadata/identities.json"
RICH_CONTRIBUTORS_PATH = "output/tracker/contributors_rich.json"
SOCIAL_STATS_PATH = "data/enriched/social_stats.json"
BADGES_PATH = "metadata/badges.json"

def main():
    print("🔄 Starting Master Registry Sync...")
    
    # 1. Initialize empty Registry
    registry = {"contributors": [], "last_sync": None}
    
    # Map for easy lookup [ID (Canonical Name) -> Record]
    reg_map = {}

    # 3c. Initialize Identity Hub (The Brain)
    resolver = IdentityResolver()

    # 2. Load Static Badges
    badges_map = {}
    if os.path.exists(BADGES_PATH):
        with open(BADGES_PATH, 'r') as f:
            badges_map = json.load(f)

    # 3. Load Fresh Builds (Discovery Data)
    discovered_people = []
    if os.path.exists(RICH_CONTRIBUTORS_PATH):
        with open(RICH_CONTRIBUTORS_PATH, 'r') as f:
            discovered_people = json.load(f)
            
    # 3b. Load Social Discovery (Researchers/Reviewers)
    social_people = []
    if os.path.exists(SOCIAL_STATS_PATH):
        with open(SOCIAL_STATS_PATH, 'r') as f:
            social_people = json.load(f).get('contributors', [])



    # 4. Bootstrap from Identities Map (The full 4,000+ list)
    # We now trust the resolver's internal state
    print(f"  Bootstrapping from {len(resolver._identities)} defined identities...")
    
    valid_cids = set()
    for item in resolver._identities:
        cid = item.get('display_name') or item.get('canonical_name')
        if not cid: continue
        valid_cids.add(cid)
        
        if cid not in reg_map:
            reg_map[cid] = {
                "id": cid,
                "display_name": cid,
                "github": {"login": None, "location": "Undisclosed"},
                "identities": {"emails": [], "aliases": []},
                "badges": {},
                "roles": []
            }
        
        entry = reg_map[cid]
        if "identities" not in entry:
            entry["identities"] = {"emails": [], "aliases": []}
            
        # Sync aliases and emails from identities.json
        entry["identities"]["aliases"] = list(set(entry["identities"].get("aliases", []) + item.get("git_signatures", {}).get("names", [])))
        entry["identities"]["emails"] = list(set(entry["identities"].get("emails", []) + item.get("git_signatures", {}).get("emails", [])))
        
        # Check for manual handles in item (Identities Source of Truth)
        platforms = item.get('platforms', {})
        if platforms.get("github"):
             entry["github"]["login"] = platforms.get("github")
        
        if platforms.get("delving"):
             entry["delving"] = {"username": platforms.get("delving")}

    # 5. Syncing Logic (Overlaying build-time activity)
    print(f"  Syncing {len(discovered_people)} active core contributors...")
    for person in discovered_people:
        p_name_raw = person['name']
        
        # RESOLVE to canonical identity
        uuid = resolver.resolve_git(p_name_raw, person.get('email'))
        record = next((r for r in resolver._identities if r["uuid"] == uuid), None)
        p_name = record["display_name"] if record else p_name_raw
        p_name_lower = p_name.lower()
        
        # Create or update entry
        if p_name not in reg_map:
            reg_map[p_name] = {
                "id": p_name,
                "display_name": p_name,
                "github": {"login": person.get("login"), "location": person.get("location")},
                "identities": {"emails": [], "aliases": [p_name]},
                "badges": {},
                "roles": []
            }
            
        entry = reg_map[p_name]
        if "identities" not in entry:
            entry["identities"] = {"emails": [], "aliases": [p_name]}
        
        # Preserve existing location if UI data is empty
        if person.get("location") and person.get("location") != "Undisclosed":
            entry["github"]["location"] = person.get("location")
            
        if person.get("login"):
            entry["github"]["login"] = person.get("login")

        # Update Base Core Discovery
        entry["badges"]["is_core_contributor"] = True 
        entry["technical_focus"] = person.get("primary_category", "General")
        
        # Apply Badges from static compilation
        b = badges_map.get(uuid, {})
        entry["badges"].update(b)
        if b.get("roles"):
            for role in b["roles"]:
                if role not in entry["roles"]:
                    entry["roles"].append(role)

    # 6. Social Layer Discovery (Researchers/Reviewers)
    print(f"  Syncing {len(social_people)} social participants...")
    merged_count = 0
    new_count = 0
    for i, person in enumerate(social_people):
        p_name_raw = person['id'] 
        
        # person['id'] is ALREADY the canonical UUID
        uuid = p_name_raw
        record = next((r for r in resolver._identities if r["uuid"] == uuid), None)
        p_name = record["display_name"] if record else p_name_raw
        
        if record and i < 20:
             print(f"      Matched: {p_name_raw} -> {p_name}")

        p_name_lower = p_name.lower()
        
        if p_name not in reg_map:
            new_count += 1
            reg_map[p_name] = {
                "id": p_name,
                "display_name": p_name,
                "github": {"login": None, "location": "Undisclosed"},
                "identities": {"emails": [], "aliases": [p_name]},
                "badges": {},
                "roles": []
            }
            
        entry = reg_map[p_name]
        
        if "Researcher" not in entry["roles"]:
            entry["roles"].append("Researcher")
        
        # Capture social metrics
        entry["badges"]["is_social_contributor"] = True
        entry["badges"]["social_source"] = person.get("dominant_source")
        if "technical_focus" not in entry or entry["technical_focus"] == "General":
            entry["technical_focus"] = person.get("top_category")

    # Filter out stale contributors not present in the current identities database
    filtered_reg_map = {k: v for k, v in reg_map.items() if k in valid_cids}
    print(f"  Garbage collected {len(reg_map) - len(filtered_reg_map)} stale contributors.")
    reg_map = filtered_reg_map

    # Final Sort and Save
    registry["contributors"] = sorted(list(reg_map.values()), key=lambda x: x['id'])
    registry["last_sync"] = time.strftime("%Y-%m-%d")
    registry["total_tracked"] = len(registry["contributors"])

    os.makedirs(os.path.dirname(REGISTRY_PATH), exist_ok=True)
    with open(REGISTRY_PATH, 'w') as f:
        json.dump(registry, f, indent=2)

    print(f"✨ Registry Sync Complete! Tracked {registry['total_tracked']} unique contributors.")

if __name__ == "__main__":
    main()
