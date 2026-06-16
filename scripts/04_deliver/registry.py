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
MAINTAINERS_PATH = "metadata/maintainers.json"
SPONSORS_PATH = "metadata/sponsors.json"
ALIASES_PATH = "metadata/identities.json"
RICH_CONTRIBUTORS_PATH = "output/tracker/contributors_rich.json"
SOCIAL_STATS_PATH = "data/enriched/social_stats.json"
BIPS_PATH = "output/tracker/bips_ui.json"

def main():
    print("🔄 Starting Master Registry Sync...")
    
    # 1. Load Registry or Initialize
    registry = {"contributors": [], "last_sync": None}
    if os.path.exists(REGISTRY_PATH):
        try:
            with open(REGISTRY_PATH, 'r') as f:
                registry = json.load(f)
        except Exception as e:
            print(f"⚠️ Warning: Could not load registry, starting fresh. {e}")
    
    # Map for easy lookup [ID (Canonical Name) -> Record]
    reg_map = {c['id']: c for c in registry.get('contributors', [])}

    # 3c. Initialize Identity Hub (The Brain)
    resolver = IdentityResolver()

    # 2. Load Manual Intelligence (Directly from consolidated metadata)
    maintainers = {}
    if os.path.exists(MAINTAINERS_PATH):
        with open(MAINTAINERS_PATH, 'r') as f:
            for m in json.load(f).get('maintainers', []):
                uuid = resolver.resolve_git(m['name'], None)
                record = next((r for r in resolver._identities if r["uuid"] == uuid), None)
                canonical_name = record["display_name"] if record else m['name']
                maintainers[canonical_name.lower()] = m
                
    sponsored = {}
    if os.path.exists(SPONSORS_PATH):
        with open(SPONSORS_PATH, 'r') as f:
            for s in json.load(f).get('sponsored_developers', []):
                uuid = resolver.resolve_git(s['canonical_name'], None)
                record = next((r for r in resolver._identities if r["uuid"] == uuid), None)
                canonical_name = record["display_name"] if record else s['canonical_name']
                sponsored[canonical_name.lower()] = s

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
            
    bip_authors = set()
    if os.path.exists(BIPS_PATH):
        with open(BIPS_PATH, 'r') as f:
            try:
                bips_data = json.load(f)
                for bip in bips_data:
                    authors_str = bip.get('authors', "")
                    if authors_str:
                        # Handle multiple formats
                        authors = authors_str.replace(" and ", ", ").split(",")
                        for name in authors:
                            if name.strip(): bip_authors.add(name.strip())
            except:
                print("⚠️ Warning: Could not parse BIPS data.")



    # 4. Bootstrap from Identities Map (The full 4,000+ list)
    # We now trust the resolver's internal state
    print(f"  Bootstrapping from {len(resolver._identities)} defined identities...")
    
    for item in resolver._identities:
        cid = item.get('display_name') or item.get('canonical_name')
        if not cid: continue
        
        if cid not in reg_map:
            reg_map[cid] = {
                "id": cid,
                "display_name": cid,
                "github": {"login": None, "location": "Undisclosed"},
                "identities": {"emails": [], "aliases": []},
                "badges": {},
                "roles": [],
                "first_seen": time.strftime("%Y-%m-%d")
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
                "roles": [],
                "first_seen": time.strftime("%Y-%m-%d"),
            }
            
        entry = reg_map[p_name]
        if "identities" not in entry:
            entry["identities"] = {"emails": [], "aliases": [p_name]}
            
        entry["last_seen"] = time.strftime("%Y-%m-%d")
        
        # Preserve existing location if UI data is empty
        if person.get("location") and person.get("location") != "Undisclosed":
            entry["github"]["location"] = person.get("location")
            
        if person.get("login"):
            entry["github"]["login"] = person.get("login")

        # Update Base Core Discovery
        entry["badges"]["is_core_contributor"] = True 
        entry["technical_focus"] = person.get("primary_category", "General")
        
        # Update Badges (Maintainer / Committer)
        if p_name_lower in maintainers:
            m = maintainers[p_name_lower]
            entry["badges"]["is_maintainer"] = True
            entry["badges"]["is_core_committer"] = m.get("merge_authority", False)
            entry["badges"]["maintainer_status"] = m.get("status")
            role_title = m.get("role", {}).get("title")
            if role_title and role_title not in entry["roles"]:
                entry["roles"].append(role_title)
            
        # Update Badges (Sponsored)
        if p_name_lower in sponsored:
            s = sponsored[p_name_lower]
            entry["badges"]["is_sponsored"] = True
            entry["badges"]["sponsor_id"] = s.get("sponsor_id")
            
        # Update Badges (BIP Author)
        if p_name in bip_authors:
            entry["badges"]["is_bip_author"] = True
            if "BIP Author" not in entry["roles"]:
                entry["roles"].append("BIP Author")

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
                "roles": [],
                "first_seen": time.strftime("%Y-%m-%d"),
            }
            
        entry = reg_map[p_name]
        entry["last_seen"] = time.strftime("%Y-%m-%d")
        
        if "Researcher" not in entry["roles"]:
            entry["roles"].append("Researcher")
        
        # Capture social metrics
        entry["badges"]["is_social_contributor"] = True
        entry["badges"]["social_source"] = person.get("dominant_source")
        if "technical_focus" not in entry or entry["technical_focus"] == "General":
            entry["technical_focus"] = person.get("top_category")

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
