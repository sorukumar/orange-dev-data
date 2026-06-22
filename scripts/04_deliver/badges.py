import json
import os
import sys
from pathlib import Path

# Add project root to path
root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.append(str(root_dir))

from scripts.utils.identity import IdentityResolver

# --- Paths ---
MAINTAINERS_PATH = "metadata/maintainers.json"
SPONSORS_PATH = "metadata/sponsors.json"
BIPS_PATH = "output/tracker/bips_ui.json"
BADGES_PATH = "metadata/badges.json"

def main():
    print("🏆 Compiling static badges...")
    resolver = IdentityResolver()
    
    # 1. Load manual sources
    maintainers = {}
    if os.path.exists(MAINTAINERS_PATH):
        with open(MAINTAINERS_PATH, 'r') as f:
            for m in json.load(f).get('maintainers', []):
                uuid = resolver.resolve_git(m['name'], None)
                maintainers[uuid] = m
                
    sponsored = {}
    if os.path.exists(SPONSORS_PATH):
        with open(SPONSORS_PATH, 'r') as f:
            for s in json.load(f).get('sponsored_developers', []):
                uuid = resolver.resolve_git(s['canonical_name'], None)
                sponsored[uuid] = s

    bip_authors = set()
    if os.path.exists(BIPS_PATH):
        with open(BIPS_PATH, 'r') as f:
            try:
                for bip in json.load(f):
                    authors_str = bip.get('authors', "")
                    if authors_str:
                        authors = authors_str.replace(" and ", ", ").split(",")
                        for name in authors:
                            if name.strip():
                                uuid = resolver.resolve_git(name.strip(), None)
                                bip_authors.add(uuid)
            except:
                pass

    # 2. Build Badges mapping (Keyed by canonical UUID)
    badges_map = {}
    
    # Pre-populate with all identities that have any badge
    all_uuids = set(list(maintainers.keys()) + list(sponsored.keys()) + list(bip_authors))
    
    for uuid in all_uuids:
        entry = {
            "is_maintainer": False,
            "is_core_committer": False,
            "maintainer_status": None,
            "is_sponsored": False,
            "sponsor_id": None,
            "is_bip_author": False,
            "roles": []
        }
        
        if uuid in maintainers:
            m = maintainers[uuid]
            entry["is_maintainer"] = True
            entry["is_core_committer"] = m.get("merge_authority", False)
            entry["maintainer_status"] = m.get("status")
            role_title = m.get("role", {}).get("title")
            if role_title:
                entry["roles"].append(role_title)
                
        if uuid in sponsored:
            s = sponsored[uuid]
            entry["is_sponsored"] = True
            entry["sponsor_id"] = s.get("sponsor_id")
            
        if uuid in bip_authors:
            entry["is_bip_author"] = True
            if "BIP Author" not in entry["roles"]:
                entry["roles"].append("BIP Author")
                
        badges_map[uuid] = entry
        
    os.makedirs(os.path.dirname(BADGES_PATH), exist_ok=True)
    with open(BADGES_PATH, 'w') as f:
        json.dump(badges_map, f, indent=2)
        
    print(f"✨ Compiled badges for {len(badges_map)} contributors.")

if __name__ == "__main__":
    main()
