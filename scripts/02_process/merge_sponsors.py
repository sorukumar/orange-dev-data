import os
import json

def main():
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    
    manual_path = os.path.join(root_dir, "metadata", "sponsors.json")
    auto_path = os.path.join(root_dir, "data", "raw", "automated_grants.json")
    
    with open(manual_path, "r") as f:
        manual_data = json.load(f)
        
    if not os.path.exists(auto_path):
        print(f"Automated grants not found at {auto_path}")
        auto_data = []
    else:
        with open(auto_path, "r") as f:
            auto_data = json.load(f)
            
    # Index manual developers by github handle (case-insensitive) for easy lookup
    dev_by_github = {}
    for dev in manual_data.get("sponsored_developers", []):
        gh = dev.get("github")
        if gh:
            dev_by_github[gh.lower()] = dev
            
    # Merge automated data
    for auto_grant in auto_data:
        gh = auto_grant.get("github")
        if not gh:
            # Skip if no github handle
            continue
            
        gh_lower = gh.lower()
        sponsor_id = auto_grant.get("sponsor")
        
        if gh_lower in dev_by_github:
            dev = dev_by_github[gh_lower]
            # Check if this sponsor is already listed
            has_sponsor = False
            for g in dev.setdefault("grants", []):
                if g.get("sponsor_id") == sponsor_id:
                    has_sponsor = True
                    break
            
            if not has_sponsor:
                dev["grants"].append({
                    "sponsor_id": sponsor_id,
                    "start_date": auto_grant.get("start_date"),
                    "end_date": None,
                    "project_name": auto_grant.get("project_name")
                })
        else:
            # Create new developer entry
            new_dev = {
                "canonical_name": auto_grant.get("name") or gh,
                "notes": f"Automated via {sponsor_id}",
                "github": gh,
                "grants": [
                    {
                        "sponsor_id": sponsor_id,
                        "start_date": auto_grant.get("start_date"),
                        "end_date": None,
                        "project_name": auto_grant.get("project_name")
                    }
                ]
            }
            manual_data.setdefault("sponsored_developers", []).append(new_dev)
            dev_by_github[gh_lower] = new_dev

    out_dir = os.path.join(root_dir, "data", "enriched")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "sponsors_merged.json")
    
    with open(out_path, "w") as f:
        json.dump(manual_data, f, indent=2)
        
    print(f"✅ Merged sponsors written to {out_path} with {len(manual_data['sponsored_developers'])} developers")

if __name__ == "__main__":
    main()
