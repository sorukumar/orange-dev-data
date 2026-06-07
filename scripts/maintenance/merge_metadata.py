import os
import json
import glob

STAGING_DIR = "metadata/staging"
METADATA_DIR = "metadata"

def merge_dict(base, updates):
    for k, v in updates.items():
        if isinstance(v, dict) and k in base and isinstance(base[k], dict):
            merge_dict(base[k], v)
        else:
            base[k] = v

def process_file(filepath):
    print(f"\n========================================")
    print(f"Processing {os.path.basename(filepath)}")
    print(f"========================================")
    
    with open(filepath, 'r') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print("Error: Invalid JSON.")
            return

    target = data.get("target")
    if not target:
        print("Error: Staging file missing 'target' key. e.g., 'target': 'sponsors.json'")
        return
        
    updates = data.get("updates")
    if not updates:
        print("Error: Staging file missing 'updates' key.")
        return
        
    operation = data.get("operation", "merge")
        
    target_path = os.path.join(METADATA_DIR, target)
    if not os.path.exists(target_path):
        print(f"Error: Target file {target_path} does not exist.")
        return
        
    with open(target_path, 'r') as f:
        target_data = json.load(f)
        
    print(f"\nProposed changes for {target} (Operation: {operation}):")
    print(json.dumps(updates, indent=2))
    
    ans = input("\nAccept and merge these changes? (y/n): ")
    if ans.lower() == 'y':
        if operation == "merge":
            merge_dict(target_data, updates)
            
        elif operation == "append_grants":
            # updates is expected to be a list of { "canonical_name": "...", "grant": { ... } }
            if not isinstance(updates, list):
                print("Error: updates must be a list for append_grants.")
                return
            
            sponsored_devs = target_data.setdefault("sponsored_developers", [])
            for update in updates:
                name = update.get("canonical_name")
                new_grant = update.get("grant")
                if not name or not new_grant: continue
                
                # Find developer
                dev_entry = next((d for d in sponsored_devs if d.get("canonical_name", "").lower() == name.lower()), None)
                if not dev_entry:
                    # Create new entry if not found
                    dev_entry = { "canonical_name": name, "grants": [] }
                    sponsored_devs.append(dev_entry)
                
                grants = dev_entry.setdefault("grants", [])
                grants.append(new_grant)
                print(f"Appended grant for {name}")

        elif operation == "append_locations":
            # updates is a list of location objects
            if not isinstance(updates, list):
                print("Error: updates must be a list for append_locations.")
                return
            
            identified_locs = target_data.setdefault("identified_locations", [])
            for loc in updates:
                name = loc.get("name")
                if not name: continue
                
                # Check if exists
                existing = next((d for d in identified_locs if d.get("name", "").lower() == name.lower()), None)
                if existing:
                    # Update existing
                    existing.update(loc)
                    print(f"Updated location for {name}")
                else:
                    identified_locs.append(loc)
                    print(f"Appended location for {name}")
        
        else:
            print(f"Error: Unknown operation {operation}")
            return
            
        with open(target_path, 'w') as f:
            json.dump(target_data, f, indent=2)
        print("✅ Merged successfully.")
        os.remove(filepath)
        print("🗑️ Removed staging file.")
    else:
        print("⏭️ Skipped.")

def main():
    if not os.path.exists(STAGING_DIR):
        print(f"Staging directory {STAGING_DIR} does not exist.")
        return
        
    files = glob.glob(os.path.join(STAGING_DIR, "*.json"))
    if not files:
        print("No staging files found in metadata/staging/.")
        return
        
    for f in files:
        process_file(f)

if __name__ == "__main__":
    main()
