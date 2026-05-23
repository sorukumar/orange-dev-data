
import json
import os

# Paths
OLD_CONTRIBUTORS = "/Users/saurabhkumar/Desktop/Work/github/orange-dev-tracker/data/core/contributors_rich.json"
NEW_LOCATIONS = "metadata/context/locations.json"

def main():
    # 1. Load legacy locations
    with open(OLD_CONTRIBUTORS, 'r') as f:
        old_data = json.load(f)
    
    legacy_map = {}
    for c in old_data:
        name = c.get('name')
        loc = c.get('location')
        if name and loc and loc != "Undisclosed":
            legacy_map[name] = loc

    # 2. Load existing new locations
    with open(NEW_LOCATIONS, 'r') as f:
        new_data = json.load(f)
    
    existing_names = {item['name'] for item in new_data['identified_locations']}
    
    # 3. Merge
    added_count = 0
    for name, loc in legacy_map.items():
        if name not in existing_names:
            new_data['identified_locations'].append({
                "name": name,
                "found_location": loc,
                "source": "Legacy Orange Dev Tracker data",
                "confidence": "8"
            })
            added_count += 1
            existing_names.add(name)
            
    # 4. Save
    with open(NEW_LOCATIONS, 'w') as f:
        json.dump(new_data, f, indent=4)
        
    print(f"Added {added_count} locations from legacy data. Total now: {len(new_data['identified_locations'])}")

if __name__ == "__main__":
    main()
