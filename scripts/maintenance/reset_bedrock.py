import json
import os
import shutil
import hashlib

def slugify(text):
    """Standard slugifier used by the IdentityResolver."""
    if not text: return ""
    return text.lower().replace(" ", "_").replace(".", "").replace("-", "_")

def reset_identities():
    curated_path = "metadata/identity_curated.json"
    target_path = "metadata/identities.json"
    backup_path = "metadata/identities.json.bak"
    
    # 1. Load Curated Seeds
    if not os.path.exists(curated_path):
        print(f"Error: {curated_path} not found.")
        return
        
    with open(curated_path, "r") as f:
        curated_data = json.load(f)
        
    seeds = curated_data.get("aliases", [])
    print(f"🌱 Loaded {len(seeds)} curated identity seeds.")
    
    # 2. Backup current identities
    if os.path.exists(target_path):
        shutil.copy2(target_path, backup_path)
        print(f"💾 Backup created at {backup_path}")
        
    # 3. Format into Master identities.json
    master_identities = []
    for s in seeds:
        name = s.get("canonical_name")
        if not name: continue
        
        slug = f"can_{slugify(name)}"
        
        master_identities.append({
            "id": slug,
            "name": name,
            "aliases": s.get("aliases", []),
            "emails": s.get("emails", []),
            "github": s.get("github", ""),
            "delving_username": s.get("delving_username", ""),
            "status": "curated"
        })
        
    # 4. Write Clean Bedrock
    with open(target_path, "w") as f:
        json.dump({"identities": master_identities}, f, indent=2)
    print(f"✨ Purified {target_path}. Now contains only {len(master_identities)} curated entries.")
    
    # 5. Purge Staging Data (to prevent stale poisoning)
    folders_to_purge = ["data/raw", "data/enriched", "output/tracker"]
    for folder in folders_to_purge:
        if os.path.exists(folder):
            print(f"🧹 Purging {folder}...")
            # We don't delete the folder, just the contents to keep the structure
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print(f'Failed to delete {file_path}. Reason: {e}')

if __name__ == "__main__":
    reset_identities()
