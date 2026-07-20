import os
import json
from pathlib import Path

def build_index():
    print("Building Core PR Subject Index...")
    data_dir = Path(__file__).resolve().parent.parent.parent / "data"
    pulls_dir = data_dir / "sources" / "bitcoin-github-metadata" / "pulls"
    output_file = data_dir / "enriched" / "core_pr_subjects.json"
    
    if not pulls_dir.exists():
        print(f"Error: {pulls_dir} does not exist.")
        return

    core_subjects = set()
    
    metadata_dirs = [
        data_dir / "sources" / "bitcoin-github-metadata" / "pulls",
        data_dir / "sources" / "gui-github-metadata" / "pulls"
    ]
    
    for pulls_dir in metadata_dirs:
        if not pulls_dir.exists():
            print(f"Warning: {pulls_dir} does not exist.")
            continue
            
        files = os.listdir(pulls_dir)
        total = len(files)
        print(f"Processing {total} files in {pulls_dir.parent.name}...")
        
        for i, f in enumerate(files):
            if i > 0 and i % 5000 == 0:
                print(f"Processed {i}/{total} files...")
                
            if not f.endswith(".json"): continue
            
            with open(pulls_dir / f, 'r', encoding='utf-8') as jf:
                try:
                    data = json.load(jf)
                    title = data.get("pull", {}).get("title")
                    if title: 
                        core_subjects.add(title.strip().lower())
                    
                    for ev in data.get("events", []):
                        if ev.get("event") == "committed":
                            msg = ev.get("message", "")
                            first_line = msg.split("\n")[0].strip().lower()
                            if first_line: 
                                core_subjects.add(first_line)
                except Exception as e:
                    pass
                    
    print(f"Finished processing. Found {len(core_subjects)} unique Core PR subjects/titles.")
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as out:
        json.dump(list(core_subjects), out)
    print(f"Saved index to {output_file}")

if __name__ == "__main__":
    build_index()
