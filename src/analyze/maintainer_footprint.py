import json
import os
import sys
import subprocess
import pandas as pd

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import RAW_DIR, TRACKER_DIR, MAINTAINERS_PATH
from src.core.lookup import MaintainerLookup

# --- Configuration (Centralized via paths.py) ---
REPO_PATH = os.path.join(RAW_DIR, "bitcoin_repo")
OUTPUT_PATH = os.path.join(TRACKER_DIR, "stats_maintainers.json")

def get_dir_distribution(repo_path, email):
    """Uses git directly for granular directory analysis of a maintainer's merges."""
    try:
        # Get merge commits commited by this email
        cmd = [
            "git", "-C", repo_path, "log", 
            f"--committer={email}", "--merges", "--first-parent", "-m", "--name-only", "--pretty=format:"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, errors='replace')
        
        files = [line for line in result.stdout.split('\n') if line.strip()]
        if not files: return {}
            
        granular_dirs = []
        for f in files:
            parts = f.split('/')
            if parts[0] == 'src' and len(parts) > 1:
                granular_dirs.append(f"src/{parts[1]}")
            else:
                granular_dirs.append(parts[0] if '/' in f else 'root')
                
        dist = pd.Series(granular_dirs).value_counts(normalize=True) * 100
        return dist.head(8).to_dict()
    except Exception as e:
        print(f"Error analyzing {email}: {e}")
        return {}

def main():
    print("--- Maintainer Footprint Analysis (Forensics) ---")
    MaintainerLookup.load()
    maintainers = MaintainerLookup.get_all()
    
    if not os.path.exists(REPO_PATH):
        print(f"Error: Git repository not found at {REPO_PATH}")
        return

    footprints = {}
    print(f"Forensically scanning activity for {len(maintainers)} potential maintainers...")
    
    for m in maintainers:
        if m['status'] not in ['active', 'emeritus', 'historical']: continue
            
        m_id = m['id']
        emails = m.get('emails', [])
        
        combined_dist = {}
        total_found = 0
        
        for email in emails:
            dist = get_dir_distribution(REPO_PATH, email)
            if dist:
                total_found += 1
                for d, val in dist.items():
                    combined_dist[d] = combined_dist.get(d, 0) + val
        
        if total_found > 0:
            total_val = sum(combined_dist.values())
            normalized = {k: round(v / total_val * 100, 1) for k, v in combined_dist.items()}
            sorted_dist = {k: v for k, v in sorted(normalized.items(), key=lambda item: item[1], reverse=True)}
            
            footprints[m_id] = {
                "name": m['name'],
                "status": m['status'],
                "top_areas": sorted_dist
            }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(footprints, f, indent=2)
    
    print(f"Maintainer footprints exported to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
