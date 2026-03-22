import pandas as pd
import json
import os
import subprocess

def get_dir_distribution(repo_path, email):
    """Uses git directly for granular directory analysis of a maintainer's merges."""
    try:
        # Get merge commits commited by this email
        # We use --first-parent -m to get the files changed in the merged branch relative to master
        cmd = [
            "git", "log", 
            f"--committer={email}", 
            "--merges",
            "--first-parent",
            "-m",
            "--name-only", 
            "--pretty=format:"
        ]
        result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, check=True)
        
        # Filter and count directories
        files = [line for line in result.stdout.split('\n') if line.strip()]
        if not files:
            return {}
            
        granular_dirs = []
        for f in files:
            parts = f.split('/')
            if parts[0] == 'src' and len(parts) > 1:
                # Use src/subfolder for more granularity
                granular_dirs.append(f"src/{parts[1]}")
            else:
                granular_dirs.append(parts[0] if '/' in f else 'root')
                
        df = pd.Series(granular_dirs).value_counts(normalize=True) * 100
        return df.head(8).to_dict()
    except Exception as e:
        print(f"Error analyzing {email}: {e}")
        return {}

def run_footprint_analysis(repo_path, maintainers_file, output_file):
    """
    Core entry point for maintainer footprint analysis.
    Called by the main data pipeline.
    """
    if not os.path.exists(maintainers_file):
        print(f"Error: {maintainers_file} not found.")
        return {}

    with open(maintainers_file, "r") as f:
        data = json.load(f)
    
    maintainers = data.get("maintainers", [])
    footprints = {}

    print(f"Analyzing footprints for {len(maintainers)} potential maintainers...")
    
    for m in maintainers:
        # Analyze active, emeritus, AND historical for footprint
        if m['status'] not in ['active', 'emeritus', 'historical']:
            continue
            
        m_id = m['id']
        emails = m.get('emails', [])
        
        # Aggregate distribution across all emails
        combined_dist = {}
        total_found = 0
        
        for email in emails:
            dist = get_dir_distribution(repo_path, email)
            if dist:
                total_found += 1
                for d, val in dist.items():
                    combined_dist[d] = combined_dist.get(d, 0) + val
        
        if total_found > 0:
            # Re-normalize
            total_val = sum(combined_dist.values())
            normalized = {k: round(v / total_val * 100, 1) for k, v in combined_dist.items()}
            
            # Sort by percentage
            sorted_dist = {k: v for k, v in sorted(normalized.items(), key=lambda item: item[1], reverse=True)}
            
            footprints[m_id] = {
                "name": m['name'],
                "status": m['status'],
                "top_areas": sorted_dist
            }

    # Save to core data
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(footprints, f, indent=2)
    
    print(f"Footprints saved to {output_file}")
    return footprints

if __name__ == "__main__":
    # Allow standalone execution if needed
    run_footprint_analysis("raw_data/bitcoin", "data/cache/maintainers_lookup.json", "data/core/maintainer_footprints.json")
