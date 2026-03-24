import pandas as pd
import json
import os
import sys

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import WORK_DIR, TRACKER_DIR

# --- Configuration (Centralized via paths.py) ---
BIPS_PATH = os.path.join(WORK_DIR, "bips_enriched.parquet")
SOCIAL_PATH = os.path.join(WORK_DIR, "social", "combined_categorized.parquet")
COMMITS_PATH = os.path.join(WORK_DIR, "core", "commits.parquet")
OUTPUT_PATH = os.path.join(TRACKER_DIR, "expertise.json")

def main():
    print("--- Expertise & Authority Mapping (New Architecture) ---")
    
    if not all(os.path.exists(p) for p in [BIPS_PATH, SOCIAL_PATH, COMMITS_PATH]):
        print("Error: Required datasets missing. Run ingestion and enrichment first.")
        return

    # 1. Load data
    df_bips = pd.read_parquet(BIPS_PATH)
    df_social = pd.read_parquet(SOCIAL_PATH)
    df_commits = pd.read_parquet(COMMITS_PATH)
    
    # 2. Identify "Full-Stack Architects"
    # Contributors who have authored BIPs AND have code commits
    print("Mapping Full-Stack Architects...")
    
    bip_authors_exploded = []
    for _, row in df_bips.iterrows():
        authors = row.get('author_names', [])
        if isinstance(authors, (list, pd.Series, pd.Index, object)):
            for a in authors:
                bip_authors_exploded.append({"canonical_id": str(a), "bip_id": row['bip_id']})
    
    df_bip_authors = pd.DataFrame(bip_authors_exploded)
    bip_counts = df_bip_authors.groupby('canonical_id').size().reset_index(name='bips') if not df_bip_authors.empty else pd.DataFrame(columns=['canonical_id', 'bips'])
    
    # Code counts (non-merge authorship)
    code_counts = df_commits[df_commits['is_merge'] == False].groupby('author_name').size().reset_index(name='commits')
    code_counts.columns = ['canonical_id', 'commits']
    
    architects = pd.merge(bip_counts, code_counts, on='canonical_id', how='inner')
    architects = architects.sort_values(['bips', 'commits'], ascending=False)
    
    # 3. Identify "The Gatekeepers" (Social Authority)
    print("Mapping Social Gatekeepers...")
    social_counts = df_social.groupby('canonical_id').size().reset_index(name='posts')
    social_counts = social_counts[~social_counts['canonical_id'].isin(['Unknown', 'system', 'admin', None])]
    social_counts = social_counts.sort_values('posts', ascending=False)
    
    # 4. Generate UI Artifact
    expertise_data = {
        "full_stack_architects": architects.head(20).to_dict(orient='records'),
        "gatekeepers": social_counts.head(20).to_dict(orient='records'),
        "last_updated": pd.Timestamp.now().isoformat()
    }
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(expertise_data, f, indent=2)
        
    print(f"Expertise mapping saved to {OUTPUT_PATH}")
    print(f"Found {len(architects)} Full-Stack Architects.")

if __name__ == "__main__":
    main()
