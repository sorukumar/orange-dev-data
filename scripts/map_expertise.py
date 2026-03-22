import pandas as pd
import json
import os

# --- Configuration ---
BIPS_PATH = "data/governance/bips_enriched.parquet"
ALIASES_PATH = "lookups/identity_mappings.json"
SOCIAL_PATH = "data/raw/social_combined_categorized.parquet"
COMMITS_PATH = "data/core/commits.parquet"
OUTPUT_EXPERTISE_JSON = "data/governance/expertise.json"

def main():
    print("--- Stage 3.5: Expertise & Authority Mapping ---")
    
    # 1. Load data
    df_bips = pd.read_parquet(BIPS_PATH)
    df_social = pd.read_parquet(SOCIAL_PATH)
    df_commits = pd.read_parquet(COMMITS_PATH)
    
    # 2. Identify "Full-Stack Architects"
    # Contributors who have authored BIPs AND have code commits
    print("Identifying Full-Stack Architects...")
    
    # Explode BIPs by author to get a list of (Author, BIP_ID)
    # The column 'author_canonical_ids' currently contains numpy arrays/lists
    bip_authors_exploded = []
    for _, row in df_bips.iterrows():
        authors = row['author_canonical_ids']
        if isinstance(authors, (list, pd.Series, pd.Index, object)):
            # Handle list-like objects
            for a in authors:
                bip_authors_exploded.append({"canonical_id": a, "bip_id": row['bip_id']})
    
    df_bip_authors = pd.DataFrame(bip_authors_exploded)
    if not df_bip_authors.empty:
        bip_counts = df_bip_authors.groupby('canonical_id').size().reset_index(name='bips_authored')
    else:
        bip_counts = pd.DataFrame(columns=['canonical_id', 'bips_authored'])
        
    # Get code counts (use authorship, not merge activity)
    # Note: df_commits has author_name, but we might need to map it if not already?
    # In this project, 'author_name' in commits.parquet is often the raw name.
    # We should ideally use a mapped ID, but for now let's use what we have.
    # A lot of contributors in commits.parquet already match canonical names from aliases.
    code_counts = df_commits[df_commits['is_merge'] == False].groupby('author_name').size().reset_index(name='commits_authored')
    code_counts.columns = ['canonical_id', 'commits_authored']
    
    # Merge
    architects = pd.merge(bip_counts, code_counts, on='canonical_id', how='inner')
    architects = architects.sort_values(['bips_authored', 'commits_authored'], ascending=False)
    
    # 3. Identify "The Gatekeepers" (Social Authority)
    print("Identifying The Gatekeepers...")
    social_counts = df_social.groupby('canonical_id').size().reset_index(name='social_post_count')
    # Filter out empty or "Unknown"
    social_counts = social_counts[~social_counts['canonical_id'].isin(['Unknown', '', None])]
    social_counts = social_counts.sort_values('social_post_count', ascending=False)
    
    # 4. Generate UI Artifact
    expertise_data = {
        "full_stack_architects": architects.head(20).to_dict(orient='records'),
        "gatekeepers": social_counts.head(20).to_dict(orient='records'),
        "last_updated": pd.Timestamp.now().isoformat()
    }
    
    os.makedirs(os.path.dirname(OUTPUT_EXPERTISE_JSON), exist_ok=True)
    with open(OUTPUT_EXPERTISE_JSON, 'w') as f:
        json.dump(expertise_data, f, indent=2)
        
    print(f"Expertise mapping saved to {OUTPUT_EXPERTISE_JSON}")
    print(f"Found {len(architects)} Full-Stack Architects.")

if __name__ == "__main__":
    main()
