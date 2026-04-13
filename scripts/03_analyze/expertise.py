import pandas as pd
import json
import os
import re

# --- Configuration ---
BIPS_PATH = "data/enriched/bips_refined.parquet"
ALIASES_PATH = "metadata/identities.json"
SOCIAL_PATH = "data/enriched/social_threads.parquet"
COMMITS_PATH = "data/raw/core_commits.parquet"
OUTPUT_EXPERTISE_JSON = "output/tracker/expertise.json"

def load_identity_resolver():
    """Loads identities and returns a function to map aliases to canonical IDs."""
    if not os.path.exists(ALIASES_PATH):
        print(f"Warning: {ALIASES_PATH} not found. Using raw names.")
        return lambda x: x
    
    with open(ALIASES_PATH, 'r') as f:
        data = json.load(f)
    
    resolver = {}
    for entry in data.get('aliases', []):
        canonical = entry['canonical_name']
        for alias in entry.get('aliases', []):
            resolver[alias.lower()] = canonical
        for email in entry.get('emails', []):
            resolver[email.lower()] = canonical
        resolver[canonical.lower()] = canonical
    
    def resolve(val):
        if not val: return "Unknown"
        clean = str(val).strip().lower()
        return resolver.get(clean, val)
        
    return resolve

def main():
    print("--- Stage 3.5: Expertise & Authority Mapping ---")
    resolve = load_identity_resolver()
    
    # 1. Load data
    df_bips = pd.read_parquet(BIPS_PATH)
    df_social = pd.read_parquet(SOCIAL_PATH)
    df_commits = pd.read_parquet(COMMITS_PATH)
    
    # 2. Identify "Full-Stack Architects"
    # Contributors who have authored BIPs AND have code commits
    print("Identifying Full-Stack Architects...")
    
    # Explode BIPs by author (already canonical_id)
    bip_authors_exploded = []
    for _, row in df_bips.iterrows():
        authors = row['author_canonical_ids']
        if isinstance(authors, (list, pd.Series, pd.Index, object)):
            for a in authors:
                resolved_id = resolve(a)
                bip_authors_exploded.append({"canonical_id": resolved_id, "bip_id": row['bip_id']})
    
    df_bip_authors = pd.DataFrame(bip_authors_exploded)
    if not df_bip_authors.empty:
        bip_counts = df_bip_authors.groupby('canonical_id').size().reset_index(name='bips_authored')
    else:
        bip_counts = pd.DataFrame(columns=['canonical_id', 'bips_authored'])
        
    # Get code counts (RESOLVE NAMES FIRST)
    print("Resolving commit author identities...")
    df_commits['canonical_id'] = df_commits['author_name'].apply(resolve)
    
    code_counts = df_commits[df_commits['is_merge'] == False].groupby('canonical_id').size().reset_index(name='commits_authored')
    
    # Merge
    architects = pd.merge(bip_counts, code_counts, on='canonical_id', how='inner')
    architects = architects.sort_values(['bips_authored', 'commits_authored'], ascending=False)
    
    # 3. Identify "The Gatekeepers" (Social Authority)
    # Already mapped in social_threads.parquet usually, but resolve again to be safe
    print("Identifying The Gatekeepers...")
    df_social['canonical_id_resolved'] = df_social['canonical_id'].apply(resolve)
    social_counts = df_social.groupby('canonical_id_resolved').size().reset_index(name='social_post_count')
    social_counts.columns = ['canonical_id', 'social_post_count']
    
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
