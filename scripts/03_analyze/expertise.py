import pandas as pd
import json
import os
import re
import sys

sys.path.append(os.getcwd())
from scripts.utils.identity import resolver

# --- Configuration ---
BIPS_PATH = "data/enriched/bips_refined.parquet"
GITHUB_PR_PATH = "data/raw/github_pr_metadata.parquet"
BIP_PR_PATH = "data/raw/bips_pr_metadata.parquet"
SOCIAL_PATH = "data/enriched/social_threads.parquet"
COMMITS_PATH = "data/raw/core_commits.parquet"
OUTPUT_EXPERTISE_JSON = "output/tracker/expertise.json"

def main():
    print("--- Stage 3.5: Expertise & Authority Mapping ---")
    
    # 1. Load data
    df_bips = pd.read_parquet(BIPS_PATH)
    df_social = pd.read_parquet(SOCIAL_PATH)
    df_commits = pd.read_parquet(COMMITS_PATH)
    
    # 2. Domain Expertise Tags (from Labels)
    print("Extracting Expertise Tags from PR labels...")
    domain_tags = {} # uuid -> {tag: count}
    
    # Check if PR metadata exists
    for path in [GITHUB_PR_PATH, BIP_PR_PATH]:
        if os.path.exists(path):
            df_prs = pd.read_parquet(path)
            for _, row in df_prs.iterrows():
                login = row.get('author')
                labels = row.get('labels')
                if not login or not labels: continue
                
                # Use resolve_github for logins
                uuid = resolver.resolve_github(login)
                if not uuid: continue
                
                if uuid not in domain_tags:
                    domain_tags[uuid] = {}
                
                tags = [l.strip() for l in labels.split("|")]
                for tag in tags:
                    domain_tags[uuid][tag] = domain_tags[uuid].get(tag, 0) + 1

    # Summarize top tags for active contributors
    user_expertise = {}
    for uid, tags in domain_tags.items():
        sorted_tags = sorted(tags.items(), key=lambda x: x[1], reverse=True)[:5]
        user_expertise[uid] = [t[0] for t in sorted_tags]

    # 3. Identify "Full-Stack Architects"
    # Contributors who have authored BIPs AND have code commits
    print("Identifying Full-Stack Architects...")
    
    bip_authors_exploded = []
    for _, row in df_bips.iterrows():
        authors = row['author_canonical_ids']
        if isinstance(authors, (list, pd.Series, pd.Index, object)):
            for a in authors:
                # If it's already a UUID (from refine), use it, else resolve
                resolved_id = a if str(a).startswith(('can_', 'auto_')) else resolver.resolve_git(a, None)
                bip_authors_exploded.append({"canonical_id": resolved_id, "bip_id": row['bip_id']})
    
    df_bip_authors = pd.DataFrame(bip_authors_exploded)
    if not df_bip_authors.empty:
        bip_counts = df_bip_authors.groupby('canonical_id').size().reset_index(name='bips_authored')
    else:
        bip_counts = pd.DataFrame(columns=['canonical_id', 'bips_authored'])
        
    print("Resolving commit author identities...")
    df_commits['canonical_id'] = df_commits.apply(lambda r: resolver.resolve_git(str(r.get('author_name', '')), str(r.get('author_email', ''))), axis=1)
    
    code_counts = df_commits[df_commits['is_merge'] == False].groupby('canonical_id').size().reset_index(name='commits_authored')
    
    # Merge
    architects = pd.merge(bip_counts, code_counts, on='canonical_id', how='inner')
    architects = architects.sort_values(['bips_authored', 'commits_authored'], ascending=False)
    
    # 4. Identify "The Gatekeepers" (Social Authority)
    print("Identifying The Gatekeepers...")
    df_social['canonical_id_resolved'] = df_social.apply(lambda r: resolver.resolve_git(str(r.get('author_name', str(r.get('canonical_id')))), None), axis=1)
    social_counts = df_social.groupby('canonical_id_resolved').size().reset_index(name='social_post_count')
    social_counts.columns = ['canonical_id', 'social_post_count']
    
    social_counts = social_counts[~social_counts['canonical_id'].isin(['Unknown', '', None])]
    social_counts = social_counts.sort_values('social_post_count', ascending=False)
    
    # 5. Build Rich Expert Profiles
    print("Assembling Expert Profiles...")
    top_uids = set(architects['canonical_id'].head(50)) | set(social_counts['canonical_id'].head(50))
    
    profiles = []
    for uid in top_uids:
        profiles.append({
            "uuid": uid,
            "tags": user_expertise.get(uid, []),
            "is_architect": uid in architects['canonical_id'].values,
            "is_gatekeeper": uid in social_counts['canonical_id'].values
        })

    # 6. Generate UI Artifact
    expertise_data = {
        "full_stack_architects": architects.head(20).to_dict(orient='records'),
        "gatekeepers": social_counts.head(20).to_dict(orient='records'),
        "expert_profiles": profiles,
        "last_updated": pd.Timestamp.now().isoformat()
    }
    
    os.makedirs(os.path.dirname(OUTPUT_EXPERTISE_JSON), exist_ok=True)
    with open(OUTPUT_EXPERTISE_JSON, 'w') as f:
        json.dump(expertise_data, f, indent=2)
        
    print(f"Expertise mapping saved to {OUTPUT_EXPERTISE_JSON}")
    print(f"Found {len(architects)} Full-Stack Architects and {len(profiles)} specialized experts.")

if __name__ == "__main__":
    main()
