import os
import json
import pandas as pd

def export_true_commits():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    commits_path = os.path.join(base_dir, "data", "enriched", "commits_resolved.parquet")
    unified_path = os.path.join(base_dir, "data", "enriched", "contributors_unified.parquet")
    output_path = os.path.join(base_dir, "output", "shared", "true_repo_commits.json")

    if not os.path.exists(commits_path) or not os.path.exists(unified_path):
        print("Required parquet files not found.")
        return

    print("Loading commits_resolved.parquet...")
    df_commits = pd.read_parquet(commits_path, columns=['hash', 'repository_name', 'is_merge', 'category', 'canonical_id'])
    
    print("Loading contributors_unified.parquet...")
    df_unified = pd.read_parquet(unified_path, columns=['uuid', 'github_login_final'])

    # Filter out merges
    df_auth = df_commits[(df_commits['is_merge'] == False) & (df_commits['category'] != 'Merge')]

    print("Grouping by canonical_id and repository_name...")
    # Group by canonical_id and repository_name to get unique hashes
    repo_commits = df_auth.groupby(['canonical_id', 'repository_name'])['hash'].nunique().reset_index()

    # Build JSON structure: uuid -> {repository_name: true_commits}
    result = {}
    for _, row in repo_commits.iterrows():
        uuid = str(row['canonical_id']).strip()
        if not uuid or uuid == 'None':
            continue
        repo = str(row['repository_name']).strip().lower()
        count = int(row['hash'])
        
        if uuid not in result:
            result[uuid] = {}
        result[uuid][repo] = count

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)

    print(f"✅ Exported {len(result)} identities with repo-level true commits to {output_path}")

if __name__ == "__main__":
    export_true_commits()
