import networkx as nx
import pandas as pd
import json
import os
from collections import defaultdict

def build_graph():
    # Load same logic as build_identities.py
    with open("metadata/identity_curated.json", "r") as f:
        curated_data = json.load(f).get("aliases", [])
    
    G = nx.Graph()
    
    # ... logic ...
    # (I'll just paste the relevant refined logic here to inspect the graph it builds)
    
    # ... indexing commits ...
    df_commits = pd.read_parquet("data/raw/core_commits.parquet")
    sha_to_git = {}
    for _, row in df_commits.iterrows():
        sha, n, e = str(row['hash']), str(row['author_name']).strip(), str(row['author_email']).strip().lower()
        if n in ["unknown", "none"] or e in [""]: continue
        sha_to_git[sha] = {"name": n, "email": e}
        G.add_edge(f"NAME:{n}", f"EMAIL:{e}")

    # ... parsing PRs ...
    df_pr = pd.read_parquet("data/raw/github_pr_metadata.parquet")
    for _, row in df_pr.iterrows():
        login, head_sha = str(row.get('author', '')), str(row.get('head_sha', ''))
        if login in ["fanquake", "sipa", "laanwj"]:
            if head_sha in sha_to_git:
                git_info = sha_to_git[head_sha]
                # print edge it WOULD add
                print(f"DEBUG: PR Author {login} tries to link to {git_info['email']}")

    # Find path in build_identities.py graph state
    # Wait, I'll just check if a path exists between fanquake and sipa email in the REAL build_identities.py logic
    
if __name__ == "__main__":
    # I'll just read the PR metadata for Michael Ford and see what he anchors
    df_pr = pd.read_parquet("data/raw/github_pr_metadata.parquet")
    df_commits = pd.read_parquet("data/raw/core_commits.parquet")
    sha_map = {str(r['hash']): {"n": str(r['author_name']), "e": str(r['author_email'])} for _, r in df_commits.iterrows()}
    
    fanquake_prs = df_pr[df_pr['author'] == 'fanquake']
    for _, row in fanquake_prs.iterrows():
        sha = str(row.get('head_sha', ''))
        if sha in sha_map:
            git = sha_map[sha]
            if git['e'].lower() != "fanquake@gmail.com":
                 print(f"Fanquake PR {row['pr_number']} anchors {git['e']} ({git['n']})")
