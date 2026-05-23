import json
import networkx as nx
import pandas as pd
import os
from tqdm import tqdm

def find_path():
    G = nx.Graph()
    
    # 1. Load Git Signatures
    df_commits = pd.read_parquet('data/raw/core_commits.parquet')
    sha_to_git = {}
    for _, row in df_commits.iterrows():
        sha = row['hash']
        name = str(row['author_name'])
        email = str(row['author_email']).lower()
        sha_to_git[sha] = {'name': name, 'email': email}
        
    # Build Graph (mimic build_identities.py)
    for sha, info in sha_to_git.items():
        n_node = f"NAME:{info['name']}"
        e_node = f"EMAIL:{info['email']}"
        G.add_edge(n_node, e_node)

    # 2. Parsing PR Metadata
    for p in ['data/raw/github_pr_metadata.parquet', 'data/raw/bips_pr_metadata.parquet']:
        if os.path.exists(p):
            df = pd.read_parquet(p)
            for _, row in df.iterrows():
                login = row.get('author')
                head_sha = row.get('head_sha')
                if head_sha and head_sha in sha_to_git:
                    git_info = sha_to_git[head_sha]
                    e_node = f"EMAIL:{git_info['email']}"
                    n_node = f"NAME:{git_info['name']}"
                    l_node = f"NAME:{login}"
                    G.add_edge(l_node, e_node)
                    G.add_edge(l_node, n_node)

    # Path from fanquake to sipa
    try:
        import sys
        source = sys.argv[1] if len(sys.argv) > 1 else "NAME:mzumsande"
        target = sys.argv[2] if len(sys.argv) > 2 else "NAME:drahtbot"
        if source in G and target in G:
            path = nx.shortest_path(G, source, target)
            print(f"Path from {source} to {target}:")
            print(" -> ".join(path))
            # Find the edges tying them together
            for i in range(len(path)-1):
                u = path[i]
                v = path[i+1]
                print(f"Edge: {u} --- {v}")
                
        else:
            print(f"One of the nodes missing: {source} ({source in G}), {target} ({target in G})")
    except Exception as e:
        print(f"No path: {e}")

if __name__ == "__main__":
    find_path()
