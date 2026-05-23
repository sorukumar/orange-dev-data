import networkx as nx
import json
import pandas as pd
from collections import defaultdict

def find_bridge():
    # We'll re-run the core logic of build_identities to find the path
    CURATED_FILE = "metadata/identity_curated.json"
    with open(CURATED_FILE, "r") as f:
        curated_data = json.load(f).get("aliases", [])
    
    G = nx.Graph()
    # (Abbreviated build logic to find the path)
    # ... indexing commits ...
    df_commits = pd.read_parquet("data/raw/core_commits.parquet")
    for _, row in df_commits.iterrows():
        n, e = str(row['author_name']).strip(), str(row['author_email']).strip().lower()
        if len(n) > 1 and len(e) > 3:
            G.add_edge(f"NAME:{n}", f"EMAIL:{e}")
            
    # ... indexing PRs ...
    for file in ["data/raw/github_pr_metadata.parquet", "data/raw/bips_pr_metadata.parquet"]:
        df_pr = pd.read_parquet(file)
        # Note: We'll assume the basic login-to-node link for now
        for _, row in df_pr.iterrows():
            login = str(row.get('author', '')).strip()
            if login:
                G.add_node(f"NAME:{login}")
                # We skip the anchor join here and just see if string matches bridge them
                
    # Check for path between two known nodes in the merged can_matt_corallo
    try:
        path = nx.shortest_path(G, "NAME:Matt Corallo", "NAME:Michael Ford")
        print(f"PATH FOUND: {' -> '.join(path)}")
    except:
        try:
             path = nx.shortest_path(G, "NAME:Matt Corallo", "NAME:fanquake")
             print(f"PATH FOUND: {' -> '.join(path)}")
        except:
             print("No direct path via NAME strings found in raw commits. Checking emails...")
             # (Add more deep checks if needed)

if __name__ == "__main__":
    find_bridge()
