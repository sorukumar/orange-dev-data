import pandas as pd
import json
import networkx as nx
import os
import re
import sys
from collections import Counter
from datetime import datetime, timedelta
import math
import numpy as np

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import WORK_DIR, NETWORK_DIR, ID_PATH

# --- Configuration (Centralized via paths.py) ---
IDENTITY_MAP_PATH = ID_PATH
INPUT_DATA_PATH = os.path.join(WORK_DIR, "social", "combined_categorized.parquet")
OUTPUT_PATH = os.path.join(NETWORK_DIR, "network_graph.json")

def load_identity_resolver():
    if not os.path.exists(IDENTITY_MAP_PATH):
        print(f"Warning: {IDENTITY_MAP_PATH} not found. Using raw IDs.")
        return lambda x: x
    
    with open(IDENTITY_MAP_PATH, 'r') as f:
        data = json.load(f)
    
    resolver = {}
    for entry in data.get('aliases', []):
        canonical = entry['canonical_name']
        for alias in entry.get('aliases', []):
            resolver[alias.lower()] = canonical
        for email in entry.get('emails', []):
            resolver[email.lower()] = canonical
        resolver[canonical.lower()] = canonical
    
    def resolve(name_or_id):
        clean = str(name_or_id).strip()
        clean = re.sub(r'^[\'"]|[\'"]$', '', clean)
        clean = clean.split(' via ')[0].strip()
        clean = re.sub(r'^[\'"]|[\'"]$', '', clean)
        return resolver.get(clean.lower(), name_or_id)
        
    return resolve

def extract_network():
    print("--- Network Influence Analysis (New Architecture) ---")
    resolve = load_identity_resolver()
    
    if not os.path.exists(INPUT_DATA_PATH):
        print(f"Error: {INPUT_DATA_PATH} not found. Run categorization first.")
        return

    print(f"Loading enriched social data from {INPUT_DATA_PATH}...")
    df = pd.read_parquet(INPUT_DATA_PATH)
    df['date'] = pd.to_datetime(df['date'])
    df['canonical_id'] = df['canonical_id'].apply(resolve)
    
    # Filter out system, unknown, and admin
    df = df[~df['canonical_id'].str.lower().isin(['system', 'unknown', 'admin'])]
    df = df[df['canonical_id'].notna()]
    
    # Define historical eras
    now = df['date'].max()
    post_2016_start = datetime(2016, 1, 1)
    modern_start = now - timedelta(days=3 * 365) 
    
    # Message ID -> Author lookup for edge recreation
    df['msg_id_clean'] = df['message_id'].str.strip('<>')
    msg_to_author = df.dropna(subset=['msg_id_clean']).set_index('msg_id_clean')['canonical_id'].to_dict()
    
    G_all = nx.DiGraph()
    G_post2016 = nx.DiGraph()
    G_modern = nx.DiGraph()
    
    print("Processing edges and expertise fingerprints across eras...")
    node_metadata = {} 
    
    for _, row in df.iterrows():
        author = row['canonical_id']
        reply_to = row['reply_to']
        source = row['source']
        date = row['date']
        
        primary_cat = row.get('primary_category', 'other')
        all_cats = row.get('all_categories', [])
        bip_refs = row.get('bip_refs', [])
        
        if author not in node_metadata:
            node_metadata[author] = {
                "sources": Counter(), 
                "categories": Counter(), 
                "bip_refs": Counter(),
                "last_active": date,
                "msg_count": 0,
                "threads_started": 0,
                "replies_sent": 0
            }
        
        node_metadata[author]["sources"][source] += 1
        node_metadata[author]["msg_count"] += 1
        
        is_reply = row.get('is_reply')
        if is_reply is None or pd.isna(is_reply):
            is_reply = pd.notna(reply_to) or str(row.get('subject', '')).lower().startswith('re:')
        
        if is_reply:
            node_metadata[author]["replies_sent"] += 1
        else:
            node_metadata[author]["threads_started"] += 1
            
        node_metadata[author]["categories"][primary_cat] += 1
        for c in (all_cats if isinstance(all_cats, (list, np.ndarray)) else []):
            if c != primary_cat:
                node_metadata[author]["categories"][c] += 0.5 
        
        for b in (bip_refs if isinstance(bip_refs, (list, np.ndarray)) else []):
            node_metadata[author]["bip_refs"][b] += 1
            
        node_metadata[author]["last_active"] = max(node_metadata[author]["last_active"], date)
        
        if pd.isna(reply_to) or not author:
            continue
            
        target_mid = reply_to.strip('<>')
        recipient = msg_to_author.get(target_mid)
        
        if recipient and recipient != author:
            # 1. All-time graph
            if G_all.has_edge(author, recipient):
                G_all[author][recipient]['weight'] += 1
            else:
                G_all.add_edge(author, recipient, weight=1, category=primary_cat, source=source)
            
            # 2. Post-2016 graph
            if date >= post_2016_start:
                if G_post2016.has_edge(author, recipient):
                    G_post2016[author][recipient]['weight'] += 1
                else:
                    G_post2016.add_edge(author, recipient, weight=1)

            # 3. Modern graph (Last 3 years)
            if date >= modern_start:
                if G_modern.has_edge(author, recipient):
                    G_modern[author][recipient]['weight'] += 1
                else:
                    G_modern.add_edge(author, recipient, weight=1)

    print("Dampening message counts for PageRank...")
    for G in [G_all, G_post2016, G_modern]:
        for u, v, d in G.edges(data=True):
            d['weight'] = 1.0 + math.log(d['weight'], 2)

    print("Calculating era-based PageRank centralities...")
    pagerank_all = nx.pagerank(G_all, weight='weight')
    pagerank_post2016 = nx.pagerank(G_post2016, weight='weight') if len(G_post2016) > 0 else {}
    pagerank_modern = nx.pagerank(G_modern, weight='weight') if len(G_modern) > 0 else {}
    
    total_population = df['canonical_id'].nunique()
    nodes_data = []
    
    sorted_nodes = sorted(G_all.nodes(), key=lambda n: pagerank_all.get(n, 0), reverse=True)
    rank_p2016 = {node: i+1 for i, node in enumerate(sorted(G_all.nodes(), key=lambda n: pagerank_post2016.get(n, 0), reverse=True))}
    rank_modern = {node: i+1 for i, node in enumerate(sorted(G_all.nodes(), key=lambda n: pagerank_modern.get(n, 0), reverse=True))}
    
    for i, node in enumerate(sorted_nodes):
        score_all = pagerank_all.get(node, 0)
        score_p2016 = pagerank_post2016.get(node, 0)
        score_modern = pagerank_modern.get(node, 0)
        growth = (score_modern / score_p2016) if score_p2016 > 0 else 0
        
        cat_counts = node_metadata[node]["categories"]
        total_cat_weight = sum(cat_counts.values())
        top_3_cats = []
        if total_cat_weight > 0:
            top_3_cats = [{"topic": c, "share": round(count/total_cat_weight, 2)} 
                         for c, count in cat_counts.most_common(3)]
        
        bips = [b for b, count in node_metadata[node]["bip_refs"].most_common(5)]
        src_counts = node_metadata[node]["sources"]
        
        nodes_data.append({
            "id": node,
            "ranks": {"all": i + 1, "p2016": rank_p2016.get(node, 999), "modern": rank_modern.get(node, 999)},
            "scores": {"all": score_all, "p2016": score_p2016, "modern": score_modern},
            "val": (score_all * 2000) + 2, 
            "growth": growth,
            "top_category": cat_counts.most_common(1)[0][0] if cat_counts else "other",
            "expertise": top_3_cats,
            "bips": bips,
            "last_active": node_metadata[node]["last_active"].isoformat()
        })

    # Export top 600 nodes for visualization
    visible_nodes = nodes_data[:600]
    visible_ids = {n['id'] for n in visible_nodes}
    
    links_data = []
    for u, v, data in G_all.edges(data=True):
        if u in visible_ids and v in visible_ids:
            links_data.append({
                "source": u,
                "target": v,
                "weight": int(data['weight']),
                "category": data.get('category', 'other')
            })

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'w') as f:
        payload = {
            "nodes": visible_nodes, 
            "links": links_data,
            "metadata": {
                "total_population": int(total_population),
                "generated_at": datetime.now().isoformat()
            }
        }
        json.dump(payload, f, indent=2)
    
    print(f"Exported richer network to {OUTPUT_PATH}")

if __name__ == "__main__":
    extract_network()
