import pandas as pd
import json
import networkx as nx
import os
import re
from collections import Counter
from datetime import datetime, timedelta
import math

# --- Configuration & Identity Resolution ---
IDENTITY_MAP_PATH = 'metadata/identities.json'
INPUT_DATA_PATH = 'data/enriched/social_threads.parquet'
OUTPUT_DIR = 'output/network'

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
    print(f"Loading identity resolver...")
    resolve = load_identity_resolver()
    
    if not os.path.exists(INPUT_DATA_PATH):
        print(f"Error: {INPUT_DATA_PATH} not found. Run categorization script first.")
        return

    print(f"Loading enriched social data from {INPUT_DATA_PATH}...")
    df = pd.read_parquet(INPUT_DATA_PATH)
    df['date'] = pd.to_datetime(df['date'])
    df['canonical_id'] = df['canonical_id'].apply(resolve)
    
    # Filter out system, unknown, and admin early
    df = df[~df['canonical_id'].str.lower().isin(['system', 'unknown', 'admin'])]
    df = df[df['canonical_id'].notna()]
    
    # Define historical eras
    now = df['date'].max()
    post_2016_start = datetime(2016, 1, 1)
    modern_start = now - timedelta(days=3 * 365) # Past 3 years
    
    # Message ID -> Author lookup for edge recreation
    df['msg_id_clean'] = df['message_id'].str.strip('<>')
    msg_to_author = df.dropna(subset=['msg_id_clean']).set_index('msg_id_clean')['canonical_id'].to_dict()
    
    # Initialize Graphs for different eras
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
        
        primary_cat = row.get('category', 'other')
        all_cats = row.get('categories', [])
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
        for c in all_cats:
            if c != primary_cat:
                node_metadata[author]["categories"][c] += 0.5 
        
        for b in bip_refs:
            node_metadata[author]["bip_refs"][b] += 1
            
        node_metadata[author]["last_active"] = max(node_metadata[author]["last_active"], date)
        
        if pd.isna(reply_to) or not author or author.lower() in ['system', 'unknown', 'admin']:
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

    print("Dampening message counts for PageRank (Logarithmic scaling)...")
    for G in [G_all, G_post2016, G_modern]:
        for u, v, d in G.edges(data=True):
            # Dampen weight so message volume != linear influence
            # weight 1 -> 1.0, 10 -> 4.3, 100 -> 7.6
            d['weight'] = 1.0 + math.log(d['weight'], 2)

    print("Calculating era-based PageRank centralities...")
    pagerank_all = nx.pagerank(G_all, weight='weight')
    pagerank_post2016 = nx.pagerank(G_post2016, weight='weight') if len(G_post2016) > 0 else {}
    pagerank_modern = nx.pagerank(G_modern, weight='weight') if len(G_modern) > 0 else {}
    
    # Try to load total population from Master Registry first for accurate "Overall" stats
    total_population = df['canonical_id'].nunique()
    REGISTRY_PATH = 'metadata/contributors.json'
    if os.path.exists(REGISTRY_PATH):
        try:
            with open(REGISTRY_PATH, 'r') as f:
                registry = json.load(f)
                total_population = len(registry.get('contributors', []))
                print(f"  Using Master Registry population count: {total_population}")
        except Exception as e:
            print(f"  Warning: Could not load registry for population count: {e}")

    print(f"Exporting enriched network data for {total_population} total contributors (interactive + observers)...")
    nodes_data = []
    
    # Sort by centrality first to assign absolute ranks
    sorted_nodes = sorted(G_all.nodes(), key=lambda n: pagerank_all.get(n, 0), reverse=True)
    
    # Pre-calculate ranks for each era
    rank_p2016 = {node: i+1 for i, node in enumerate(sorted(G_all.nodes(), key=lambda n: pagerank_post2016.get(n, 0), reverse=True))}
    rank_modern = {node: i+1 for i, node in enumerate(sorted(G_all.nodes(), key=lambda n: pagerank_modern.get(n, 0), reverse=True))}
    
    for i, node in enumerate(sorted_nodes):
        score_all = pagerank_all.get(node, 0)
        score_p2016 = pagerank_post2016.get(node, 0)
        score_modern = pagerank_modern.get(node, 0)
        
        # Calculate growth based on modern vs post-2016 activity
        growth = (score_modern / score_p2016) if score_p2016 > 0 else 0
        
        cat_counts = node_metadata[node]["categories"]
        total_cat_weight = sum(cat_counts.values())
        top_3_cats = []
        if total_cat_weight > 0:
            top_3_cats = [{"topic": c, "share": round(count/total_cat_weight, 2)} 
                         for c, count in cat_counts.most_common(3)]
        
        bips = [b for b, count in node_metadata[node]["bip_refs"].most_common(5)]
        
        src_counts = node_metadata[node]["sources"]
        dominant_source = "Mixed"
        if src_counts["delving"] > src_counts.get("mailing_list", 0) * 2: dominant_source = "delving"
        elif src_counts.get("mailing_list", 0) > src_counts["delving"] * 2: dominant_source = "mailing_list"
        
        nodes_data.append({
            "id": node,
            "ranks": {
                "all": i + 1,
                "p2016": rank_p2016[node],
                "modern": rank_modern[node]
            },
            "scores": {
                "all": score_all,
                "p2016": score_p2016,
                "modern": score_modern
            },
            "val": (score_all * 2000) + 2, 
            "growth": growth,
            "top_category": cat_counts.most_common(1)[0][0] if cat_counts else "other",
            "expertise": top_3_cats,
            "bips": bips,
            "dominant_source": dominant_source,
            "source_breakdown": {s: count for s, count in src_counts.items()},
            "threads_started": node_metadata[node]["threads_started"],
            "replies_sent": node_metadata[node]["replies_sent"],
            "replies_received": int(G_all.in_degree(node, weight='weight')),
            "last_active": node_metadata[node]["last_active"].isoformat()
        })

    # Create a lookup for social nodes
    social_node_lookup = {n['id']: n for n in nodes_data}
    
    # Process the entire registry to include code-only contributors
    print(f"Enriching all {total_population} contributors with archetypes and hybrid weighting...")
    RICH_CODE_STATS = 'output/tracker/contributors_rich.json'
    code_stats_data = {}
    if os.path.exists(RICH_CODE_STATS):
        try:
            with open(RICH_CODE_STATS, 'r') as f:
                for c in json.load(f):
                    code_stats_data[c['name']] = c
        except: pass

    all_enriched_nodes = []
    registry_contributors = registry.get('contributors', []) if 'registry' in locals() else []
    
    for reg_entry in registry_contributors:
        cid = reg_entry['id']
        social_data = social_node_lookup.get(cid, {})
        c_stats = code_stats_data.get(cid, {})
        
        # 1. Base Metrics
        commits = c_stats.get('total_commits', 0)
        impact = c_stats.get('impact', 0)
        is_bip_author = reg_entry.get('badges', {}).get('is_bip_author', False)
        social_score = social_data.get('scores', {}).get('all', 0)
        
        # 2. Hybrid Influence Weight calculation
        # Normalized factors
        commit_factor = math.log(commits + 1, 2) / 10.0 # log2(1024) = 10 -> factor 1.0
        social_factor = social_score * 100 
        
        hybrid_score = (social_factor * 0.4) + (commit_factor * 0.45) 
        if is_bip_author: hybrid_score += 1.5
        if c_stats.get('is_maintainer'): hybrid_score += 2.0
        
        # 3. Archetype Logic
        if is_bip_author and commits > 50 and social_score > 0.005:
            dev_type = "Protocol Architect"
        elif commits > 100:
            dev_type = "Core Engineer"
        elif social_score > 0.01:
            dev_type = "Social Researcher"
        elif is_bip_author:
            dev_type = "BIP Author"
        elif commits > 0 and social_score == 0:
            dev_type = "Silent Contributor"
        elif social_data.get('expertise') and social_data['expertise'][0]['share'] > 0.6:
            dev_type = "Specialist"
        else:
            dev_type = "Protocol Participant"

        # Combine data
        node_obj = social_data.copy() if social_data else {
            "id": cid,
            "ranks": {"all": 9999, "p2016": 9999, "modern": 9999},
            "scores": {"all": 0, "p2016": 0, "modern": 0},
            "val": 2, # Base size
            "growth": 0,
            "top_category": "code",
            "expertise": [],
            "bips": reg_entry.get('badges', {}).get('bips', []),
            "dominant_source": "github",
            "source_breakdown": {"github": 1},
            "threads_started": 0,
            "replies_sent": 0,
            "replies_received": 0,
            "last_active": datetime.now().isoformat() # Placeholder for code-only
        }
        
        node_obj.update({
            "dev_type": dev_type,
            "hybrid_score": round(hybrid_score, 4),
            "val": (hybrid_score * 10) + 2, # Scale node size by hybrid influence
            "code_stats": {
                "commits": commits,
                "impact": impact,
                "is_maintainer": c_stats.get('is_maintainer', False)
            }
        })
        all_enriched_nodes.append(node_obj)

    # Sort ALL contributors by hybrid influence
    all_enriched_nodes.sort(key=lambda x: x['hybrid_score'], reverse=True)
    
    # Save FULL list for Registry Sync
    SOCIAL_STATS_PATH = 'data/enriched/social_stats.json'
    os.makedirs(os.path.dirname(SOCIAL_STATS_PATH), exist_ok=True)
    with open(SOCIAL_STATS_PATH, 'w') as f:
        json.dump({"contributors": all_enriched_nodes}, f, indent=2)
    print(f"Exported comprehensive contributor dataset ({len(all_enriched_nodes)} people) to {SOCIAL_STATS_PATH}")

    # Take top 500 for visualization (Signal over Noise)
    visible_nodes = all_enriched_nodes[:500]
    visible_ids = {n['id'] for n in visible_nodes}
    
    links_data = []
    for u, v, data in G_all.edges(data=True):
        if u in visible_ids and v in visible_ids:
            links_data.append({
                "source": u,
                "target": v,
                "weight": int(data['weight']),
                "source_plat": data.get('source', 'unknown'),
                "category": data.get('category', 'other')
            })


    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, 'network_graph.json'), 'w') as f:
        json.dump({
            "nodes": visible_nodes, 
            "links": links_data,
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_population": total_population,
                "visible_count": len(visible_nodes),
                "link_count": len(links_data)
            }
        }, f, indent=2)
    
    print(f"Exported richer network to {OUTPUT_DIR}/network_graph.json")

if __name__ == "__main__":
    extract_network()
