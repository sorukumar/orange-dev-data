import pandas as pd
import json
import networkx as nx
import os
import re
from collections import Counter
from datetime import datetime, timedelta
import math
import sys

sys.path.append(os.getcwd())
from scripts.utils.identity import resolver

# --- Configuration & Identity Resolution ---
IDENTITY_MAP_PATH = 'metadata/identities.json'
INPUT_DATA_PATH = 'data/enriched/social_threads.parquet'
OUTPUT_DIR = 'output/network'

def extract_network():
    print(f"Loading identity resolver...")
    
    if not os.path.exists(INPUT_DATA_PATH):
        print(f"Error: {INPUT_DATA_PATH} not found. Run categorization script first.")
        return

    print(f"Loading enriched social data from {INPUT_DATA_PATH}...")
    df = pd.read_parquet(INPUT_DATA_PATH)
    df['date'] = pd.to_datetime(df['date'])

    # canonical_ids in social_threads.parquet are already correct Phase 2 UUIDs.
    # restamp_social_ids.py and restamp_delving_ids.py re-stamp both raw parquets
    # after build_identities.py runs, so no re-resolution is needed here.
    # This safety net only catches rows where canonical_id is genuinely absent
    # (e.g. a brand-new source that hasn't been through restamp yet).
    if 'canonical_id' not in df.columns:
        df['canonical_id'] = None

    def resolve_row(row):
        src = row.get('source')
        if src == 'delving':
            username = str(row.get('author_username') or row.get('author_name') or '')
            return resolver.resolve_delving(username)
        return resolver.resolve_git(
            str(row.get('author_name') or ''),
            str(row.get('author_email') or ''),
        )

    needs_resolve = (
        df['canonical_id'].isna() |
        (df['canonical_id'].astype(str).str.strip() == '') |
        (df['canonical_id'].astype(str).str.lower() == 'unknown')
    )
    if needs_resolve.any():
        print(f"  Safety-net: resolving {needs_resolve.sum()} rows with missing canonical_id...")
        df.loc[needs_resolve, 'canonical_id'] = df[needs_resolve].apply(resolve_row, axis=1)

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
                "first_active": date,
                "last_active": date,
                "msg_count": 0,
                "threads_started": 0,
                "replies_sent": 0,
                "ml_threads": 0,
                "delving_threads": 0,
                "ml_responses": 0,
                "delving_responses": 0
            }
        
        node_metadata[author]["sources"][source] += 1
        node_metadata[author]["msg_count"] += 1
        
        is_reply = row.get('is_reply')
        if is_reply is None or pd.isna(is_reply):
            is_reply = pd.notna(reply_to) or str(row.get('subject', '')).lower().startswith('re:')
        
        if is_reply:
            node_metadata[author]["replies_sent"] += 1
            if source == "mailing_list": node_metadata[author]["ml_responses"] += 1
            elif source == "delving": node_metadata[author]["delving_responses"] += 1
        else:
            node_metadata[author]["threads_started"] += 1
            if source == "mailing_list": node_metadata[author]["ml_threads"] += 1
            elif source == "delving": node_metadata[author]["delving_threads"] += 1
            
        node_metadata[author]["categories"][primary_cat] += 1
        for c in all_cats:
            if c != primary_cat:
                node_metadata[author]["categories"][c] += 0.5 
        
        for b in bip_refs:
            node_metadata[author]["bip_refs"][b] += 1
            
        node_metadata[author]["first_active"] = min(node_metadata[author]["first_active"], date)
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
    IDENTITIES_PATH = 'metadata/identities.json'
    if os.path.exists(IDENTITIES_PATH):
        try:
            with open(IDENTITIES_PATH, 'r') as f:
                identities_data = json.load(f).get('identities', [])
                total_population = len(identities_data)
                print(f"  Using Universal Identities population count: {total_population}")
        except Exception as e:
            print(f"  Warning: Could not load registry for population count: {e}")

    print(f"Exporting enriched network data for {total_population} total contributors (interactive + observers)...")
    nodes_data = []
    
    # Sort by centrality first to assign absolute ranks
    all_nodes_in_data = list(node_metadata.keys())
    sorted_nodes = sorted(all_nodes_in_data, key=lambda n: pagerank_all.get(n, 0), reverse=True)
    
    # Pre-calculate ranks for each era
    # Note: Using G_all.nodes() for ranking since they are the ones with actual edge-based influence
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
                "p2016": rank_p2016.get(node, 9999),
                "modern": rank_modern.get(node, 9999)
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
            "ml_threads": node_metadata[node]["ml_threads"],
            "delving_threads": node_metadata[node]["delving_threads"],
            "ml_responses": node_metadata[node]["ml_responses"],
            "delving_responses": node_metadata[node]["delving_responses"],
            "replies_received": int(G_all.in_degree(node, weight='weight')) if node in G_all else 0,
            "first_active": node_metadata[node]["first_active"].isoformat(),
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
                    uid = resolver.resolve_git(str(c['name']), None)
                    code_stats_data[uid] = c
        except: pass

    all_enriched_nodes = []
    processed_cids = set()

    # Use identities.json as the canonical population base — it is rebuilt fresh every run.
    # contributors.json is additive/never-pruned and accumulates zombie entries.
    with open(IDENTITY_MAP_PATH, 'r') as f:
        identities_list = json.load(f)['identities']

    # Build a role/badge lookup from contributors.json (if it exists) keyed by UUID.
    # Multiple contributors.json records may resolve to the same canonical UUID.
    # Preserve positive badge values and merged roles rather than overwriting.
    contrib_lookup = {}
    if os.path.exists('metadata/contributors.json'):
        with open('metadata/contributors.json', 'r') as f:
            for c in json.load(f).get('contributors', []):
                _raw_id = c.get('display_name') or c.get('id')
                uid = resolver.resolve_git(str(_raw_id), None)
                existing = contrib_lookup.get(uid)
                if existing is None:
                    contrib_lookup[uid] = c
                else:
                    existing_badges = existing.setdefault('badges', {})
                    for badge, value in c.get('badges', {}).items():
                        if value and not existing_badges.get(badge):
                            existing_badges[badge] = value
                    existing_roles = set(existing.get('roles', []))
                    existing_roles.update(c.get('roles', []))
                    existing['roles'] = sorted(existing_roles)
                    if not existing.get('github', {}).get('login') and c.get('github', {}).get('login'):
                        existing.setdefault('github', {})['login'] = c['github']['login']
                    if not existing.get('identities') and c.get('identities'):
                        existing['identities'] = c['identities']

    # Load reviews_count from contributor_review_metrics.parquet (keyed by canonical_id).
    # Produced by efficiency.py which runs immediately before influence.py in the pipeline.
    EFFICIENCY_PATH = 'data/enriched/contributor_review_metrics.parquet'
    registry_stats = {}  # uuid -> {reviews_count, bips_authored}
    if os.path.exists(EFFICIENCY_PATH):
        try:
            eff_df = pd.read_parquet(EFFICIENCY_PATH, columns=['canonical_id', 'reviews_count', 'prs_authored'])
            for _, row in eff_df.iterrows():
                uid = row.get('canonical_id')
                if uid:
                    registry_stats.setdefault(uid, {})['reviews_count'] = row.get('reviews_count') or 0
                    registry_stats.setdefault(uid, {})['prs_authored'] = row.get('prs_authored') or 0
        except Exception as e:
            print(f"  Warning: Could not load contributor_review_metrics for review data: {e}")

    # Derive bips_authored count per UUID from bips_refined.parquet (Phase 1 ingest output).
    # author_canonical_ids is a list column; explode it to count BIPs per author.
    BIPS_PATH = 'data/enriched/bips_refined.parquet'
    if os.path.exists(BIPS_PATH):
        try:
            bips_df = pd.read_parquet(BIPS_PATH, columns=['author_canonical_ids'])
            bip_counts = Counter()
            for ids in bips_df['author_canonical_ids'].dropna():
                # column is stored as numpy.ndarray; coerce to iterable safely
                for uid in (ids.tolist() if hasattr(ids, 'tolist') else list(ids)):
                    if uid:
                        bip_counts[uid] += 1
            for uid, count in bip_counts.items():
                registry_stats.setdefault(uid, {})['bips_authored'] = count
        except Exception as e:
            print(f"  Warning: Could not load bips_refined for BIP count data: {e}")

    for identity in identities_list:
        cid = identity['uuid']

        if cid in processed_cids:
            continue
        processed_cids.add(cid)

        social_data = social_node_lookup.get(cid, {})
        c_stats = code_stats_data.get(cid, {})
        reg_entry = contrib_lookup.get(cid, {})

        # 1. Base Metrics
        commits = c_stats.get('total_commits', 0)
        impact = c_stats.get('impact', 0)
        is_bip_author = reg_entry.get('badges', {}).get('is_bip_author', False)
        social_score = social_data.get('scores', {}).get('all', 0)
        reg_data = registry_stats.get(cid, {})
        reviews = reg_data.get('reviews_count', 0) or 0
        prs_authored = reg_data.get('prs_authored', 0) or 0
        bips_authored = reg_data.get('bips_authored', 0) or 0
        # Fall back to boolean badge if registry count unavailable
        if bips_authored == 0 and is_bip_author:
            bips_authored = 1

        # 2. Hybrid Influence Weight calculation
        # Weights: commit=0.40, review=0.25, social=0.35 → sum=1.00
        # All continuous factors are log-scaled to dampen outliers.
        # log2(1024)=10 anchors commit_factor and review_factor at 1.0 for ~1k activity.
        # Review signal combines PR reviews + authored PRs (PRs are a weaker but real engagement signal).
        review_signal = reviews + (prs_authored * 0.5)
        commit_factor = math.log(commits + 1, 2) / 10.0           # weight: 0.40
        review_factor = math.log(review_signal + 1, 2) / 10.0     # weight: 0.25
        # social_factor is capped at 1.0: PageRank is graph-normalized, so raw value shrinks
        # as the contributor pool grows. Capping keeps scores comparable across monthly builds.
        social_factor = min(social_score * 100, 1.0)               # weight: 0.35

        hybrid_score = (social_factor * 0.35) + (commit_factor * 0.40) + (review_factor * 0.25)

        # Qualitative bonuses (log-scaled BIP count; flat maintainer recognition)
        # Coefficient 0.35: 1 BIP→0.35, 3 BIPs→0.70, 7 BIPs→1.05, 15 BIPs→1.40
        # (reduced from 0.50 to prevent BIP count from dominating over sustained engineering)
        if bips_authored > 0:
            hybrid_score += math.log(bips_authored + 1, 2) * 0.35
        if c_stats.get('is_maintainer'):
            hybrid_score += 1.0
        
        # 3. Archetype Logic — 4 groups + Creator singleton
        # PM-friendly grouping: each label answers "what is their primary role?"
        #
        # Protocol Designer  — shapes the protocol through standards or architecturally
        #                       influential work (BIP authorship, OR strong hybrid + visibility signal)
        # Builder            — ships code (any commits; not a Protocol Designer)
        # Reviewer           — no code commits but active in review or discussion
        # Participant        — general/occasional participation across any activity
        #
        # Evaluation order: most specific first so Builders with BIPs → Designer, not Builder.
        designer_commit_threshold = 100
        designer_social_threshold = 0.01
        designer_hybrid_threshold = 2.0
        designer_bip_count_threshold = 3
        designer_bip_hybrid_threshold = 1.0

        if cid == 'can_satoshi_nakamoto':
            dev_type = "Creator"
        elif (
            (bips_authored >= designer_bip_count_threshold and hybrid_score > designer_bip_hybrid_threshold)
            or (
                (commits > designer_commit_threshold or bips_authored > 0)
                and (
                    social_score > designer_social_threshold or
                    hybrid_score > designer_hybrid_threshold
                )
            )
        ):
            # Strong Designer signal requires either:
            # 1. multi-BIP authorship with sufficient hybrid influence, or
            # 2. a strong engineering/social signal from commits + visibility.
            dev_type = "Protocol Designer"
        elif commits > 0:
            # Any code committer who didn't clear the Designer bar.
            # Covers former Core Engineer + Silent Contributor — the distinction
            # (prolific vs occasional) adds labels without adding insight.
            dev_type = "Builder"
        elif reviews >= 10 or social_score > 0.005:
            # No commits but actively scrutinizes: code reviewer or protocol discussant.
            # Covers former Active Reviewer + Social Researcher.
            # Threshold 10 reviews / 0.5% PageRank separates signal from noise.
            dev_type = "Reviewer"
        else:
            dev_type = "Participant"

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
            "ml_threads": 0,
            "delving_threads": 0,
            "ml_responses": 0,
            "delving_responses": 0,
            "replies_received": 0,
            "first_active": None,  # Code-only: no social activity date
            "last_active": None    # Will be derived from last_commit in unify step
        }
        
        node_obj.update({
            # Always use identities.json as the authoritative display name source.
            # contributors.json entries keyed by resolve_git(display_name) can collide
            # with impersonator accounts whose names are in a real person's git aliases.
            "display_name": identity.get('display_name', cid),
            "dev_type": dev_type,
            "hybrid_score": round(hybrid_score, 4),
            "reviews_count": int(reviews),
            "bips_authored": int(bips_authored),
            "val": (hybrid_score * 10) + 2, # Scale node size by hybrid influence
            "code_stats": {
                "commits": commits,
                "reviews": int(reviews),
                "bips_authored": int(bips_authored),
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

    # Take top 150 for visualization — covers ~top 2.5% by hybrid_score.
    # Force layout is cleaner and lower-signal nodes add visual noise beyond this.
    visible_nodes = all_enriched_nodes[:150]
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
