import pandas as pd
import json
import networkx as nx
from networkx.algorithms.community import louvain_communities
import numpy as np
import os
import re
from collections import Counter
from datetime import datetime, timedelta
import math
import sys

sys.path.append(os.getcwd())
from scripts.utils.identity import resolver
from scripts.utils.subsystem import SubsystemResolver

# --- Configuration & Identity Resolution ---
IDENTITY_MAP_PATH = 'metadata/identities.json'
INPUT_DATA_PATH = 'data/enriched/social_threads.parquet'
OUTPUT_DIR = 'output/network'


def compute_expertise_signals(node, commit_hist, bip_theme_counts,
                               commit_cat_to_domain, bip_theme_to_domain, sub_resolver):
    """Synthesize expertise signals from code commits, BIP authorship, and discussion activity.

    Weights: BIPs (0.50) > code commits (0.30) > discussion (0.20).
    BIP authorship is the strongest explicit expertise signal; code commits reflect sustained
    effort; discussion participation is the weakest (engagement != deep expertise).

    Returns dict with:
      expertise_domains: list of 1-3 domain IDs (synthesized, most relevant first)
      expertise_by_source: {source: {domain: share}} — stored for future source-split UI
    """
    expertise_by_source = {}

    # 1. Code signal: aggregate across all years, map commit category → domain
    code_domain_counts = Counter()
    for year_data in commit_hist.values():
        for cat, count in year_data.items():
            domain = commit_cat_to_domain.get(cat)
            if domain:
                code_domain_counts[domain] += count
    if code_domain_counts:
        total = sum(code_domain_counts.values())
        expertise_by_source['code'] = {
            d: round(c / total, 3) for d, c in code_domain_counts.most_common()
        }

    # 2. BIP signal: map BIP theme → domain
    bip_domain_counts = Counter()
    for theme, count in (bip_theme_counts or {}).items():
        domain = bip_theme_to_domain.get(theme)
        if domain:
            bip_domain_counts[domain] += count
    if bip_domain_counts:
        total = sum(bip_domain_counts.values())
        expertise_by_source['bips'] = {
            d: round(c / total, 3) for d, c in bip_domain_counts.most_common()
        }

    # 3. Discussion signal: map subsystem slugs from social expertise list → domain.
    # Exclude Infrastructure here (social chatter about build/test topics is noise).
    discussion_domain_counts = Counter()
    for exp in node.get('expertise', []):
        slug = exp.get('topic', '')
        domain = sub_resolver.get_expertise_domain(slug)
        if domain and domain != 'Infrastructure':
            discussion_domain_counts[domain] += exp.get('share', 0.0)
    if discussion_domain_counts:
        total = sum(discussion_domain_counts.values())
        expertise_by_source['discussion'] = {
            d: round(c / total, 3)
            for d, c in sorted(discussion_domain_counts.items(), key=lambda x: -x[1])
        }

    # 4. Weighted synthesis
    combined = Counter()
    for domain, share in expertise_by_source.get('bips', {}).items():
        combined[domain] += share * 0.50
    for domain, share in expertise_by_source.get('code', {}).items():
        combined[domain] += share * 0.30
    for domain, share in expertise_by_source.get('discussion', {}).items():
        combined[domain] += share * 0.20

    # Fallback when no structured signals: map top_category slug → domain
    if not combined:
        tc = node.get('top_category', 'other')
        domain = sub_resolver.get_expertise_domain(tc)
        if domain:
            combined[domain] = 1.0

    top_domains = [d for d, _ in combined.most_common(3)]
    if not top_domains:
        top_domains = ['Infrastructure']

    # Normalize the full weighted synthesis into a continuous score dict.
    # Stored as expertise_domain_scores for threshold-based filtering and
    # weighted ranking in the frontend (e.g. hybrid_score × domain_score = domain authority).
    # Only domains with a meaningful score (>0.01) are emitted to keep JSON compact.
    total_combined = sum(combined.values()) or 1.0
    expertise_domain_scores = {
        d: round(s / total_combined, 4)
        for d, s in combined.most_common()
        if s / total_combined > 0.01
    }

    return {
        'expertise_domains': top_domains,
        'expertise_by_source': expertise_by_source,
        'expertise_domain_scores': expertise_domain_scores,
    }


def extract_network():
    print(f"Loading identity resolver...")
    sub_resolver = SubsystemResolver()

    # Load expertise domain definitions (single source of truth for domain metadata).
    EXPERTISE_DOMAINS_PATH = 'metadata/expertise_domains.json'
    expertise_domains_def = []
    commit_cat_to_domain = {}   # "Tests (QA)" → "Infrastructure"
    bip_theme_to_domain = {}    # "Consensus & Soft Forks" → "Consensus"
    if os.path.exists(EXPERTISE_DOMAINS_PATH):
        with open(EXPERTISE_DOMAINS_PATH) as _f:
            expertise_domains_def = json.load(_f).get('domains', [])
        for _d in expertise_domains_def:
            for _cc in _d.get('commit_categories', []):
                commit_cat_to_domain[_cc] = _d['id']
            for _bt in _d.get('bip_themes', []):
                bip_theme_to_domain[_bt] = _d['id']
    else:
        print(f"  Warning: {EXPERTISE_DOMAINS_PATH} not found; expertise synthesis will use discussion signals only.")

    
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
                "delving_responses": 0,
                "p2016_posts": 0,
                "modern_posts": 0,
                "p2016_ml_posts": 0,
                "p2016_delving_posts": 0,
                "modern_ml_posts": 0,
                "modern_delving_posts": 0
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
        if date >= post_2016_start:
            node_metadata[author]["p2016_posts"] += 1
            if source == "mailing_list":
                node_metadata[author]["p2016_ml_posts"] += 1
            elif source == "delving":
                node_metadata[author]["p2016_delving_posts"] += 1
        if date >= modern_start:
            node_metadata[author]["modern_posts"] += 1
            if source == "mailing_list":
                node_metadata[author]["modern_ml_posts"] += 1
            elif source == "delving":
                node_metadata[author]["modern_delving_posts"] += 1
        
        if pd.isna(reply_to) or not author or author.lower() in ['system', 'unknown', 'admin']:
            continue
            
        target_mid = reply_to.strip('<>')
        recipient = msg_to_author.get(target_mid)
        
        if recipient and recipient != author:
            # Time-decay weight for community detection.
            # λ=0.15 → 5yr ago≈0.47, 10yr ago≈0.22, 15yr ago≈0.10.
            # Preserves historical signal but makes recent activity dominant,
            # so the old-guard cluster separates from the modern generation.
            _years_ago = max(0.0, (now - date).total_seconds() / (365.25 * 24 * 3600))
            _decay = math.exp(-0.15 * _years_ago)

            # 1. All-time graph
            if G_all.has_edge(author, recipient):
                G_all[author][recipient]['weight'] += 1
                G_all[author][recipient]['decay_weight'] = (
                    G_all[author][recipient].get('decay_weight', 0.0) + _decay
                )
            else:
                G_all.add_edge(author, recipient, weight=1, decay_weight=_decay, category=primary_cat, source=source)
            
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
            "p2016_posts": node_metadata[node]["p2016_posts"],
            "modern_posts": node_metadata[node]["modern_posts"],
            "p2016_ml_posts": node_metadata[node]["p2016_ml_posts"],
            "p2016_delving_posts": node_metadata[node]["p2016_delving_posts"],
            "modern_ml_posts": node_metadata[node]["modern_ml_posts"],
            "modern_delving_posts": node_metadata[node]["modern_delving_posts"],
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
            eff_df = pd.read_parquet(EFFICIENCY_PATH, columns=['canonical_id', 'reviews_count', 'prs_authored', 'p2016_reviews_count', 'modern_reviews_count'])
            for _, row in eff_df.iterrows():
                uid = row.get('canonical_id')
                if uid:
                    registry_stats.setdefault(uid, {})['reviews_count'] = row.get('reviews_count') or 0
                    registry_stats.setdefault(uid, {})['prs_authored'] = row.get('prs_authored') or 0
                    registry_stats.setdefault(uid, {})['p2016_reviews_count'] = row.get('p2016_reviews_count') or 0
                    registry_stats.setdefault(uid, {})['modern_reviews_count'] = row.get('modern_reviews_count') or 0
        except Exception as e:
            print(f"  Warning: Could not load contributor_review_metrics for review data: {e}")

    # Derive bips_authored count per UUID from bips_refined.parquet (Phase 1 ingest output).
    # author_canonical_ids is a list column; explode it to count BIPs per author.
    # Also collect per-author BIP theme distribution for expertise synthesis.
    BIPS_PATH = 'data/enriched/bips_refined.parquet'
    bip_author_themes = {}  # uuid → Counter{theme: bip_count}
    if os.path.exists(BIPS_PATH):
        try:
            bips_df = pd.read_parquet(BIPS_PATH, columns=['author_canonical_ids', 'theme'])
            bip_counts = Counter()
            for _, brow in bips_df.iterrows():
                theme = brow.get('theme')
                ids = brow['author_canonical_ids']
                for uid in (ids.tolist() if hasattr(ids, 'tolist') else list(ids or [])):
                    if uid:
                        bip_counts[uid] += 1
                        if theme:
                            bip_author_themes.setdefault(uid, Counter())[theme] += 1
            for uid, count in bip_counts.items():
                registry_stats.setdefault(uid, {})['bips_authored'] = count
        except Exception as e:
            print(f"  Warning: Could not load bips_refined for BIP count data: {e}")

    # Load contributor commit history for code-signal expertise synthesis.
    # Written by 02_process/core.py. Format: {uuid: {year: {category: count}}}
    COMMIT_HISTORY_PATH = 'data/enriched/contributor_commit_history.json'
    commit_history = {}
    if os.path.exists(COMMIT_HISTORY_PATH):
        try:
            with open(COMMIT_HISTORY_PATH) as _f:
                commit_history = json.load(_f)
            print(f"  Loaded commit history for {len(commit_history)} contributors.")
        except Exception as e:
            print(f"  Warning: Could not load contributor_commit_history: {e}")

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
        # BIP bonus is capped at ~32 BIPs worth (1.765) to prevent runaway scores from
        # prolific BIP authors and keep the theoretical maximum stable at ~3.75.
        BIP_BONUS_CAP = math.log(33, 2) * 0.35  # ≈ 1.765
        if bips_authored > 0:
            hybrid_score += min(math.log(bips_authored + 1, 2) * 0.35, BIP_BONUS_CAP)
        if c_stats.get('is_maintainer'):
            hybrid_score += 1.0
        
        # 2b. Modern Hybrid Score — same formula but uses era-specific inputs.
        # modern_hybrid_score answers "how active/influential is this person RIGHT NOW?"
        # Uses: last-3Y commit count + all-time reviews (no era split available) + modern PageRank.
        # BIP/maintainer bonuses are intentionally omitted: those are career-level signals,
        # not a measure of current momentum.
        modern_cutoff_year = modern_start.year
        cid_commit_hist = commit_history.get(cid, {})
        modern_commits = sum(
            sum(cat_counts.values())
            for year_str, cat_counts in cid_commit_hist.items()
            if isinstance(cat_counts, dict) and int(year_str) >= modern_cutoff_year
        )
        modern_social_score = pagerank_modern.get(cid, 0)
        modern_commit_factor = math.log(modern_commits + 1, 2) / 10.0
        modern_social_factor = min(modern_social_score * 100, 1.0)
        modern_hybrid_score = (
            (modern_social_factor * 0.35)
            + (modern_commit_factor * 0.40)
            + (review_factor * 0.25)  # all-time reviews — era-split not available
        )

        # 2c. P2016 Hybrid Score — same formula as modern but for the post-2016 era.
        # Answers "how influential has this person been since 2016?"
        # Uses: post-2016 commits + all-time reviews + post-2016 PageRank.
        # No BIP/maintainer bonuses — era-specific signal, not career-level.
        p2016_commits = sum(
            sum(cat_counts.values())
            for year_str, cat_counts in cid_commit_hist.items()
            if isinstance(cat_counts, dict) and int(year_str) >= post_2016_start.year
        )
        p2016_commit_factor = math.log(p2016_commits + 1, 2) / 10.0
        p2016_social_factor = min(pagerank_post2016.get(cid, 0) * 100, 1.0)
        p2016_hybrid_score = (
            (p2016_social_factor * 0.35)
            + (p2016_commit_factor * 0.40)
            + (review_factor * 0.25)
        )

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
            "p2016_posts": 0,
            "modern_posts": 0,
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
            "p2016_hybrid_score": round(p2016_hybrid_score, 4),
            "modern_hybrid_score": round(modern_hybrid_score, 4),
            "impact_score": None,  # Populated after full sort; placeholder until then
            "reviews_count": int(reviews),
            "p2016_reviews_count": int(reg_data.get('p2016_reviews_count', 0)),
            "modern_reviews_count": int(reg_data.get('modern_reviews_count', 0)),
            "bips_authored": int(bips_authored),
            "val": (hybrid_score * 10) + 2, # Scale node size by hybrid influence
            "code_stats": {
                "commits": commits,
                "p2016_commits": int(p2016_commits),
                "modern_commits": int(modern_commits),
                "reviews": int(reviews),
                "bips_authored": int(bips_authored),
                "impact": impact,
                "is_maintainer": c_stats.get('is_maintainer', False)
            }
        })

        # Synthesize expertise signals from code, BIPs, and discussion.
        signals = compute_expertise_signals(
            node_obj,
            commit_history.get(cid, {}),
            bip_author_themes.get(cid),
            commit_cat_to_domain,
            bip_theme_to_domain,
            sub_resolver,
        )
        node_obj.update(signals)
        # scores dict (raw PageRank per era) is now superseded by the three composite
        # hybrid scores (hybrid_score, p2016_hybrid_score, modern_hybrid_score).
        # Drop it from the output to keep the JSON lean.
        node_obj.pop('scores', None)

        all_enriched_nodes.append(node_obj)

    # Sort ALL contributors by hybrid influence
    all_enriched_nodes.sort(key=lambda x: x['hybrid_score'], reverse=True)

    # Compute percentile-based impact_score (0–100 integer, stable across builds).
    # Uses a fixed theoretical max anchor (3.75 = 1.0 base + 1.765 BIP cap + 1.0 maintainer)
    # so scores don't shift when new contributors join.
    # Satoshi (can_satoshi_nakamoto) is excluded — his archetype is "Creator" and his
    # data footprint (early mailing list only) would understate his true impact.
    IMPACT_SCORE_MAX = 3.75
    SATOSHI_ID = 'can_satoshi_nakamoto'
    for node in all_enriched_nodes:
        if node['id'] == SATOSHI_ID:
            node['impact_score'] = None  # Rendered as "Creator" in the frontend
        else:
            node['impact_score'] = min(round(node['hybrid_score'] / IMPACT_SCORE_MAX * 100), 100)

    # Save FULL list for Registry Sync
    SOCIAL_STATS_PATH = 'data/enriched/social_stats.json'
    os.makedirs(os.path.dirname(SOCIAL_STATS_PATH), exist_ok=True)
    with open(SOCIAL_STATS_PATH, 'w') as f:
        json.dump({"contributors": all_enriched_nodes}, f, indent=2)
    print(f"Exported comprehensive contributor dataset ({len(all_enriched_nodes)} people) to {SOCIAL_STATS_PATH}")

    # Take top 250 for visualization.
    # 150 was only the dense generalist core — adding 100 more brings in the
    # domain specialists (Lightning devs, Covenant researchers, etc.) whose
    # connections are narrower and form clearer satellite clusters.
    visible_nodes = all_enriched_nodes[:250]
    visible_ids = {n['id'] for n in visible_nodes}

    print("Building undirected subgraph for community detection (time-decay weighted)...")
    # Use decay_weight (not raw weight) so recent interactions dominate cluster
    # structure.  Old-guard devs who only interacted pre-2018 will have weak
    # edges to newer developers, letting them form a distinct historical cluster.
    G_visible = nx.Graph()
    for n_id in visible_ids:
        G_visible.add_node(n_id)
    for u, v, data in G_all.edges(data=True):
        if u in visible_ids and v in visible_ids:
            dw = data.get('decay_weight', data['weight'])
            if G_visible.has_edge(u, v):
                G_visible[u][v]['weight'] += dw
            else:
                G_visible.add_edge(u, v, weight=dw)

    # Louvain works only on connected (non-isolated) nodes.
    # Isolated nodes (no social edges) receive community_id = -1.
    connected = {n for n in G_visible.nodes() if G_visible.degree(n) > 0}
    G_social = G_visible.subgraph(connected).copy()

    node_to_community = {n: -1 for n in visible_ids}
    communities_list = []
    if len(G_social) > 2:
        print(f"Running Louvain community detection on {len(G_social)} connected nodes...")
        # resolution=1.5 finds finer-grained communities than the default (1.0),
        # which is needed now that decay weighting makes the graph less dense.
        communities_list = louvain_communities(G_social, weight='weight', seed=42, resolution=1.5)
        for cid, members in enumerate(communities_list):
            for node in members:
                node_to_community[node] = cid

    # Label each community by majority theme of its members.
    community_topic_votes: dict = {}
    for n in visible_nodes:
        cid = node_to_community.get(n['id'], -1)
        # Use synthesized expertise_domains[0] if available; fall back to top_category slug → domain.
        theme = (n.get('expertise_domains') or [None])[0]
        if not theme:
            raw_topic = n.get('top_category', 'other')
            theme = sub_resolver.get_expertise_domain(raw_topic) if raw_topic not in ('other', 'code') else 'Ecosystem'
        community_topic_votes.setdefault(cid, Counter())[theme] += 1
    community_labels = {
        cid: votes.most_common(1)[0][0]
        for cid, votes in community_topic_votes.items()
    }

    # Seed spring_layout with community-aware initial positions so the solver
    # starts with clusters already pre-separated — this biases the layout toward
    # clean community groupings and dramatically speeds convergence.
    n_communities = max((v for v in node_to_community.values() if v >= 0), default=0) + 1
    community_centers = {
        i: (
            math.cos(i / n_communities * 2 * math.pi) * 0.4,
            math.sin(i / n_communities * 2 * math.pi) * 0.4,
        )
        for i in range(n_communities)
    }
    community_placed: Counter = Counter()
    community_member_count = Counter(v for v in node_to_community.values() if v >= 0)
    pos_init: dict = {}
    isolated_idx = 0
    for n in visible_nodes:
        n_id = n['id']
        cid = node_to_community.get(n_id, -1)
        if cid >= 0:
            cx, cy = community_centers[cid]
            idx = community_placed[cid]
            count = max(community_member_count[cid], 1)
            spread_angle = (idx / count) * 2 * math.pi
            pos_init[n_id] = (
                cx + math.cos(spread_angle) * 0.1,
                cy + math.sin(spread_angle) * 0.1,
            )
            community_placed[cid] += 1
        else:
            # Isolated nodes scattered on outer ring using golden-angle spacing.
            pos_init[n_id] = (
                math.cos(isolated_idx * 2.399) * 0.75,
                math.sin(isolated_idx * 2.399) * 0.75,
            )
            isolated_idx += 1

    print("Computing spring layout (Fruchterman-Reingold)...")
    pos = nx.spring_layout(
        G_visible,
        pos=pos_init,
        k=0.20,   # slightly tighter than 0.25 to handle 250 nodes cleanly
        iterations=80,
        seed=42,
        weight='weight',
    )

    # Normalise positions to [-1, 1] for canvas-independent storage.
    if pos:
        xs = [p[0] for p in pos.values()]
        ys = [p[1] for p in pos.values()]
        min_x, max_x = min(xs), max(xs)
        min_y, max_y = min(ys), max(ys)
        rx = max(max_x - min_x, 1e-6)
        ry = max(max_y - min_y, 1e-6)
        norm_pos = {
            n_id: (
                round((x - min_x) / rx * 2 - 1, 4),
                round((y - min_y) / ry * 2 - 1, 4),
            )
            for n_id, (x, y) in pos.items()
        }
    else:
        norm_pos = {}

    # Annotate each visible node with community membership and layout position.
    for n in visible_nodes:
        cid = node_to_community.get(n['id'], -1)
        n['community_id'] = cid
        n['community_label'] = community_labels.get(cid, 'other')
        lp = norm_pos.get(n['id'], (0.0, 0.0))
        n['layout_x'] = lp[0]
        n['layout_y'] = lp[1]

    # Build community metadata block for the frontend legend.
    communities_meta = []
    for cid in range(n_communities):
        members = [n['id'] for n in visible_nodes if n.get('community_id') == cid]
        communities_meta.append({
            'id': cid,
            'label': community_labels.get(cid, 'other'),
            'size': len(members),
        })
    unconnected_count = sum(1 for n in visible_nodes if n.get('community_id') == -1)
    if unconnected_count:
        communities_meta.append({'id': -1, 'label': 'Independent', 'size': unconnected_count})
    print(f"  {n_communities} communication communities detected, {unconnected_count} isolated nodes")

    # --- Expertise-Similarity Community Detection ---
    # Cluster by WHAT developers work on (not who they reply to).
    # Uses synthesized expertise_domains for clean, multi-signal representation.
    EXPERTISE_THEMES = ['Consensus', 'Script', 'L2', 'Privacy', 'Wallet',
                        'Mempool', 'Network', 'Mining', 'Cryptography', 'Infrastructure', 'Ecosystem']

    def build_expertise_vector(node):
        vec = np.zeros(len(EXPERTISE_THEMES))
        domain_scores = node.get('expertise_domain_scores', {})
        if domain_scores:
            # Use continuous weighted scores for richer similarity — exclude Infrastructure
            # to prevent the majority of developers from clustering together on build/test work.
            has_non_infra = any(
                d != 'Infrastructure' for d in domain_scores
            )
            for domain, score in domain_scores.items():
                if domain in EXPERTISE_THEMES and score > 0:
                    if domain != 'Infrastructure' or not has_non_infra:
                        vec[EXPERTISE_THEMES.index(domain)] = score
        else:
            # Fallback for nodes without expertise_domain_scores (backward compat).
            domains = node.get('expertise_domains', [])
            use_domains = [d for d in domains if d != 'Infrastructure'] or domains
            for domain in use_domains:
                if domain in EXPERTISE_THEMES:
                    vec[EXPERTISE_THEMES.index(domain)] += 1.0
        # Fallback: map top_category slug → domain when no expertise signal at all.
        if vec.sum() < 0.05:
            tc_domain = sub_resolver.get_expertise_domain(node.get('top_category', 'other'))
            if tc_domain and tc_domain != 'Infrastructure' and tc_domain in EXPERTISE_THEMES:
                vec[EXPERTISE_THEMES.index(tc_domain)] = 1.0
        norm = np.linalg.norm(vec)
        return vec / norm if norm > 0 else vec  # zero vector = generalist (no clear specialty)

    print("Building expertise similarity graph...")
    node_vecs = [(n['id'], build_expertise_vector(n)) for n in visible_nodes]

    G_expertise = nx.Graph()
    for n_id, _ in node_vecs:
        G_expertise.add_node(n_id)
    for i in range(len(node_vecs)):
        for j in range(i + 1, len(node_vecs)):
            id_i, vec_i = node_vecs[i]
            id_j, vec_j = node_vecs[j]
            if np.linalg.norm(vec_i) < 1e-8 or np.linalg.norm(vec_j) < 1e-8:
                continue  # both are generalists with no measurable specialty — skip
            sim = float(np.dot(vec_i, vec_j))  # cosine similarity (vectors are normalised)
            if sim > 0.20:  # threshold: connect only meaningfully similar developers
                G_expertise.add_edge(id_i, id_j, weight=sim)

    expertise_node_to_community = {n['id']: -1 for n in visible_nodes}
    exp_connected = {n for n in G_expertise.nodes() if G_expertise.degree(n) > 0}
    G_exp_social = G_expertise.subgraph(exp_connected).copy()
    n_exp_communities = 0
    if len(G_exp_social) > 2:
        print(f"Running Louvain on expertise graph ({len(G_exp_social)} nodes)...")
        exp_communities_list = louvain_communities(
            G_exp_social, weight='weight', seed=42, resolution=1.2
        )
        n_exp_communities = len(exp_communities_list)
        for ecid, members in enumerate(exp_communities_list):
            for node in members:
                expertise_node_to_community[node] = ecid

    exp_community_topic_votes: dict = {}
    for n in visible_nodes:
        ecid = expertise_node_to_community.get(n['id'], -1)
        theme = (n.get('expertise_domains') or [None])[0]
        if not theme:
            raw_topic = n.get('top_category', 'other')
            theme = sub_resolver.get_expertise_domain(raw_topic) if raw_topic not in ('other', 'code') else 'Ecosystem'
        exp_community_topic_votes.setdefault(ecid, Counter())[theme] += 1
    exp_community_labels = {
        ecid: votes.most_common(1)[0][0]
        for ecid, votes in exp_community_topic_votes.items()
    }

    for n in visible_nodes:
        ecid = expertise_node_to_community.get(n['id'], -1)
        n['expertise_community_id'] = ecid
        n['expertise_community_label'] = exp_community_labels.get(ecid, 'other')

    exp_communities_meta = []
    for ecid in range(n_exp_communities):
        members = [n['id'] for n in visible_nodes if n.get('expertise_community_id') == ecid]
        exp_communities_meta.append({
            'id': ecid,
            'label': exp_community_labels.get(ecid, 'other'),
            'size': len(members),
        })
    exp_unconnected = sum(1 for n in visible_nodes if n.get('expertise_community_id') == -1)
    if exp_unconnected:
        exp_communities_meta.append({'id': -1, 'label': 'Generalist', 'size': exp_unconnected})
    print(f"  {n_exp_communities} expertise communities detected, {exp_unconnected} generalists")

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
                "link_count": len(links_data),
                "communities": communities_meta,
                "expertise_communities": exp_communities_meta,
                "domains": expertise_domains_def,
            }
        }, f, indent=2)

    print(f"Exported richer network to {OUTPUT_DIR}/network_graph.json")

if __name__ == "__main__":
    extract_network()
