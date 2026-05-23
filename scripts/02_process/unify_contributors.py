import pandas as pd
import json
import os
import numpy as np
import sys

sys.path.append(os.getcwd())
import scripts.utils.identity 
from scripts.utils.identity import resolver

# --- Configuration ---
CONTRIBUTORS_REGISTRY = "metadata/contributors.json"
COMMITS_PARQUET = "data/raw/core_commits.parquet"
EFFICIENCY_PARQUET = "data/enriched/contributor_review_metrics.parquet"
SOCIAL_STATS_JSON = "data/enriched/social_stats.json"
BIPS_PARQUET = "data/enriched/bips_refined.parquet"
DELVING_PARQUET = "data/raw/social_delving.parquet"
IDENTITIES_JSON = "metadata/identities.json"
OUTPUT_PARQUET = "data/enriched/contributors_unified.parquet"

def unify():
    print("Loading Master UUID Identities...")
    with open(IDENTITIES_JSON, 'r') as f:
        identities_data = json.load(f)['identities']
    
    # Base unified DataFrame from identities list
    base_rows = []
    for identity in identities_data:
        base_rows.append({
            'uuid': identity['uuid'],
            'display_name': identity['display_name'],
            'github_login_final': (lambda g: g[0] if isinstance(g, list) else g)(identity.get('platforms', {}).get('github')),
            'delving_username_final': identity.get('platforms', {}).get('delving')
        })
    df_id = pd.DataFrame(base_rows)
    
    # --- The Union Master List ---
    # We take all UUIDs from identities.json, and all IDs from social and commits
    # ensuring we never drop someone who exists in any source.
    print("Discovering all active contributors across sources...")
    all_source_uuids = set(df_id['uuid'])
    
    # 1. Discover from Commits
    df_commits = pd.read_parquet(COMMITS_PARQUET)
    
    def map_author(row):
        return resolver.resolve_git(str(row.get('author_name', '')), str(row.get('author_email', '')))
        
    df_commits['canonical_id'] = df_commits.apply(map_author, axis=1)
    
    if 'is_merge' in df_commits.columns:
        df_commits['is_auth'] = (~df_commits['is_merge']).astype(int)
        df_commits['is_merg'] = df_commits['is_merge'].astype(int)
    else:
        df_commits['is_auth'] = 1
        df_commits['is_merg'] = 0
        
    commit_stats = df_commits.groupby('canonical_id').agg(
        total_commits=('hash', 'count'),
        authored_commits=('is_auth', 'sum'),
        merge_commits=('is_merg', 'sum'),
        total_additions=('additions', 'sum'),
        total_deletions=('deletions', 'sum'),
        first_commit=('date_utc', 'min'),
        last_commit=('date_utc', 'max')
    ).reset_index()
    
    all_source_uuids.update(commit_stats['canonical_id'])
    
    # 2. Discover from Social
    df_soc = pd.DataFrame(columns=['canonical_id'])
    if os.path.exists(SOCIAL_STATS_JSON):
        with open(SOCIAL_STATS_JSON, 'r') as f:
            soc_data = json.load(f).get('contributors', [])
        df_soc = pd.DataFrame(soc_data)
        if 'id' in df_soc.columns:
            df_soc.rename(columns={'id': 'canonical_id'}, inplace=True)
            all_source_uuids.update(df_soc['canonical_id'].dropna().unique())
            
    # 3. Discover from Efficiency (Pure Reviewers)
    if os.path.exists(EFFICIENCY_PARQUET):
        df_eff_disc = pd.read_parquet(EFFICIENCY_PARQUET)
        if 'canonical_id' in df_eff_disc.columns:
            all_source_uuids.update(df_eff_disc['canonical_id'].dropna().unique())
            
    print(f"Master Union: {len(all_source_uuids)} unique contributors discovered.")
    
    # --- Rebuild df_unified from the union ---
    final_base = []
    id_lookup = df_id.set_index('uuid').to_dict('index')
    
    for uid in all_source_uuids:
        if uid in id_lookup:
            rec = id_lookup[uid].copy()
            rec['uuid'] = uid
            final_base.append(rec)
        else:
            final_base.append({
                'uuid': uid,
                'display_name': uid,
                'github_login_final': None,
                'delving_username_final': None
            })
    df_unified = pd.DataFrame(final_base)

    print("Aggregating contributors.json metadata...")
    with open(CONTRIBUTORS_REGISTRY, 'r') as f:
        registry_data = json.load(f)['contributors']
    df_reg = pd.DataFrame(registry_data)
    def map_contrib_uuid(row):
        return resolver.resolve_git(row.get('display_name') or row.get('id'), None)
    df_reg['uuid'] = df_reg.apply(map_contrib_uuid, axis=1)
    
    df_reg['first_seen'] = pd.to_datetime(df_reg['first_seen'], errors='coerce')
    df_reg['last_seen'] = pd.to_datetime(df_reg['last_seen'], errors='coerce')
    df_reg_agg = df_reg.groupby('uuid').agg({
        'badges': 'first',
        'roles': 'first',
        'first_seen': 'min',
        'last_seen': 'max',
        'technical_focus': 'first'
    }).reset_index()

    df_unified = df_unified.merge(df_reg_agg, on='uuid', how='left')
    
    # Join code stats
    df_unified = df_unified.merge(commit_stats, left_on='uuid', right_on='canonical_id', how='left').drop(columns=['canonical_id'])
    
    # Join BIP stats
    # author_canonical_ids is already resolved by bips.py at ingest time.
    # Do NOT pass through resolver.resolve_git() again — canonical IDs like
    # "can_luke_dashjr" are not in the name index, so the resolver would
    # mint "auto_can_luke_dashjr" as a ghost, severing the BIP count from
    # the actual contributor row.
    print("Processing BIP data...")
    df_bips = pd.read_parquet(BIPS_PARQUET)
    bip_counts = {}
    for ids in df_bips['author_canonical_ids']:
        if ids is None:
            continue
        if isinstance(ids, str):
            try: ids = json.loads(ids.replace("'", '"'))
            except: continue
        for cid in ids:
            cid = str(cid).strip()
            if cid:
                bip_counts[cid] = bip_counts.get(cid, 0) + 1
    df_bip_stats = pd.DataFrame(list(bip_counts.items()), columns=['uuid', 'bips_authored'])
    df_unified = df_unified.merge(df_bip_stats, on='uuid', how='left')
    
    # Join Efficiency
    if os.path.exists(EFFICIENCY_PARQUET):
        df_eff = pd.read_parquet(EFFICIENCY_PARQUET)
        eff_core = df_eff.drop(columns=['github_login'], errors='ignore')
        df_unified = df_unified.merge(eff_core, left_on='uuid', right_on='canonical_id', how='left').drop(columns=['canonical_id'])
    
    # Join Social
    if not df_soc.empty and 'canonical_id' in df_soc.columns:
        soc_cols = [c for c in ['canonical_id', 'hybrid_score', 'pagerank', 'threads_started', 'replies_sent', 'ml_threads', 'delving_threads', 'ml_responses', 'delving_responses', 'first_active', 'last_active', 'dev_type'] if c in df_soc.columns]
        df_soc_filtered = df_soc[soc_cols]
        df_unified = df_unified.merge(df_soc_filtered, left_on='uuid', right_on='canonical_id', how='left', suffixes=('', '_soc')).drop(columns=['canonical_id'])
    
    # Fill defaults
    df_unified = df_unified.fillna({
        'total_commits': 0, 'authored_commits': 0, 'merge_commits': 0,
        'bips_authored': 0, 'hybrid_score': 0, 'threads_started': 0,
        'replies_sent': 0, 'ml_threads': 0, 'delving_threads': 0,
        'ml_responses': 0, 'delving_responses': 0
    })
    
    # Global Timeline
    for col in ['first_active', 'last_active', 'first_seen', 'last_seen', 'first_commit', 'last_commit']:
        if col in df_unified.columns:
            df_unified[col] = pd.to_datetime(df_unified[col], errors='coerce', utc=True).dt.tz_localize(None)

    # Exclude first_seen/last_seen: those are set to today's build date in registry.py
    # and would incorrectly override real last_commit / last_active values.
    first_cols = [c for c in ['first_commit', 'first_active'] if c in df_unified.columns]
    last_cols = [c for c in ['last_commit', 'last_active'] if c in df_unified.columns]
    df_unified['global_first_active'] = df_unified[first_cols].min(axis=1) if first_cols else pd.NaT
    df_unified['global_last_active'] = df_unified[last_cols].max(axis=1) if last_cols else pd.NaT

    # --- GitHub Profile Enrichment ---
    # Join location, company, bio, twitter, blog, followers from the scraped GitHub profiles.
    # This covers all contributors with a known github_login_final (both commit-only and social-only devs).
    # Source: metadata/github_profiles.json (populated by scripts/enrichment/fetch_github_profiles.py)
    GITHUB_PROFILES_FILE = "metadata/github_profiles.json"
    if os.path.exists(GITHUB_PROFILES_FILE):
        print("Enriching with GitHub profile data (location, company, bio, twitter)...")
        with open(GITHUB_PROFILES_FILE) as f:
            gh_prof_data = json.load(f)
        # Keyed by numeric GitHub ID, each value has a 'login' field
        gh_login_map = {
            p['login']: p
            for p in gh_prof_data.get('profiles', {}).values()
            if p.get('login')
        }
        PROFILE_FIELDS = [
            ('location',         'github_location'),
            ('company',          'github_company'),
            ('bio',              'github_bio'),
            ('twitter_username', 'github_twitter'),
            ('blog',             'github_blog'),
            ('followers',        'github_followers'),
        ]
        for src_field, col_name in PROFILE_FIELDS:
            df_unified[col_name] = df_unified['github_login_final'].apply(
                lambda login, f=src_field: gh_login_map[login].get(f) if (login and login in gh_login_map) else None
            )
        enriched_count = df_unified['github_location'].notna().sum()
        print(f"  Profile enrichment: {enriched_count} / {len(df_unified)} contributors with location data")
    else:
        print(f"Warning: {GITHUB_PROFILES_FILE} not found — skipping profile enrichment.")
        for _, col_name in [('location', 'github_location'), ('company', 'github_company'),
                            ('bio', 'github_bio'), ('twitter_username', 'github_twitter'),
                            ('blog', 'github_blog'), ('followers', 'github_followers')]:
            df_unified[col_name] = None

    os.makedirs(os.path.dirname(OUTPUT_PARQUET), exist_ok=True)
    df_unified.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"Grand Join complete. {len(df_unified)} contributors unified in {OUTPUT_PARQUET}")

if __name__ == "__main__":
    unify()
