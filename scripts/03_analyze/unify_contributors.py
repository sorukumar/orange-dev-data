import pandas as pd
import json
import os
import numpy as np
import sys

sys.path.append(os.getcwd())
import scripts.utils.identity 
from scripts.utils.identity import resolver

# --- Configuration ---
BADGES_JSON = "metadata/badges.json"
COMMITS_PARQUET = "data/enriched/commits_resolved.parquet"
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
    
    def github_final(g):
        if isinstance(g, list):
            return g[0] if len(g) else None
        return g

    # Base unified DataFrame from identities list
    base_rows = []
    for identity in identities_data:
        base_rows.append({
            'uuid': identity['uuid'],
            'display_name': identity['display_name'],
            'github_login_final': github_final(identity.get('platforms', {}).get('github')),
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

    if not df_commits.empty and 'hash' in df_commits.columns:
        # canonical_id is already provided in commits_resolved.parquet
        
        if 'is_merge' in df_commits.columns:
            df_commits['is_auth'] = (~df_commits['is_merge']).astype(int)
            df_commits['is_merg'] = df_commits['is_merge'].astype(int)
        else:
            df_commits['is_auth'] = 1
            df_commits['is_merg'] = 0
            
        # Drop duplicate hashes for a single contributor to prevent double-counting category-exploded rows
        tier1_repos = ['bitcoin/bitcoin', 'bitcoin-core/secp256k1', 'bitcoin-core/gui']
        tier2_repos = ['bitcoin-core/guix.sigs', 'bitcoin-core/qa-assets', 'bitcoin-core/HWI']
        
        df_commits_dedup = df_commits.drop_duplicates(subset=['canonical_id', 'hash']).assign(
            is_tier1=lambda d: d['repository_name'].isin(tier1_repos).astype(int),
            is_tier2=lambda d: d['repository_name'].isin(tier2_repos).astype(int),
        )
        df_commits_dedup = df_commits_dedup.assign(
            tier1_auth=lambda d: d['is_auth'] * d['is_tier1'],
            tier2_auth=lambda d: d['is_auth'] * d['is_tier2'],
        )

        if 'integration_date' in df_commits_dedup.columns:
            # GUARDRAIL: Use integration_date for normal PRs, but fall back to author date
            # if the gap exceeds 3 years (1095 days). This prevents administrative
            # repository migrations from reviving retired developers' timelines.
            _intg = pd.to_datetime(df_commits_dedup['integration_date'], utc=True, errors='coerce')
            _auth = pd.to_datetime(df_commits_dedup['date_utc'], utc=True, errors='coerce')
            _gap = (_intg - _auth).dt.days
            effective_date = _intg.where(_gap <= 1095, _auth)
            df_commits_dedup = df_commits_dedup.copy()
            df_commits_dedup['_effective_date'] = effective_date
            date_col = '_effective_date'
        else:
            date_col = 'date_utc'
        df_commits_dedup['core_date'] = df_commits_dedup[date_col].where(df_commits_dedup['is_tier1'] == 1)
        df_commits_dedup['ecosystem_date'] = df_commits_dedup[date_col].where(df_commits_dedup['is_tier2'] == 1)

        commit_stats = df_commits_dedup.groupby('canonical_id').agg(
            total_commits=('hash', 'count'),
            authored_commits=('is_auth', 'sum'),
            tier1_authored_commits=('tier1_auth', 'sum'),
            tier2_authored_commits=('tier2_auth', 'sum'),
            merge_commits=('is_merg', 'sum'),
            total_additions=('commit_total_adds', 'sum') if 'commit_total_adds' in df_commits_dedup.columns else ('additions', 'sum'),
            total_deletions=('commit_total_dels', 'sum') if 'commit_total_dels' in df_commits_dedup.columns else ('deletions', 'sum'),
            first_core_commit=('core_date', 'min'),
            last_core_commit=('core_date', 'max'),
            first_ecosystem_commit=('ecosystem_date', 'min'),
            last_ecosystem_commit=('ecosystem_date', 'max')
        ).reset_index()

        commit_stats['first_commit'] = commit_stats['first_core_commit'].combine_first(commit_stats['first_ecosystem_commit'])
        commit_stats['last_commit'] = commit_stats['last_core_commit'].combine_first(commit_stats['last_ecosystem_commit'])
        # Keep first/last core/ecosystem commits for downstream split timeline UI

        # Era-specific authored commit counts
        dates_utc = pd.to_datetime(df_commits_dedup['date_utc'], utc=True, errors='coerce')
        p2016_start = pd.Timestamp('2016-01-01', tz='UTC')
        modern_cutoff = dates_utc.max() - pd.DateOffset(years=3)
        mask_auth = df_commits_dedup['is_auth'] == 1
        p2016_auth = df_commits_dedup[mask_auth & (dates_utc >= p2016_start)].groupby('canonical_id')['hash'].nunique().rename('p2016_authored_commits').reset_index()
        modern_auth = df_commits_dedup[mask_auth & (dates_utc >= modern_cutoff)].groupby('canonical_id')['hash'].nunique().rename('modern_authored_commits').reset_index()
        commit_stats = commit_stats.merge(p2016_auth, on='canonical_id', how='left')
        commit_stats = commit_stats.merge(modern_auth, on='canonical_id', how='left')

        all_source_uuids.update(commit_stats['canonical_id'])
    else:
        commit_stats = pd.DataFrame(columns=['canonical_id', 'total_commits', 'authored_commits', 'tier1_authored_commits', 'tier2_authored_commits', 'merge_commits', 'total_additions', 'total_deletions', 'first_commit', 'last_commit', 'p2016_authored_commits', 'modern_authored_commits'])
    
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

    print("Aggregating badges.json metadata...")
    badges_data = {}
    if os.path.exists(BADGES_JSON):
        with open(BADGES_JSON, 'r') as f:
            badges_data = json.load(f)
            
    badges_rows = []
    for uuid, badge_info in badges_data.items():
        badges_rows.append({
            'uuid': uuid,
            'badges': badge_info,
            'roles': badge_info.get('roles', [])
        })
    df_reg_agg = pd.DataFrame(badges_rows)

    if not df_reg_agg.empty:
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
    p2016_bip_counts = {}
    modern_bip_counts = {}
    bip_first_dates = {}
    bip_last_dates = {}
    # Era cutoffs for BIP dates (timezone-naive, matching git_created_at dtype)
    bip_p2016_start = pd.Timestamp('2016-01-01')
    _bip_dates = pd.to_datetime(df_bips['git_created_at'], errors='coerce')
    if _bip_dates.dt.tz is not None:
        _bip_dates = _bip_dates.dt.tz_localize(None)
    bip_modern_start = _bip_dates.max() - pd.DateOffset(years=3)
    for _, brow in df_bips.iterrows():
        ids = brow['author_canonical_ids']
        if ids is None:
            continue
        if isinstance(ids, str):
            try: ids = json.loads(ids.replace("'", '"'))
            except: continue
        raw_ts = brow.get('git_created_at')
        bip_ts = pd.Timestamp(raw_ts).replace(tzinfo=None) if pd.notna(raw_ts) else None
        is_p2016 = bip_ts is not None and bip_ts >= bip_p2016_start
        is_modern = bip_ts is not None and bip_ts >= bip_modern_start
        for cid in ids:
            cid = str(cid).strip()
            if cid:
                bip_counts[cid] = bip_counts.get(cid, 0) + 1
                if is_p2016:
                    p2016_bip_counts[cid] = p2016_bip_counts.get(cid, 0) + 1
                if is_modern:
                    modern_bip_counts[cid] = modern_bip_counts.get(cid, 0) + 1
                if bip_ts is not None:
                    if cid not in bip_first_dates or bip_ts < bip_first_dates[cid]:
                        bip_first_dates[cid] = bip_ts
                    if cid not in bip_last_dates or bip_ts > bip_last_dates[cid]:
                        bip_last_dates[cid] = bip_ts
    df_bip_stats = pd.DataFrame(list(bip_counts.items()), columns=['uuid', 'bips_authored'])
    if p2016_bip_counts:
        df_bip_stats = df_bip_stats.merge(
            pd.DataFrame(list(p2016_bip_counts.items()), columns=['uuid', 'p2016_bips_authored']),
            on='uuid', how='left')
    if modern_bip_counts:
        df_bip_stats = df_bip_stats.merge(
            pd.DataFrame(list(modern_bip_counts.items()), columns=['uuid', 'modern_bips_authored']),
            on='uuid', how='left')
    if bip_first_dates:
        df_bip_stats = df_bip_stats.merge(
            pd.DataFrame(list(bip_first_dates.items()), columns=['uuid', 'first_bip_date']),
            on='uuid', how='left')
    if bip_last_dates:
        df_bip_stats = df_bip_stats.merge(
            pd.DataFrame(list(bip_last_dates.items()), columns=['uuid', 'last_bip_date']),
            on='uuid', how='left')
    df_unified = df_unified.merge(df_bip_stats, on='uuid', how='left')
    
    # Join Efficiency
    if os.path.exists(EFFICIENCY_PARQUET):
        df_eff = pd.read_parquet(EFFICIENCY_PARQUET)
        eff_core = df_eff.drop(columns=['github_login'], errors='ignore')
        df_unified = df_unified.merge(eff_core, left_on='uuid', right_on='canonical_id', how='left').drop(columns=['canonical_id'])
    
    # Join Social
    if not df_soc.empty and 'canonical_id' in df_soc.columns:
        soc_cols = [c for c in ['canonical_id', 'hybrid_score', 'p2016_hybrid_score', 'modern_hybrid_score', 'impact_score', 'p2016_impact_score', 'modern_impact_score', 'pagerank', 'threads_started', 'replies_sent', 'ml_threads', 'delving_threads', 'ml_responses', 'delving_responses', 'first_active', 'last_active', 'dev_type', 'expertise_domains', 'expertise_by_source', 'expertise_domain_scores', 'p2016_posts', 'modern_posts', 'p2016_ml_posts', 'p2016_delving_posts', 'modern_ml_posts', 'modern_delving_posts', 'is_engineer', 'is_reviewer', 'is_researcher', 'is_bip_author'] if c in df_soc.columns]
        df_soc_filtered = df_soc[soc_cols]
        df_unified = df_unified.merge(df_soc_filtered, left_on='uuid', right_on='canonical_id', how='left', suffixes=('', '_soc')).drop(columns=['canonical_id'])
    
    # Fill defaults (impact_score intentionally excluded — None means "Creator", 0 means unranked)
    df_unified = df_unified.fillna({
        'total_commits': 0, 'authored_commits': 0, 'tier1_authored_commits': 0, 'tier2_authored_commits': 0, 'merge_commits': 0,
        'p2016_authored_commits': 0, 'modern_authored_commits': 0,
        'bips_authored': 0, 'p2016_bips_authored': 0, 'modern_bips_authored': 0, 'hybrid_score': 0, 'threads_started': 0,
        'replies_sent': 0, 'ml_threads': 0, 'delving_threads': 0,
        'ml_responses': 0, 'delving_responses': 0,
        'p2016_posts': 0, 'modern_posts': 0,
        'p2016_ml_posts': 0, 'p2016_delving_posts': 0,
        'modern_ml_posts': 0, 'modern_delving_posts': 0,
        'reviews_count': 0, 'tier1_reviews_count': 0, 'tier2_reviews_count': 0,
        'prs_authored': 0, 'tier1_prs_authored': 0, 'tier2_prs_authored': 0,
        'is_engineer': False, 'is_reviewer': False, 'is_researcher': False, 'is_bip_author': False
    })
    
    # Global Timeline
    for col in ['first_active', 'last_active', 'first_commit', 'last_commit', 'first_review_date', 'last_review_date', 'first_bip_date', 'last_bip_date']:
        if col in df_unified.columns:
            df_unified[col] = pd.to_datetime(df_unified[col], errors='coerce', utc=True).dt.tz_localize(None)

    first_cols = [c for c in ['first_commit', 'first_active', 'first_review_date', 'first_bip_date'] if c in df_unified.columns]
    last_cols = [c for c in ['last_commit', 'last_active', 'last_review_date', 'last_bip_date'] if c in df_unified.columns]
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

    # Normalize expertise columns: pyarrow cannot serialize a column with mixed
    # list/NaN values; coerce NaN → empty list/dict so the type is consistent.
    if 'expertise_domains' in df_unified.columns:
        df_unified['expertise_domains'] = df_unified['expertise_domains'].apply(
            lambda x: x if isinstance(x, list) else []
        )
    if 'expertise_by_source' in df_unified.columns:
        df_unified['expertise_by_source'] = df_unified['expertise_by_source'].apply(
            lambda x: x if isinstance(x, dict) else {}
        )
    if 'expertise_domain_scores' in df_unified.columns:
        df_unified['expertise_domain_scores'] = df_unified['expertise_domain_scores'].apply(
            lambda x: x if isinstance(x, dict) else {}
        )

    # --- Activity Status Logic ---
    now = pd.Timestamp.utcnow().tz_localize(None)
    
    def calculate_status(row):
        m = row.get('modern_hybrid_score', 0)
        if pd.isna(m): m = 0
        p = row.get('p2016_hybrid_score', 0)
        if pd.isna(p): p = 0
        c = row.get('total_commits', 0)
        if pd.isna(c): c = 0
        
        last = row.get('global_last_active')
        first = row.get('global_first_active')
        
        years_inactive = (now - last).days / 365.25 if pd.notna(last) else 999
        days_since_first = (now - first).days if pd.notna(first) else 999
        
        is_historically_significant = (c > 5) or (p > 0.5)
        
        if is_historically_significant and years_inactive > 2.5:
            return "Retired"
            
        if days_since_first <= 365 and m > 0:
            return "New"
            
        tenure_years = max(days_since_first / 365.25, 3.0)
        p2016_annual_rate = p / tenure_years
        modern_annual_rate = m / 3.0
        
        growth = modern_annual_rate / p2016_annual_rate if p2016_annual_rate > 0 else (2 if m > 0 else 0)
        
        if growth >= 1.25: return "Rising"
        if growth >= 0.75 or m >= 0.35: return "Steady"
        if m > 0: return "Fading"
        return ""
        
    df_unified['activity_status'] = df_unified.apply(calculate_status, axis=1)

    # --- Bot Exclusion ---
    # Filter out known automation bots from the human developer dataset
    KNOWN_BOTS = {
        'auto_bitcoin_core_merge_script',
        'auto_github_actions_bot',
        'auto_drahtbot',
        'auto_dependabot',
        'auto_dependabot_bot',
        'auto_inclusive_coding_bot',
        'auto_pull_bot',
        'auto_github_project_automation_bot'
    }
    df_unified = df_unified[~df_unified['uuid'].isin(KNOWN_BOTS)]

    os.makedirs(os.path.dirname(OUTPUT_PARQUET), exist_ok=True)
    df_unified.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"Grand Join complete. {len(df_unified)} contributors unified in {OUTPUT_PARQUET}")

if __name__ == "__main__":
    unify()
