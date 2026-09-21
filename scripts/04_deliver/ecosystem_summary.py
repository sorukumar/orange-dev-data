import json
import os
import sys
from datetime import datetime, timedelta, timezone

try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# --- Configuration ---
REGISTRY_INPUT = os.path.join(ROOT_DIR, "output", "shared", "contributors", "registry_index.json")
BIPS_INPUT = os.path.join(ROOT_DIR, "output", "tracker", "bips_ui.json")
SOCIAL_THREADS_INPUT = os.path.join(ROOT_DIR, "data", "enriched", "social_threads.parquet")
CONTRIBUTORS_UNIFIED_INPUT = os.path.join(ROOT_DIR, "data", "enriched", "contributors_unified.parquet")
ENRICHED_PRS_INPUT = os.path.join(ROOT_DIR, "data", "enriched", "enriched_prs.parquet")
PR_METADATA_INPUT = os.path.join(ROOT_DIR, "data", "raw", "github_pr_metadata.parquet")
COMMITS_INPUT = os.path.join(ROOT_DIR, "data", "enriched", "commits_resolved.parquet")
OUTPUT_FILE = os.path.join(ROOT_DIR, "output", "shared", "ecosystem_summary.json")
SUBSYSTEMS_INPUT = os.path.join(ROOT_DIR, "metadata", "subsystems.json")
IDENTITIES_INPUT = os.path.join(ROOT_DIR, "metadata", "identities.json")

def _load_category_labels() -> dict:
    """Load human-readable labels from subsystems.json (single source of truth).
    Falls back to the slug itself if the file is missing.
    """
    if not os.path.exists(SUBSYSTEMS_INPUT):
        return {}
    with open(SUBSYSTEMS_INPUT, 'r', encoding='utf-8') as f:
        subsystems = json.load(f)
    return {slug: data.get("name", slug) for slug, data in subsystems.items()}

# Populated at module load; keyed by subsystem slug → human-readable label.
CATEGORY_LABELS = _load_category_labels()

def generate_ecosystem_summary():
    print("Generating Ecosystem Summary (Pure Python)...")
    if not os.path.exists(REGISTRY_INPUT):
        print(f"Error: {REGISTRY_INPUT} not found.")
        return
        
    with open(REGISTRY_INPUT, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    contributors = data['contributors']
    
    # 1. Primary technical estates
    # We define 4 sets for a clean 4-oval Euler diagram
    # Set A: Code (Committers)
    # Set B: Review (Reviewers / Participants)
    # Set C: Research (Mailing List + Delving)
    # Set D: Standards (BIP Authors)
    
    venn_4 = {} # Keyed by "A B C D" binary string "0101"
    
    groups = {
        "committers": 0, "reviewers": 0, "research": 0, "standards": 0,
        "total_active": 0, "total_registry": len(contributors)
    }
    
    active_uuids = set()
    focus_counts = {}
    # Enforce T-1 boundary for all time windows to prevent partial day data
    now = datetime.now() - timedelta(days=1)
    cutoff_90 = now - timedelta(days=90)
    # Focus labels to skip — unmapped, generic, or social-topic labels that pollute the chart
    _SKIP_FOCUS = {'None', 'none', 'code', 'other', 'Other', ''}
    
    # Pre-initialize 16 regions (0000 is excluded/ignored)
    for i in range(1, 16):
        venn_4[format(i, '04b')] = 0

    for c in contributors:
        uid = c.get('uuid') or c.get('id')
        
        has_code = (c.get('authored_commits') or 0) > 0 or (c.get('merge_commits') or 0) > 0
        has_review = (c.get('reviews_count') or 0) > 0 or (c.get('prs_authored') or 0) > 0
        has_research = (c.get('ml_responses') or 0) > 0 or (c.get('ml_threads') or 0) > 0 or (c.get('delving_responses') or 0) > 0 or (c.get('delving_threads') or 0) > 0
        has_standards = (c.get('bips_authored') or 0) > 0
        
        # Binary state: [Code, Review, Research, Standards]
        binary_id = f"{1 if has_code else 0}{1 if has_review else 0}{1 if has_research else 0}{1 if has_standards else 0}"
        
        if binary_id != "0000":
            venn_4[binary_id] += 1
            active_uuids.add(uid)
            
            if has_code: groups["committers"] += 1
            if has_review: groups["reviewers"] += 1
            if has_research: groups["research"] += 1
            if has_standards: groups["standards"] += 1
            
            # Subsystem Focus — skip unmapped and generic labels
            focus = c.get('technical_focus') or ''
            if focus and focus not in _SKIP_FOCUS:
                focus_counts[focus] = focus_counts.get(focus, 0) + 1
    
    groups["total_active"] = len(active_uuids)

    # Simplified Venn summary for landing page storytelling
    venn_summary = {
        "code_only": venn_4["1000"],
        "review_only": venn_4["0100"],
        "research_only": venn_4["0010"],
        "standards_only": venn_4["0001"],
        "all_four": venn_4["1111"],
        "code_review": venn_4["1100"],
        "review_research": venn_4["0110"],
        "research_standards": venn_4["0011"]
    }

    # Calculate R&D Focus percentages
    total_focus = sum(focus_counts.values())
    focus_pct = {k: round((v / total_focus * 100), 1) for k, v in focus_counts.items()} if total_focus > 0 else {}
    
    # Deriving Protocol Hotspot — top 3 BIPs by social mention count
    top_bips = []
    if os.path.exists(BIPS_INPUT):
        try:
            with open(BIPS_INPUT, 'r') as f:
                bips_data = json.load(f)
                bips_sorted = sorted(bips_data, key=lambda x: x.get('social_mention_count', 0), reverse=True)
                for b in bips_sorted[:3]:
                    top_bips.append({
                        "bip_id": b['bip_id'],
                        "title": b.get('title', ''),
                        "mentions": b.get('social_mention_count', 0),
                        "theme": b.get('theme', '')
                    })
        except: pass

    new_coders_90d = 0
    new_discussants_90d = 0
    total_prs_merged = 0
    prs_merged_30d = 0
    prs_merged_prev_30d = 0
    total_bips = 0
    active_bips = 0

    try:
        import pandas as pd
        pr_source_path = PR_METADATA_INPUT if os.path.exists(PR_METADATA_INPUT) else ENRICHED_PRS_INPUT
        if os.path.exists(pr_source_path):
            pr_df = pd.read_parquet(pr_source_path)
            # Filter to Bitcoin Core PRs
            if 'repository_name' in pr_df.columns:
                pr_df = pr_df[pr_df['repository_name'] == 'bitcoin/bitcoin'].copy()
            pr_df_merged = pr_df[pr_df['merged_at'].notna()].copy()
            pr_df_merged['merged_at'] = pd.to_datetime(pr_df_merged['merged_at'], errors='coerce', utc=True)
            pr_df_merged = pr_df_merged[pr_df_merged['merged_at'].notna()]
            
            # Overall PRs
            total_prs_merged = len(pr_df_merged)

            cutoff_30 = pd.Timestamp(cutoff_90 + timedelta(days=60), tz='UTC')
            cutoff_60 = pd.Timestamp(cutoff_90 + timedelta(days=30), tz='UTC')
            cutoff_7 = pd.Timestamp(now - timedelta(days=7), tz='UTC')
            cutoff_14 = pd.Timestamp(now - timedelta(days=14), tz='UTC')
            cutoff_90_ts = pd.Timestamp(cutoff_90, tz='UTC')

            prs_merged_30d = int((pr_df_merged['merged_at'] >= cutoff_30).sum())
            prs_merged_prev_30d = int(((pr_df_merged['merged_at'] >= cutoff_60) & (pr_df_merged['merged_at'] < cutoff_30)).sum())
            prs_merged_7d = int((pr_df_merged['merged_at'] >= cutoff_7).sum())
            prs_merged_prev_7d = int(((pr_df_merged['merged_at'] >= cutoff_14) & (pr_df_merged['merged_at'] < cutoff_7)).sum())

            # First-ever merged PR per author (for real-time new coder tracking)
            first_prs = pr_df_merged.groupby('author')['merged_at'].min().reset_index()
            first_prs.columns = ['author', 'first_merged_at']
            new_coders_90d = int((first_prs['first_merged_at'] >= cutoff_90_ts).sum())
            new_coders_7d = int((first_prs['first_merged_at'] >= cutoff_7).sum())
    except Exception as e:
        print(f"  Warning: Could not compute PRs merged: {e}")

    commits_30d = 0
    commits_prev_30d = 0
    try:
        import pandas as pd
        if os.path.exists(COMMITS_INPUT):
            df_commits = pd.read_parquet(COMMITS_INPUT, columns=['date_utc'])
            df_commits['date_utc'] = pd.to_datetime(df_commits['date_utc'], utc=True)
            cutoff_30_ts = pd.Timestamp(cutoff_90 + timedelta(days=60), tz='UTC')
            cutoff_60_ts = pd.Timestamp(cutoff_90 + timedelta(days=30), tz='UTC')
            cutoff_7_ts = pd.Timestamp(now - timedelta(days=7), tz='UTC')
            cutoff_14_ts = pd.Timestamp(now - timedelta(days=14), tz='UTC')

            commits_30d = int((df_commits['date_utc'] >= cutoff_30_ts).sum())
            commits_prev_30d = int(((df_commits['date_utc'] >= cutoff_60_ts) & (df_commits['date_utc'] < cutoff_30_ts)).sum())
            commits_7d = int((df_commits['date_utc'] >= cutoff_7_ts).sum())
            commits_prev_7d = int(((df_commits['date_utc'] >= cutoff_14_ts) & (df_commits['date_utc'] < cutoff_7_ts)).sum())
    except Exception as e:
        print(f"  Warning: Could not compute commits_30d: {e}")

    try:
        if os.path.exists(BIPS_INPUT):
            with open(BIPS_INPUT, 'r', encoding='utf-8') as f:
                bips_data = json.load(f)
                total_bips = len(bips_data)
                active_bips = len([b for b in bips_data if b.get('social_mention_count', 0) > 0])
    except Exception as e:
        print(f"  Warning: Could not compute active BIPs: {e}")

    spotlight_data = None
    try:
        import pandas as pd
        
        # Load identities lookup map
        github_to_identity = {}
        if os.path.exists(IDENTITIES_INPUT):
            try:
                with open(IDENTITIES_INPUT, 'r', encoding='utf-8') as f:
                    for ident in json.load(f).get('identities', []):
                        gh = ident.get('platforms', {}).get('github', [])
                        if isinstance(gh, str): gh = [gh]
                        for g in gh:
                            github_to_identity[g.lower()] = ident
            except Exception as e:
                print(f"  Warning: Could not load identities: {e}")

        # Primary source for spotlight: real-time PR metadata
        if 'first_prs' in locals() and not first_prs.empty and not pr_df_merged.empty:
            cutoff_30_ts = pd.Timestamp(cutoff_90 + timedelta(days=60), tz='UTC')
            cutoff_90_ts = pd.Timestamp(cutoff_90, tz='UTC')
            
            # Check 30d window first, fallback to 90d, then latest first-time author
            newbies = first_prs[first_prs['first_merged_at'] >= cutoff_30_ts].sort_values('first_merged_at', ascending=False)
            if newbies.empty:
                newbies = first_prs[first_prs['first_merged_at'] >= cutoff_90_ts].sort_values('first_merged_at', ascending=False)
            if newbies.empty:
                newbies = first_prs.sort_values('first_merged_at', ascending=False)

            if not newbies.empty:
                top_author = str(newbies.iloc[0]['author'])
                author_prs = pr_df_merged[pr_df_merged['author'] == top_author].sort_values('merged_at', ascending=False)
                latest_pr = author_prs.iloc[0]
                
                ident = github_to_identity.get(top_author.lower(), {})
                display_name = ident.get('display_name') or top_author
                uuid_val = ident.get('uuid') or f'auto_{top_author}'
                pr_num = latest_pr.get('pr_number', '')
                pr_title = latest_pr.get('title', '')
                
                desc = f"First-time core contributor recently merged PR #{pr_num} ({pr_title})." if pr_num else "First-time core contributor."
                spotlight_data = {
                    "name": str(display_name),
                    "uuid": str(uuid_val),
                    "github_login": str(top_author),
                    "description": desc
                }
        elif os.path.exists(CONTRIBUTORS_UNIFIED_INPUT):
            _df_u = pd.read_parquet(CONTRIBUTORS_UNIFIED_INPUT, columns=['first_commit', 'first_core_commit', 'tier1_authored_commits', 'display_name', 'github_login_final', 'uuid'])
            _fc = pd.to_datetime(_df_u['first_commit'], errors='coerce', utc=True)
            _cutoff_ts = pd.Timestamp(cutoff_90, tz='UTC')
            _cutoff_7_ts = pd.Timestamp(now - timedelta(days=7), tz='UTC')
            if new_coders_90d == 0:
                new_coders_90d = int((_fc >= _cutoff_ts).sum())
                new_coders_7d = int((_fc >= _cutoff_7_ts).sum())
            
            _fc_core = pd.to_datetime(_df_u['first_core_commit'], errors='coerce', utc=True)
            _cutoff_30 = pd.Timestamp(cutoff_90 + timedelta(days=60), tz='UTC')
            newbies_30 = _df_u[_fc_core >= _cutoff_30].copy()
            if newbies_30.empty:
                newbies_30 = _df_u[_fc_core.notna()].sort_values('first_core_commit', ascending=False).head(1)
            if not newbies_30.empty:
                top_newbie = newbies_30.iloc[0]
                name = top_newbie.get('display_name') or top_newbie.get('github_login_final') or 'New Contributor'
                commits = int(top_newbie.get('tier1_authored_commits', 1))
                spotlight_data = {
                    "name": str(name),
                    "uuid": str(top_newbie.get('uuid', '')),
                    "github_login": str(top_newbie.get('github_login_final', '')),
                    "description": f"First-time core contributor recently merged {commits} commit{'s' if commits != 1 else ''}."
                }
    except Exception as e:
        print(f"  Warning: Could not compute new coders or spotlight: {e}")

    # Active Discussion Topics + new discussants — both derived from social_threads.parquet
    top_topics = []
    try:
        import pandas as pd
        if os.path.exists(SOCIAL_THREADS_INPUT):
            df_threads = pd.read_parquet(SOCIAL_THREADS_INPUT, columns=['date', 'category', 'canonical_id'])
            df_threads['date'] = pd.to_datetime(df_threads['date'])
            cutoff_ts_naive = pd.Timestamp(cutoff_90)
            cutoff_7_ts_naive = pd.Timestamp(now - timedelta(days=7))

            # New discussion voices: canonical IDs whose first message ever is within window
            first_msg = df_threads.groupby('canonical_id')['date'].min()
            new_discussants_90d = int((first_msg >= cutoff_ts_naive).sum())
            new_discussants_7d = int((first_msg >= cutoff_7_ts_naive).sum())

            # Top discussion topics in the window
            recent = df_threads[df_threads['date'] >= cutoff_ts_naive]
            recent = recent[~recent['category'].isin(['other', None])]
            topic_counts = recent['category'].value_counts()
            total_recent = len(recent)
            for cat, count in topic_counts.head(3).items():
                top_topics.append({
                    "category": cat,
                    "label": CATEGORY_LABELS.get(cat, cat.replace('-', ' ').title()),
                    "count": int(count),
                    "share": round(count / total_recent * 100, 1) if total_recent > 0 else 0
                })
    except Exception as e:
        print(f"  Warning: Could not compute discussion data: {e}")

    summary = {
        "generated_at": now.isoformat(),
        "groups": groups,
        "venn_4": venn_4,
        "venn_summary": venn_summary,
        "rd_focus": focus_pct,
        # Legacy single-value hotspot kept for backward compatibility
        "hotspot": {
            "title": "Trending BIP Discussions",
            "value": "BIP " + str(top_bips[0]['bip_id']) if top_bips else "BIP Audit"
        },
        "spotlight": spotlight_data,
        "onboarding": {
            "new_coders_90d": new_coders_90d,
            "new_coders_7d": locals().get('new_coders_7d', 0),
            "new_discussants_90d": new_discussants_90d,
            "new_discussants_7d": locals().get('new_discussants_7d', 0),
            "window_days": 90
        },
        "prs": {
            "total_merged": total_prs_merged,
            "merged_30d": prs_merged_30d,
            "merged_prev_30d": prs_merged_prev_30d,
            "merged_7d": locals().get('prs_merged_7d', 0),
            "merged_prev_7d": locals().get('prs_merged_prev_7d', 0)
        },
        "commits": {
            "commits_30d": commits_30d,
            "commits_prev_30d": commits_prev_30d,
            "commits_7d": locals().get('commits_7d', 0),
            "commits_prev_7d": locals().get('commits_prev_7d', 0)
        },
        "bips": {
            "total": total_bips,
            "active_recently": active_bips
        },
        # Richer discussion pulse: top topics + top BIPs
        "discussion_pulse": {
            "topics": top_topics,
            "top_bips": top_bips,
            "window_days": 90
        }
    }
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    
    print(f"Ecosystem summary saved to {OUTPUT_FILE}")
    print(f"Stats: Total Active={groups['total_active']}, New Coders={new_coders_90d}, New Discussants={new_discussants_90d}")

if __name__ == "__main__":
    generate_ecosystem_summary()
