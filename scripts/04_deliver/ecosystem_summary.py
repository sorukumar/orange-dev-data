import json
import os
from datetime import datetime, timedelta

# --- Configuration ---
REGISTRY_INPUT = "output/shared/contributors/registry_index.json"
BIPS_INPUT = "output/tracker/bips_ui.json"
SOCIAL_THREADS_INPUT = "data/enriched/social_threads.parquet"
CONTRIBUTORS_UNIFIED_INPUT = "data/enriched/contributors_unified.parquet"
OUTPUT_FILE = "output/shared/ecosystem_summary.json"
SUBSYSTEMS_INPUT = "metadata/subsystems.json"

def _load_category_labels() -> dict:
    """Load human-readable labels from subsystems.json (single source of truth).
    Falls back to the slug itself if the file is missing.
    """
    if not os.path.exists(SUBSYSTEMS_INPUT):
        return {}
    with open(SUBSYSTEMS_INPUT) as f:
        subsystems = json.load(f)
    return {slug: data.get("name", slug) for slug, data in subsystems.items()}

# Populated at module load; keyed by subsystem slug → human-readable label.
CATEGORY_LABELS = _load_category_labels()

def generate_ecosystem_summary():
    print("Generating Ecosystem Summary (Pure Python)...")
    if not os.path.exists(REGISTRY_INPUT):
        print(f"Error: {REGISTRY_INPUT} not found.")
        return
        
    with open(REGISTRY_INPUT, 'r') as f:
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
    now = datetime.now()
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

    # Onboarding — derived from source parquets so we're not blocked by registry rebuild
    # new_coders_90d  : distinct contributors whose first commit is within 90 days
    # new_discussants_90d : canonical IDs whose very first social message is within 90 days
    new_coders_90d = 0
    new_discussants_90d = 0
    try:
        import pandas as pd
        if os.path.exists(CONTRIBUTORS_UNIFIED_INPUT):
            _df_u = pd.read_parquet(CONTRIBUTORS_UNIFIED_INPUT, columns=['first_commit'])
            _fc = pd.to_datetime(_df_u['first_commit'], errors='coerce', utc=True)
            _cutoff_ts = pd.Timestamp(cutoff_90, tz='UTC')
            new_coders_90d = int((_fc >= _cutoff_ts).sum())
    except Exception as e:
        print(f"  Warning: Could not compute new coders: {e}")

    # Active Discussion Topics + new discussants — both derived from social_threads.parquet
    top_topics = []
    try:
        import pandas as pd
        if os.path.exists(SOCIAL_THREADS_INPUT):
            df_threads = pd.read_parquet(SOCIAL_THREADS_INPUT, columns=['date', 'category', 'canonical_id'])
            df_threads['date'] = pd.to_datetime(df_threads['date'])
            cutoff_ts_naive = pd.Timestamp(cutoff_90)

            # New discussion voices: canonical IDs whose first message ever is within window
            first_msg = df_threads.groupby('canonical_id')['date'].min()
            new_discussants_90d = int((first_msg >= cutoff_ts_naive).sum())

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
        "onboarding": {
            "new_coders_90d": new_coders_90d,
            "new_discussants_90d": new_discussants_90d,
            "window_days": 90
        },
        # Richer discussion pulse: top topics + top BIPs
        "discussion_pulse": {
            "topics": top_topics,
            "top_bips": top_bips,
            "window_days": 90
        }
    }
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"Ecosystem summary saved to {OUTPUT_FILE}")
    print(f"Stats: Total Active={groups['total_active']}, New Coders={new_coders_90d}, New Discussants={new_discussants_90d}")

if __name__ == "__main__":
    generate_ecosystem_summary()
