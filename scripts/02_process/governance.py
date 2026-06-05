import pandas as pd
import numpy as np
import re
import os
import json
from datetime import datetime

# --- Configuration ---
BIPS_PATH = "data/raw/bips.parquet"
SOCIAL_PATH = "data/raw/social_combined.parquet"
COMMITS_PATH = "data/raw/core_commits.parquet"
COMMIT_MSGS_PATH = "data/raw/core_messages.parquet"
OUTPUT_BIPS_ENRICHED = "data/enriched/bips_refined.parquet"
OUTPUT_THEMES_JSON = "data/enriched/bips_themes.json"
EXPERTISE_DOMAINS_PATH = "metadata/expertise_domains.json"

def _load_themes_from_domains() -> dict:
    """Build the BIP theme → keyword pattern dict from expertise_domains.json.

    This is the single source of truth for both:
      - the theme name string (the bip_themes[0] value)
      - the keyword patterns used to assign that theme to a BIP

    governance.py assigns theme strings; influence.py maps them to domain IDs.
    Both use expertise_domains.json so the theme names can never desync.
    """
    if not os.path.exists(EXPERTISE_DOMAINS_PATH):
        raise FileNotFoundError(
            f"{EXPERTISE_DOMAINS_PATH} not found. "
            "This file is required for BIP theme assignment."
        )
    with open(EXPERTISE_DOMAINS_PATH) as f:
        domains = json.load(f).get("domains", [])

    themes = {}
    for d in domains:
        bip_themes = d.get("bip_themes", [])
        keywords = d.get("bip_keywords", [])
        if bip_themes and keywords:
            # Use the first bip_theme string as the canonical theme name.
            themes[bip_themes[0]] = keywords

    return themes  # {"Consensus & Soft Forks": ["segwit", ...], ...}


# THEMES is now derived — do not edit patterns here.
# Edit bip_keywords in metadata/expertise_domains.json instead.
THEMES = _load_themes_from_domains()

def categorize(text):
    if not text: return "Other"
    text = text.lower()
    for theme, patterns in THEMES.items():
        for p in patterns:
            if re.search(p, text):
                return theme
    return "Other"

def main():
    print("--- Stage 3: Forensic Enrichment Starting ---")
    
    # 1. Load Datasets
    print("Loading datasets...")
    df_bips = pd.read_parquet(BIPS_PATH)
    df_social = pd.read_parquet(SOCIAL_PATH)
    
    # Ensure date columns are datetime
    df_social['date'] = pd.to_datetime(df_social['date'], utc=True)
    if 'git_created_at' in df_bips.columns:
        df_bips['git_created_at'] = pd.to_datetime(df_bips['git_created_at'], utc=True)

    # 2. Map BIPs to Themes
    print("Categorizing BIPs and Social threads into themes...")
    df_bips['theme'] = df_bips.apply(lambda row: categorize(f"{row['title']} {row['layer']}"), axis=1)
    df_social['theme'] = df_social['subject'].apply(categorize)

    # 3. Link BIPs to Social Discussion (Count mentions)
    print("Linking BIPs to social discussions...")
    # This is an O(N*M) search, but with 200 BIPs and 25k emails it's manageable (5M checks)
    bip_mentions = {}
    
    # Pre-compile regex for speed
    bip_patterns = {}
    for bip_id in df_bips['bip_id'].unique():
        # Look for "BIP 123" or "BIP123" or "bip-123"
        # Pad with leading zeros if numeric to handle "BIP 0123" vs "BIP 123"
        if bip_id.isdigit():
            padded = bip_id.zfill(4)
            short = str(int(bip_id))
            pattern = re.compile(rf"\bBIP[- ]?({short}|{padded})\b", re.IGNORECASE)
        else:
            pattern = re.compile(rf"\b{re.escape(bip_id)}\b", re.IGNORECASE)
        bip_patterns[bip_id] = pattern

    # Count mentions in social data
    social_subjects = df_social['subject'].fillna("").tolist()
    
    mentions_count = []
    for bip_id in df_bips['bip_id']:
        pattern = bip_patterns[bip_id]
        count = sum(1 for s in social_subjects if pattern.search(s))
        mentions_count.append(count)
    
    df_bips['social_mention_count'] = mentions_count

    # 4. Link BIPs to Code (Commits)
    # We look for "BIP 123" in commit messages
    print("Linking BIPs to repository commits...")
    if os.path.exists(COMMIT_MSGS_PATH):
        df_msgs = pd.read_parquet(COMMIT_MSGS_PATH)
        df_msgs['subject_body'] = (df_msgs['subject'].fillna("") + " " + df_msgs['body'].fillna("")).str.lower()
        
        code_mentions = []
        for bip_id in df_bips['bip_id']:
            pattern = bip_patterns[bip_id]
            # Simple count of commits mentioning this BIP
            count = sum(1 for msg in df_msgs['subject_body'] if pattern.search(msg))
            code_mentions.append(count)
        df_bips['code_mention_count'] = code_mentions
    else:
        df_bips['code_mention_count'] = 0

    # 5. Calculate "Maturity Score" (Shadow Activity + Revision complexity)
    # Higher score = more discussion and more code churn
    print("Calculating Maturity Scores...")
    # Normalize revision_count and social_mentions
    rev_max = df_bips['revision_count'].max() or 1
    soc_max = df_bips['social_mention_count'].max() or 1
    
    df_bips['maturity_score'] = (
        (df_bips['revision_count'] / rev_max * 0.4) + 
        (df_bips['social_mention_count'] / soc_max * 0.6)
    ).round(2)

    # 6. Save Enriched Artifacts
    print(f"Saving enriched BIPs to {OUTPUT_BIPS_ENRICHED}...")
    df_bips.to_parquet(OUTPUT_BIPS_ENRICHED, index=False)
    
    # Save Theme Stats for UI
    theme_stats = df_bips['theme'].value_counts().to_dict()
    with open(OUTPUT_THEMES_JSON, 'w') as f:
        json.dump({
            "bip_themes": theme_stats,
            "social_themes": df_social['theme'].value_counts().to_dict(),
            "last_updated": datetime.now().isoformat()
        }, f, indent=2)

    # 7. Write bips_ui.json for registry.py / ecosystem_summary.py / governance dashboard
    #    This file was previously generated by orange-dev-tracker; it now lives here
    #    so the entire pipeline stays within orange-dev-data.
    OUTPUT_BIPS_UI = "output/tracker/bips_ui.json"
    os.makedirs(os.path.dirname(OUTPUT_BIPS_UI), exist_ok=True)

    def _flatten_authors(val):
        if isinstance(val, (list, np.ndarray)):
            return ", ".join(str(x) for x in val if x)
        return str(val) if val else ""

    df_ui = df_bips[[
        'bip_id', 'title', 'status', 'theme', 'maturity_score',
        'social_mention_count', 'revision_count', 'code_mention_count',
        'author_names',
    ]].sort_values('maturity_score', ascending=False).copy()
    df_ui['authors'] = df_ui['author_names'].apply(_flatten_authors)
    df_ui = df_ui.drop(columns=['author_names'])

    with open(OUTPUT_BIPS_UI, 'w') as f:
        json.dump(df_ui.to_dict(orient='records'), f, indent=2)
    print(f"Wrote {len(df_ui)} BIP records to {OUTPUT_BIPS_UI}")

    print("\n--- Enrichment Complete ---")
    print(f"BIPs with code links: {len(df_bips[df_bips['code_mention_count'] > 0])}")
    print(f"BIPs with social links: {len(df_bips[df_bips['social_mention_count'] > 0])}")
    
    print("\nTop BIPs by Maturity Score:")
    print(df_bips.sort_values('maturity_score', ascending=False)[['bip_id', 'title', 'theme', 'maturity_score']].head(10))

    # 8. Write per-contributor BIP index for network profile shards.
    #    Keyed by canonical_id → list of {number, title, status, theme, link}.
    OUTPUT_CONTRIBUTOR_BIPS = "data/enriched/contributor_bips.json"
    contributor_bips: dict = {}
    for _, row in df_bips.iterrows():
        raw_ids = row.get('author_canonical_ids')
        if raw_ids is None:
            canonical_ids = []
        else:
            canonical_ids = list(raw_ids) if hasattr(raw_ids, '__iter__') and not isinstance(raw_ids, str) else [raw_ids]
        try:
            bip_num = int(row['bip_id'])
        except (ValueError, TypeError):
            bip_num = None
        bip_link = (
            f"https://github.com/bitcoin/bips/blob/master/bip-{bip_num:04d}.mediawiki"
            if bip_num is not None else None
        )
        bip_entry = {
            "number": bip_num,
            "title": str(row.get('title') or ''),
            "status": str(row.get('status') or ''),
            "theme": str(row.get('theme') or ''),
            "link": bip_link,
        }
        for cid in canonical_ids:
            cid = str(cid)
            if not cid or cid in ('nan', 'None'):
                continue
            contributor_bips.setdefault(cid, []).append(bip_entry)

    # Sort each contributor's BIPs by number
    for cid in contributor_bips:
        contributor_bips[cid].sort(key=lambda b: b['number'] or 9999)

    os.makedirs(os.path.dirname(OUTPUT_CONTRIBUTOR_BIPS) or '.', exist_ok=True)
    with open(OUTPUT_CONTRIBUTOR_BIPS, 'w') as f:
        json.dump(contributor_bips, f, indent=2)
    print(f"Exported per-contributor BIP index to {OUTPUT_CONTRIBUTOR_BIPS} ({len(contributor_bips)} contributors)")

if __name__ == "__main__":
    main()
