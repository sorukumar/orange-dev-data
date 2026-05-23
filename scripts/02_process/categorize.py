#!/usr/bin/env python3
"""
Categorize Threads in Bitcoin Social Data

This script analyzes threads in the combined social data to:
- Identify BIP references (e.g., BIP 141, BIP-340)
- Assign rich, multi-label categories that tell the story of Bitcoin's
  technical evolution from 2011 to the present.

Categories are designed so that someone browsing by category can trace
Bitcoin's history: from early P2SH and payment-protocol debates, through
the block-size wars, SegWit activation, Schnorr/Taproot, the Ordinals
controversy, covenant proposals, quantum resistance, and beyond.

It processes threads by aggregating content, applying keyword/regex
matching with priority scoring, and updates the dataset with new fields:
  - bip_refs        (list[str])  – BIP numbers referenced
  - category        (str)        – single best-fit category
  - categories      (list[str])  – all matching categories
  - category_conf   (float)      – confidence score 0-1 for primary

Usage:
    python scripts/analysis/categorize_threads.py
"""

import pandas as pd
import re
import json
import os
import sys

# Add project root to path so we can import from scripts.utils
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

# ── paths ────────────────────────────────────────────────────────────
INPUT_PARQUET = "data/raw/social_combined.parquet"
OUTPUT_PARQUET = "data/enriched/social_threads.parquet"

# =====================================================================
# CORE FUNCTIONS
# =====================================================================

from scripts.utils.subsystem import score_with_details, get_subsystems

def extract_bips(text: str) -> list[str]:
    """Extract BIP references from text using regex.

    Handles: BIP 141, BIP-141, BIP141, bip0141, BIP #141
    Returns de-duplicated list of BIP number strings (no leading zeros).
    """
    if not text:
        return []
    pattern = r'\bBIP[\s\-#]*0*(\d{1,4})\b'
    matches = re.findall(pattern, text, re.IGNORECASE)
    return sorted(set(matches), key=lambda x: int(x))


def categorize_thread(text: str, bip_refs: list[str]
                      ) -> tuple[str, list[str], float]:
    """Return (primary_category, all_categories, confidence).
    Uses the unified subsystem resolver.
    """
    return score_with_details(text, bip_refs)


# =====================================================================
# MAIN
# =====================================================================

def main():
    print("=" * 60)
    print("  Categorize Threads – Bitcoin Social Data")
    print("=" * 60)

    if not os.path.exists(INPUT_PARQUET):
        print(f"ERROR: Input file {INPUT_PARQUET} not found.")
        return

    print(f"\nLoading {INPUT_PARQUET} ...")
    df = pd.read_parquet(INPUT_PARQUET)
    print(f"  {len(df):,} messages loaded.")

    # ── Build thread-level aggregated text ───────────────────────────
    thread_groups = df.groupby("thread_id")
    n_threads = len(thread_groups)
    print(f"  {n_threads:,} unique threads.\n")

    print("Categorizing threads ...")

    # Pre-build thread text + compute results
    thread_results: dict = {}   # thread_id → (bips, primary, all_cats, conf)
    done = 0

    for thread_id, group in thread_groups:
        texts = []
        for _, row in group.iterrows():
            subj = str(row.get("subject") or "")
            body = str(row.get("body_snippet") or "")
            texts.append(subj)
            texts.append(body)

        combined = " ".join(texts)

        bips = extract_bips(combined)
        primary, all_cats, conf = categorize_thread(combined, bips)
        thread_results[thread_id] = (bips, primary, all_cats, conf)

        done += 1
        if done % 2000 == 0 or done == n_threads:
            print(f"  {done:>6,} / {n_threads:,} threads processed")

    # ── Map results back to every message row ────────────────────────
    print("\nMapping results to messages ...")
    bip_col = []
    cat_col = []
    cats_col = []
    conf_col = []

    for _, row in df.iterrows():
        tid = row["thread_id"]
        bips, primary, all_cats, conf = thread_results.get(
            tid, ([], "other", ["other"], 0.0)
        )
        bip_col.append(bips)
        cat_col.append(primary)
        cats_col.append(all_cats)
        conf_col.append(conf)

    df["bip_refs"] = bip_col
    df["category"] = cat_col
    df["categories"] = cats_col
    df["category_conf"] = conf_col

    # ── Save ─────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(OUTPUT_PARQUET) or ".", exist_ok=True)
    df.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"\nSaved → {OUTPUT_PARQUET}")

    # ── Summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  CATEGORIZATION SUMMARY")
    print("=" * 60)

    # Unique thread-level stats
    thread_df = pd.DataFrame([
        {"thread_id": tid, "bip_refs": r[0], "category": r[1],
         "categories": r[2], "category_conf": r[3]}
        for tid, r in thread_results.items()
    ])

    print(f"\n  Total threads:          {len(thread_df):,}")
    has_bips = (thread_df["bip_refs"].apply(len) > 0).sum()
    print(f"  Threads with BIP refs:  {has_bips:,}  "
          f"({100*has_bips/len(thread_df):.1f}%)")

    other_count = (thread_df["category"] == "other").sum()
    print(f"  Categorized (non-other):{len(thread_df) - other_count:,}  "
          f"({100*(len(thread_df)-other_count)/len(thread_df):.1f}%)")
    print(f"  Uncategorized (other):  {other_count:,}  "
          f"({100*other_count/len(thread_df):.1f}%)")

    print(f"\n  {'Category':<28s} {'Threads':>8s}  {'%':>6s}")
    print("  " + "-" * 46)
    cat_counts = thread_df["category"].value_counts()
    subsystems = get_subsystems()
    for cat, count in cat_counts.items():
        pct = 100 * count / len(thread_df)
        desc = subsystems.get(cat, {}).get("description", "")[:40]
        print(f"  {cat:<28s} {count:>8,}  {pct:>5.1f}%")

    # Multi-label stats
    multi = (thread_df["categories"].apply(len) > 1).sum()
    print(f"\n  Threads with 2+ categories: {multi:,}")

    print("\nDone.")

    # ── Shared enriched exports for network profile shards ───────────────────
    # These two files are consumed by scripts/04_deliver/ui_artifacts.py and
    # must be recomputed here since social_threads.parquet is the source.

    # 1. First & last message bookmarks per contributor
    # We want the earliest and latest message (by date) for each canonical_id
    # that has a real link (excludes null/empty links).
    print("\nComputing contributor message bookmarks...")
    df_linked = df[df['link'].notna() & (df['link'] != '')]
    df_linked = df_linked[df_linked['canonical_id'].notna() & (df_linked['canonical_id'] != '')]

    bookmarks: dict = {}
    if not df_linked.empty:
        df_linked = df_linked.copy()
        df_linked['date'] = pd.to_datetime(df_linked['date'], errors='coerce')
        df_valid = df_linked.dropna(subset=['date'])

        # First message per contributor
        idx_first = df_valid.groupby('canonical_id')['date'].idxmin()
        df_first = df_valid.loc[idx_first][['canonical_id', 'source', 'subject', 'date', 'link']].copy()

        # Last message per contributor
        idx_last = df_valid.groupby('canonical_id')['date'].idxmax()
        df_last  = df_valid.loc[idx_last][['canonical_id', 'source', 'subject', 'date', 'link']].copy()

        for row in df_first.itertuples(index=False):
            cid = str(row.canonical_id)
            bookmarks.setdefault(cid, {})['first_message'] = {
                'source': str(row.source),
                'subject': str(row.subject or ''),
                'date': row.date.date().isoformat(),
                'link': str(row.link),
            }
        for row in df_last.itertuples(index=False):
            cid = str(row.canonical_id)
            bookmarks.setdefault(cid, {})['last_message'] = {
                'source': str(row.source),
                'subject': str(row.subject or ''),
                'date': row.date.date().isoformat(),
                'link': str(row.link),
            }

    BOOKMARKS_PATH = "data/enriched/contributor_message_bookmarks.json"
    with open(BOOKMARKS_PATH, 'w') as f:
        json.dump(bookmarks, f, indent=2)
    print(f"  Saved → {BOOKMARKS_PATH} ({len(bookmarks)} contributors)")

    # 2. Social history by topic per contributor (year → topic → count)
    # Explode multi-label categories, then count by (canonical_id, year, topic).
    # We keep each contributor's top-8 topics by total volume; everything else
    # rolls into an "other" bucket.
    print("Computing contributor social history by topic...")
    df_social = df[df['canonical_id'].notna() & (df['canonical_id'] != '')].copy()
    df_social['date'] = pd.to_datetime(df_social['date'], errors='coerce')
    df_social = df_social.dropna(subset=['date'])
    df_social['year'] = df_social['date'].dt.year.astype(int)

    # Explode list column (categories) into one row per tag
    df_exploded = df_social.explode('categories')
    df_exploded = df_exploded[df_exploded['categories'].notna() & (df_exploded['categories'] != '')]

    # Count (canonical_id, year, topic)
    counts = df_exploded.groupby(['canonical_id', 'year', 'categories']).size().reset_index(name='count')

    # Build per-contributor top-8 topic set
    topic_totals = counts.groupby(['canonical_id', 'categories'])['count'].sum().reset_index()
    top_topics: dict = {}
    for cid, grp in topic_totals.groupby('canonical_id'):
        top8 = grp.nlargest(8, 'count')['categories'].tolist()
        top_topics[str(cid)] = set(top8)

    social_history: dict = {}
    for row in counts.itertuples(index=False):
        cid = str(row.canonical_id)
        year = str(row.year)
        topic = row.categories if row.categories in top_topics.get(cid, set()) else 'other'
        social_history.setdefault(cid, {}).setdefault(year, {})
        social_history[cid][year][topic] = social_history[cid][year].get(topic, 0) + row.count

    SOCIAL_HISTORY_PATH = "data/enriched/contributor_social_history.json"
    with open(SOCIAL_HISTORY_PATH, 'w') as f:
        json.dump(social_history, f, indent=2)
    print(f"  Saved → {SOCIAL_HISTORY_PATH} ({len(social_history)} contributors)")


if __name__ == "__main__":
    main()