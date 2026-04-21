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


if __name__ == "__main__":
    main()