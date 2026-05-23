"""
restamp_social_ids.py — Phase 2 identity re-stamp for mailing list data.

Run AFTER build_identities.py and BEFORE merge_social.py.

Problem:
  mailing_list.py (Phase 1) writes canonical_id into social_mailing_list.parquet
  using whatever identities.json existed from the PREVIOUS pipeline run.
  build_identities.py (Phase 2) then rebuilds identities.json, potentially
  merging identities and changing UUIDs. The raw parquet is now stale.

Fix:
  Re-resolve every mailing list row using real sender emails (not the list
  delivery address) against the freshly written identities.json. Overwrite
  the canonical_id column in social_mailing_list.parquet so merge_social.py
  and all downstream scripts see correct Phase 2 UUIDs.
"""

import os
import sys
import pandas as pd

sys.path.append(os.getcwd())
from scripts.utils.identity import resolver

MAILING_LIST_PATH = "data/raw/social_mailing_list.parquet"


def restamp():
    if not os.path.exists(MAILING_LIST_PATH):
        print(f"restamp_social_ids: {MAILING_LIST_PATH} not found, skipping.")
        return

    print("Re-stamping canonical_ids in social_mailing_list.parquet ...")
    df = pd.read_parquet(MAILING_LIST_PATH)
    n_before = df["canonical_id"].nunique()

    df["canonical_id"] = df.apply(
        lambda r: resolver.resolve_git(
            str(r.get("author_name") or ""),
            str(r.get("author_email") or ""),
        ),
        axis=1,
    )

    n_after = df["canonical_id"].nunique()
    df.to_parquet(MAILING_LIST_PATH, index=False)

    print(
        f"  {len(df):,} rows re-stamped: "
        f"{n_before:,} unique IDs before → {n_after:,} after"
    )
    if n_after < n_before:
        print(
            f"  ✓ Collapsed {n_before - n_after} stale auto_ IDs "
            f"into canonical Phase 2 UUIDs."
        )


if __name__ == "__main__":
    restamp()
