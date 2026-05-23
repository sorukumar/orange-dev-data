"""
restamp_delving_ids.py — Phase 2 identity re-stamp for Delving Bitcoin data.

Run AFTER build_identities.py and BEFORE merge_social.py.

Problem:
  delving.py (Phase 1) writes canonical_id into social_delving.parquet
  using whatever identities.json existed from the PREVIOUS pipeline run.
  build_identities.py (Phase 2) then rebuilds identities.json, potentially
  merging identities and changing UUIDs. The raw parquet is now stale.

Fix:
  Re-resolve every delving row using the Discourse username (author_username)
  against the freshly written identities.json. Overwrite the canonical_id
  column in social_delving.parquet so merge_social.py and all downstream
  scripts see correct Phase 2 UUIDs.

  author_username is used (not author_name) because the identity map is keyed
  on delving:{username} — the display name does not match these keys.
"""

import os
import sys
import pandas as pd

sys.path.append(os.getcwd())
from scripts.utils.identity import resolver

DELVING_PATH = "data/raw/social_delving.parquet"


def restamp():
    if not os.path.exists(DELVING_PATH):
        print(f"restamp_delving_ids: {DELVING_PATH} not found, skipping.")
        return

    print("Re-stamping canonical_ids in social_delving.parquet ...")
    df = pd.read_parquet(DELVING_PATH)
    n_before = df["canonical_id"].nunique()

    df["canonical_id"] = df.apply(
        lambda r: resolver.resolve_delving(
            str(r.get("author_username") or r.get("author_name") or "")
        ),
        axis=1,
    )

    n_after = df["canonical_id"].nunique()
    df.to_parquet(DELVING_PATH, index=False)

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
