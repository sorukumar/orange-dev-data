import pandas as pd
df = pd.read_parquet("data/enriched/contributors_unified.parquet")
for cid in ["can_steven_roose", "can_brandon_black"]:
    row = df[df["uuid"] == cid]
    if not row.empty:
        r = row.iloc[0]
        print(f"{cid}: first_commit={r.get('first_commit')} last_commit={r.get('last_commit')} first_active={r.get('first_active')} last_active={r.get('last_active')} global_first={r.get('global_first_active')} global_last={r.get('global_last_active')} bips={r.get('bips_authored')} bips_modern={r.get('modern_bips_authored')} reviews={r.get('reviews_count')}")
