
import pandas as pd
import json
import os

enriched_df = pd.read_parquet("data/contributors_enriched.parquet")
cache_path = "data/enrichment_cache.json"
cache = {}
if os.path.exists(cache_path):
    with open(cache_path, "r") as f:
        cache = json.load(f)

total_contributors = len(enriched_df)
enriched_count = enriched_df[enriched_df['is_enriched'] == True].shape[0]
not_enriched_count = enriched_df[enriched_df['is_enriched'] == False].shape[0]

# Fresh data from cache (API lookups)
fresh_data_count = len(cache)

# Check missing locations for those not enriched
missing_location = enriched_df[enriched_df['location'].isna()].shape[0]
missing_login = enriched_df[enriched_df['login'].isna()].shape[0]

print(f"Total Contributors: {total_contributors}")
print(f"Enriched (Legacy + Fresh): {enriched_count}")
print(f"Not Enriched: {not_enriched_count}")
print(f"Freshly Enriched (via API Cache): {fresh_data_count}")
print(f"Remaining to Enrich (Candidates): {not_enriched_count}")

# Look at some of the not enriched to see if they have info
print("\nSample of Not Enriched:")
print(enriched_df[enriched_df['is_enriched'] == False][['name', 'login', 'location']].head(10))

# Feedback on pipeline:
# 1. Enrichment is limited by MAX_API_CALLS = 50 per run in enrich.py.
# 2. background_enricher.py runs every 15 minutes.
# 3. So it does 50 * 4 = 200 calls per hour if rate limit allows.
# 4. GitHub search API limit is usually 30/min for authenticated users.
# 5. The pipeline is quite pragmatic but could be optimized by:
#    - Increasing MAX_API_CALLS if rate limit is high.
#    - Avoiding full rebuild if only enrichment changed (or make rebuild incremental).
