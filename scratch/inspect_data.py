import pandas as pd
from datetime import datetime, timedelta

now = datetime.now()
cutoff90 = now - timedelta(days=90)

# 1. Unified parquet columns
df = pd.read_parquet('data/enriched/contributors_unified.parquet')
print('Unified parquet columns:', list(df.columns))
has_fc = df['first_commit'].notna().sum() if 'first_commit' in df.columns else 0
print(f'first_commit non-null: {has_fc}')
print()

# 2. New social participants (first message in last 90 days)
threads = pd.read_parquet('data/enriched/social_threads.parquet', columns=['date','canonical_id'])
threads['date'] = pd.to_datetime(threads['date'])
first_seen = threads.groupby('canonical_id')['date'].min()
new_social = (first_seen >= cutoff90).sum()
print(f'New social participants (first message in 90d): {new_social}')

# 3. Review metrics columns
rev = pd.read_parquet('data/enriched/contributor_review_metrics.parquet')
print('Review metrics columns:', list(rev.columns))
print()

# 4. Unified parquet - first_commit sample
if 'first_commit' in df.columns:
    sample = df[df['first_commit'].notna()][['canonical_id','first_commit']].head(5)
    print(sample)
