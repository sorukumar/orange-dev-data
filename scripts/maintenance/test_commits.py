import pandas as pd
df = pd.read_parquet("data/raw/core_commits.parquet")
btc = df[df['repository_name'] == 'bitcoin/bitcoin']
print(f"Total rows in btc: {len(btc)}")
print(f"Unique hashes in btc: {btc['hash'].nunique()}")
print(btc.head())
