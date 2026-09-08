import pandas as pd
df = pd.read_parquet('/Users/saurabhkumar/Desktop/Work/github/orange-dev-data/data/enriched/commits_resolved.parquet')
print("Columns:", df.columns.tolist())
print(df.head(2))
