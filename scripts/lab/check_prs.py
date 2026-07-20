import pandas as pd

parquet_path = "/Users/saurabhkumar/Desktop/Work/github/orange-dev-data/data/raw/github_pr_metadata.parquet"
df = pd.read_parquet(parquet_path)

prs_to_check = [35295, 34495, 35087]
for pr in prs_to_check:
    row = df[df['pr_number'] == pr]
    if not row.empty:
        print(f"PR {pr}:")
        print(f"  Title: {row.iloc[0]['title']}")
        print(f"  Labels: {row.iloc[0]['labels']}")
    else:
        print(f"PR {pr} not found in parquet.")
