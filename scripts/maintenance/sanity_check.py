import pandas as pd

files = {
    "core_commits": "data/raw/core_commits.parquet",
    "core_messages": "data/raw/core_messages.parquet",
    "github_pr_metadata": "data/raw/github_pr_metadata.parquet",
    "github_review_events": "data/raw/github_review_events.parquet"
}

for name, filepath in files.items():
    print(f"--- {name} ---")
    try:
        df = pd.read_parquet(filepath)
        print(f"Total Rows: {len(df)}")
        if "repository_name" in df.columns:
            print("Row counts by repository:")
            counts = df['repository_name'].value_counts()
            for repo, count in counts.items():
                print(f"  {repo}: {count}")
        else:
            print("WARNING: 'repository_name' column not found!")
    except Exception as e:
        print(f"Failed to read {filepath}: {e}")
    print()
