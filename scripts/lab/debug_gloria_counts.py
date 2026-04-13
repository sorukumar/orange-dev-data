import pandas as pd

df = pd.read_parquet('data/commits.parquet')
gloria_aliases = ["Gloria Zhao", "glozow"]
gloria_emails = ["gloriajzhao@gmail.com", "gzhao408@berkeley.edu"]

# Authored (excluding merges)
is_author = df['author_name'].isin(gloria_aliases) | df['author_email'].str.lower().isin(gloria_emails)
authored = df[is_author & (df['category'] != 'Merge')]['hash'].nunique()

# Merged (maintenance actions)
is_committer = df['committer_name'].isin(gloria_aliases) | df['committer_email'].str.lower().isin(gloria_emails)
merged = df[is_committer & (df['is_merge'] == True) & (df['category'] == 'Merge')]['hash'].nunique()

# Total unique hashes where she is author OR committer
all_activity = df[is_author | is_committer]['hash'].nunique()

print(f"Authored (Non-merge): {authored}")
print(f"Merged (Maintainer Action): {merged}")
print(f"Sum: {authored + merged}")
print(f"Total Unique Activity: {all_activity}")

# Check for latest commits
latest = df[is_author | is_committer].sort_values('date_utc', ascending=False).head(5)
print("\nLatest activity:")
print(latest[['date_utc', 'hash', 'author_name', 'category']])
