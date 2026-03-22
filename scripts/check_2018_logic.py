import pandas as pd
import os

commits = pd.read_parquet('data/commits.parquet')
# 1. New devs of 2018
first_years = commits.groupby('canonical_id')['year'].min().reset_index()
new_2018 = first_years[first_years['year'] == 2018]['canonical_id']
total_new = len(new_2018)

# 2. Commit counts for these specific people in 2018
commits_2018 = commits[commits['year'] == 2018]
new_dev_activity = commits_2018[commits_2018['canonical_id'].isin(new_2018)]
counts = new_dev_activity.groupby('canonical_id').size()

# 3. Filtering
regulars = counts[counts >= 3]
num_regulars = len(regulars)

print(f"Total New Devs in 2018: {total_new}")
print(f"New Devs with 3+ commits in 2018: {num_regulars}")
print("\nDistribution of commits for 2018 new devs:")
print(counts.value_counts().sort_index())
