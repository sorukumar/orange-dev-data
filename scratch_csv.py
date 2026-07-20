import os
import json
import pandas as pd
import numpy as np
import re

INPUT_PR_PARQUET = "data/raw/github_pr_metadata.parquet"
REVIEW_EVENTS_PARQUET = "data/raw/github_review_events.parquet"

def parse_version(v_str):
    matches = re.findall(r'\d+', str(v_str))
    if not matches: return (0, 0, 0)
    ints = [int(m) for m in matches]
    while len(ints) < 3: ints.append(0)
    return tuple(ints[:3])

df = pd.read_parquet(INPUT_PR_PARQUET)
df = df[(df['repository_name'] == 'bitcoin/bitcoin') & (df['merged_at'].notna())].copy()
df_rev = pd.read_parquet(REVIEW_EVENTS_PARQUET)
review_counts = df_rev.groupby('pr_number').size().to_dict()
df['review_count'] = df['pr_number'].map(review_counts).fillna(0)

pr_to_milestone = {}
release_notes_dir = "data/sources/bitcoin/doc/release-notes"
if os.path.exists(release_notes_dir):
    for filename in os.listdir(release_notes_dir):
        if filename.startswith("release-notes-") and filename.endswith(".md"):
            version = filename[len("release-notes-"):-3]
            with open(os.path.join(release_notes_dir, filename), 'r', encoding='utf-8') as f:
                content = f.read()
                pr_nums = re.findall(r'^-\s+#(\d+)', content, re.MULTILINE)
                for pr_num in pr_nums: pr_to_milestone[int(pr_num)] = version

def override_milestone(row):
    pr_id = row['pr_number']
    if pd.notna(pr_id) and int(pr_id) in pr_to_milestone: return pr_to_milestone[int(pr_id)]
    return row['milestone']
df['milestone'] = df.apply(override_milestone, axis=1)

tagged_df = df[df['milestone'].notna()]
cutoff_dates = {}
for ms, group in tagged_df.groupby('milestone'):
    cutoff_dates[ms] = pd.to_datetime(group['merged_at'], utc=True).max()
sorted_cutoffs = sorted([(ms, date) for ms, date in cutoff_dates.items() if pd.notna(date)], key=lambda x: parse_version(x[0]))

def infer_milestone_only(row):
    if pd.notna(row['milestone']): return row['milestone']
    merged = pd.to_datetime(row['merged_at'], utc=True)
    if pd.isna(merged): return None
    inferred_ms = None
    for ms, cutoff in sorted_cutoffs:
        if merged <= cutoff:
            inferred_ms = ms
            break
    if not inferred_ms and sorted_cutoffs:
        inferred_ms = sorted_cutoffs[-1][0]
    return inferred_ms
df['inferred_milestone'] = df.apply(infer_milestone_only, axis=1)
df = df[df['inferred_milestone'].notna()]

results = []
all_labels = set()
for labels_str in df['labels']:
    if pd.isna(labels_str) or not labels_str:
        all_labels.add('[No Label]')
    else:
        for l in str(labels_str).split('|'):
            all_labels.add(l.strip())

for ms, group in df.groupby('inferred_milestone'):
    ms_version = parse_version(ms)
    
    label_stats = {l: {'Total': 0, 'ReviewCounts': []} for l in all_labels}
    
    for _, row in group.iterrows():
        labels_str = row['labels']
        if pd.isna(labels_str) or not labels_str:
            pr_labels = ['[No Label]']
        else:
            pr_labels = [l.strip() for l in str(labels_str).split('|')]
            
        rc = row['review_count']
        
        for l in pr_labels:
            if l in label_stats:
                label_stats[l]['Total'] += 1
                label_stats[l]['ReviewCounts'].append(rc)
                
    for l, stats in label_stats.items():
        n = stats['Total']
        if n > 0:
            counts_series = pd.Series(stats['ReviewCounts'])
            results.append({
                'Version': ms,
                'Label': l,
                'Total PRs': n,
                'Min Review Count': counts_series.min(),
                '25th Percentile': counts_series.quantile(0.25) if n >= 4 else None,
                '50th Percentile (Median)': counts_series.median() if n >= 3 else None,
                '75th Percentile': counts_series.quantile(0.75) if n >= 4 else None,
                'Max Review Count': counts_series.max(),
                'sort_key': ms_version
            })

res_df = pd.DataFrame(results)
res_df = res_df.sort_values(['sort_key', 'Label'], ascending=[False, True]).drop('sort_key', axis=1)

output_path = "/Users/saurabhkumar/Desktop/pr_counts_analysis.csv"
res_df.to_csv(output_path, index=False)
print(f"Saved CSV to {output_path}")

