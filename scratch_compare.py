import os
import json
import pandas as pd
import re

INPUT_PR_PARQUET = "data/raw/github_pr_metadata.parquet"
REVIEW_EVENTS_PARQUET = "data/raw/github_review_events.parquet"

def parse_version(v_str):
    matches = re.findall(r'\d+', str(v_str))
    if not matches: return (0, 0, 0)
    ints = [int(m) for m in matches]
    while len(ints) < 3: ints.append(0)
    return tuple(ints[:3])

def is_high_signal_old(labels_str, is_recent):
    if pd.isna(labels_str): return False
    labels = str(labels_str).lower()
    if any(drop in labels for drop in ['test', 'doc', 'refactor', 'build', 'ci']):
        return False
    if is_recent:
        return any(keep in labels for keep in ['consensus', 'validation', 'p2p', 'rpc', 'rest', 'zmq', 'wallet', 'mempool', 'gui', 'policy'])
    else:
        return any(keep in labels for keep in ['consensus', 'cryptography', 'p2p'])

def is_high_signal_new(labels_str, is_recent, review_count):
    labels = str(labels_str).lower() if pd.notna(labels_str) else ""
    tier_1 = ['consensus', 'validation', 'cryptography', 'p2p', 'wallet', 'mempool', 'policy']
    tier_3 = ['test', 'doc', 'refactor', 'build', 'ci']
    if not is_recent:
        return any(keep in labels for keep in ['consensus', 'cryptography', 'p2p'])
    if any(keep in labels for keep in tier_1): return True
    if any(drop in labels for drop in tier_3): return review_count >= 50
    return review_count >= 25

def is_high_signal_strict(labels_str, is_recent, review_count):
    labels = str(labels_str).lower() if pd.notna(labels_str) else ""
    tier_1 = ['consensus', 'validation', 'cryptography', 'p2p', 'wallet', 'mempool', 'policy']
    tier_3 = ['test', 'doc', 'refactor', 'build', 'ci']
    if not is_recent:
        return any(keep in labels for keep in ['consensus', 'cryptography', 'p2p'])
    if any(keep in labels for keep in tier_1): return True
    if any(drop in labels for drop in tier_3): return review_count >= 75
    return review_count >= 40

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
    if pd.notna(pr_id) and int(pr_id) in pr_to_milestone:
        return pr_to_milestone[int(pr_id)]
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
for ms, group in df.groupby('inferred_milestone'):
    ms_version = parse_version(ms)
    is_recent = ms_version >= (24, 0, 0)
    
    total = len(group)
    old_count = sum(group.apply(lambda r: is_high_signal_old(r['labels'], is_recent), axis=1))
    new_count = sum(group.apply(lambda r: is_high_signal_new(r['labels'], is_recent, r['review_count']), axis=1))
    strict_count = sum(group.apply(lambda r: is_high_signal_strict(r['labels'], is_recent, r['review_count']), axis=1))
    
    results.append({
        'Version': ms,
        'Total PRs': total,
        'Old Method (Labels Only)': old_count,
        'New Method (Thresholds 25/50)': new_count,
        'Strict Method (Thresholds 40/75)': strict_count
    })

res_df = pd.DataFrame(results)
res_df['sort_key'] = res_df['Version'].apply(parse_version)
res_df = res_df.sort_values('sort_key', ascending=False).drop('sort_key', axis=1)

# only show recent releases >= 24.0
print(res_df[res_df['Version'].apply(lambda x: parse_version(x) >= (24,0,0))].to_markdown(index=False))

