import pandas as pd
import re

def parse_version(v_str):
    matches = re.findall(r'\d+', str(v_str))
    if not matches: return (0,0,0)
    ints = [int(m) for m in matches]
    while len(ints) < 3: ints.append(0)
    return tuple(ints[:3])

def is_high_signal(labels_str, is_recent):
    if pd.isna(labels_str): return False
    labels = str(labels_str).lower()
    if any(drop in labels for drop in ['test', 'doc', 'refactor', 'build', 'ci']): return False
    if is_recent: return any(keep in labels for keep in ['consensus', 'p2p', 'rpc', 'rest', 'zmq', 'wallet', 'mempool', 'gui', 'policy'])
    else: return any(keep in labels for keep in ['consensus', 'cryptography', 'p2p'])

df = pd.read_parquet("data/raw/github_pr_metadata.parquet")
df = df[(df['repository_name'] == 'bitcoin/bitcoin') & (df['merged_at'].notna())].copy()

tagged_df = df[df['milestone'].notna()]
cutoff_dates = {}
for ms, group in tagged_df.groupby('milestone'):
    cutoff_dates[ms] = pd.to_datetime(group['merged_at'], utc=True).max()
sorted_cutoffs = sorted([(ms, date) for ms, date in cutoff_dates.items() if pd.notna(date)], key=lambda x: parse_version(x[0]))

explicit_count = len(tagged_df)

def infer_milestone(row):
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
    
    if inferred_ms:
        ms_version = parse_version(inferred_ms)
        is_recent = ms_version >= (24, 0, 0)
        if not is_high_signal(row['labels'], is_recent):
            return None
    return inferred_ms

df['final_milestone'] = df.apply(infer_milestone, axis=1)

final_count = df['final_milestone'].notna().sum()
inferred_count = final_count - explicit_count

print(f"Explicit: {explicit_count}")
print(f"Total after inference: {final_count}")
print(f"Additional PRs gained: {inferred_count}")
