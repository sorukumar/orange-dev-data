import json, pandas as pd, re

def parse_version(v_str):
    matches = re.findall(r'\d+', str(v_str))
    if not matches: return (0, 0, 0)
    ints = [int(m) for m in matches]
    while len(ints) < 3: ints.append(0)
    return tuple(ints[:3])

CACHE_PATH = "data/raw/release_highlights_cache.json"
PR_CACHE_PATH = "data/raw/pr_summaries_cache.json"
PARQUET_PATH = "data/raw/github_pr_metadata.parquet"

with open(CACHE_PATH) as f:
    cache = json.load(f)

with open(PR_CACHE_PATH) as f:
    pr_cache = json.load(f)

df = pd.read_parquet(PARQUET_PATH)
df = df[(df['repository_name'] == 'bitcoin/bitcoin') & (df['merged_at'].notna())].copy()

# Build milestone inference (same logic as the main script)
tagged_df = df[df['milestone'].notna()]
cutoff_dates = {}
for ms, group in tagged_df.groupby('milestone'):
    cutoff_dates[ms] = pd.to_datetime(group['merged_at'], utc=True).max()

sorted_cutoffs = sorted(
    [(ms, date) for ms, date in cutoff_dates.items() if pd.notna(date)],
    key=lambda x: parse_version(x[0])
)

def is_high_signal(labels_str, is_recent):
    if pd.isna(labels_str): return False
    labels = str(labels_str).lower()
    if any(drop in labels for drop in ['test', 'doc', 'refactor', 'build', 'ci']):
        return False
    if is_recent:
        return any(keep in labels for keep in ['consensus', 'p2p', 'rpc', 'rest', 'zmq', 'wallet', 'mempool', 'gui', 'policy'])
    else:
        return any(keep in labels for keep in ['consensus', 'cryptography', 'p2p'])

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

df['milestone'] = df.apply(infer_milestone, axis=1)
df = df[df['milestone'].notna()]

updated = 0
for ms_str in cache:
    ms_prs = df[df['milestone'].astype(str) == ms_str]
    count = 0
    for _, pr in ms_prs.iterrows():
        labels_str = str(pr.get('labels', ''))
        if 'Tests' in labels_str or 'Docs' in labels_str or 'Refactoring' in labels_str:
            continue
        count += 1
    cache[ms_str]["pr_count"] = count
    updated += 1

with open(CACHE_PATH, 'w') as f:
    json.dump(cache, f, indent=2)

print(f"Backfilled pr_count for {updated} milestones.")
