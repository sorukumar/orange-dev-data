import pandas as pd, re
df = pd.read_parquet("data/raw/github_pr_metadata.parquet")
def parse_version(v_str):
    matches = re.findall(r"\d+", str(v_str))
    if not matches: return (0, 0, 0)
    ints = [int(m) for m in matches]
    while len(ints) < 3: ints.append(0)
    return tuple(ints[:3])
print(sorted(df["milestone"].dropna().unique(), key=parse_version, reverse=True)[:10])
