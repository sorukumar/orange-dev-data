import pandas as pd
from pathlib import Path
from datetime import datetime

path = Path('data/enriched/contributors_unified.parquet')
df = pd.read_parquet(path)
cols = ['first_commit','last_commit','global_first_active','global_last_active','first_active','last_active']

def clean_object(obj):
    if obj is None:
        return None
    if isinstance(obj, str) and obj == 'NaT':
        return None
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if pd.isna(obj):
        return None
    if isinstance(obj, dict):
        return {k: clean_object(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [clean_object(i) for i in obj]
    return obj

for i in range(5):
    row = df.iloc[i]
    print('row', i)
    for c in cols:
        v = row[c]
        print(c, type(v).__name__, repr(v), pd.isna(v), clean_object(v))
