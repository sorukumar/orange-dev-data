import pandas as pd
import json
import sys
sys.path.append('.')
from scripts.utils.identity import resolver

with open('metadata/identities.json') as f:
    ids = json.load(f)['identities']

# Build lookup by github platform
gh_to_uuid = {}
for i in ids:
    gh = i.get('platforms', {}).get('github')
    if gh:
        gh_to_uuid[gh.lower()] = i['uuid']

df = pd.read_parquet('data/enriched/contributor_efficiency.parquet')
mask = ~df['canonical_id'].str.startswith('auto_') & ~df['canonical_id'].str.startswith('can_')
raw_handles = df[mask]['canonical_id'].unique()
print(f"Raw handles in efficiency: {len(raw_handles)}")

already_in_ids = [h for h in raw_handles if h.lower() in gh_to_uuid]
print(f"Of those, matched in identities.json github platform: {len(already_in_ids)}")
print(f"Truly unknown (not in identities.json at all): {len(raw_handles) - len(already_in_ids)}")
print("\nSample already_in_ids (resolver should have caught these):")
for h in already_in_ids[:10]:
    print(f"  {h!r}  => {gh_to_uuid[h.lower()]}")
