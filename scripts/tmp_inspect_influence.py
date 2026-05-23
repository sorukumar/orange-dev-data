import json
import pandas as pd
import os

ids = ['can_ava_chow', 'can_gloria_zhao']
print('IDS', ids)

ss = json.load(open('data/enriched/social_stats.json'))['contributors']
for c in ss:
    if c['id'] in ids:
        print('SOCIAL', c['id'], c['display_name'], c['dev_type'], c['hybrid_score'], c['scores'], c.get('code_stats'))

if os.path.exists('output/tracker/contributors_rich.json'):
    rich = json.load(open('output/tracker/contributors_rich.json'))
    for c in rich:
        if c.get('name') in ['Ava Chow', 'Gloria Zhao']:
            print('RICH', c['name'], c.get('total_commits'), c.get('impact'), c.get('is_maintainer'))

if os.path.exists('data/enriched/contributors_unified.parquet'):
    df = pd.read_parquet('data/enriched/contributors_unified.parquet')
    for uid in ids:
        row = df[df['canonical_id'] == uid]
        print('UNIFIED', uid, row[['canonical_id','total_commits','reviews_count','prs_authored','bips_authored','ml_threads','delving_threads']].to_dict('records'))
