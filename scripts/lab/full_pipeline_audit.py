"""
Full source audit: count unique identifiers from every raw source,
then show how they flow through the identity resolution pipeline.
"""
import pandas as pd
import json
import sys
sys.path.append('.')
from scripts.utils.identity import resolver

print("=" * 70)
print("SOURCE 1: core_commits.parquet  (Git commit history)")
print("=" * 70)
df_commits = pd.read_parquet('data/raw/core_commits.parquet')
unique_emails = df_commits['author_email'].dropna().str.lower().unique()
unique_names  = df_commits['author_name'].dropna().unique()
print(f"  Total commits:          {len(df_commits):,}")
print(f"  Unique emails:          {len(unique_emails):,}")
print(f"  Unique names:           {len(unique_names):,}")
commit_pairs = df_commits[['author_email','author_name']].drop_duplicates()
print(f"  Unique (email+name) pairs: {len(commit_pairs):,}")
df_commits['canonical_id'] = df_commits.apply(
    lambda r: resolver.resolve_git(str(r['author_name']), str(r['author_email'])), axis=1)
unique_uuids_commits = df_commits['canonical_id'].nunique()
print(f"  → Resolved to unique UUIDs: {unique_uuids_commits:,}")

print()
print("=" * 70)
print("SOURCE 2: github_pr_metadata.parquet  (PR authors)")
print("=" * 70)
df_prs = pd.read_parquet('data/raw/github_pr_metadata.parquet')
unique_gh_logins = df_prs['author'].dropna().unique()
unique_gh_ids    = df_prs['github_id'].dropna().unique() if 'github_id' in df_prs.columns else []
print(f"  Total PRs:              {len(df_prs):,}")
print(f"  Unique GitHub logins:   {len(unique_gh_logins):,}")
print(f"  Unique GitHub IDs:      {len(unique_gh_ids):,}")
df_prs['canonical_id'] = df_prs['author'].apply(lambda x: resolver.resolve_github(x))
print(f"  → Resolved to unique UUIDs: {df_prs['canonical_id'].nunique():,}")

print()
print("=" * 70)
print("SOURCE 3: github_review_events.parquet  (PR reviewers)")
print("=" * 70)
df_events = pd.read_parquet('data/raw/github_review_events.parquet')
unique_reviewers = df_events['user'].dropna().unique()
print(f"  Total review events:    {len(df_events):,}")
print(f"  Unique reviewer logins: {len(unique_reviewers):,}")
df_events['canonical_id'] = df_events['user'].apply(lambda x: resolver.resolve_github(x))
print(f"  → Resolved to unique UUIDs: {df_events['canonical_id'].nunique():,}")

print()
print("=" * 70)
print("SOURCE 4: social_mailing_list.parquet  (Mailing list posters)")
print("=" * 70)
df_ml = pd.read_parquet('data/raw/social_mailing_list.parquet')
ml_emails = df_ml['author_email'].dropna().str.lower().unique() if 'author_email' in df_ml.columns else []
ml_names  = df_ml['author_name'].dropna().unique() if 'author_name' in df_ml.columns else []
print(f"  Total messages:         {len(df_ml):,}")
print(f"  Unique emails:          {len(ml_emails):,}")
print(f"  Unique names:           {len(ml_names):,}")
df_ml['canonical_id'] = df_ml.apply(
    lambda r: resolver.resolve_mailing_list(r.get('author_email') or r.get('author_name')), axis=1)
print(f"  → Resolved to unique UUIDs: {df_ml['canonical_id'].nunique():,}")

print()
print("=" * 70)
print("SOURCE 5: social_delving.parquet  (Delving Bitcoin forum)")
print("=" * 70)
df_dlv = pd.read_parquet('data/raw/social_delving.parquet')
dlv_usernames = df_dlv['author_username'].dropna().unique() if 'author_username' in df_dlv.columns else \
                df_dlv['author_name'].dropna().unique()
print(f"  Total posts:            {len(df_dlv):,}")
print(f"  Unique usernames:       {len(dlv_usernames):,}")
df_dlv['canonical_id'] = df_dlv.apply(
    lambda r: resolver.resolve_delving(r.get('author_username') or r.get('author_name')), axis=1)
print(f"  → Resolved to unique UUIDs: {df_dlv['canonical_id'].nunique():,}")

print()
print("=" * 70)
print("SOURCE 6: bips.parquet  (BIP header authors)")
print("=" * 70)
df_bips = pd.read_parquet('data/raw/bips.parquet')
bip_authors_raw = []
for _, row in df_bips.iterrows():
    auth_list = row.get('author_names')
    if auth_list is None: auth_list = []
    if not hasattr(auth_list, '__iter__') or isinstance(auth_list, str): auth_list = [auth_list]
    for a in auth_list:
        bip_authors_raw.append(str(a).strip())
bip_authors_raw = [a for a in bip_authors_raw if a]
print(f"  Total BIPs:             {len(df_bips):,}")
print(f"  Total author entries:   {len(bip_authors_raw):,}")
print(f"  Unique author names:    {len(set(bip_authors_raw)):,}")
bip_uuids = set(resolver.resolve_git(a, None) for a in set(bip_authors_raw))
print(f"  → Resolved to unique UUIDs: {len(bip_uuids):,}")

print()
print("=" * 70)
print("IDENTITIES.JSON  (after build_identities.py — the identity graph)")
print("=" * 70)
with open('metadata/identities.json') as f:
    ids_data = json.load(f)['identities']
can_ids = [i for i in ids_data if i['uuid'].startswith('can_')]
auto_ids = [i for i in ids_data if i['uuid'].startswith('auto_')]
has_github = [i for i in ids_data if i.get('platforms',{}).get('github')]
has_email  = [i for i in ids_data if i.get('git_signatures',{}).get('emails')]
multi_source = [i for i in ids_data if len(i.get('sources', [])) > 1]
print(f"  Total identities:       {len(ids_data):,}")
print(f"    can_ (curated/multi-alias): {len(can_ids):,}")
print(f"    auto_ (single-source):      {len(auto_ids):,}")
print(f"  With GitHub handle:     {len(has_github):,}")
print(f"  With email address:     {len(has_email):,}")
print(f"  Seen in 2+ sources:     {len(multi_source):,}")
source_counts = {}
for i in ids_data:
    for s in i.get('sources', []):
        source_counts[s] = source_counts.get(s, 0) + 1
print(f"  Identities per source:")
for s, c in sorted(source_counts.items(), key=lambda x: -x[1]):
    print(f"    {s:<20} {c:,}")

print()
print("=" * 70)
print("CONTRIBUTOR_EFFICIENCY.PARQUET  (after efficiency.py)")
print("=" * 70)
df_eff = pd.read_parquet('data/enriched/contributor_efficiency.parquet')
eff_auto = df_eff['canonical_id'].str.startswith('auto_').sum()
eff_can  = df_eff['canonical_id'].str.startswith('can_').sum()
eff_raw  = len(df_eff) - eff_auto - eff_can
print(f"  Total rows:             {len(df_eff):,}")
print(f"    can_ IDs:  {eff_can:,}")
print(f"    auto_ IDs: {eff_auto:,}")
print(f"    raw handles (bugs): {eff_raw:,}")
print(f"  Unique canonical_ids:   {df_eff['canonical_id'].nunique():,}")

print()
print("=" * 70)
print("SOCIAL_STATS.JSON  (after influence.py)")
print("=" * 70)
with open('data/enriched/social_stats.json') as f:
    soc_stats = json.load(f)['contributors']
soc_can  = sum(1 for c in soc_stats if c['id'].startswith('can_'))
soc_auto = sum(1 for c in soc_stats if c['id'].startswith('auto_'))
soc_raw  = len(soc_stats) - soc_can - soc_auto
print(f"  Total entries:          {len(soc_stats):,}")
print(f"    can_ IDs:  {soc_can:,}")
print(f"    auto_ IDs: {soc_auto:,}")
print(f"    raw handles (bugs): {soc_raw:,}")

print()
print("=" * 70)
print("CONTRIBUTORS_UNIFIED.PARQUET  (after unify_contributors.py — THE GRAND JOIN)")
print("=" * 70)
df_uni = pd.read_parquet('data/enriched/contributors_unified.parquet')
uni_can  = df_uni['uuid'].str.startswith('can_').sum()
uni_auto = df_uni['uuid'].str.startswith('auto_').sum()
uni_raw  = len(df_uni) - uni_can - uni_auto
reviewers_nonzero = (df_uni.get('reviews_count', pd.Series(0)) > 0).sum() if 'reviews_count' in df_uni.columns else 'N/A'
committers_nonzero = (df_uni.get('total_commits', pd.Series(0)) > 0).sum() if 'total_commits' in df_uni.columns else 'N/A'
social_nonzero = ((df_uni.get('ml_threads', pd.Series(0)) > 0) | (df_uni.get('delving_threads', pd.Series(0)) > 0)).sum() \
                 if 'ml_threads' in df_uni.columns else 'N/A'
print(f"  Total rows:             {len(df_uni):,}")
print(f"    can_ IDs:  {uni_can:,}")
print(f"    auto_ IDs: {uni_auto:,}")
print(f"    raw handles (bugs): {uni_raw:,}")
print(f"  Has commit activity:    {committers_nonzero:,}" if committers_nonzero != 'N/A' else "  Has commit activity:    N/A")
print(f"  Has review activity:    {reviewers_nonzero:,}"  if reviewers_nonzero != 'N/A' else "  Has review activity:    N/A")
print(f"  Has social activity:    {social_nonzero:,}"     if social_nonzero != 'N/A' else "  Has social activity:    N/A")
print(f"  Columns: {list(df_uni.columns)}")

print()
print("=" * 70)
print("ECOSYSTEM_SUMMARY.JSON  (final output)")
print("=" * 70)
with open('output/shared/ecosystem_summary.json') as f:
    eco = json.load(f)['groups']
for k, v in eco.items():
    print(f"  {k:<25} {v:,}")
