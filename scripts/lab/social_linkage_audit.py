"""
Check: for the 1,316 mailing-list and 370 Delving identities in identities.json,
how many are linked to a GitHub handle vs completely social-only islands?
"""
import json

with open('metadata/identities.json') as f:
    ids = json.load(f)['identities']

ml_ids    = [i for i in ids if 'mailinglist' in i.get('sources', [])]
dlv_ids   = [i for i in ids if 'delving'     in i.get('sources', [])]
social    = [i for i in ids if 'mailinglist' in i.get('sources', []) or
                                'delving'    in i.get('sources', [])]

def has_github(i):  return bool(i.get('platforms', {}).get('github'))
def has_email(i):   return bool(i.get('git_signatures', {}).get('emails'))
def has_commit(i):  return 'corecommit' in i.get('sources', [])
def has_pr(i):      return 'prgithub'   in i.get('sources', [])

print("Mailing list people in identities.json:", len(ml_ids))
print(f"  of those, also have a GitHub handle:   {sum(has_github(i) for i in ml_ids)}")
print(f"  of those, also have a git commit:       {sum(has_commit(i) for i in ml_ids)}")
print(f"  of those, also seen in GH PRs:          {sum(has_pr(i) for i in ml_ids)}")
print(f"  SOCIAL-ONLY (no github/commit/pr):      {sum(not has_github(i) and not has_commit(i) and not has_pr(i) for i in ml_ids)}")

print()
print("Delving people in identities.json:", len(dlv_ids))
print(f"  of those, also have a GitHub handle:   {sum(has_github(i) for i in dlv_ids)}")
print(f"  of those, also have a git commit:       {sum(has_commit(i) for i in dlv_ids)}")
print(f"  of those, also seen in GH PRs:          {sum(has_pr(i) for i in dlv_ids)}")
print(f"  SOCIAL-ONLY (no github/commit/pr):      {sum(not has_github(i) and not has_commit(i) and not has_pr(i) for i in dlv_ids)}")

print()
print("KEY QUESTION: how does the graph link a social person to their GitHub identity?")
print("  → Only if: same NAME: node appears in both (name in commit == name on mailing list)")
print("  → OR:      they are in identity_curated.json (hand-curated)")
print()

# How many social-only people are linked purely because name matched a git commit?
cross_linked = [i for i in social
                if (has_github(i) or has_commit(i)) and
                'corecommit' in i.get('sources', [])]
print(f"Social people linked to a commit via matching name:  {len(cross_linked)}")

purely_social = [i for i in ids
                 if ('mailinglist' in i.get('sources',[]) or 'delving' in i.get('sources',[]))
                 and not has_github(i) and not has_commit(i) and not has_pr(i)
                 and 'bipgithub' not in i.get('sources',[])]
print(f"Truly social-only (no code signals at all):          {len(purely_social)}")
print(f"\nSample truly social-only:")
for i in purely_social[:10]:
    print(f"  {i['uuid']:<35}  sources={i['sources']}")
