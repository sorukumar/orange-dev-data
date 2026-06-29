import pandas as pd
import json
import os
import sys

sys.path.append(os.getcwd())
from scripts.utils.identity import resolver

# --- Configuration ---
COMMITS_FILE = "data/raw/core_commits.parquet"
OUTPUT_FILE = "data/enriched/commits_resolved.parquet"
MESSAGES_FILE = "data/raw/core_messages.parquet"
PR_METADATA_FILE = "data/raw/github_pr_metadata.parquet"
IDENTITIES_FILE = "metadata/identities.json"
SPONSORS_FILE = "data/enriched/sponsors_merged.json"
GITHUB_PROFILES_FILE = "metadata/github_profiles.json"

# --- Sponsor Lookup ---
class SponsorLookup:
    _instance = None
    _email_to_sponsor = {}
    _sponsors = {}
    _rules = {}
    
    @classmethod
    def load(cls):
        if cls._instance is not None:
            return cls._instance
        cls._instance = cls()
        if not os.path.exists(SPONSORS_FILE):
            print(f"Warning: {SPONSORS_FILE} not found. Using fallback heuristics.")
            return cls._instance
        with open(SPONSORS_FILE, "r") as f:
            data = json.load(f)
        for s in data.get("sponsors", []):
            cls._sponsors[s["id"]] = s
        for dev in data.get("sponsored_developers", []):
            grants = dev.get("grants", [])
            
            uuid = None
            if dev.get("github"):
                uuid = resolver.resolve_github(dev["github"])
            else:
                uuid = resolver.resolve_git(dev.get("canonical_name"), None)
                
            record = next((r for r in resolver._identities if r["uuid"] == uuid), None)
            emails = record.get("git_signatures", {}).get("emails", []) if record else []
            
            for email in emails:
                cls._email_to_sponsor.setdefault(email.lower(), []).extend(grants)
        cls._rules = data.get("classification_rules", {})
        return cls._instance

    @classmethod
    def get_sponsor_id_for_date(cls, email_lower, commit_date_str):
        if not commit_date_str:
            commit_date_str = "9999-12-31"
        commit_date_str = str(commit_date_str)[:10]
        
        grants = cls._email_to_sponsor.get(email_lower, [])
        for grant in grants:
            sponsor_id = grant.get("sponsor_id")
            if not sponsor_id: continue
            
            start_fuzzy = grant.get("start_date")
            end_fuzzy = grant.get("end_date")
            
            start_date = "0000-00-00"
            if start_fuzzy:
                parts = start_fuzzy.split('-')
                if len(parts) == 1: start_date = f"{parts[0]}-01-01"
                elif len(parts) == 2: start_date = f"{parts[0]}-{parts[1]}-01"
                else: start_date = start_fuzzy
                
            end_date = "9999-12-31"
            if end_fuzzy:
                parts = end_fuzzy.split('-')
                if len(parts) == 1: end_date = f"{parts[0]}-12-31"
                elif len(parts) == 2: end_date = f"{parts[0]}-{parts[1]}-31"
                else: end_date = end_fuzzy
                
            if start_date <= commit_date_str <= end_date:
                return sponsor_id
        return None
    
    @classmethod
    def classify(cls, email, commit_date_str, enrich_company=None):
        email_lower = email.lower() if email else ""
        domain = email_lower.split('@')[-1] if '@' in email_lower else ""
        
        if cls.get_sponsor_id_for_date(email_lower, commit_date_str):
            return "Sponsored"
            
        if enrich_company and isinstance(enrich_company, str) and len(enrich_company.strip()) > 1:
            return "Corporate"
            
        corporate_domains = cls._rules.get("corporate_domains", [])
        if domain in corporate_domains:
            return "Sponsored"
            
        academic_domains = cls._rules.get("academic_domains", [])
        if domain in academic_domains:
            return "Corporate"
            
        personal_domains = cls._rules.get("personal_domains", [])
        if domain in personal_domains:
            return "Personal"
            
        return "Personal"
    
    @classmethod
    def get_sponsor_name(cls, email, commit_date_str):
        email_lower = email.lower() if email else ""
        sponsor_id = cls.get_sponsor_id_for_date(email_lower, commit_date_str)
        if sponsor_id and sponsor_id in cls._sponsors:
            return cls._sponsors[sponsor_id].get("name")
        return None

def is_merge_script(name_or_email):
    if not isinstance(name_or_email, str): return False
    val = name_or_email.lower()
    return 'merge script' in val or 'merge-script' in val or 'bitcoin-core-merge-script' in val

def map_integration_dates(commits_df):
    print("Mapping integration dates from PR metadata...")
    if not os.path.exists(PR_METADATA_FILE):
        print(f"Warning: {PR_METADATA_FILE} not found. Falling back to date_utc.")
        commits_df['integration_date'] = commits_df['date_utc']
        return commits_df
        
    prs_df = pd.read_parquet(PR_METADATA_FILE)
    pr_dict = {}
    pr_author_dict = {}
    for _, row in prs_df.iterrows():
        repo = row['repository_name']
        pr_number = row['pr_number']
        merged_at = row['merged_at']
        author_login = row.get('author')
        
        pr_author_cid = resolver.resolve_github(author_login) if pd.notna(author_login) and author_login else None
        
        if pd.notna(merged_at):
            pr_dict.setdefault(repo, {})[str(pr_number)] = pd.to_datetime(merged_at)
            pr_dict.setdefault(repo, {})[int(pr_number)] = pd.to_datetime(merged_at)
            
        if pr_author_cid:
            pr_author_dict.setdefault(repo, {})[str(pr_number)] = pr_author_cid
            pr_author_dict.setdefault(repo, {})[int(pr_number)] = pr_author_cid
            
    if not os.path.exists(MESSAGES_FILE):
        print(f"Warning: {MESSAGES_FILE} not found.")
        commits_df['integration_date'] = commits_df['date_utc']
        return commits_df
        
    messages_df = pd.read_parquet(MESSAGES_FILE)
    msg_map = messages_df.set_index('hash')['subject'].to_dict()
    
    integration_dates = {}
    merge_author_cids = {}
    global_merge_commits = {}
    import re
    pr_regex = re.compile(r'Merge\s+(?:pull\s+request\s+|.*?#)(\d+)', re.IGNORECASE)
    
    for repo, repo_commits in commits_df.groupby('repository_name'):
        repo_parents = repo_commits.set_index('hash')['parents'].fillna('').apply(lambda x: x.split()).to_dict()
        repo_dates = repo_commits.set_index('hash')['date_utc'].to_dict()
        
        merge_commits = {}
        for h, p in repo_parents.items():
            if len(p) > 1:
                subject = msg_map.get(h, "")
                match = pr_regex.search(subject)
                if match:
                    merge_commits[h] = match.group(1)
                    global_merge_commits[h] = match.group(1)
                    
        sorted_merges = sorted(merge_commits.keys(), key=lambda x: repo_dates.get(x, pd.Timestamp.min))
        
        for mh in sorted_merges:
            pr_num = merge_commits[mh]
            merged_at = pr_dict.get(repo, {}).get(pr_num)
            if pd.isna(merged_at):
                merged_at = repo_dates.get(mh)
                
            integration_dates[mh] = merged_at
            
            author_cid = pr_author_dict.get(repo, {}).get(pr_num)
            if author_cid:
                merge_author_cids[mh] = author_cid
            
            p_list = repo_parents.get(mh, [])
            if len(p_list) > 1:
                p2 = p_list[1]
                queue = [p2]
                visited = set()
                while queue:
                    curr = queue.pop(0)
                    if curr in visited: continue
                    visited.add(curr)
                    
                    if curr in integration_dates:
                        continue
                        
                    integration_dates[curr] = merged_at
                    
                    for p in repo_parents.get(curr, []):
                        queue.append(p)
                        
    commits_df['integration_date'] = commits_df['hash'].map(integration_dates)
    commits_df['integration_date'] = commits_df['integration_date'].fillna(commits_df['date_utc'])
    commits_df['integration_date'] = pd.to_datetime(commits_df['integration_date'], utc=True)
    
    commits_df['pr_author_cid'] = commits_df['hash'].map(merge_author_cids)
    commits_df['is_self_merge'] = (commits_df['is_merge'] == True) & (commits_df['canonical_id'] == commits_df['pr_author_cid']) & commits_df['canonical_id'].notna()
    commits_df['pr_number'] = commits_df['hash'].map(global_merge_commits)
    
    return commits_df

# --- Maintainer Lookup ---
class MaintainerLookup:
    _email_to_id = {}
    _is_loaded = False
    
    @classmethod
    def load(cls):
        if cls._is_loaded:
            return
        path = "metadata/maintainers.json"
        if not os.path.exists(path):
            print("Warning: maintainers.json not found.")
            return
        with open(path, "r") as f:
            data = json.load(f)
        for m in data.get("maintainers", []):
            for email in m.get("emails", []):
                cls._email_to_id[email.lower()] = m["id"]
            if m.get("github"):
                cls._email_to_id[m["github"].lower()] = m["id"]
        cls._is_loaded = True
        
    @classmethod
    def is_maintainer(cls, email_or_github):
        cls.load()
        if not email_or_github:
            return False
        return email_or_github.lower() in cls._email_to_id


def resolve_commits():
    print("Loading raw commits...")
    if not os.path.exists(COMMITS_FILE):
        raise FileNotFoundError(f"Missing {COMMITS_FILE}")
    
    commits = pd.read_parquet(COMMITS_FILE)
    
    print("Filtering out branch sync merges...")
    # Load message subjects
    if os.path.exists(MESSAGES_FILE):
        messages_df = pd.read_parquet(MESSAGES_FILE)
        msg_map = messages_df.set_index('hash')['subject'].to_dict()
    else:
        msg_map = {}

    MaintainerLookup.load()

    # Identify branch sync merges:
    # 1. Subject contains 'into ' (case-insensitive)
    # 2. Or committer is not a whitelisted maintainer
    def is_branch_sync(row):
        if not row['is_merge']:
            return False
        h = row['hash']
        subject = msg_map.get(h, '')
        if 'into ' in subject.lower():
            return True
        # If it's an integration merge but committer is not a whitelisted maintainer, it's a sync merge
        committer_email = str(row.get('committer_email', '')).lower()
        committer_name = str(row.get('committer_name', '')).lower()
        if not MaintainerLookup.is_maintainer(committer_email) and not MaintainerLookup.is_maintainer(committer_name):
            return True
        return False

    is_sync = commits.apply(is_branch_sync, axis=1)
    print(f"Dropped {is_sync.sum()} branch sync merges from commit history.")
    commits = commits[~is_sync].copy()
    
    # Load profile metadata for company enrichment
    gh_profiles = {}
    if os.path.exists(GITHUB_PROFILES_FILE):
        with open(GITHUB_PROFILES_FILE) as f:
            gh_data = json.load(f)
            for profile in gh_data.get("profiles", {}).values():
                if profile.get("login"):
                    gh_profiles[profile["login"]] = profile.get("company")
    
    print("Normalizing identities using master resolver...")
    
    # We resolve the correct git identity based on author vs committer (merge script logic)
    def map_identity(row):
        is_merge = row.get('is_merge') or is_merge_script(row.get('author_name')) or is_merge_script(row.get('author_email'))
        if is_merge:
            target_name = str(row.get('committer_name', ''))
            target_email = str(row.get('committer_email', ''))
        else:
            target_name = str(row.get('author_name', ''))
            target_email = str(row.get('author_email', ''))
            
        cid = resolver.resolve_git(target_name, target_email)
        cname = target_name
        return pd.Series([cid, cname, target_email])
        
    overrides = commits.apply(map_identity, axis=1)
    commits['canonical_id'] = overrides[0]
    commits['canonical_name'] = overrides[1]
    
    # Get primary email for sponsor matching
    target_emails = overrides[2]
    
    print("Applying sponsor classifications...")
    SponsorLookup.load()
    
    # We need to find the github login for the canonical id to get company enrichment
    # But since canonical_id is just an id, we map from identities.json
    identities_path = "metadata/identities.json"
    id_to_company = {}
    if os.path.exists(identities_path):
        with open(identities_path, "r") as f:
            identities_data = json.load(f).get("identities", [])
            for identity in identities_data:
                login = identity.get("platforms", {}).get("github")
                if isinstance(login, list) and login: login = login[0]
                company = gh_profiles.get(login) if login else None
                if company:
                    id_to_company[identity["uuid"]] = company

    def map_sponsor(row):
        email = row['target_email']
        cid = row['canonical_id']
        date_utc = row.get('date_utc')
        company = id_to_company.get(cid)
        classification = SponsorLookup.classify(email, date_utc, enrich_company=company)
        sponsor_name = SponsorLookup.get_sponsor_name(email, date_utc)
        return pd.Series([classification, sponsor_name])
    
    # We'll temporarily add target_email to compute sponsor
    commits['target_email'] = target_emails
    sponsor_data = commits.apply(map_sponsor, axis=1)
    commits['classification'] = sponsor_data[0]
    commits['sponsor_name'] = sponsor_data[1]
    commits.drop(columns=['target_email'], inplace=True)
    
    # Map integration dates (merged_at)
    commits = map_integration_dates(commits)
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    commits.to_parquet(OUTPUT_FILE, index=False)
    print(f"✅ Resolved {len(commits)} commits to {OUTPUT_FILE}")

if __name__ == "__main__":
    resolve_commits()
