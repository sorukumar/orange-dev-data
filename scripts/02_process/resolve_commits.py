import pandas as pd
import json
import os
import sys

sys.path.append(os.getcwd())
from scripts.utils.identity import resolver

# --- Configuration ---
COMMITS_FILE = "data/raw/core_commits.parquet"
OUTPUT_FILE = "data/enriched/commits_resolved.parquet"
SPONSORS_FILE = "metadata/sponsors.json"
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
            for email in dev.get("emails", []):
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

def resolve_commits():
    print("Loading raw commits...")
    if not os.path.exists(COMMITS_FILE):
        raise FileNotFoundError(f"Missing {COMMITS_FILE}")
    
    commits = pd.read_parquet(COMMITS_FILE)
    
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
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    commits.to_parquet(OUTPUT_FILE, index=False)
    print(f"✅ Resolved {len(commits)} commits to {OUTPUT_FILE}")

if __name__ == "__main__":
    resolve_commits()
