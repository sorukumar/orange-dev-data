import re

path = "scripts/02_process/resolve_commits.py"
with open(path, "r") as f:
    code = f.read()

# Replace SponsorLookup
new_lookup = """class SponsorLookup:
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
        return None"""

code = re.sub(r'class SponsorLookup:.*?return None', new_lookup, code, flags=re.DOTALL)

# Update map_sponsor
old_map = """    def map_sponsor(row):
        email = row['target_email']
        cid = row['canonical_id']
        company = id_to_company.get(cid)
        classification = SponsorLookup.classify(email, enrich_company=company)
        sponsor_name = SponsorLookup.get_sponsor_name(email)
        return pd.Series([classification, sponsor_name])"""

new_map = """    def map_sponsor(row):
        email = row['target_email']
        cid = row['canonical_id']
        date_utc = row.get('date_utc')
        company = id_to_company.get(cid)
        classification = SponsorLookup.classify(email, date_utc, enrich_company=company)
        sponsor_name = SponsorLookup.get_sponsor_name(email, date_utc)
        return pd.Series([classification, sponsor_name])"""

code = code.replace(old_map, new_map)

with open(path, "w") as f:
    f.write(code)

print("Patched resolve_commits.py")
