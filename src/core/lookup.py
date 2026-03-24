import json
import os
import sys

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import MAINTAINERS_PATH, SPONSORS_PATH

class MaintainerLookup:
    _instance = None
    _email_to_id = {}
    _id_to_record = {}
    _maintainers = []
    
    @classmethod
    def load(cls):
        if cls._instance is not None: return cls._instance
        cls._instance = cls()
        
        if not os.path.exists(MAINTAINERS_PATH):
            print(f"Warning: {MAINTAINERS_PATH} not found.")
            return cls._instance
        
        with open(MAINTAINERS_PATH, "r") as f:
            data = json.load(f)
        
        cls._maintainers = data.get("maintainers", [])
        for m in cls._maintainers:
            cls._id_to_record[m["id"]] = m
            for email in m.get("emails", []):
                cls._email_to_id[email.lower()] = m["id"]
        
        print(f"Loaded {len(cls._maintainers)} maintainers.")
        return cls._instance
    
    @classmethod
    def identify(cls, email):
        mid = cls._email_to_id.get(str(email).lower() if email else "")
        return cls._id_to_record.get(mid) if mid else None

    @classmethod
    def get_all(cls): return cls._maintainers

class SponsorLookup:
    _instance = None
    _email_to_sponsor = {} 
    _sponsors = {} 
    _rules = {} 
    
    @classmethod
    def load(cls):
        if cls._instance is not None: return cls._instance
        cls._instance = cls()
        
        if not os.path.exists(SPONSORS_PATH):
            print(f"Warning: {SPONSORS_PATH} not found.")
            return cls._instance
        
        with open(SPONSORS_PATH, "r") as f:
            data = json.load(f)
        
        for s in data.get("sponsors", []):
            cls._sponsors[s["id"]] = s
        
        for dev in data.get("sponsored_developers", []):
            sponsor_id = dev.get("sponsor_id")
            for email in dev.get("emails", []):
                cls._email_to_sponsor[email.lower()] = sponsor_id
        
        cls._rules = data.get("classification_rules", {})
        print(f"Loaded {len(cls._sponsors)} sponsors.")
        return cls._instance
    
    @classmethod
    def classify(cls, email, company=None):
        email_lower = str(email).lower() if email else ""
        domain = email_lower.split('@')[-1] if '@' in email_lower else ""
        
        # 1. Known sponsored developer
        if email_lower in cls._email_to_sponsor: return "Sponsored"
        # 2. Strategic Corporate Domains
        if domain in cls._rules.get("corporate_domains", []): return "Sponsored"
        # 3. Academic
        if domain in cls._rules.get("academic_domains", []): return "Institutional"
        # 4. Enriched company
        if company and isinstance(company, str) and len(company.strip()) > 1: return "Corporate"
        
        return "Personal"
