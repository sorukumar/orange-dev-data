import pandas as pd
import json
import os
import sys
from datetime import datetime, timedelta

# Dynamically add parent directories to path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    
PROCESS_DIR = os.path.join(PROJECT_ROOT, "scripts", "02_process")
if PROCESS_DIR not in sys.path:
    sys.path.append(PROCESS_DIR)

import footprint  # type: ignore
from scripts.utils.identity import resolver

# --- Configuration ---
class Config:
    COMMITS_FILE = "data/enriched/commits_resolved.parquet"
    UNIFIED_FILE = "data/enriched/contributors_unified.parquet"
    MAINTAINERS_FILE = "metadata/maintainers.json"
    SPONSORS_FILE = "data/enriched/sponsors_merged.json"
    OUTPUT_DIR = "output/shared/maintainers"
    OUTPUT_FILE = f"{OUTPUT_DIR}/stats_maintainers.json"

# --- Maintainer Lookup ---
class MaintainerLookup:
    _instance = None
    _email_to_id = {}
    _id_to_record = {}
    _maintainers = []
    
    @classmethod
    def load(cls):
        if cls._instance is not None:
            return cls._instance
        
        cls._instance = cls()
        
        if not os.path.exists(Config.MAINTAINERS_FILE):
            print(f"Warning: {Config.MAINTAINERS_FILE} not found.")
            return cls._instance
        
        with open(Config.MAINTAINERS_FILE, "r") as f:
            data = json.load(f)
        
        cls._maintainers = data.get("maintainers", [])
        
        for m in cls._maintainers:
            cls._id_to_record[m["id"]] = m
            for email in m.get("emails", []):
                cls._email_to_id[email.lower()] = m["id"]
            if m.get("github"):
                cls._email_to_id[m["github"].lower()] = m["id"]
        
        return cls._instance
    
    @classmethod
    def get_all_maintainers(cls):
        return cls._maintainers

# --- Sponsor Lookup ---
class SponsorLookup:
    _instance = None
    _email_to_sponsor = {}
    _sponsors = {}
    
    @classmethod
    def load(cls):
        if cls._instance is not None:
            return cls._instance
        
        cls._instance = cls()
        
        if not os.path.exists(Config.SPONSORS_FILE):
            print(f"Warning: {Config.SPONSORS_FILE} not found.")
            return cls._instance
        
        with open(Config.SPONSORS_FILE, "r") as f:
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
    def get_sponsor_name(cls, email, commit_date_str=None):
        email_lower = email.lower() if email else ""
        sponsor_id = cls.get_sponsor_id_for_date(email_lower, commit_date_str)
        if sponsor_id and sponsor_id in cls._sponsors:
            return cls._sponsors[sponsor_id].get("name")
        return None

def main():
    print("🚀 Starting Maintainer Artifact Generation Pipeline...")
    
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    
    # Load inputs
    if not os.path.exists(Config.COMMITS_FILE):
        raise FileNotFoundError(f"Missing {Config.COMMITS_FILE}")
    commits = pd.read_parquet(Config.COMMITS_FILE)
    
    # Filter commits to cutoff (monthly data logic)
    from datetime import timezone as dt_timezone
    now = datetime.now(dt_timezone.utc)
    first_day_curr = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    cutoff_date = first_day_curr - timedelta(seconds=1)
    
    commits['date_utc'] = pd.to_datetime(commits['date_utc'], utc=True)
    commits = commits[commits['date_utc'] <= cutoff_date]
    
    # Determine cohort years based on integration date
    if 'integration_date' in commits.columns:
        commits['integration_date'] = pd.to_datetime(commits['integration_date'], utc=True)
        commits['year'] = commits['integration_date'].dt.year
        gap_days = (commits['integration_date'] - commits['date_utc']).dt.days
        commits.loc[gap_days > 1095, 'year'] = commits.loc[gap_days > 1095, 'date_utc'].dt.year
    else:
        commits['year'] = commits['date_utc'].dt.year
        
    enrich_map = {}
    if os.path.exists(Config.UNIFIED_FILE):
        enriched_df = pd.read_parquet(Config.UNIFIED_FILE)
        enrich_map = enriched_df.set_index('uuid').to_dict(orient='index')
        
    reviews_file = "data/enriched/enriched_reviews.parquet"
    reviews_df = pd.DataFrame()
    if os.path.exists(reviews_file):
        reviews_df = pd.read_parquet(reviews_file)
        # Filter to active reviews (commented/reviewed) and drop duplicates per PR
        ACTIVE_REVIEW_TYPES = {'commented', 'reviewed'}
        if 'event_type' in reviews_df.columns:
            reviews_df = reviews_df[reviews_df['event_type'].isin(ACTIVE_REVIEW_TYPES)]
        reviews_df['timestamp'] = pd.to_datetime(reviews_df['timestamp'], utc=True)
        # Deduplicate to count unique PRs reviewed, or just total review interactions? Let's stick to unique PRs per person
        reviews_df = reviews_df.drop_duplicates(subset=['uuid', 'repository_name', 'pr_number'])
        
    MaintainerLookup.load()
    SponsorLookup.load()
    
    # Footprint Analysis
    print("Running Footprint Analysis...")
    try:
        footprints = footprint.run_footprint_analysis("data/sources/bitcoin", Config.MAINTAINERS_FILE, "data/enriched/maintainer_footprints.json")
    except Exception as e:
        print(f"  Warning: Footprint analysis failed: {e}")
        footprints = {}
        
    # Process all maintainers (both Core and Ecosystem)
    maintainers = MaintainerLookup.get_all_maintainers()
    maintainer_profiles = []
    
    # Merges count mapping
    maintainer_commits = commits[commits['is_merge'] == True]
    
    for m in maintainers:
        m_id = m.get("id")
        m_name_raw = m.get("name", m_id)
        
        # Resolve identity
        uuid = resolver.resolve_git(m_name_raw, None)
        record = next((r for r in resolver._identities if r["uuid"] == uuid), None)
        m_name = record["display_name"] if record else m_name_raw
        
        m_status = m.get("status", "unknown")
        m_type = m.get("type", "core")
        emails = m.get("emails", [])
        
        # Find Sponsor
        sponsor_name = None
        for email in emails:
            sponsor_name = SponsorLookup.get_sponsor_name(email)
            if sponsor_name:
                break
                
        # Find canonical ID and commits
        cid = None
        for email in emails:
            email_lower = email.lower()
            match = commits[commits['committer_email'].str.lower() == email_lower]
            if not match.empty:
                cid = match.iloc[0]['canonical_id']
                break
                
        if not sponsor_name and cid:
            company = enrich_map.get(cid, {}).get('company')
            if company and len(str(company).strip()) > 1:
                sponsor_name = company
                
        # Calculate active years from segments/dates
        active_years = []
        if m.get("segments"):
            for seg in m["segments"]:
                start_yr = int(seg["start"].split('-')[0])
                end_yr = int(seg["end"].split('-')[0]) if "end" in seg else commits['year'].max()
                active_years.extend(list(range(start_yr, end_yr + 1)))
        elif m.get("role") and m["role"].get("appointed"):
            start_yr = int(m["role"]["appointed"].split('-')[0])
            end_yr = int(m["role"]["stepped_down"].split('-')[0]) if "stepped_down" in m["role"] else commits['year'].max()
            active_years.extend(list(range(start_yr, end_yr + 1)))
            
        active_years = sorted(list(set(active_years)))
        
        # Merges count
        emails_lower = [e.lower() for e in emails]
        m_actions = maintainer_commits[maintainer_commits['committer_email'].str.lower().isin(emails_lower)]
        # Deduplicate by hash to avoid double-counting across categories/repositories
        m_actions_dedup = m_actions.drop_duplicates(subset=['hash'])
        merges_count = len(m_actions_dedup)
        
        # Split by tier1 (core) and tier2 (ecosystem) repositories
        tier1_repos = {'bitcoin/bitcoin', 'bitcoin-core/secp256k1', 'bitcoin-core/gui'}
        tier2_repos = {'bitcoin-core/guix.sigs', 'bitcoin-core/qa-assets', 'bitcoin-core/HWI'}
        
        m_actions_core = m_actions_dedup[m_actions_dedup['repository_name'].isin(tier1_repos)]
        m_actions_eco = m_actions_dedup[m_actions_dedup['repository_name'].isin(tier2_repos)]
        
        merges_core = len(m_actions_core)
        merges_ecosystem = len(m_actions_eco)
        
        # Path to maintainer metrics
        appointment_date = None
        if m.get("role") and m["role"].get("appointed"):
            appointment_date = m["role"]["appointed"]
        elif m.get("segments") and m["segments"]:
            appointment_date = m["segments"][0]["start"]
            
        first_active_year = None
        prior_authored_commits = 0
        post_authored_commits = 0
        prior_review_count = 0
        post_review_count = 0
        self_merges = 0
        
        if cid:
            enrich_data = enrich_map.get(cid, {})
            self_merges = int(enrich_data.get('self_merges', 0)) if pd.notna(enrich_data.get('self_merges')) else 0
            global_first = enrich_data.get('global_first_active')
            if pd.notna(global_first):
                first_active_year = int(pd.to_datetime(global_first).year)
            else:
                cid_commits = commits[commits['canonical_id'] == cid]
                if not cid_commits.empty:
                    first_active_year = int(cid_commits['date_utc'].min().year)
                    
            if appointment_date:
                app_ts = pd.to_datetime(appointment_date, utc=True)
                cid_commits = commits[commits['canonical_id'] == cid]
                cid_authored = cid_commits[cid_commits['is_merge'] == False]
                # Deduplicate by hash to avoid double-counting across categories/repositories
                cid_authored_dedup = cid_authored.drop_duplicates(subset=['hash'])
                
                prior_authored = cid_authored_dedup[cid_authored_dedup['date_utc'] < app_ts]
                prior_authored_commits = len(prior_authored)
                
                post_authored = cid_authored_dedup[cid_authored_dedup['date_utc'] >= app_ts]
                post_authored_commits = len(post_authored)
        
        # Calculate pre/post reviews based on UUID
        if uuid and not reviews_df.empty:
            uuid_reviews = reviews_df[reviews_df['uuid'] == uuid]
            if appointment_date:
                app_ts = pd.to_datetime(appointment_date, utc=True)
                prior_review_count = len(uuid_reviews[uuid_reviews['timestamp'] < app_ts])
                post_review_count = len(uuid_reviews[uuid_reviews['timestamp'] >= app_ts])
            else:
                prior_review_count = len(uuid_reviews)
                post_review_count = 0
                
        # Profile compilation
        profile = {
            "id": m_id,
            "name": m_name,
            "status": m_status,
            "type": m_type,
            "sponsor": sponsor_name if sponsor_name else "Independent",
            "active_years": [int(y) for y in active_years],
            "merges_count": merges_count,
            "merges_core": merges_core,
            "merges_ecosystem": merges_ecosystem,
            "merges_active": merges_count > 0 or m_status == 'active',
            "merge_authority": m.get("merge_authority", True),
            "first_active_year": first_active_year,
            "prior_authored_commits": prior_authored_commits,
            "post_authored_commits": post_authored_commits,
            "prior_review_count": prior_review_count,
            "post_review_count": post_review_count,
            "self_merges": self_merges
        }
        
        if m.get("role"):
            profile["role"] = m["role"]
        if m.get("gpg_fingerprint"):
            profile["gpg_fingerprint"] = m["gpg_fingerprint"]
        if m.get("evidence"):
            profile["evidence"] = m["evidence"]
        if m.get("segments"):
            profile["segments"] = m["segments"]
        if m_id in footprints:
            profile["footprint"] = footprints[m_id].get("top_areas", {})
            
        maintainer_profiles.append(profile)

    # Separate Core and Ecosystem profiles
    core_profiles = [p for p in maintainer_profiles if p["type"] == "core"]
    ecosystem_profiles = [p for p in maintainer_profiles if p["type"] == "ecosystem"]
    
    # Active Core Maintainers (for pie chart counts)
    active_core = [p for p in core_profiles if p["status"] == "active"]
    all_core = [p for p in core_profiles]
    
    sponsor_counts_active = {}
    for m in active_core:
        s = m["sponsor"]
        sponsor_counts_active[s] = sponsor_counts_active.get(s, 0) + 1
        
    sponsor_counts_all = {}
    for m in all_core:
        s = m["sponsor"]
        sponsor_counts_all[s] = sponsor_counts_all.get(s, 0) + 1
        
    # JSON output structure
    output_data = {
        "title": "Maintainer Independence",
        "subtitle": "Who funds the gatekeepers?",
        "active": {
            "total": len(active_core),
            "by_sponsor": [{"name": k, "value": v} for k, v in sorted(sponsor_counts_active.items(), key=lambda x: -x[1])]
        },
        "all_time": {
            "total": len(all_core),
            "by_sponsor": [{"name": k, "value": v} for k, v in sorted(sponsor_counts_all.items(), key=lambda x: -x[1])]
        },
        "maintainers": core_profiles,
        "ecosystem_committers": ecosystem_profiles
    }
    
    with open(Config.OUTPUT_FILE, "w") as f:
        json.dump(output_data, f)
        
    print(f"✅ Generated maintainers data: {len(core_profiles)} core, {len(ecosystem_profiles)} ecosystem.")
    print(f"Saved to {Config.OUTPUT_FILE}")

if __name__ == "__main__":
    main()
