import pandas as pd
import os
import json
import time
import requests
import clean
from process import DataFactory, Config

class EnrichmentCache:
    CACHE_FILE = "data/cache/enrichment_cache.json"
    
    @staticmethod
    def load():
        if os.path.exists(EnrichmentCache.CACHE_FILE):
            try:
                with open(EnrichmentCache.CACHE_FILE, "r") as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading cache: {e}")
                return {}
        return {}
        
    @staticmethod
    def save(cache):
        # Merge with existing file to avoid racing or losing data if multiple processes run
        # Though currently it's sequential, it's safer.
        existing = EnrichmentCache.load()
        existing.update(cache)
        with open(EnrichmentCache.CACHE_FILE, "w") as f:
            json.dump(existing, f, indent=2)

class GitHubAPI:
    TOKEN = os.environ.get("GITHUB_TOKEN")
    HEADERS = {"Authorization": f"token {TOKEN}"} if TOKEN else {}
    
    @staticmethod
    def search_user(query, key_type="email"):
        if not GitHubAPI.TOKEN:
            if Config.DATA_DIR == "data/core": # Only warn once or if specifically relevant
                 pass 
            return None
            
        print(f"  API Call: Searching {key_type}='{query}'...")
        url = f"https://api.github.com/search/users?q={query} in:{key_type}&per_page=1"
        
        try:
            resp = requests.get(url, headers=GitHubAPI.HEADERS)
            if resp.status_code == 200:
                data = resp.json()
                if data['total_count'] > 0:
                    item = data['items'][0]
                    # Get full details for company/location/followers
                    return GitHubAPI.get_user_details(item['login'])
            elif resp.status_code == 403:
                print("  API Rate Limit Hit. Skipping further calls.")
                return "RATE_LIMIT"
        except Exception as e:
            print(f"  API Error: {e}")
            
        time.sleep(2) # Stricter delay for Search API to avoid 403
        return None

    @staticmethod
    def get_user_details(username):
        if not username: return None
        url = f"https://api.github.com/users/{username}"
        try:
            resp = requests.get(url, headers=GitHubAPI.HEADERS)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 403:
                return "RATE_LIMIT"
        except:
            pass
        time.sleep(0.02) # Respectful 20ms delay for high-rate API (allow 50/sec)
        return None

class Enricher:
    LEGACY_FILE = "data/core/bitcoin_contributors_data.parquet"
    OUTPUT_FILE = "data/core/contributors_enriched.parquet"

    @staticmethod
    def load_legacy():
        if not os.path.exists(Enricher.LEGACY_FILE):
            print(f"Warning: Legacy file {Enricher.LEGACY_FILE} not found. Skipping enrichment.")
            return None
        return pd.read_parquet(Enricher.LEGACY_FILE)

    @staticmethod
    def enrich(commits):
        print("Starting enrichment...")
        legacy_df = Enricher.load_legacy()
        cache = EnrichmentCache.load()
        
        if legacy_df is None:
            return None

        # 1. Build Lookup Maps
        email_map = {}
        name_map = {}
        
        for idx, row in legacy_df.iterrows():
            if pd.notna(row.get('Email')):
                emails = [e.strip().lower() for e in str(row['Email']).split(',')]
                for e in emails:
                    if e: email_map[e] = row
            if pd.notna(row.get('Name')):
                names = [n.strip().lower() for n in str(row['Name']).split(',')]
                for n in names:
                    if n: name_map[n] = row
                    
        print(f"Legacy Index: {len(email_map)} emails, {len(name_map)} names.")

        # 2. Iterate Canonical Groups (PRIORITIZED BY IMPACT)
        contributors = []
        # 2. Sort Canonical IDs to prioritize unidentified/recent contributors (STRICT for 2025+)
        grouped = commits.groupby('canonical_id')
        
        # Calculate scores for sorting
        id_scores = []
        for cid, group in grouped:
            c_emails = set(group['author_email'].str.lower().dropna())
            canonical_name = group.iloc[0]['canonical_name']
            
            has_legacy = any(e in email_map for e in c_emails)
            has_cache = any(e in cache for e in c_emails)
            max_year = group['date_utc'].dt.year.max()
            
            # PRIORITIZE: 2025/2026 lack of location
            priority = 0
            if max_year >= 2025: priority += 10000 
            if not has_legacy: priority += 1000
            if not has_cache: priority += 500
            priority += (max_year - 2000) 
            
            id_scores.append({"cid": cid, "priority": priority})
            
        sorted_ids = [item['cid'] for item in sorted(id_scores, key=lambda x: x['priority'], reverse=True)]
        
        count_mapped = 0
        api_calls_made = 0
        search_limit_hit = False
        core_limit_hit = False
        
        # Load Whitelists once
        maintainers = []
        if os.path.exists("data/cache/maintainers_lookup.json"):
            with open("data/cache/maintainers_lookup.json", "r") as f:
                maintainers = json.load(f).get("maintainers", [])
        
        sponsored = []
        if os.path.exists("data/cache/sponsors_lookup.json"):
            with open("data/cache/sponsors_lookup.json", "r") as f:
                sponsored = json.load(f).get("sponsored_developers", [])

        for cid in sorted_ids:
            group = grouped.get_group(cid)
            c_emails = set(group['author_email'].str.lower().dropna())
            c_names = set(group['author_name'].str.lower().dropna())
            canonical_name = group.iloc[0]['canonical_name']
            
            entry = {
                "canonical_id": cid,
                "name": canonical_name,
                "login": None,
                "location": None,
                "company": None,
                "followers": 0,
                "is_enriched": False
            }
            
            # --- STRATEGY 1: LEGACY DATA ---
            match_row = None
            for e in c_emails:
                if e in email_map: match_row = email_map[e]; break
            if match_row is None:
                for n in c_names:
                    if n in name_map: match_row = name_map[n]; break
                        
            if match_row is not None:
                entry["login"] = match_row.get("Login")
                entry["location"] = match_row.get("Location")
                entry["company"] = match_row.get("Company")
                entry["followers"] = int(match_row.get("Followers", 0)) if pd.notna(match_row.get("Followers")) else 0
                entry["is_enriched"] = True
                
            # --- STRATEGY 2: WHITELISTS ---
            for m in maintainers:
                if m.get("name") == canonical_name or any(e in c_emails for e in m.get("emails", [])):
                    entry["login"] = entry["login"] or m.get("github")
                    entry["is_enriched"] = True
            
            for s in sponsored:
                if s.get("canonical_name") == canonical_name or any(e in c_emails for e in s.get("emails", [])):
                    entry["login"] = entry["login"] or s.get("github")
                    entry["is_enriched"] = True

            # --- STRATEGY 3: MANUAL OVERRIDES ---
            MANUAL_OVERRIDES = {
                "Wladimir J. van der Laan": {"login": "laanwj", "location": "Netherlands"},
                "Wladimir van der Laan": {"login": "laanwj", "location": "Netherlands"},
                "Marco Falke": {"login": "MarcoFalke"},
                "Satoshi Nakamoto": {"login": "satoshi", "location": "P2P Space"},
                "Salvatore Ingala": {"login": "bigspider", "location": "Switzerland"},
                "Calin Culianu": {"login": "cculianu", "location": "USA"},
                "Gleb Naumenko": {"login": "rkrux", "location": "USA"},
                "rkrux": {"login": "rkrux", "location": "USA"},
                "Musa Haruna": {"login": "Moozay", "location": "Morocco"}
            }
            if canonical_name in MANUAL_OVERRIDES:
                ov = MANUAL_OVERRIDES[canonical_name]
                for k, v in ov.items(): entry[k] = v
                entry["is_enriched"] = True

            # --- STRATEGY 4: API/CACHE FALLBACK ---
            ENABLE_API = True 
            MAX_API_CALLS = 1500 # Full sweep

            # Condition for refresh: No login OR no location (if we want more regions)
            needs_details = (pd.isna(entry["login"]) or pd.isna(entry["location"]))
            
            if needs_details:
                cached_data = None
                for e in c_emails:
                    if e in cache: cached_data = cache[e]; break
                
                if cached_data:
                    # Apply cached data
                    if cached_data.get("login") and cached_data.get("login") != "Not Found":
                        entry["login"] = entry["login"] or cached_data.get("login")
                        entry["location"] = entry["location"] or cached_data.get("location")
                        entry["company"] = entry["company"] or cached_data.get("company")
                        entry["followers"] = entry["followers"] or cached_data.get("followers", 0)
                        entry["is_enriched"] = True
                    
                    # RECOVERY LOGIC: If we still lack location, refresh ONLY if we haven't 
                    # successfully scanned this profile yet (indicated by verified_at)
                    needs_refresh = (pd.isna(entry["location"]) and "verified_at" not in cached_data)
                else:
                    needs_refresh = True

                # 4b. API Call 
                # We split limits: Search API (Strict) vs Core API (Generous 5000/hr)
                if ENABLE_API and needs_refresh and not core_limit_hit and GitHubAPI.TOKEN and api_calls_made < MAX_API_CALLS:
                    result = None
                    
                    # Try 1: Direct Username from noreply email (High Rate Limit)
                    for e in c_emails:
                        if "users.noreply.github.com" in e:
                             username = e.split('@')[0]
                             if '+' in username: username = username.split('+')[1]
                             if username:
                                 print(f"  Direct lookup (noreply): {username}")
                                 result = GitHubAPI.get_user_details(username)
                                 if result: break
                    
                    # Try 1b: Direct lookup by Name if it looks like a login (High Rate Limit)
                    if not result and canonical_name and " " not in canonical_name and len(canonical_name) > 3:
                         print(f"  Direct lookup (handle-like name): {canonical_name}")
                         result = GitHubAPI.get_user_details(canonical_name)

                    # Try 1c: Direct lookup by existing Login (High Rate Limit)
                    if not result and entry["login"] and entry["login"] != "Anonymous":
                        print(f"  Deep profile refresh for legacy login: {entry['login']}")
                        result = GitHubAPI.get_user_details(entry["login"])
                    
                    # Check for Core Rate Limit
                    if result == "RATE_LIMIT": 
                        core_limit_hit = True
                        result = None

                    # Try 2: Search by Email (Stricter Rate Limit - 30/min)
                    if not result and not search_limit_hit:
                        for e in c_emails:
                            if "users.noreply" in e: continue
                            result = GitHubAPI.search_user(e, "email")
                            if result: break
                        
                        if result == "RATE_LIMIT":
                            search_limit_hit = True
                            result = None
                    
                    # Try 3: Search by Name (Stricter Rate Limit)
                    if not result and not search_limit_hit:
                        result = GitHubAPI.search_user(canonical_name, "user")
                        if result == "RATE_LIMIT":
                            search_limit_hit = True
                            result = None
                    
                    if result and result != "RATE_LIMIT":
                        entry["login"] = entry["login"] or result.get("login")
                        entry["location"] = entry["location"] or result.get("location")
                        entry["company"] = entry["company"] or result.get("company")
                        entry["followers"] = entry["followers"] or result.get("followers", 0)
                        entry["is_enriched"] = True
                        
                        # Cache enrichment (with timestamp)
                        res_dict = {
                            "login": result.get("login"),
                            "location": result.get("location") or "Undisclosed",
                            "company": result.get("company"),
                            "followers": result.get("followers", 0),
                            "verified_at": time.time()
                        }
                        for e in c_emails: cache[e] = res_dict
                        api_calls_made += 1
                        count_mapped += 1
                    
                    elif not result and not search_limit_hit and not core_limit_hit:
                        # Mark as Not Found in cache to avoid re-searching
                        for e in c_emails:
                            cache[e] = {"login": "Not Found", "verified_at": time.time()}
                        api_calls_made += 1

            if entry["is_enriched"]:
                count_mapped += 1
            contributors.append(entry)
            
        print(f"Enriched {count_mapped} out of {len(contributors)} contributors.")
        if api_calls_made > 0:
            print(f"Made {api_calls_made} API calls. Updating cache.")
            EnrichmentCache.save(cache)
        
        enriched_df = pd.DataFrame(contributors)
        enriched_df.to_parquet(Enricher.OUTPUT_FILE, index=False)
        print(f"Saved enriched data to {Enricher.OUTPUT_FILE}")
        
        return enriched_df

def main():
    commits, _ = DataFactory.load()
    commits = clean.Consolidator.normalize(commits)
    Enricher.enrich(commits)

if __name__ == "__main__":
    main()
