import pandas as pd
import os
import json
import time
import requests
import sys

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import WORK_DIR, CACHE_DIR, MAINTAINERS_PATH, SPONSORS_PATH
from src.core.identity import Consolidator

# --- Configuration (Centralized via paths.py) ---
CACHE_FILE = os.path.join(CACHE_DIR, "enrichment_cache.json")
LEGACY_FILE = os.path.join(WORK_DIR, "core", "bitcoin_contributors_data.parquet")
OUTPUT_FILE = os.path.join(WORK_DIR, "core", "contributors_enriched.parquet")
TOKEN = os.environ.get("GITHUB_TOKEN")

class EnrichmentCache:
    @staticmethod
    def load():
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "r") as f: return json.load(f)
            except: return {}
        return {}
        
    @staticmethod
    def save(cache):
        existing = EnrichmentCache.load()
        existing.update(cache)
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump(existing, f, indent=2)

class GitHubAPI:
    HEADERS = {"Authorization": f"token {TOKEN}"} if TOKEN else {}
    
    @staticmethod
    def get_details(username):
        if not TOKEN or not username: return None
        url = f"https://api.github.com/users/{username}"
        try:
            resp = requests.get(url, headers=GitHubAPI.HEADERS)
            if resp.status_code == 200: return resp.json()
        except: pass
        return None

def main():
    print("--- GitHub Profile Enrichment (New Architecture) ---")
    if not os.path.exists(os.path.join(WORK_DIR, "core", "commits.parquet")):
        print("Error: commits.parquet not found. Run repository ingestion first.")
        return

    commits = pd.read_parquet(os.path.join(WORK_DIR, "core", "commits.parquet"))
    commits = Consolidator.normalize(commits)
    
    cache = EnrichmentCache.load()
    output = []
    
    # Simple strategy: prioritize whitelisted maintainers/sponsors
    # In a full run, we would iterate and call API (omitted here for speed/demo)
    print(f"Enriching {commits['canonical_id'].nunique()} unique humans...")
    
    for cid, group in commits.groupby('canonical_id'):
        author_name = group.iloc[0]['canonical_name']
        output.append({
            "canonical_id": cid,
            "name": author_name,
            "login": cache.get(str(cid), {}).get("login"),
            "location": cache.get(str(cid), {}).get("location"),
            "is_enriched": str(cid) in cache
        })
        
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    pd.DataFrame(output).to_parquet(OUTPUT_FILE, index=False)
    print(f"Saved enriched profiles to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
