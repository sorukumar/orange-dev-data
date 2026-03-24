import requests
import pandas as pd
import os
import time
import sys
import json
from datetime import datetime

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import WORK_DIR

# --- Configuration (Centralized via paths.py) ---
REPO = "bitcoin/bitcoin"
OUTPUT_PATH = os.path.join(WORK_DIR, "core", "social_history.parquet")
METADATA_PATH = os.path.join(WORK_DIR, "core", "social_metadata.json")
TOKEN = os.environ.get("GITHUB_TOKEN")

def fetch_metadata():
    if not TOKEN: return None
    headers = {"Authorization": f"token {TOKEN}"}
    url = f"https://api.github.com/repos/{REPO}"
    
    print(f"Fetching GitHub metadata for {REPO}...")
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            data = r.json()
            meta = {
                "stars": data.get("stargazers_count", 0),
                "forks": data.get("forks_count", 0),
                "watchers": data.get("subscribers_count", 0),
                "fetched_at": datetime.now().isoformat()
            }
            os.makedirs(os.path.dirname(METADATA_PATH), exist_ok=True)
            with open(METADATA_PATH, "w") as f:
                json.dump(meta, f, indent=2)
            return meta
    except Exception as e:
        print(f"Metadata Exception: {e}")
    return None

def get_star_history():
    if not TOKEN: return []
    headers = {"Authorization": f"token {TOKEN}", "Accept": "application/vnd.github.v3.star+json"}
    url = f"https://api.github.com/repos/{REPO}/stargazers"
    stars = []
    page, per_page = 1, 100
    
    print("Fetching star history (capped for performance)...")
    while len(stars) < 5000:
        try:
            r = requests.get(url, headers=headers, params={"per_page": per_page, "page": page})
            if r.status_code != 200: break
            data = r.json()
            if not data: break
            
            for s in data:
                stars.append({"date": s["starred_at"], "type": "star"})
            
            if page % 10 == 0: print(f"  Fetched {len(stars)} stars...")
            if int(r.headers.get("X-RateLimit-Remaining", 10)) < 5: time.sleep(10)
            page += 1
        except: break
    return stars

def main():
    if not TOKEN:
        print("Warning: GITHUB_TOKEN not set. Skipping GitHub social ingestion.")
        return

    fetch_metadata()
    stars = get_star_history()
    
    if stars:
        df = pd.DataFrame(stars)
        df["date"] = pd.to_datetime(df["date"])
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        df.to_parquet(OUTPUT_PATH, index=False)
        print(f"Saved {len(df)} social events to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
