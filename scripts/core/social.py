import requests
import pandas as pd
import os
import time
from datetime import datetime

# --- Config ---
REPO = "bitcoin/bitcoin"
OUTPUT_PATH = "data/core/social_history.parquet"
METADATA_PATH = "data/core/social_metadata.json"
# Use provided token or env var
TOKEN = os.environ.get("GITHUB_TOKEN")

def fetch_metadata(repo, token):
    """
    Fetches high-level repo metadata (Stars, Forks, Subscribers/Watchers)
    so we have accurate totals even if history is truncated.
    """
    headers = {"Authorization": f"token {token}"}
    url = f"https://api.github.com/repos/{repo}"
    
    print(f"Fetching metadata for {repo}...")
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            data = r.json()
            meta = {
                "stars": data.get("stargazers_count", 0),
                "forks": data.get("forks_count", 0),
                "watchers": data.get("subscribers_count", 0), # GitHub API 'watchers_count' is usually stars, 'subscribers_count' is watchers
                "fetched_at": datetime.now().isoformat()
            }
            
            # Save to JSON
            import json
            with open(METADATA_PATH, "w") as f:
                json.dump(meta, f)
            print(f"Saved metadata: {meta}")
            return meta
        else:
            print(f"Error fetching metadata: {r.status_code}")
    except Exception as e:
        print(f"Metadata Exception: {e}")
    return None

def get_star_history(repo, token):
    """
    Fetches stargazer history.
    Note: Enumerating all 70k stars is heavy. 
    We will iterate backwards from the last page? No, GitHub API pagination is strict.
    We iterate forward.
    """
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3.star+json" 
    }
    url = f"https://api.github.com/repos/{repo}/stargazers"
    stars = []
    page = 1
    per_page = 100
    
    print(f"Fetching stars for {repo}...")
    
    while True:
        try:
            params = {"per_page": per_page, "page": page}
            r = requests.get(url, headers=headers, params=params)
            
            if r.status_code != 200:
                print(f"Error: {r.status_code} - {r.text}")
                break
                
            data = r.json()
            if not data:
                break
                
            for s in data:
                # Format: {'starred_at': '2011-...', 'user': ...}
                stars.append({
                    "date": s["starred_at"],
                    "type": "star"
                })
            
            if page % 10 == 0:
                print(f"Fetched {len(stars)} stars...")
                
            # Rate limit handling (Basic)
            remaining = int(r.headers.get("X-RateLimit-Remaining", 10))
            if remaining < 5:
                print("Rate limit low, sleeping...")
                time.sleep(10)
                
            # STOPPING CONDITION FOR DEMO:
            # Fetching 70k stars takes 700 requests. 
            # If we just want "Annual" history, we actually need all of them to plot the curve accurately?
            # Or we can accept a cap.
            # Let's cap at 5000 for the demo to be fast, unless run with FULL_HISTORY=1 env var.
            if len(stars) >= 5000 and not os.environ.get("FULL_HISTORY"):
                 print("Capping at 5000 stars for demo speed. Set FULL_HISTORY=1 for all.")
                 break
            
            page += 1
            
        except Exception as e:
            print(f"Exception: {e}")
            break
            
    return stars

def get_fork_history(repo, token):
    headers = {"Authorization": f"token {token}"}
    url = f"https://api.github.com/repos/{repo}/forks"
    forks = []
    page = 1
    per_page = 100
    
    print(f"Fetching forks for {repo}...")
    
    while True:
        try:
            params = {"per_page": per_page, "page": page, "sort": "oldest"} # Get oldest first if possible? API default is newest.
            # actually sort=oldest helps to build timeline from start.
            
            r = requests.get(url, headers=headers, params=params)
            
            if r.status_code != 200:
                break
                
            data = r.json()
            if not data:
                break
                
            for f in data:
                forks.append({
                    "date": f["created_at"],
                    "type": "fork"
                })
            
            if len(forks) >= 2000 and not os.environ.get("FULL_HISTORY"):
                 print("Capping at 2000 forks for demo speed.")
                 break
                 
            page += 1
        except Exception as e:
            break
            
    return forks

def main():
    if not TOKEN:
        print("Error: GITHUB_TOKEN env var not set.")
        return

    if not TOKEN:
        print("Error: GITHUB_TOKEN env var not set.")
        return

    # 1. Fetch Metadata (Real totals)
    fetch_metadata(REPO, TOKEN)

    # 2. Ingest History (Capped)
    stars = get_star_history(REPO, TOKEN)
    forks = get_fork_history(REPO, TOKEN)
    
    # Combine
    df = pd.DataFrame(stars + forks)
    df["date"] = pd.to_datetime(df["date"])
    
    # Sort
    df = df.sort_values("date")
    
    # Save
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"Saved {len(df)} social events to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
