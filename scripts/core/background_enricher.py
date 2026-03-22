
import os
import time
import requests
import subprocess
import json
from datetime import datetime

class RateLimitManager:
    @staticmethod
    def get_status():
        token = os.environ.get("GITHUB_TOKEN")
        if not token:
            return None, 0
        
        url = "https://api.github.com/rate_limit"
        headers = {"Authorization": f"token {token}"}
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                core = resp.json().get("resources", {}).get("core", {})
                remaining = core.get("remaining", 0)
                reset_time = core.get("reset", 0)
                return remaining, reset_time
        except:
            pass
        return None, 0

def run_rebuild():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚙️  Running rebuild cycle...")
    subprocess.run(["python", "code/core/rebuild.py"])

def get_repo_hash():
    try:
        # Check the HEAD of the bitcoin repo
        res = subprocess.run(["git", "-C", "raw_data/bitcoin", "rev-parse", "HEAD"], 
                             capture_output=True, text=True)
        return res.stdout.strip()
    except:
        return None

def main():
    print("🕵️  Bitcoin Dev Tracker Background Enricher Started")
    
    # Track state
    last_processed_hash = None
    enrichment_complete = False
    
    # Load env
    env_path = os.path.join(os.getcwd(), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value

    while True:
        current_hash = get_repo_hash()
        remaining, reset_time = RateLimitManager.get_status()
        
        if remaining is None:
            print("⚠️  No GITHUB_TOKEN found or API error. Sleeping for 1 hour...")
            time.sleep(3600)
            continue
            
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 📊 API: {remaining} | Hash: {current_hash[:8] if current_hash else 'None'}")
        
        # DECISION LOGIC: 
        # Run if: 1. Repo updated (new hash) OR 2. Last enrichment was cut short by rate limit
        should_run = (current_hash != last_processed_hash) or (not enrichment_complete)
        
        if not should_run:
            print("✨ Everything up to date. No new commits and enrichment complete. Sleeping 30 mins...")
            time.sleep(1800)
            continue

        if remaining > 100:
            run_rebuild()
            
            # Post-run check: Did enrichment actually finish?
            # We check the logs or we can assume if we didn't hit rate limit, we are good.
            # For simplicity, we'll check if API calls were still available after run.
            new_remaining, _ = RateLimitManager.get_status()
            
            # If we still have plenty of room, it means enrich.py reached the end of its list.
            if new_remaining and new_remaining > 50:
                enrichment_complete = True
                last_processed_hash = current_hash
            else:
                # We probably hit the 100 limit or rate limit, so we need another pass
                enrichment_complete = False
                print("🔄 Enrichment partially complete. Will resume next cycle.")
                
            print("😴 Cycle complete. Waiting 15 minutes...")
            time.sleep(900)
        else:
            now = time.time()
            wait_time = max(reset_time - now + 60, 60)
            print(f"🛑 Low rate limit. Sleeping for {int(wait_time/60)} minutes...")
            time.sleep(wait_time)

if __name__ == "__main__":
    main()
