import os
import json
import time
from dotenv import load_dotenv
import pandas as pd

try:
    from google import genai
    from google.genai.errors import APIError
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

CACHE_FILE = "data/raw/pr_summaries_cache.json"
RELEASES_FILE = "output/tracker/releases.json"

load_dotenv("/Users/saurabhkumar/Desktop/Work/github/orange-dev-data/.env")

api_keys = []
for k, v in os.environ.items():
    if k.startswith("GEMINI_API_KEY") and v.strip():
        api_keys.append(v.strip())

TARGET_MODEL = 'gemini-2.5-flash'

def load_cache(path):
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return {}

def save_cache(path, data):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)

def get_impact_categories(pr_batch):
    if not HAS_GENAI or not api_keys or not pr_batch:
        return {}

    prompt = f"""You are a Bitcoin Core developer analyzing PR summaries.
For each PR in the input JSON below, assign an "impact_category" from one of the following exact options:
- "Security & Consensus"
- "Performance & Optimization"
- "Network & Privacy"
- "Wallet & User Tools"
- "Strategic Initiatives"
- "Maintenance & Tech Debt"

Input JSON:
{json.dumps(pr_batch, indent=2)}

You MUST return a valid JSON object where the keys are the exact PR numbers, and the values are objects containing exactly one key: "impact_category" (string).
Do NOT wrap in markdown blocks, just raw JSON.
"""

    for key_index, current_key in enumerate(api_keys):
        client = genai.Client(api_key=current_key)
        
        for attempt in range(2):
            try:
                response = client.models.generate_content(
                    model=TARGET_MODEL,
                    contents=prompt,
                )
                text = response.text.strip()
                if text.startswith('```json'):
                    text = text[7:-3]
                elif text.startswith('```'):
                    text = text[3:-3]
                
                parsed = json.loads(text.strip())
                return parsed
            except Exception as e:
                time.sleep(2)
                
    return {}

def main():
    pr_cache = load_cache(CACHE_FILE)
    
    # Get all PR numbers currently in releases.json
    with open(RELEASES_FILE, 'r') as f:
        releases = json.load(f)
    
    active_prs = set()
    for rel in releases:
        for pr in rel.get('prs', []):
            active_prs.add(pr['pr'].replace('#', ''))
            
    print(f"Total active PRs in releases.json: {len(active_prs)}")

    batch = {}
    for pr_num, data in pr_cache.items():
        if pr_num in active_prs and "impact_category" not in data:
            batch[pr_num] = data.get("public_summary", "")

    if not batch:
        print("All active PRs already have impact categories!")
        return

    print(f"Found {len(batch)} active PRs needing impact categories.")
    
    items = list(batch.items())
    for i in range(0, len(items), 50):
        chunk = dict(items[i:i+50])
        print(f"Processing chunk {i} to {i+len(chunk)}...")
        
        new_cats = get_impact_categories(chunk)
        if not new_cats:
            print("API limits hit or failed. Saving progress and stopping.")
            break
            
        for k, v in new_cats.items():
            if k in pr_cache:
                pr_cache[k]["impact_category"] = v.get("impact_category", "Maintenance & Tech Debt")
                
        save_cache(CACHE_FILE, pr_cache)
        print("Chunk saved.")
        time.sleep(3)

if __name__ == "__main__":
    main()
