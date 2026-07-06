import os
import json
import time
from dotenv import load_dotenv

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

def main():
    print("Starting script...")
    pr_cache = load_cache(CACHE_FILE)
    
    with open(RELEASES_FILE, 'r') as f:
        releases = json.load(f)
    
    active_prs = set()
    for rel in releases:
        for pr in rel.get('prs', []):
            active_prs.add(pr['pr'].replace('#', ''))
            
    batch = {}
    for pr_num in active_prs:
        if pr_num in pr_cache:
            batch[pr_num] = pr_cache[pr_num].get("public_summary", "")

    items = list(batch.items())[:5]
    print(f"Testing with {len(items)} PRs")
    
    prompt = f"""You are a Bitcoin Core developer analyzing PR summaries.
For each PR in the input JSON below, assign an "impact_category" from one of the following exact options based on these strict guidelines:
- "Security & Consensus": ONLY for consensus rules, soft/hard forks, and Denial of Service (DoS) protections or security vulnerabilities.
- "Performance & Optimization": IBD speedups, memory usage reduction, cryptography speedups.
- "Network & Privacy": P2P relay policies, Tor/I2P, transaction privacy.
- "Wallet & User Tools": RPCs, GUI, PSBTs, hardware wallets, coin selection.
- "Strategic Initiatives": Major architectural milestones (e.g., Cluster Mempool, v3 Tx, Kernel).
- "Maintenance & Tech Debt": ONLY use this for routine CI updates, pure tests, documentation, and pure refactoring. If it changes behavior or protection bounds, it is NOT maintenance.

Input JSON:
{json.dumps(dict(items), indent=2)}

You MUST return a valid JSON object where the keys are the exact PR numbers, and the values are objects containing exactly one key: "impact_category" (string).
Do NOT wrap in markdown blocks, just raw JSON.
"""
    try:
        client = genai.Client(api_key=api_keys[0])
        print("Calling API...")
        response = client.models.generate_content(model=TARGET_MODEL, contents=prompt)
        print("API Response:", response.text)
    except Exception as e:
        print("API Error:", str(e))

if __name__ == "__main__":
    main()
