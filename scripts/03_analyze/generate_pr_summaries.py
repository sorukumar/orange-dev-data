import os
import json
import pandas as pd
import time
from dotenv import load_dotenv

try:
    from google import genai
    from google.genai.errors import APIError
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

INPUT_PR_PARQUET = "data/raw/github_pr_metadata.parquet"
CACHE_FILE = "data/raw/pr_summaries_cache.json"

# Load multiple API keys from .env
load_dotenv("/Users/saurabhkumar/Desktop/Work/github/orange-dev-data/.env")

api_keys = []
for k, v in os.environ.items():
    if k.startswith("GEMINI_API_KEY") and v.strip():
        api_keys.append(v.strip())

if not api_keys:
    print("Warning: No GEMINI_API_KEY found in .env")

# The BEST model available for the free tier constraints
TARGET_MODEL = 'gemini-2.5-flash'

def load_cache(path):
    if os.path.exists(path):
        with open(path, 'r') as f:
            try:
                return json.load(f)
            except:
                return {}
    return {}

def save_cache(path, data):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)

def get_bulk_llm_summaries_with_rotation(pr_dict):
    """
    Takes a dictionary {pr_num: title}
    Rotates through API keys to bypass rate limits.
    """
    if not HAS_GENAI or not api_keys or not pr_dict:
        return {}

    prompt = f"""You are a Bitcoin Core developer acting as a technical writer.
Analyze this dictionary of Pull Requests where the key is the PR number and the value is the PR title.

Input Data:
{json.dumps(pr_dict, indent=2)}

You MUST return a valid JSON object where the keys are the exact PR numbers, and the values are objects containing exactly three keys:
1. "public_summary": Exactly 1-2 short sentences. Explain what this PR is, what it does, and why it adds value to Bitcoin Core.
2. "technical_summary": A detailed 3-5 sentence summary for engineers explaining the architectural implementation.
3. "impact_category": MUST be exactly one of the following strings based on these strict guidelines:
   - "Security & Consensus": ONLY for consensus rules, soft/hard forks, and Denial of Service (DoS) protections or security vulnerabilities.
   - "Performance & Optimization": IBD speedups, memory usage reduction, cryptography speedups.
   - "Network & Privacy": P2P relay policies, Tor/I2P, transaction privacy.
   - "Wallet & User Tools": RPCs, GUI, PSBTs, hardware wallets, coin selection.
   - "Strategic Initiatives": Major architectural milestones (e.g., Cluster Mempool, v3 Tx, Kernel).
   - "Maintenance & Tech Debt": ONLY use this for routine CI updates, pure tests, documentation, and pure refactoring. If it changes behavior or protection bounds, it is NOT maintenance.

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
            
            except APIError as e:
                # If we hit a quota or model not found error, break out of the attempt loop and try the NEXT API key.
                if e.code == 429 or e.code == 404:
                    print(f"Key {key_index + 1} hit {e.code}. Rotating to next key...")
                    break # Break inner loop, go to next key
                else:
                    print(f"Error on Key {key_index + 1} (attempt {attempt+1}): {e}")
                    time.sleep(2)
            except Exception as e:
                print(f"Unexpected Error on Key {key_index + 1} (attempt {attempt+1}): {e}")
                time.sleep(2)
                
    # If we exhausted all keys
    print("All API keys exhausted or rate limited.")
    return {}

def run_summarizer():
    if not os.path.exists(INPUT_PR_PARQUET):
        print(f"{INPUT_PR_PARQUET} does not exist.")
        return

    print(f"Found {len(api_keys)} API Keys loaded.")

    pr_cache = load_cache(CACHE_FILE)
    
    import re
    def parse_version(v_str):
        matches = re.findall(r'\d+', str(v_str))
        if not matches:
            return (0, 0, 0)
        ints = [int(m) for m in matches]
        while len(ints) < 3:
            ints.append(0)
        return tuple(ints[:3])
        
    def is_high_signal(labels_str, is_recent):
        if pd.isna(labels_str):
            return False
        labels = str(labels_str).lower()
        if any(drop in labels for drop in ['test', 'doc', 'refactor', 'build', 'ci']):
            return False
        if is_recent:
            return any(keep in labels for keep in ['consensus', 'p2p', 'rpc', 'rest', 'zmq', 'wallet', 'mempool', 'gui', 'policy'])
        else:
            return any(keep in labels for keep in ['consensus', 'cryptography', 'p2p'])

    df = pd.read_parquet(INPUT_PR_PARQUET)
    df = df[(df['repository_name'] == 'bitcoin/bitcoin') & (df['merged_at'].notna())].copy()
    
    tagged_df = df[df['milestone'].notna()]
    cutoff_dates = {}
    for ms, group in tagged_df.groupby('milestone'):
        cutoff_dates[ms] = pd.to_datetime(group['merged_at'], utc=True).max()
        
    sorted_cutoffs = sorted([(ms, date) for ms, date in cutoff_dates.items() if pd.notna(date)], key=lambda x: parse_version(x[0]))
    
    def infer_milestone(row):
        if pd.notna(row['milestone']):
            return row['milestone']
        merged = pd.to_datetime(row['merged_at'], utc=True)
        if pd.isna(merged):
            return None
        inferred_ms = None
        for ms, cutoff in sorted_cutoffs:
            if merged <= cutoff:
                inferred_ms = ms
                break
        if not inferred_ms and sorted_cutoffs:
            inferred_ms = sorted_cutoffs[-1][0]
        
        if inferred_ms:
            ms_version = parse_version(inferred_ms)
            is_recent = ms_version >= (24, 0, 0)
            if not is_high_signal(row['labels'], is_recent):
                return None
        return inferred_ms

    df['milestone'] = df.apply(infer_milestone, axis=1)
    df = df[df['milestone'].notna()]
        
    unique_milestones = list(df['milestone'].unique())
    target_milestones = sorted(unique_milestones, key=parse_version, reverse=True)
    
    # Sort dataframe by merged_at/created_at descending (most recent first)
    if 'merged_at' in df.columns and 'created_at' in df.columns:
        df['sort_time'] = df['merged_at'].fillna(df['created_at'])
        df = df.sort_values(by='sort_time', ascending=False)
    
    pr_batch_to_fetch = {}
    
    # Process milestones in descending version order
    for ms in target_milestones:
        ms_str = str(ms)
        ms_prs = df[df['milestone'].astype(str) == ms_str]
        
        for _, pr in ms_prs.iterrows():
            pr_num = str(pr['pr_number'])
            title = pr['title']
            if pr_num not in pr_cache:
                pr_batch_to_fetch[pr_num] = title

    if not pr_batch_to_fetch:
        print("All PRs are already summarized!")
        return
        
    print(f"Need to summarize {len(pr_batch_to_fetch)} PRs.")
    
    items = list(pr_batch_to_fetch.items())
    for i in range(0, len(items), 10):  # Chunk of 10 per the user's request for higher quality & safety
        chunk = dict(items[i:i+10])
        print(f"Processing chunk {i} to {i+len(chunk)}...")
        new_summaries = get_bulk_llm_summaries_with_rotation(chunk)
        
        if not new_summaries:
            print("Stopping script due to API limits across all keys.")
            break
            
        for k, v in new_summaries.items():
            pr_cache[k] = v
        save_cache(CACHE_FILE, pr_cache)
        print("Chunk saved successfully.")
        time.sleep(3)
        
    print("Summarization pass complete.")

if __name__ == "__main__":
    run_summarizer()
