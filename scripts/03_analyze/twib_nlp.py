import os
import json
import time
from google import genai
from google.genai.errors import APIError
import sys

# Add parent directory to path so we can import utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from scripts.utils.twib_data import get_weekly_activity

def _call_gemini(api_keys, prompt, is_json=False):
    for key_index, current_key in enumerate(api_keys):
        client = genai.Client(api_key=current_key)
        for attempt in range(2):
            try:
                response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
                text = response.text.strip()
                if is_json:
                    if text.startswith("```json"): text = text[7:]
                    if text.endswith("```"): text = text[:-3]
                    return json.loads(text.strip())
                return text
            except APIError as e:
                if e.code == 429 or e.code == 404 or e.code == 403:
                    print(f"Key {key_index + 1} hit {e.code}. Rotating...")
                    break # try next key
                print(f"APIError on Key {key_index + 1}: {e}")
                time.sleep(3)
            except Exception as e:
                err_str = str(e)
                if '429' in err_str or 'RESOURCE_EXHAUSTED' in err_str:
                    print(f"Key {key_index + 1} hit rate limit. Rotating...")
                    break
                print(f"Error on Key {key_index + 1}: {e}")
                time.sleep(3)
    return {} if is_json else None

def get_llm_summary(api_keys, text, instruction):
    prompt = f"You are a Bitcoin Core developer. {instruction}\n\nContext:\n{text}\n\nOutput only the summary, no markdown headers or extra fluff."
    res = _call_gemini(api_keys, prompt, is_json=False)
    if not res:
        return "*(Failed to summarize)*"
    return res

def get_bulk_llm_summaries(api_keys, items_dict, instruction):
    if not items_dict:
        return {}
        
    prompt = f"""You are a Bitcoin Core developer writing a newsletter.
{instruction}

You MUST return a valid JSON object where the keys are exactly the keys provided, and the values are objects containing two keys: "public_summary" and "technical_summary". Do NOT wrap in markdown blocks, just raw JSON.

Input Data:
{json.dumps(items_dict, indent=2)}
"""
    return _call_gemini(api_keys, prompt, is_json=True)

def categorize_misc_prs(api_keys, prs, valid_categories):
    if not prs:
        return {}
    
    prompt = f"""You are a Bitcoin Core developer. Categorize each of these Pull Requests into one of the following exact categories. 
If it doesn't clearly fit, keep it as '🔄 Misc / Other'.

Valid categories:
{json.dumps(valid_categories, indent=2)}

You MUST return a valid JSON object where the keys are the PR numbers (as strings) and the values are the chosen category strings.

PRs to categorize:
{json.dumps([{ 'pr_number': str(pr['pr_number']), 'title': pr['title'] } for pr in prs], indent=2)}
"""
    return _call_gemini(api_keys, prompt, is_json=True)

def load_env(root_dir):
    env_path = os.path.join(root_dir, ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    key, val = line.strip().split("=", 1)
                    os.environ[key] = val.strip("\"'")

def main():
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    load_env(root_dir)
    
    api_keys = []
    for k, v in os.environ.items():
        if k.startswith("GEMINI_API_KEY") and v.strip():
            api_keys.append(v.strip())
            
    if not api_keys:
        print("Error: No GEMINI_API_KEY found in environment.")
        return

    print("Fetching weekly data for LLM processing...")
    weekly_data = get_weekly_activity(root_dir, days_back=7)
    
    cache_path = os.path.join(root_dir, "data", "cache", "twib_summaries.json")
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    cache = {}
    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            try: cache = json.load(f)
            except: pass

    # 1. Smart Categorization for Misc PRs
    valid_categories = [
        '👛 Wallet & Keys', '⚡ P2P & Network', '🛡️ Consensus & Cryptography', 
        '📡 RPC, APIs & ZMQ', '🖥️ GUI', '🛠️ Build, CI & Testing', '📝 Documentation', '🔄 Misc / Other'
    ]
    misc_prs = weekly_data.get('categorized_merged_prs', {}).get('🔄 Misc / Other', [])
    if misc_prs:
        print(f"Categorizing {len(misc_prs)} miscellaneous PRs...")
        new_cats = categorize_misc_prs(api_keys, misc_prs, valid_categories)
        if new_cats:
            if "misc_categories" not in cache: cache["misc_categories"] = {}
            cache["misc_categories"].update(new_cats)
            with open(cache_path, "w") as f: json.dump(cache, f, indent=2)

    # Gather TL;DR input
    tldr_input = ""
    for cat, prs in weekly_data.get('categorized_merged_prs', {}).items():
        for pr in prs: tldr_input += f"- Merged PR #{pr['pr_number']}: {pr['title']}\n"
    for thread in weekly_data.get('active_threads', []):
        tldr_input += f"- Discussed: {thread['subject']}\n"

    print("Generating TL;DR summary...")
    tldr_summary = get_llm_summary(api_keys, tldr_input, "Write 2 bullet points summarizing the most important technical shift or discussion from these events.")
    if tldr_summary and tldr_summary != "*(Failed to summarize)*":
        cache["tldr_summary"] = tldr_summary
        with open(cache_path, "w") as f: json.dump(cache, f, indent=2)

    else:
        print("No new items to summarize for TL;DR. Cache is fully warmed.")

if __name__ == "__main__":
    main()
