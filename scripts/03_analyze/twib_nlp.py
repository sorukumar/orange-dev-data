import os
import json
import time
from google import genai
import sys

# Add parent directory to path so we can import utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from scripts.utils.twib_data import get_weekly_activity

def get_llm_summary(client, text, instruction):
    prompt = f"You are a Bitcoin Core developer. {instruction}\n\nContext:\n{text}\n\nOutput only the summary, no markdown headers or extra fluff."
    for attempt in range(3):
        try:
            response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
            return response.text.strip()
        except Exception as e:
            if attempt == 2:
                return f"*(Failed to summarize after retries: {e})*"
            print(f"Rate limit hit. Retrying in {2 ** attempt * 5} seconds... ({e})")
            time.sleep(2 ** attempt * 5)

def get_bulk_llm_summaries(client, items_dict, instruction):
    if not items_dict:
        return {}
        
    prompt = f"""You are a Bitcoin Core developer writing a newsletter.
{instruction}

You MUST return a valid JSON object where the keys are exactly the keys provided, and the values are objects containing two keys: "public_summary" and "technical_summary". Do NOT wrap in markdown blocks, just raw JSON.

Input Data:
{json.dumps(items_dict, indent=2)}
"""
    for attempt in range(3):
        try:
            response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
            text = response.text.strip()
            if text.startswith("```json"): text = text[7:]
            if text.endswith("```"): text = text[:-3]
            return json.loads(text.strip())
        except Exception as e:
            if attempt == 2:
                print(f"Failed bulk summarize: {e}")
                return {}
            print(f"Rate limit hit. Retrying in {2 ** attempt * 5} seconds... ({e})")
            time.sleep(2 ** attempt * 5)

def categorize_misc_prs(client, prs, valid_categories):
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
    for attempt in range(3):
        try:
            response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
            text = response.text.strip()
            if text.startswith("```json"): text = text[7:]
            if text.endswith("```"): text = text[:-3]
            return json.loads(text.strip())
        except Exception as e:
            if attempt == 2:
                print(f"Failed to categorize: {e}")
                return {}
            time.sleep(2 ** attempt * 5)

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
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GEMINI_API_KEY_1")
    if not api_key:
        print("Error: GEMINI_API_KEY environment variable not set.")
        return

    client = genai.Client(api_key=api_key)
    
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
        new_cats = categorize_misc_prs(client, misc_prs, valid_categories)
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
    tldr_summary = get_llm_summary(client, tldr_input, "Write 2 bullet points summarizing the most important technical shift or discussion from these events.")
    if tldr_summary:
        cache["tldr_summary"] = tldr_summary
        with open(cache_path, "w") as f: json.dump(cache, f, indent=2)

    # Gather all items to summarize
    items_to_summarize = {}
    for cat, prs in weekly_data.get('categorized_merged_prs', {}).items():
        for pr in prs:
            key = f"pr_{pr['pr_number']}"
            if key not in cache:
                items_to_summarize[key] = f"Merged PR #{pr['pr_number']}: {pr['title']}"

    for cat, prs in weekly_data.get('categorized_hot_prs', {}).items():
        for pr in prs:
            key = f"pr_{pr['pr_number']}"
            if key not in cache:
                items_to_summarize[key] = f"Hot PR #{pr['pr_number']} under review: {pr['title']}"

    top_2_threads = weekly_data.get('active_threads', [])[:2]
    for thread in top_2_threads:
        key = f"thread_{thread['subject'][:20]}"
        if key not in cache:
            items_to_summarize[key] = f"Discussion Thread: {thread['subject']}"

    if items_to_summarize:
        print(f"Generating bulk summaries for {len(items_to_summarize)} new items...")
        instruction = """For each item, generate two summaries:
1. "public_summary": Exactly 1-2 short lines accessible to the general public. Explain exactly what is being done, and focus on the value and benefit of the work rather than just technical details.
2. "technical_summary": A detailed 4-5 line summary explaining the technical implementation, architectural value, and exactly what needs to be done."""
        
        new_summaries = get_bulk_llm_summaries(client, items_to_summarize, instruction)
        for k, v in new_summaries.items():
            cache[k] = v
        with open(cache_path, "w") as f: json.dump(cache, f, indent=2)
    else:
        print("No new items to summarize. Cache is fully warmed.")

if __name__ == "__main__":
    main()
