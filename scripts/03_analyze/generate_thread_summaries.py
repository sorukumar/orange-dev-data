import os
import json
import time
import pandas as pd
from datetime import datetime
import urllib.request
import urllib.error
from dotenv import load_dotenv

import sys
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(ROOT_DIR)
from scripts.utils.thread_context import build_thread_context

SOCIAL_THREADS_INPUT = os.path.join(ROOT_DIR, "data", "enriched", "social_threads.parquet")
CACHE_FILE = os.path.join(ROOT_DIR, "data", "cache", "thread_summaries_cache.json")

env_path = os.path.join(ROOT_DIR, ".env")
load_dotenv(env_path)
TARGET_MODEL = os.environ.get('GEMINI_TARGET_MODEL', 'gemini-2.5-flash')
api_keys = []
for k, v in sorted(os.environ.items()):
    if k.startswith("GEMINI_API_KEY") and v.strip():
        api_keys.append(v.strip())

def load_cache(path):
    if os.path.exists(path):
        with open(path, 'r') as f:
            try: return json.load(f)
            except: pass
    return {}

def save_cache(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)

def generate_llm_summary(api_key, context, model=TARGET_MODEL):
    prompt = f"""You are a Bitcoin Core developer writing a newsletter and pulse report.
Below is the original context of a discussion thread and the latest replies.

Input Context:
{context}

You MUST return a valid JSON object with exactly three keys:
1. "public_summary": Exactly 1-2 short lines accessible to the general public. Explain exactly what is being discussed, and focus on the value and benefit rather than just technical details.
2. "technical_summary": A detailed 4-5 line summary explaining the technical arguments, architectural debate, and exactly what needs to be done.
3. "pulse_insight": 1 concise sentence summarizing the core technical question or shift in this thread.

Do NOT wrap in markdown blocks, just raw JSON.
"""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2}
    }).encode('utf-8')
    req = urllib.request.Request(url, data=payload, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=30) as response:
        result = json.loads(response.read().decode('utf-8'))
        text = result['candidates'][0]['content']['parts'][0]['text'].strip()
        if text.startswith('```json'): text = text[7:-3]
        elif text.startswith('```'): text = text[3:-3]
        return json.loads(text.strip())

def run_thread_summarizer():
    if not api_keys:
        print("Warning: Gemini API not configured. Skipping thread summaries.")
        return

    if not os.path.exists(SOCIAL_THREADS_INPUT):
        print(f"Error: {SOCIAL_THREADS_INPUT} not found.")
        return

    print("Loading social threads...")
    df = pd.read_parquet(SOCIAL_THREADS_INPUT)
    df['date'] = pd.to_datetime(df['date'])
    
    # Enforce T-1 boundary
    t1_end = (datetime.now() - pd.Timedelta(days=1)).replace(hour=23, minute=59, second=59)
    df = df[df['date'] <= t1_end]

    # Find threads with activity in the last 30 days
    window_start = t1_end - pd.Timedelta(days=30)
    recent_activity = df[df['date'] >= window_start]
    
    active_thread_ids = recent_activity['thread_id'].unique()
    print(f"Found {len(active_thread_ids)} threads with activity in the last 30 days.")

    cache = load_cache(CACHE_FILE)
    updates_made = 0
    
    api_key_idx = 0

    for tid in active_thread_ids:
        # We need the thread subject to construct the legacy cache key for backward compatibility
        thread_rows = df[df['thread_id'] == tid]
        if thread_rows.empty:
            continue
            
        subject = thread_rows.iloc[0]['subject']
        cache_key = f"thread_{subject[:20]}"
        
        context, context_hash = build_thread_context(df, tid, window_start, t1_end)
        if not context:
            continue
            
        existing_entry = cache.get(cache_key, {})
        if existing_entry.get("context_hash") == context_hash:
            # No new meaningful replies that change the context
            continue
            
        print(f"Generating summary for thread: {subject[:40]}...")
        
        # Call LLM with key and model rotation
        result = None
        while api_key_idx < len(api_keys) and not result:
            curr_key = api_keys[api_key_idx]
            for m in [TARGET_MODEL, "gemini-2.5-flash-lite"]:
                try:
                    result = generate_llm_summary(curr_key, context, model=m)
                    if result:
                        break
                except urllib.error.HTTPError as e:
                    if e.code in (429, 404, 503):
                        print(f"Key {api_key_idx + 1} ({m}) HTTP {e.code}. Rotating...")
                        break
                    else:
                        print(f"Key {api_key_idx + 1} ({m}) HTTP {e.code}: {e}")
                except Exception as e:
                    print(f"Key {api_key_idx + 1} ({m}) error: {e}")
            if not result:
                api_key_idx += 1
        
        if result:
            result['context_hash'] = context_hash
            cache[cache_key] = result
            updates_made += 1
            # Also store by thread_id for safer new lookups
            cache[f"tid_{tid}"] = result
            
            # Save incrementally
            if updates_made % 5 == 0:
                save_cache(CACHE_FILE, cache)
            time.sleep(2)
        
        if api_key_idx >= len(api_keys):
            print("All API keys exhausted.")
            break

    if updates_made > 0:
        save_cache(CACHE_FILE, cache)
        print(f"Updated {updates_made} thread summaries.")
    else:
        print("All thread summaries are up to date.")

if __name__ == "__main__":
    run_thread_summarizer()
