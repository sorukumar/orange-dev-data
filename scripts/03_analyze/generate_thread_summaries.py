import os
import json
import time
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from scripts.utils.thread_context import build_thread_context

try:
    from google import genai
    from google.genai.errors import APIError
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

SOCIAL_THREADS_INPUT = "data/enriched/social_threads.parquet"
CACHE_FILE = "data/cache/thread_summaries_cache.json"
TARGET_MODEL = 'gemini-2.5-flash'

load_dotenv("/Users/saurabhkumar/Desktop/Work/github/orange-dev-data/.env")
api_keys = []
for k, v in os.environ.items():
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

def generate_llm_summary(client, context):
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
    for attempt in range(2):
        try:
            response = client.models.generate_content(
                model=TARGET_MODEL,
                contents=prompt,
            )
            text = response.text.strip()
            if text.startswith('```json'): text = text[7:-3]
            elif text.startswith('```'): text = text[3:-3]
            
            parsed = json.loads(text.strip())
            return parsed
        except APIError as e:
            if e.code == 429 or e.code == 404:
                raise e # Throw to outer loop to rotate key
            time.sleep(2)
        except Exception as e:
            time.sleep(2)
    return None

def run_thread_summarizer():
    if not HAS_GENAI or not api_keys:
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
    client = genai.Client(api_key=api_keys[api_key_idx])

    for tid in active_thread_ids:
        # We need the thread subject to construct the legacy cache key for backward compatibility
        thread_rows = df[df['thread_id'] == tid]
        if thread_rows.empty:
            continue
            
        subject = thread_rows.iloc[0]['subject']
        # Same cache key logic used in older twib_nlp.py
        # Clean subject like in twib_data? In twib_data, subject is just 'subject' column.
        cache_key = f"thread_{subject[:20]}"
        
        context, context_hash = build_thread_context(df, tid, window_start, t1_end)
        if not context:
            continue
            
        existing_entry = cache.get(cache_key, {})
        if existing_entry.get("context_hash") == context_hash:
            # No new meaningful replies that change the context
            continue
            
        print(f"Generating summary for thread: {subject[:40]}...")
        
        # Call LLM
        result = None
        while api_key_idx < len(api_keys) and not result:
            try:
                result = generate_llm_summary(client, context)
            except APIError as e:
                if e.code == 429 or e.code == 404:
                    print(f"Key {api_key_idx + 1} hit {e.code}. Rotating...")
                    api_key_idx += 1
                    if api_key_idx < len(api_keys):
                        client = genai.Client(api_key=api_keys[api_key_idx])
                else:
                    print(f"API Error: {e}")
                    break
        
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
