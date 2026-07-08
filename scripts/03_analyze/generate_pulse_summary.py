import os
import json
import hashlib
import time
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '04_deliver'))
from discussions_pulse import _compute_window, _add_trends, SOCIAL_THREADS_INPUT

try:
    from google import genai
    from google.genai.errors import APIError
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

PULSE_SUMMARY_FILE = "data/raw/pulse_summary.json"
TARGET_MODEL = 'gemini-2.5-flash'

# Load API keys
load_dotenv("/Users/saurabhkumar/Desktop/Work/github/orange-dev-data/.env")
api_keys = []
for k, v in os.environ.items():
    if k.startswith("GEMINI_API_KEY") and v.strip():
        api_keys.append(v.strip())

def generate_pulse_summary():
    if not HAS_GENAI or not api_keys:
        print("Warning: Gemini API not configured. Skipping pulse summary.")
        return

    if not os.path.exists(SOCIAL_THREADS_INPUT):
        print(f"Error: {SOCIAL_THREADS_INPUT} not found.")
        return

    df = pd.read_parquet(SOCIAL_THREADS_INPUT)
    df['date'] = pd.to_datetime(df['date'])
    t1_end = (datetime.now() - pd.Timedelta(days=1)).replace(hour=23, minute=59, second=59)
    df = df[df['date'] <= t1_end]

    w90 = _compute_window(df, 90)
    w30 = _compute_window(df, 30)

    if not w90 or not w30:
        print("Not enough data to compute windows.")
        return

    _add_trends(w30, w90)

    # Check staleness based on top 8 threads
    hot_threads = w30.get('hot_threads', [])
    if not hot_threads:
        print("No hot threads found.")
        return

    top_8_thread_ids = sorted([str(t['thread_id']) for t in hot_threads[:8]])
    threads_hash = hashlib.md5(",".join(top_8_thread_ids).encode()).hexdigest()

    existing_summary = {}
    if os.path.exists(PULSE_SUMMARY_FILE):
        try:
            with open(PULSE_SUMMARY_FILE, 'r') as f:
                existing_summary = json.load(f)
        except:
            pass

    if existing_summary.get("_threads_hash") == threads_hash:
        print("Pulse summary is fresh (threads haven't changed). Skipping LLM call.")
        return

    # Assembly input for LLM
    prompt_context = "THEME SHIFTS (30-day vs 90-day baseline):\n"
    shares_90 = w90.get('_cat_shares', {})
    for theme in w30.get('themes', [])[:5]:
        cat = theme['category']
        s30 = theme['share']
        s90 = shares_90.get(cat, 0)
        delta = s30 - s90
        trend_str = theme['trend'].upper()
        prompt_context += f"- {theme['label']}: {s30:.1f}% ({'+' if delta >= 0 else ''}{delta:.1f}pp from 90d) — {trend_str}\n"

    prompt_context += "\nTOP THREADS (by engagement breadth):\n"
    
    # We need to collect opener snippet + top-3 reply snippets
    for i, thread in enumerate(hot_threads[:8]):
        tid = thread['thread_id']
        t_df = df[df['thread_id'] == tid].sort_values('date')
        
        # Get opener snippet
        opener_mask = ~t_df['is_reply']
        if opener_mask.any():
            op_snippet = str(t_df[opener_mask].iloc[0].get('body_snippet', '')).strip()
        else:
            op_snippet = ""
            
        # Get reply snippets
        reply_mask = t_df['is_reply']
        replies = t_df[reply_mask].drop_duplicates('canonical_id').head(3)
        rep_snippets = [str(s).strip() for s in replies['body_snippet'] if pd.notna(s)]
        
        combined_snippet = op_snippet + " " + " ".join(rep_snippets)
        if len(combined_snippet) > 800:
            combined_snippet = combined_snippet[:797] + "..."
            
        prompt_context += f"{i+1}. [{thread['source']}] \"{thread['subject']}\" (ID: {tid}, {thread['reply_count']} replies, {thread['unique_authors']} voices)\n"
        prompt_context += f"   Context: {combined_snippet}\n"

    prompt = f"""You are a senior Bitcoin protocol researcher writing a monthly briefing for fellow contributors.

Input Context:
{prompt_context}

Your Directives:
- Lead with **the biggest shift** — what changed between the 90d baseline and the 30d window.
- Avoid corporate or managerial terms like "re-prioritization" or "strategic focus." Bitcoin development is a decentralized, open forum. Use natural, human phrasing like "developers are spending more time discussing X" or "attention has organically shifted toward Y".
- Identify **1 concrete proposal** that gained traction (with BIP number if applicable).
- Note **1 open question** the community hasn't resolved.
- Tone: informed, precise, human-like, no hype. You're writing for people who read the actual threads. Make it sound like a human observation of an open discussion.
- Do NOT use markdown wrappers. Output strictly valid JSON.

Output format (JSON):
{{
  "summary": "Two-sentence overview of the month's most significant shift...",
  "insights": [
    "Insight bullet 1 — leads with the biggest delta...",
    "Insight bullet 2...",
    "Insight bullet 3..."
  ],
  "thread_insights": {{
    "<thread_id>": "One sentence: What's the core technical question?"
  }}
}}
"""

    for key_index, current_key in enumerate(api_keys):
        client = genai.Client(api_key=current_key)
        for attempt in range(2):
            try:
                print(f"Calling LLM for pulse summary (Key {key_index + 1})...")
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
                parsed["_threads_hash"] = threads_hash
                
                os.makedirs(os.path.dirname(PULSE_SUMMARY_FILE), exist_ok=True)
                with open(PULSE_SUMMARY_FILE, 'w') as f:
                    json.dump(parsed, f, indent=2)
                
                print(f"Successfully generated and saved pulse summary to {PULSE_SUMMARY_FILE}")
                return
            
            except APIError as e:
                if e.code == 429 or e.code == 404:
                    print(f"Key {key_index + 1} hit {e.code}. Rotating to next key...")
                    break 
                else:
                    print(f"Error on Key {key_index + 1} (attempt {attempt+1}): {e}")
                    time.sleep(2)
            except Exception as e:
                print(f"Unexpected Error on Key {key_index + 1} (attempt {attempt+1}): {e}")
                time.sleep(2)
                
    print("Failed to generate pulse summary. All API keys exhausted or rate limited.")

if __name__ == "__main__":
    generate_pulse_summary()
