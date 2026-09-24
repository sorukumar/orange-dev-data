import os
import sys
import json
import hashlib
import time
import re
from datetime import datetime
import urllib.request
import urllib.error

INPUT_FILE = "data/raw/irc_meetings.json"
OUTPUT_FILE = "data/raw/meeting_summaries.json"
HAS_GENAI = True

# Load API keys
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
if os.path.exists(env_path):
    with open(env_path, "r") as f:
        for line in f:
            if "=" in line and not line.strip().startswith("#"):
                key, val = line.strip().split("=", 1)
                os.environ[key.strip()] = val.strip().strip("'").strip('"')

TARGET_MODEL = os.environ.get('GEMINI_TARGET_MODEL', 'gemini-2.5-flash')
api_keys = []
for k, v in os.environ.items():
    if k.startswith("GEMINI_API_KEY") and v.strip():
        api_keys.append(v.strip())

def generate_meeting_summary():
    if not HAS_GENAI or not api_keys:
        print("Warning: Gemini API not configured. Skipping meeting summary.")
        return

    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    with open(INPUT_FILE, 'r') as f:
        meetings = json.load(f)

    existing_summaries = []
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r') as f:
                existing_summaries = json.load(f)
        except:
            pass

    existing_dict = {m['date']: m for m in existing_summaries}
    new_summaries = []
    changed = False

    # Bots to ignore in participant counts
    bots = {'bitcoin-git', 'bitcoin-merge', 'gribble', 'lightningbot', 'lnd-git', 'corebot'}

    for meeting in meetings:
        date = meeting['date']
        messages = meeting.get('messages', [])
        
        # Calculate hash to detect changes
        text_hash = hashlib.md5(json.dumps(messages, sort_keys=True).encode('utf-8')).hexdigest()
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        existing = existing_dict.get(date)
        force_today = ("--force-today" in sys.argv or "--force" in sys.argv) and (date == today_str)
        if force_today:
            needs_update = True
        elif date == today_str:
            needs_update = not (existing and existing.get('_text_hash') == text_hash and existing.get('linkedin_context'))
        else:
            needs_update = not (existing and existing.get('_text_hash') == text_hash)

        if not needs_update:
            new_summaries.append(existing)
            continue
            
        print(f"Processing meeting for {date}...")
        changed = True
        
        # Count participants
        counts = {}
        for m in messages:
            sender = m.get('sender', '').strip()
            if sender and sender not in bots:
                counts[sender] = counts.get(sender, 0) + 1
                
        participant_count = len(counts)
        top_nicks = sorted(counts.keys(), key=lambda k: counts[k], reverse=True)[:5]
        
        # Construct log_text dynamically for LLM consumption
        raw_text_lines = []
        for m in messages:
            ts = m.get("timestamp", "")
            time_str = ts.split("T")[1][:5] if "T" in ts else ""
            sender = m.get("sender", "")
            payload = m.get("payload", "")
            raw_text_lines.append(f"{time_str} < {sender}> {payload}")
            
        log_text = "\n".join(raw_text_lines)
        if len(log_text) > 30000:
            log_text = log_text[-30000:]
            
        prompt = f"""You are a senior Bitcoin Core developer summarizing an IRC meeting log for technical Bitcoiners and developers.
The meeting took place on {date}.

IRC Log Transcript:
{log_text}

Extract and generate two distinct layers of summary:

LAYER 1 (For the website meeting archive card & visual screenshot):
1. topics_discussed: An array of 3-6 substantive, factual bullet point strings covering every distinct discussion topic from the meeting.
   - Do NOT just write generic topic titles (e.g. avoid empty headers like "Fuzzing Working Group update").
   - Follow the format: "[Topic / Working Group]: [Substantive record of what was discussed, who was involved, and what technical outcomes/status were shared]".
   - Retain all developer names, contributor handoffs, milestone numbers, and PR numbers.
   - Zero corporate fluff, no passive filler words, but high technical information density.
2. decisions_made: Technical decisions reached during the meeting (empty array if none).
3. action_items: Concrete next steps or testing calls with PR/issue numbers (empty array if none).
4. mentioned_prs: Array of integers for PR or issue numbers explicitly discussed (no bot spam).

LAYER 2 (For Social Broadcasts — X & LinkedIn):
5. x_priority_takeaway: A single, super-precise engineering takeaway or call to action (under 70 characters, active voice, zero fluff). E.g. "v32.0rc2 binaries are ready for community testing (#36315)."
6. x_bullet_points: 2-3 concise bullet points (under 45 characters each) covering the secondary technical updates.
7. linkedin_context: Exactly 2 single-sentence takeaways separated by a blank line (\n\n). Maximum 35 words total.
   - Sentence 1: Primary release or protocol takeaway explaining why active testing/review is needed right now.
   - Sentence 2: Secondary Working Group or architectural takeaway (e.g. fuzzing leadership transition).
   - Strict style: Zero corporate speak, zero platitudes (no 'vital for security', 'crucial milestone', etc.). Short, punchy, direct facts.

Output strictly valid JSON matching this schema:
{{
    "topics_discussed": ["..."],
    "decisions_made": [],
    "action_items": ["..."],
    "mentioned_prs": [1234],
    "x_priority_takeaway": "...",
    "x_bullet_points": ["...", "..."],
    "linkedin_context": "..."
}}
Do NOT use markdown wrappers. Output only JSON.
"""

        success = False
        for key_index, current_key in enumerate(api_keys):
            if success:
                break
                
            for attempt in range(2):
                try:
                    url = f"https://generativelanguage.googleapis.com/v1beta/models/{TARGET_MODEL}:generateContent?key={current_key}"
                    payload = json.dumps({
                        "contents": [{"parts": [{"text": prompt}]}],
                        "generationConfig": {"temperature": 0.2}
                    }).encode('utf-8')
                    
                    req = urllib.request.Request(url, data=payload, headers={'Content-Type': 'application/json'})
                    with urllib.request.urlopen(req) as response:
                        result = json.loads(response.read().decode('utf-8'))
                        
                        text = result['candidates'][0]['content']['parts'][0]['text'].strip()
                        
                        if text.startswith('```json'):
                            text = text[7:-3]
                        elif text.startswith('```'):
                            text = text[3:-3]
                        
                        parsed = json.loads(text.strip())
                        
                        summary_obj = {
                            "date": date,
                            "url": meeting.get("url", ""),
                            "participant_count": participant_count,
                            "key_participants": top_nicks,
                            "topics_discussed": parsed.get("topics_discussed", []),
                            "decisions_made": parsed.get("decisions_made", []),
                            "action_items": parsed.get("action_items", []),
                            "mentioned_prs": parsed.get("mentioned_prs", []),
                            "x_priority_takeaway": parsed.get("x_priority_takeaway", ""),
                            "x_bullet_points": parsed.get("x_bullet_points", []),
                            "linkedin_context": parsed.get("linkedin_context", ""),
                            "_text_hash": text_hash
                        }
                        
                        new_summaries.append(summary_obj)
                        success = True
                        time.sleep(1)
                        break

                except urllib.error.HTTPError as e:
                    if e.code == 429 or e.code == 404:
                        print(f"Key {key_index + 1} hit {e.code}. Rotating to next key...")
                        break 
                    else:
                        print(f"Error on Key {key_index + 1} (attempt {attempt+1}): {e}")
                        time.sleep(2)
                except Exception as e:
                    print(f"Unexpected Error on Key {key_index + 1} (attempt {attempt+1}): {e}")
                    time.sleep(2)
                    
        if not success:
            print(f"Failed to generate summary for {date}. Appending empty structure.")
            new_summaries.append({
                "date": date,
                "url": meeting.get("url", ""),
                "participant_count": participant_count,
                "key_participants": top_nicks,
                "topics_discussed": ["Summary generation failed."],
                "decisions_made": [],
                "action_items": [],
                "mentioned_prs": [],
                "_text_hash": text_hash
            })

    if changed:
        # Sort by date descending
        new_summaries.sort(key=lambda x: x['date'], reverse=True)
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(new_summaries, f, indent=2)
        print(f"Successfully generated and saved meeting summaries to {OUTPUT_FILE}")
    else:
        print("All meeting summaries are up to date.")

if __name__ == "__main__":
    generate_meeting_summary()
