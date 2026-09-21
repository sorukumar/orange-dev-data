import os
import json
import pandas as pd
import time
import re
import urllib.request
import urllib.error
from dotenv import load_dotenv

import sys
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(ROOT_DIR)
from scripts.utils.pr_utils import is_high_signal

INPUT_PR_PARQUET = os.path.join(ROOT_DIR, "data", "raw", "github_pr_metadata.parquet")
PR_CACHE_FILE = os.path.join(ROOT_DIR, "data", "raw", "pr_summaries_cache.json")
HIGHLIGHTS_CACHE_FILE = os.path.join(ROOT_DIR, "data", "raw", "release_highlights_cache.json")

# Load multiple API keys from .env dynamically
env_path = os.path.join(ROOT_DIR, ".env")
load_dotenv(env_path)

api_keys = []
for k, v in sorted(os.environ.items()):
    if k.startswith("GEMINI_API_KEY") and v.strip():
        api_keys.append(v.strip())

if not api_keys:
    print("Warning: No GEMINI_API_KEY found in .env")

TARGET_MODEL = os.environ.get('GEMINI_TARGET_MODEL', 'gemini-2.5-flash')

def load_cache(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except:
                return {}
    return {}

def save_cache(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

def generate_version_highlight(version, pr_subset, official_notes=None):
    if not api_keys or not pr_subset:
        return None

    notes_section = ""
    if official_notes:
        notes_section = f"\nOfficial Release Notes Anchor:\n{official_notes[:8000]}\n" # Trim to avoid token limits

    prompt = f"""You are writing a release summary for Bitcoin Core version {version}, in the style of Bitcoin Optech. 
Your task is to analyze a list of Pull Request summaries and synthesize a highly readable, insightful "Release Highlights" summary.

Input Data (List of PR Summaries):
{json.dumps(pr_subset, indent=2)}
{notes_section}
### Your Directives:
1. THE SO WHAT?: Do not just list technical changes. You must explain *why* these changes matter and who they impact. For example, instead of saying "Refactored mempool eviction," explain "This improves memory efficiency for node runners."
2. TONE: Be professional, insightful, and clear. Avoid marketing fluff or emojis, but make it highly readable and engaging for a technical audience.
3. EVALUATION RUBRIC: When deciding what constitutes a "Highlight", prioritize changes with the biggest user impact:
   - Consensus rules and network security
   - P2P network privacy and node performance (CPU/Mem/Disk) for node runners
   - Wallet architecture and new RPC methods for wallet developers
   Ignore minor refactors, typo fixes, and routine dependency updates.

### Output format:
You MUST return a valid JSON object with exactly these two keys:

1. "release_summary": A 3-4 sentence paragraph explaining the overarching themes of this release. Focus heavily on "The So What" and the tangible impact on the ecosystem.
2. "highlights": An array of 3 to 5 strings. Each string must be a single-sentence bullet point describing a major change and its direct impact. Start each bullet with a bolded category tag based on the affected user (e.g., "**For Node Runners:**", "**For Wallet Developers:**", or "**Core Stability:**").

Do NOT wrap the output in markdown blocks, return raw JSON only.
"""

    models_to_try = [TARGET_MODEL]
    if "gemini-2.5-flash-lite" not in models_to_try:
        models_to_try.append("gemini-2.5-flash-lite")

    for key_index, current_key in enumerate(api_keys):
        for model in models_to_try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={current_key}"
            payload = json.dumps({
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2}
            }).encode('utf-8')
            
            for attempt in range(2):
                try:
                    req = urllib.request.Request(url, data=payload, headers={'Content-Type': 'application/json'})
                    with urllib.request.urlopen(req, timeout=30) as response:
                        result = json.loads(response.read().decode('utf-8'))
                        text = result['candidates'][0]['content']['parts'][0]['text'].strip()
                        if text.startswith('```json'):
                            text = text[7:-3]
                        elif text.startswith('```'):
                            text = text[3:-3]
                        
                        parsed = json.loads(text.strip())
                        return parsed
                
                except urllib.error.HTTPError as e:
                    if e.code in (429, 404, 503):
                        print(f"Key {key_index + 1} ({model}) HTTP {e.code}. Rotating...")
                        break 
                    else:
                        print(f"Key {key_index + 1} ({model}) HTTP error {e.code}: {e}")
                        time.sleep(2)
                except Exception as e:
                    print(f"Key {key_index + 1} ({model}) error: {e}")
                    time.sleep(2)
                    
    print(f"Failed to generate highlights for {version}. All API keys exhausted or rate limited.")
    return None

def parse_version(v_str):
    matches = re.findall(r'\d+', str(v_str))
    if not matches:
        return (0, 0, 0)
    ints = [int(m) for m in matches]
    while len(ints) < 3:
        ints.append(0)
    return tuple(ints[:3])

def run_highlights_generator():
    if not os.path.exists(INPUT_PR_PARQUET):
        print(f"{INPUT_PR_PARQUET} does not exist.")
        return

    print(f"Found {len(api_keys)} API Keys loaded.")

    df = pd.read_parquet(INPUT_PR_PARQUET)
    df = df[(df['repository_name'] == 'bitcoin/bitcoin') & (df['merged_at'].notna())].copy()
    
    # NEW STEP: Load review counts
    events_path = os.path.join(ROOT_DIR, "data", "raw", "github_review_events.parquet")
    if os.path.exists(events_path):
        df_rev = pd.read_parquet(events_path)
        review_counts = df_rev.groupby('pr_number').size().to_dict()
        df['review_count'] = df['pr_number'].map(review_counts).fillna(0)
    else:
        df['review_count'] = 0
    
    tagged_df = df[df['milestone'].notna()]
    cutoff_dates = {}
    for ms, group in tagged_df.groupby('milestone'):
        cutoff_dates[ms] = pd.to_datetime(group['merged_at'], utc=True).max()
        
    def is_major_release(ms):
        pv = parse_version(ms)
        if pv[0] >= 22:
            return pv[1] == 0 and pv[2] == 0
        else:
            return pv[2] == 0

    sorted_cutoffs = sorted([(ms, date) for ms, date in cutoff_dates.items() if pd.notna(date) and is_major_release(ms)], key=lambda x: parse_version(x[0]))
    
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
            rc = row.get('review_count', 0)
            if not is_high_signal(row['labels'], is_recent, rc):
                return None
        return inferred_ms

    df['milestone'] = df.apply(infer_milestone, axis=1)
    df = df[df['milestone'].notna()]
    
    pr_cache = load_cache(PR_CACHE_FILE)
    highlights_cache = load_cache(HIGHLIGHTS_CACHE_FILE)
    
    unique_milestones = list(df['milestone'].unique())
    target_milestones = sorted(unique_milestones, key=parse_version, reverse=True)
    
    for ms in target_milestones:
        ms_str = str(ms)
        ms_prs = df[df['milestone'].astype(str) == ms_str]
        
        pr_subset = []
        for _, pr in ms_prs.iterrows():
            pr_num = str(pr['pr_number'])
            title = pr['title']
            labels_str = str(pr.get('labels', ''))
            
            # Skip low-signal PRs
            if 'Tests' in labels_str or 'Docs' in labels_str or 'Refactoring' in labels_str:
                continue
                
            summary_obj = pr_cache.get(pr_num, {})
            pub_summary = summary_obj.get('public_summary', title)
            
            pr_subset.append({
                "pr_number": pr_num,
                "title": title,
                "labels": labels_str,
                "summary": pub_summary
            })
            
        # --- Staleness check: skip if the PR count hasn't changed ---
        current_pr_count = len(pr_subset)
        cached_entry = highlights_cache.get(ms_str)
        
        # Preserve history: do not regenerate summaries for older versions even if PR count changes
        if cached_entry and parse_version(ms_str) < (28, 0, 0):
            continue
            
        if cached_entry and cached_entry.get("pr_count") == current_pr_count:
            continue  # Summary is still fresh, skip
            
        if cached_entry:
            print(f"Regenerating highlights for {ms_str} (PR count changed: {cached_entry.get('pr_count')} → {current_pr_count})...")
        else:
            print(f"Generating highlights for {ms_str} ({current_pr_count} high-signal PRs)...")
            
        if not pr_subset:
            print(f"No high-signal PRs found for {ms_str}, skipping.")
            continue
            
        # Limit to top 150 PRs if too large, to avoid token limits
        if len(pr_subset) > 150:
            pr_subset = pr_subset[:150]
            
        # Try to read official release notes directly from the cloned repository
        official_notes = None
        
        clean_ms = ms_str.lstrip('v')
        
        # Possible locations in the bitcoin/bitcoin repository
        possible_files = [
            os.path.join(ROOT_DIR, "data", "sources", "bitcoin", "doc", "release-notes", f"release-notes-{clean_ms}.md"),
            os.path.join(ROOT_DIR, "data", "sources", "bitcoin", "doc", "release-notes", f"release-notes-{clean_ms}.0.md"),
            os.path.join(ROOT_DIR, "data", "sources", "bitcoin", "doc", "release-notes.md") # Fallback for active/unreleased
        ]
        
        for pfile_path in possible_files:
            if os.path.exists(pfile_path):
                try:
                    with open(pfile_path, 'r', encoding='utf-8') as f:
                        official_notes = f.read()
                    print(f"Loaded official notes from {pfile_path}")
                    break
                except Exception as e:
                    print(f"Error reading {pfile_path}: {e}")
            
        result = generate_version_highlight(ms_str, pr_subset, official_notes)
        if result:
            result["pr_count"] = current_pr_count
            highlights_cache[ms_str] = result
            save_cache(HIGHLIGHTS_CACHE_FILE, highlights_cache)
            print(f"Successfully generated and cached highlights for {ms_str}.")
            time.sleep(3) # Be nice to the API
        else:
            print(f"Failed to generate highlights for {ms_str}. Stopping.")
            break
            
    print("Highlights generation pass complete.")

if __name__ == "__main__":
    run_highlights_generator()
