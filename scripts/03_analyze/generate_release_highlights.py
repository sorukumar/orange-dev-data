import os
import json
import pandas as pd
import time
import re
from dotenv import load_dotenv

try:
    from google import genai
    from google.genai.errors import APIError
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

INPUT_PR_PARQUET = "data/raw/github_pr_metadata.parquet"
PR_CACHE_FILE = "data/raw/pr_summaries_cache.json"
HIGHLIGHTS_CACHE_FILE = "data/raw/release_highlights_cache.json"

# Load multiple API keys from .env
load_dotenv("/Users/saurabhkumar/Desktop/Work/github/orange-dev-data/.env")

api_keys = []
for k, v in os.environ.items():
    if k.startswith("GEMINI_API_KEY") and v.strip():
        api_keys.append(v.strip())

if not api_keys:
    print("Warning: No GEMINI_API_KEY found in .env")

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

def generate_version_highlight(version, pr_subset, official_notes=None):
    if not HAS_GENAI or not api_keys or not pr_subset:
        return None

    notes_section = ""
    if official_notes:
        notes_section = f"\nOfficial Release Notes Anchor:\n{official_notes[:8000]}\n" # Trim to avoid token limits

    prompt = f"""You are the lead technical writer for Bitcoin Core. Your task is to analyze a list of Pull Request summaries that were merged into version {version} and synthesize a strict, factual "Release Highlights" summary.

Input Data (List of PR Summaries):
{json.dumps(pr_subset, indent=2)}
{notes_section}
### Your Directives:
1. TONE: Be dry, objective, and strictly factual. Do NOT use marketing fluff, hyperbole (e.g., "game-changing", "revolutionary"), or emojis. 
2. EVALUATION RUBRIC: When deciding what constitutes a "Highlight", prioritize changes in this strict order:
   - Consensus rules and network security
   - P2P network privacy and node performance (CPU/Mem/Disk)
   - Wallet architecture and new RPC methods
   - Build systems and significant testing frameworks
   Ignore all minor refactors, typo fixes, and routine dependency updates.

### Output format:
You MUST return a valid JSON object with exactly these two keys:

1. "release_summary": A 2-3 sentence paragraph explaining the overarching technical themes of this release. Focus only on the facts of what changed.
2. "highlights": An array of 3 to 5 strings. Each string must be a single-sentence bullet point describing a major architectural or feature change. Start each bullet with a bolded category tag (e.g., "**P2P:** Added support for V2 transport protocol.").

Do NOT wrap the output in markdown blocks, return raw JSON only.
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
                if e.code == 429 or e.code == 404:
                    print(f"Key {key_index + 1} hit {e.code}. Rotating to next key...")
                    break 
                else:
                    print(f"Error on Key {key_index + 1} (attempt {attempt+1}): {e}")
                    time.sleep(2)
            except Exception as e:
                print(f"Unexpected Error on Key {key_index + 1} (attempt {attempt+1}): {e}")
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

def run_highlights_generator():
    if not os.path.exists(INPUT_PR_PARQUET):
        print(f"{INPUT_PR_PARQUET} does not exist.")
        return

    print(f"Found {len(api_keys)} API Keys loaded.")

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
            f"data/sources/bitcoin/doc/release-notes/release-notes-{clean_ms}.md",
            f"data/sources/bitcoin/doc/release-notes/release-notes-{clean_ms}.0.md",
            "data/sources/bitcoin/doc/release-notes.md" # Fallback for active/unreleased
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
