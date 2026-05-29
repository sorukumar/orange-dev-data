import os
import json
import pandas as pd
import subprocess
from glob import glob
from datetime import datetime
import tqdm

# --- Configuration ---
METADATA_REPO_URL = "https://github.com/bitcoin-data/github-metadata-backup-bitcoin-bitcoin"
METADATA_PATH = "data/sources/bitcoin-github-metadata"
OUTPUT_PR_PARQUET = "data/raw/github_pr_metadata.parquet"
OUTPUT_REVIEW_PARQUET = "data/raw/github_review_events.parquet"
STATE_PATH = "data/state.json"


def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_PATH, 'w') as f:
        json.dump(state, f, indent=2)


def setup_metadata():
    """Clones or pulls the GitHub metadata backup repository."""
    if not os.path.isdir(os.path.join(METADATA_PATH, '.git')):
        if os.path.exists(METADATA_PATH):
            print(f"Warning: {METADATA_PATH} exists but has no .git — removing and re-cloning...")
            import shutil
            shutil.rmtree(METADATA_PATH)
        print(f"Cloning GitHub metadata archive to {METADATA_PATH}...")
        # Using shallow clone to keep the data footprint manageable
        subprocess.run(["git", "clone", "--depth", "1", METADATA_REPO_URL, METADATA_PATH], check=True)
    else:
        print(f"Updating GitHub metadata archive in {METADATA_PATH}...")
        subprocess.run(["git", "-C", METADATA_PATH, "pull"], check=True)

def process_metadata():
    # State checkpoint: skip if archive HEAD hasn't changed since last build.
    latest_commit = subprocess.run(
        ["git", "-C", METADATA_PATH, "rev-parse", "HEAD"], capture_output=True, text=True
    ).stdout.strip()
    state = load_state()
    if (state.get("github_metadata", {}).get("latest_commit") == latest_commit
            and os.path.exists(OUTPUT_PR_PARQUET) and os.path.exists(OUTPUT_REVIEW_PARQUET)):
        print(f"GitHub metadata archive is up to date at commit {latest_commit[:12]}. Skipping re-parse.")
        return

    source_dir = os.path.join(METADATA_PATH, "pulls")
    
    json_files = glob(os.path.join(source_dir, "*.json"))
    print(f"Processing {len(json_files)} PR metadata files...")
    
    pr_data = []
    review_data = []
    
    for f in tqdm.tqdm(json_files):
        try:
            with open(f, 'r') as jfile:
                data = json.load(jfile)
                pull = data.get('pull', {})
                if not pull:
                    continue
                    
                pr_num = pull.get('number')
                user_obj = pull.get('user') or {}
                author = user_obj.get('login')
                github_id = user_obj.get('id')
                
                created_at = pull.get('created_at')
                merged_at = pull.get('merged_at')
                closed_at = pull.get('closed_at')
                
                # High-fidelity anchors: Labels & Head SHA
                labels = [l.get('name') for l in pull.get('labels', []) if l.get('name')]
                head_sha = (pull.get('head') or {}).get('sha')
                
                pr_data.append({
                    'pr_number': pr_num,
                    'author': author,
                    'github_id': github_id,
                    'head_sha': head_sha,
                    'labels': "|".join(labels) if labels else None,
                    'created_at': created_at,
                    'merged_at': merged_at,
                    'closed_at': closed_at,
                    'title': pull.get('title')
                })
                
                # Process events for review signals
                events = data.get('events', [])
                for ev in events:
                    ev_user = ev.get('user') or ev.get('actor') or {}
                    user = ev_user.get('login')
                    ev_github_id = ev_user.get('id')
                    
                    if not user:
                        continue
                        
                    ts = ev.get('created_at') or ev.get('submitted_at')
                    if not ts:
                        continue
                        
                    body = ev.get('body', '') or ''
                    state = ev.get('state', '')
                    
                    review_data.append({
                        'pr_number': pr_num,
                        'user': user,
                        'github_id': ev_github_id,
                        'timestamp': ts,
                        'body': body[:200], # Keep it small
                        'state': state,
                        'event_type': ev.get('event', ev.get('state', 'commented'))
                    })
        except Exception as e:
            print(f"Error processing {f}: {e}")
            
    df_prs = pd.DataFrame(pr_data)
    df_reviews = pd.DataFrame(review_data)
    
    # Save to parquet
    os.makedirs("data/raw", exist_ok=True)
    df_prs.to_parquet(OUTPUT_PR_PARQUET, index=False)
    df_reviews.to_parquet(OUTPUT_REVIEW_PARQUET, index=False)
    
    print(f"Saved {len(df_prs)} PRs to {OUTPUT_PR_PARQUET}")
    print(f"Saved {len(df_reviews)} review events to {OUTPUT_REVIEW_PARQUET}")

    # Persist state checkpoint
    state = load_state()
    state.setdefault("github_metadata", {})["latest_commit"] = latest_commit
    state["github_metadata"]["total_prs"] = len(df_prs)
    state["github_metadata"]["total_events"] = len(df_reviews)
    save_state(state)

if __name__ == "__main__":
    setup_metadata()
    process_metadata()
