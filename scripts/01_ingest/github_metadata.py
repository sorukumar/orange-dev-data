import os
import json
import pandas as pd
import subprocess
from glob import glob
from datetime import datetime
import tqdm

METADATA_REPOS = {
    "bitcoin/bitcoin": "data/sources/bitcoin-github-metadata",
    "bitcoin-core/secp256k1": "data/sources/secp256k1-github-metadata",
    "bitcoin-core/gui": "data/sources/gui-github-metadata"
}
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
    pass  # Cloning is now handled by scripts/maintenance/fetch_github_prs.py

def process_metadata():
    state = load_state()
    pr_data = []
    review_data = []
    
    for repo_name, meta_path in METADATA_REPOS.items():
        if not os.path.exists(meta_path):
            print(f"Skipping {repo_name} metadata, {meta_path} does not exist.")
            continue
            
        latest_commit = subprocess.run(
            ["git", "-C", meta_path, "rev-parse", "HEAD"], capture_output=True, text=True
        ).stdout.strip()
        
        print(f"Processing {repo_name} metadata...")
        source_dir = os.path.join(meta_path, "pulls")
        json_files = glob(os.path.join(source_dir, "*.json"))
        
        state.setdefault("github_metadata_repos", {})[repo_name] = {"latest_commit": latest_commit}
        
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
                    milestone = (pull.get('milestone') or {}).get('title')
                
                    pr_data.append({
                        'repository_name': repo_name,
                        'pr_number': pr_num,
                        'author': author,
                        'github_id': github_id,
                        'head_sha': head_sha,
                        'labels': "|".join(labels) if labels else None,
                        'created_at': created_at,
                        'merged_at': merged_at,
                        'closed_at': closed_at,
                        'title': pull.get('title'),
                        'milestone': milestone
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
                        review_state = ev.get('state', '')
                    
                        review_data.append({
                            'repository_name': repo_name,
                            'pr_number': pr_num,
                            'user': user,
                            'github_id': ev_github_id,
                            'timestamp': ts,
                            'body': body[:200], # Keep it small
                            'state': review_state,
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
    state["github_metadata_total_prs"] = len(df_prs)
    state["github_metadata_total_events"] = len(df_reviews)
    save_state(state)

if __name__ == "__main__":
    setup_metadata()
    process_metadata()
