import os
import subprocess
import pandas as pd
import json
import re
from datetime import datetime
import time
import sys

sys.path.append(os.getcwd())
from scripts.utils.identity import resolver

# --- Configuration ---
ARCHIVE_REPO_URL = "https://github.com/jamesob/delving-bitcoin-archive"
ARCHIVE_PATH = "data/sources/delving"
OUTPUT_PARQUET = "data/raw/social_delving.parquet"
IDENTITY_CURATED_PATH = "metadata/identity_curated.json"
STATE_PATH = "data/state.json"


def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_PATH, 'w') as f:
        json.dump(state, f, indent=2)


def _load_bots():
    """Return a lowercase set of bot/system usernames from identity_curated.json."""
    if not os.path.exists(IDENTITY_CURATED_PATH):
        return set()
    with open(IDENTITY_CURATED_PATH) as f:
        curated = json.load(f)
    return {b.lower() for b in curated.get("special_nodes", {}).get("bots", [])}

def setup_archive():
    """Clones or pulls the Delving Bitcoin archive repository."""
    if not os.path.isdir(os.path.join(ARCHIVE_PATH, '.git')):
        if os.path.exists(ARCHIVE_PATH):
            print(f"Warning: {ARCHIVE_PATH} exists but has no .git — removing and re-cloning...")
            import shutil
            shutil.rmtree(ARCHIVE_PATH)
        print(f"Cloning Delving archive to {ARCHIVE_PATH}...")
        subprocess.run(["git", "clone", "--depth", "1", ARCHIVE_REPO_URL, ARCHIVE_PATH], check=True)
    else:
        print(f"Updating Delving archive in {ARCHIVE_PATH}...")
        subprocess.run(["git", "-C", ARCHIVE_PATH, "pull"], check=True)

def map_author(username):
    return resolver.resolve_delving(username)

def process_archive():
    print("Processing Delving archive files...")
    BOTS = _load_bots()
    all_records = []
    posts_root = os.path.join(ARCHIVE_PATH, "archive", "posts")
    
    if not os.path.exists(posts_root):
        print(f"Error: Posts directory not found at {posts_root}")
        return []

    count = 0
    skipped_bots = 0
    for root, dirs, files in os.walk(posts_root):
        for file in files:
            if file.endswith(".json"):
                path = os.path.join(root, file)
                try:
                    with open(path, 'r') as f:
                        post = json.load(f)
                    
                    post_id = post["id"]
                    topic_id = post["topic_id"]
                    topic_title = post.get("topic_title", "Unknown Topic")
                    topic_slug = post.get("topic_slug", "unknown")
                    created_at = post["created_at"]
                    post_number = post["post_number"]
                    reply_to_post_number = post.get("reply_to_post_number")
                    cooked = post.get("cooked", "")
                    
                    author_name = post.get("name") or post.get("username")
                    author_username = post.get("username")

                    # Skip known bot/system accounts at ingest time
                    if author_username and author_username.lower() in BOTS:
                        skipped_bots += 1
                        continue

                    canonical_id = map_author(author_username)
                    
                    # Clean snippet
                    body_snippet = re.sub(r'<[^>]+>', '', cooked)[:200].strip()
                    
                    all_records.append({
                        "source": "delving",
                        "message_id": f"post_{post_id}",
                        "date": pd.to_datetime(created_at).tz_localize(None),
                        "author_name": author_name,
                        "author_username": author_username,
                        "author_email": None,
                        "canonical_id": canonical_id,
                        "subject": topic_title if post_number == 1 else f"Re: {topic_title}",
                        "body_snippet": body_snippet,
                        "thread_id": f"topic_{topic_id}",
                        "reply_to": f"post_{reply_to_post_number}" if reply_to_post_number else None,
                        "is_reply": reply_to_post_number is not None,
                        "link": f"https://delvingbitcoin.org/t/{topic_slug}/{topic_id}/{post_number}"
                    })
                    
                    count += 1
                    if count % 1000 == 0:
                        print(f"  Processed {count} posts...")
                        
                except Exception as e:
                    print(f"Error processing {path}: {e}")
                    
    if skipped_bots:
        print(f"  Skipped {skipped_bots} post(s) from bot/system accounts.")
    return all_records

def main():
    setup_archive()

    # State checkpoint: skip if archive HEAD hasn't changed since last build.
    latest_commit = subprocess.run(
        ["git", "-C", ARCHIVE_PATH, "rev-parse", "HEAD"], capture_output=True, text=True
    ).stdout.strip()
    state = load_state()
    if (state.get("delving", {}).get("latest_commit") == latest_commit
            and os.path.exists(OUTPUT_PARQUET)):
        print(f"Delving archive is up to date at commit {latest_commit[:12]}. Skipping re-parse.")
        return

    records = process_archive()
    
    if records:
        df = pd.DataFrame(records)
        df = df.sort_values('date', ascending=False)
        # Deduplicate
        df = df.drop_duplicates(subset=['message_id'])
        
        os.makedirs(os.path.dirname(OUTPUT_PARQUET), exist_ok=True)
        df.to_parquet(OUTPUT_PARQUET, index=False)
        print(f"\nSaved {len(df)} Delving posts to {OUTPUT_PARQUET}")

        # Persist state checkpoint
        state = load_state()
        state.setdefault("delving", {})["latest_commit"] = latest_commit
        state["delving"]["max_topic_id"] = max(
            (int(r["thread_id"].replace("topic_", ""))
             for r in records if (r.get("thread_id") or "").startswith("topic_")),
            default=state.get("delving", {}).get("max_topic_id", 0)
        )
        state["delving"]["total_posts"] = len(df)
        save_state(state)

        # Summary for sanity check
        print("\nTop 5 Delving Contributors:")
        print(df['canonical_id'].value_counts().head(5))
    else:
        print("No Delving records found in archive.")

if __name__ == "__main__":
    main()
