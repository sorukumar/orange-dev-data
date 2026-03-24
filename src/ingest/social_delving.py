import os
import subprocess
import pandas as pd
import json
import re
from datetime import datetime
import time
import sys

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import RAW_DIR, WORK_DIR, ID_PATH

# --- Configuration (Centralized via paths.py) ---
ARCHIVE_REPO_URL = "https://github.com/jamesob/delving-bitcoin-archive"
ARCHIVE_PATH = os.path.join(RAW_DIR, "social", "delving_archive")
OUTPUT_PARQUET = os.path.join(WORK_DIR, "social", "delving.parquet")
ALIASES_PATH = ID_PATH

def setup_archive():
    """Clones or pulls the Delving Bitcoin archive repository."""
    if not os.path.exists(ARCHIVE_PATH):
        print(f"Cloning Delving archive to {ARCHIVE_PATH}...")
        os.makedirs(os.path.dirname(ARCHIVE_PATH), exist_ok=True)
        # Shallow clone is fine for Delving archive as we just read JSONs
        subprocess.run(["git", "clone", "--depth", "1", ARCHIVE_REPO_URL, ARCHIVE_PATH], check=True)
    else:
        print(f"Updating Delving archive in {ARCHIVE_PATH}...")
        subprocess.run(["git", "-C", ARCHIVE_PATH, "pull"], check=True)

def load_aliases():
    """Loads and flattens the aliases lookup for rapid searching."""
    if not os.path.exists(ALIASES_PATH):
        print(f"Warning: Aliases file not found at {ALIASES_PATH}")
        return {}
    
    with open(ALIASES_PATH, 'r') as f:
        data = json.load(f)
    
    lookup = {}
    for entry in data.get("aliases", []):
        canonical = entry["canonical_name"]
        lookup[canonical.lower()] = canonical
        for alias in entry.get("aliases", []):
            lookup[alias.lower()] = canonical
        for email in entry.get("emails", []):
            lookup[email.lower()] = canonical
            
    return lookup

def map_author(name, username, lookup):
    """Maps a name or username to a canonical ID (name)."""
    if name and name.lower() in lookup:
        return lookup[name.lower()]
    if username and username.lower() in lookup:
        return lookup[username.lower()]
    return name or username

def process_archive():
    """Parses individual JSON files from the Delving archive."""
    print("Processing Delving archive files...")
    all_records = []
    lookup = load_aliases()
    posts_root = os.path.join(ARCHIVE_PATH, "archive", "posts")
    
    if not os.path.exists(posts_root):
        print(f"Error: Posts directory not found at {posts_root}")
        return []

    count = 0
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
                    
                    canonical_id = map_author(author_name, author_username, lookup)
                    
                    # Clean snippet (Remove HTML tags)
                    body_snippet = re.sub(r'<[^>]+>', '', cooked)[:300].strip()
                    # Remove multiple spaces/newlines
                    body_snippet = re.sub(r'\s+', ' ', body_snippet)
                    
                    all_records.append({
                        "source": "delving",
                        "message_id": f"post_{post_id}",
                        "date": pd.to_datetime(created_at).tz_localize(None),
                        "author_name": author_name,
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
                    if count % 2000 == 0:
                        print(f"  Processed {count} posts...")
                        
                except Exception as e:
                    print(f"Error processing {path}: {e}")
                    
    return all_records

def main():
    print("--- Delving Ingestion (New Architecture) ---")
    setup_archive()
    records = process_archive()
    
    if records:
        df = pd.DataFrame(records)
        df = df.sort_values('date', ascending=False)
        # Deduplicate
        df = df.drop_duplicates(subset=['message_id'])
        
        os.makedirs(os.path.dirname(OUTPUT_PARQUET), exist_ok=True)
        df.to_parquet(OUTPUT_PARQUET, index=False)
        print(f"\nFinalized {len(df)} Delving posts in {OUTPUT_PARQUET}")
        
        # Summary for sanity check
        print("\nTop 5 Delving Contributors:")
        print(df['canonical_id'].value_counts().head(5))
    else:
        print("No Delving records found to process.")

if __name__ == "__main__":
    main()
