import os
import subprocess
import pandas as pd
import email
from email.utils import parseaddr, parsedate_to_datetime
import re
import json
from datetime import datetime
import sys

sys.path.append(os.getcwd())
from scripts.utils.identity import resolver

# --- Configuration ---
MAILING_LIST_PATH = "data/sources/mailing_list" # Full local archive
OUTPUT_PARQUET = "data/raw/social_mailing_list.parquet"
STATE_PATH = "data/state.json"

def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, 'w') as f:
        json.dump(state, f, indent=2)

def map_author(name, email_addr):
    return resolver.resolve_git(name, email_addr)

def parse_email_content(content):
    try:
        msg = email.message_from_bytes(content)
            
        subject = msg.get('Subject')
        from_hdr = msg.get('From')
        date_hdr = msg.get('Date')
        msg_id = msg.get('Message-ID')
        in_reply_to = msg.get('In-Reply-To')
        
        name, addr = parseaddr(from_hdr)
        
        try:
            dt = parsedate_to_datetime(date_hdr)
            # Ensure it's timezone naive for parquet if needed
            dt = dt.astimezone().replace(tzinfo=None)
        except:
            dt = None
            
        # Extract body
        body = ""
        try:
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        payload = part.get_payload(decode=True)
                        if payload:
                            body = payload.decode('utf-8', errors='replace')
                        break
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    body = payload.decode('utf-8', errors='replace')
        except:
            pass
            
        snippet = body[:200].replace('\n', ' ').strip()
        
        return {
            "source": "mailing_list",
            "message_id": msg_id,
            "date": dt,
            "author_name": name,
            "author_email": addr,
            "subject": subject,
            "body_snippet": snippet,
            "thread_id": in_reply_to or msg_id,
            "reply_to": in_reply_to,
            "is_reply": in_reply_to is not None
        }
    except Exception as e:
        return None

def get_available_shards():
    return ["0"]

def main():
    state = load_state()
    SHARDS = get_available_shards()
    all_records = []
    all_records = []
    
    existing_ids = set()
    if os.path.exists(OUTPUT_PARQUET):
        existing_df = pd.read_parquet(OUTPUT_PARQUET)
        existing_ids = set(existing_df['message_id'].dropna())
    
    for shard in SHARDS:
        path = f"data/sources/mailing_list/shard_{shard}"
        if not os.path.exists(os.path.join(path, 'HEAD')):
            if os.path.exists(path):
                print(f"Warning: shard_{shard} exists but is not a valid bare repo — removing and re-cloning...")
                import shutil
                shutil.rmtree(path)
            print(f"Cloning shard {shard}...")
            clone_url = "https://gnusha.org/pi/bitcoindev/"
            subprocess.run(["git", "clone", "--bare", clone_url, path], check=True)
        
        print(f"Ingesting mailing list from Git repo (Shard {shard}): {path}...")

        # Fetch latest from remote before comparing
        print(f"  Fetching latest from remote for shard {shard}...")
        fetch_result = subprocess.run(
            ["git", "-C", path, "fetch", "origin"],
            capture_output=True, text=True
        )
        if fetch_result.returncode != 0:
            print(f"Warning: fetch failed for shard {shard}: {fetch_result.stderr}")

        # Use FETCH_HEAD (latest from remote) rather than local HEAD
        cmd = ["git", "-C", path, "rev-parse", "FETCH_HEAD"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            # Fall back to local HEAD if no FETCH_HEAD
            cmd = ["git", "-C", path, "rev-parse", "HEAD"]
            result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error getting commit for shard {shard}: {result.stderr}")
            continue
        latest_commit = result.stdout.strip()

        last_commit = state.get("mailing_list", {}).get(shard, "")
        if last_commit == latest_commit:
            print(f"Shard {shard} is up to date.")
            continue

        # Get list of all blobs in the fetched commit
        cmd = ["git", "-C", path, "ls-tree", "-r", latest_commit]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error running git ls-tree for shard {shard}: {result.stderr}")
            continue
            
        lines = result.stdout.splitlines()
        total_files = len(lines)
        print(f"Found {total_files} potential email files in shard {shard}.")
        
        # Start git cat-file --batch
        batch_cmd = ["git", "-C", path, "cat-file", "--batch"]
        process = subprocess.Popen(batch_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        processed = 0
        for i, line in enumerate(lines):
            parts = line.split()
            if len(parts) < 4 or parts[1] != 'blob':
                continue
            sha = parts[2]
            
            # Send SHA to batch process
            process.stdin.write(f"{sha}\n".encode())
            process.stdin.flush()
            
            # Read header: <sha> <type> <size>
            header = process.stdout.readline().decode().split()
            if not header or header[1] == 'missing':
                continue
                
            size = int(header[2])
            content = process.stdout.read(size)
            process.stdout.read(1) # Read trailing newline
            
            res = parse_email_content(content)
            if res and res['message_id'] not in existing_ids:
                res["canonical_id"] = map_author(res["author_name"], res["author_email"])
                all_records.append(res)
                processed += 1
                
            if (i + 1) % 5000 == 0:
                print(f"  Processed {i + 1}/{total_files} emails in shard {shard}...")
        
        process.stdin.close()
        process.wait()
        
        print(f"Added {processed} new emails from shard {shard}.")
        
        # Update state
        state.setdefault("mailing_list", {})[shard] = latest_commit

    # Save all new records to parquet
    if all_records:
        df_new = pd.DataFrame(all_records)
        if os.path.exists(OUTPUT_PARQUET):
            df_old = pd.read_parquet(OUTPUT_PARQUET)
            df_all = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(subset=['message_id'])
        else:
            df_all = df_new
        
        os.makedirs(os.path.dirname(OUTPUT_PARQUET), exist_ok=True)
        df_all.to_parquet(OUTPUT_PARQUET, index=False)
        print(f"Saved {len(df_all)} total messages to {OUTPUT_PARQUET}")
    
    # Post-processing to update human-readable state
    if os.path.exists(OUTPUT_PARQUET):
        df_all = pd.read_parquet(OUTPUT_PARQUET)
        if not df_all.empty:
            state.setdefault("mailing_list", {})["latest_date"] = df_all['date'].max().isoformat()
            state["mailing_list"]["total_messages"] = len(df_all)
            print(f"Mailing list state updated: {len(df_all)} messages, latest from {state['mailing_list']['latest_date']}")
    
    save_state(state)

if __name__ == "__main__":
    main()
