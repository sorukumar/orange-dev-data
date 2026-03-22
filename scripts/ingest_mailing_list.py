import os
import subprocess
import pandas as pd
import email
from email.utils import parseaddr, parsedate_to_datetime
import re
import json
from datetime import datetime

# --- Configuration ---
MAILING_LIST_PATH = "raw_data/mailing_list" # Full local archive
OUTPUT_PARQUET = "data/raw/social_mailing_list.parquet"
ALIASES_PATH = "lookups/identity_mappings.json"
STATE_PATH = "data/state.json"

def load_aliases():
    if not os.path.exists(ALIASES_PATH):
        return {}
    with open(ALIASES_PATH, 'r') as f:
        data = json.load(f)
    lookup = {}
    for entry in data.get("aliases", []):
        canonical = entry["canonical_name"]
        lookup[canonical.lower()] = canonical
        for alias in entry.get("aliases", []):
            lookup[alias.lower()] = canonical
        for email_addr in entry.get("emails", []):
            lookup[email_addr.lower()] = canonical
    return lookup

def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, 'w') as f:
        json.dump(state, f, indent=2)

def map_author(name, email_addr, lookup):
    if email_addr and email_addr.lower() in lookup:
        return lookup[email_addr.lower()]
    if name and name.lower() in lookup:
        return lookup[name.lower()]
    return name or email_addr

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
    shards = []
    for i in range(10):  # Check up to shard 9, assuming sequential
        url = f"https://lore.kernel.org/bitcoindev/{i}.git"
        cmd = ["git", "ls-remote", url, "HEAD"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            shards.append(str(i))
        else:
            break  # Stop at first missing shard
    return shards

def main():
    state = load_state()
    SHARDS = get_available_shards()
    all_records = []
    lookup = load_aliases()
    
    existing_ids = set()
    if os.path.exists(OUTPUT_PARQUET):
        existing_df = pd.read_parquet(OUTPUT_PARQUET)
        existing_ids = set(existing_df['message_id'].dropna())
    
    for shard in SHARDS:
        if shard == '0':
            path = "raw_data/mailing_list"
        else:
            path = f"data/raw_archives/bitcoindev{shard}.git"
        if not os.path.exists(path):
            print(f"Cloning shard {shard}...")
            subprocess.run(["git", "clone", "--bare", f"https://lore.kernel.org/bitcoindev/{shard}.git", path], check=True)
        
        print(f"Ingesting mailing list from Git repo (Shard {shard}): {path}...")
        
        # Get latest commit
        cmd = ["git", "-C", path, "rev-parse", "HEAD"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error getting HEAD for shard {shard}: {result.stderr}")
            continue
        latest_commit = result.stdout.strip()
        
        last_commit = state.get("mailing_list", {}).get(shard, "")
        if last_commit == latest_commit:
            print(f"Shard {shard} is up to date.")
            continue
        
        # Get list of all blobs in HEAD
        cmd = ["git", "-C", path, "ls-tree", "-r", "HEAD"]
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
                res["canonical_id"] = map_author(res["author_name"], res["author_email"], lookup)
                all_records.append(res)
                processed += 1
                
            if (i + 1) % 5000 == 0:
                print(f"  Processed {i + 1}/{total_files} emails in shard {shard}...")
        
        process.stdin.close()
        process.wait()
        
        print(f"Added {processed} new emails from shard {shard}.")
        
        # Update state
        state.setdefault("mailing_list", {})[shard] = latest_commit
    
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
