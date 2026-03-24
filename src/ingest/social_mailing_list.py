import os
import subprocess
import pandas as pd
import email
from email.utils import parseaddr, parsedate_to_datetime
import re
import json
from datetime import datetime
import sys

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import RAW_DIR, WORK_DIR, ID_PATH, DATA_DIR

# --- Configuration (Centralized via paths.py) ---
MAILING_LIST_PATH = os.path.join(RAW_DIR, "social", "mailing_list_repo") 
OUTPUT_PARQUET = os.path.join(WORK_DIR, "social", "mailing_list.parquet")
ALIASES_PATH = ID_PATH
STATE_PATH = os.path.join(DATA_DIR, "state", "state.json")

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
            # Ensure it's timezone naive for parquet
            dt = dt.astimezone().replace(tzinfo=None)
        except:
            dt = None
            
        # Extract body snippet
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
            
        snippet = body[:300].replace('\n', ' ').strip()
        snippet = re.sub(r'\s+', ' ', snippet)
        
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
    # Always check for local shard 0 (The legacy master repo)
    if os.path.exists(os.path.join(MAILING_LIST_PATH, ".git")):
        shards.append("0")

    # Check for additional lore.kernel.org shards
    for i in range(1, 20):  
        url = f"https://lore.kernel.org/bitcoindev/{i}.git"
        cmd = ["git", "ls-remote", url, "HEAD"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            shards.append(str(i))
        else:
            break  
    return shards

def main():
    print("--- Mailing List Ingestion (New Architecture) ---")
    state = load_state()
    shards = get_available_shards()
    all_records = []
    lookup = load_aliases()
    
    existing_ids = set()
    if os.path.exists(OUTPUT_PARQUET):
        existing_df = pd.read_parquet(OUTPUT_PARQUET)
        existing_ids = set(existing_df['message_id'].dropna())
    
    for shard in shards:
        # Shard 0 is the primary local clone
        if shard == '0':
            path = MAILING_LIST_PATH
        else:
            # Additional shards live in a structured raw-archive location
            path = os.path.join(RAW_DIR, "social", f"bitcoindev_shard_{shard}.git")
            
        if not os.path.exists(path):
            print(f"Cloning shard {shard} to {path}...")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            subprocess.run(["git", "clone", "--bare", f"https://lore.kernel.org/bitcoindev/{shard}.git", path], check=True)
        
        print(f"Reading emails from Git repo (Shard {shard})...")
        
        # Get latest commit for state tracking
        latest_commit = subprocess.run(["git", "-C", path, "rev-parse", "HEAD"], 
                                      capture_output=True, text=True).stdout.strip()
        
        last_commit = state.get("mailing_list", {}).get(shard, "")
        if last_commit == latest_commit:
            print(f"  Shard {shard} is up to date.")
            continue
        
        # Get list of all blobs via git ls-tree (fast)
        cmd = ["git", "-C", path, "ls-tree", "-r", "HEAD"]
        lines = subprocess.run(cmd, capture_output=True, text=True).stdout.splitlines()
        total_files = len(lines)
        
        # Process emails using git cat-file --batch for high performance
        batch_cmd = ["git", "-C", path, "cat-file", "--batch"]
        process = subprocess.Popen(batch_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        processed = 0
        for i, line in enumerate(lines):
            parts = line.split()
            if len(parts) < 4 or parts[1] != 'blob':
                continue
            sha = parts[2]
            
            process.stdin.write(f"{sha}\n".encode())
            process.stdin.flush()
            
            header = process.stdout.readline().decode().split()
            if not header or header[1] == 'missing':
                continue
                
            size = int(header[2])
            content = process.stdout.read(size)
            process.stdout.read(1) # trailing newline
            
            res = parse_email_content(content)
            if res and res['message_id'] not in existing_ids:
                res["canonical_id"] = map_author(res["author_name"], res["author_email"], lookup)
                all_records.append(res)
                processed += 1
                
            if (i + 1) % 10000 == 0:
                print(f"  Processed {i + 1}/{total_files} emails in shard {shard}...")
        
        process.stdin.close()
        process.wait()
        
        print(f"  Added {processed} new emails from shard {shard}.")
        state.setdefault("mailing_list", {})[shard] = latest_commit

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
    
    # Update global state
    if os.path.exists(OUTPUT_PARQUET):
        df_all = pd.read_parquet(OUTPUT_PARQUET)
        if not df_all.empty:
            state.setdefault("mailing_list", {})["latest_date"] = df_all['date'].max().isoformat()
            state["mailing_list"]["total_messages"] = len(df_all)
    
    save_state(state)
    print("--- Mailing List Ingestion Complete ---")

if __name__ == "__main__":
    main()
