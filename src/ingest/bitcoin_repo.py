import subprocess
import pandas as pd
import re
import os
import sys
import json
from datetime import datetime, timezone, timedelta

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import RAW_DIR, WORK_DIR

# --- Configuration (Centralized via paths.py) ---
REPO_PATH = os.path.join(RAW_DIR, "bitcoin_repo")
OUTPUT_PATH = os.path.join(WORK_DIR, "core", "commits.parquet")
MESSAGES_OUTPUT_PATH = os.path.join(WORK_DIR, "core", "commit_messages.parquet")

# --- Categorization Logic (The Subsystem Mapping) ---
CATEGORY_RULES = {
    "Tests (QA)": [
        r"/test/", r"/fuzz/", r"/bench/", 
        r"src/test/", r"test/", r"src/bench/"
    ],
    "Build & CI (DevOps)": [
        r"Makefile", r"ci/", r"\.github/", r"build_msvc", r"configure\.ac",
        r"CMakeLists\.txt", r"depends/", r"share/"
    ],
    "Documentation": [r"doc/", r".*\.md$", r".*\.txt$", r".*\.rst$"],
    "Consensus (Domain Logic)": [
        r"src/consensus/", r"src/kernel/", r"src/script/", r"src/primitives/",
        r"src/chain", r"src/coins", r"src/pow", r"src/validation\.", r"src/policy/"
    ],
    "Node & RPC (App/Interface)": [
        r"src/node/", r"src/rpc/", r"src/index/", r"src/zmq/",
        r"src/init\.", r"src/bitcoind\.", r"src/bitcoin-cli\.", r"src/txmempool\."
    ],
    "P2P Network (Infrastructure)": [r"src/net", r"src/protocol", r"src/addrman"],
    "Wallet (Client App)": [r"src/wallet/", r"src/interfaces/"],
    "GUI (Presentation Layer)": [r"src/qt/", r"src/forms/"],
    "Database (Persistence)": [r"src/leveldb/", r"src/crc32c/", r"src/dbwrapper\."],
    "Cryptography (Primitives)": [r"src/crypto/", r"src/secp256k1/", r"src/minisketch/"],
    "Utilities (Shared Libs)": [
        r"src/util/", r"src/support/", r"src/common/",
        r"src/univalue/", r"src/compat/", r"src/ipc/"
    ]
}

def get_git_log(repo_path):
    """Extracts raw git log with numstat and structured formatting."""
    cmd = [
        "git",
        "-C", repo_path,
        "log",
        "master",
        "--format=COMMIT_Start^|^%H^|^%at^|^%an^|^%ae^|^%cn^|^%ce^|^%ct^|^%P^|^%ai^|^%s",
        "--numstat",
        "-m" 
    ]
    print(f"Running git log in {repo_path}...")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, errors='replace', bufsize=10*1024*1024)
    return process

def get_git_log_with_messages(repo_path):
    """Extracts full commit bodies for ACK/reviewer parsing."""
    cmd = [
        "git",
        "-C", repo_path,
        "log",
        "master",
        "--format=MESSAGE_START^|^%H^|^%s^|^%b^|^MESSAGE_END",
    ]
    print(f"Extracting commit messages for reviewer parsing...")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, errors='replace', bufsize=10*1024*1024)
    return process

def parse_log(process):
    stream = process.stdout
    commits = []
    seen_hashes = set()
    
    curr_meta = None
    curr_stats = []
    
    for line in stream:
        line = line.strip()
        if not line:
            continue
            
        if line.startswith("COMMIT_Start^|^"):
            if curr_meta:
                if curr_meta["hash"] not in seen_hashes:
                    process_commit(curr_meta, curr_stats, commits)
                    seen_hashes.add(curr_meta["hash"])
            
            parts = line.split("^|^")
            curr_meta = {
                "hash": parts[1],
                "author_ts": int(parts[2]),
                "author_name": parts[3],
                "author_email": parts[4],
                "committer_name": parts[5],
                "committer_email": parts[6],
                "committer_ts": int(parts[7]),
                "parents": parts[8],
                "timezone": parts[9],
                "subject": parts[10] if len(parts) > 10 else ""
            }
            curr_stats = []
        else:
            stat_parts = line.split(maxsplit=2)
            if len(stat_parts) == 3:
                adds = stat_parts[0]
                dels = stat_parts[1]
                path = stat_parts[2]
                
                if adds == '-': adds = 0
                if dels == '-': dels = 0
                
                curr_stats.append({
                    "adds": int(adds),
                    "dels": int(dels),
                    "path": path
                })
    
    if curr_meta and curr_meta["hash"] not in seen_hashes:
        process_commit(curr_meta, curr_stats, commits)
        
    stderr = process.stderr.read()
    if process.wait() != 0:
        print(f"Git command failed: {stderr}")
        
    return commits

def parse_messages(process):
    """Parse commit messages to extract hash, subject, and body."""
    stream = process.stdout
    messages = []
    seen_hashes = set()
    
    current_hash = None
    current_subject = None
    current_body_lines = []
    in_message = False
    
    for line in stream:
        line = line.rstrip('\n\r')
        
        if line.startswith("MESSAGE_START^|^"):
            # Save previous message
            if current_hash and current_hash not in seen_hashes:
                messages.append({
                    "hash": current_hash,
                    "subject": current_subject,
                    "body": "\n".join(current_body_lines)
                })
                seen_hashes.add(current_hash)
            
            parts = line.split("^|^")
            current_hash = parts[1] if len(parts) > 1 else None
            current_subject = parts[2] if len(parts) > 2 else ""
            body_start = parts[3] if len(parts) > 3 else ""
            current_body_lines = [body_start] if body_start else []
            in_message = True
            
        elif "^|^MESSAGE_END" in line or line.strip() == "MESSAGE_END":
            in_message = False
            
        elif in_message and current_hash:
            current_body_lines.append(line)
    
    if current_hash and current_hash not in seen_hashes:
        messages.append({
            "hash": current_hash,
            "subject": current_subject,
            "body": "\n".join(current_body_lines)
        })
    
    stderr = process.stderr.read()
    if process.wait() != 0:
        print(f"Git command failed: {stderr}")
    
    return messages

def categorize_file(path):
    for category, regexes in CATEGORY_RULES.items():
        for pattern in regexes:
            if re.search(pattern, path, re.IGNORECASE):
                return category
    return "Core Libs"

def process_commit(meta, stats, commits_list):
    total_adds = sum(x["adds"] for x in stats)
    total_dels = sum(x["dels"] for x in stats)
    
    is_merge = len(meta["parents"].split()) > 1
    cat_deltas = {}
    ext_deltas = {}

    if is_merge:
        cat_deltas = {"Merge": {"adds": 0, "dels": 0}}
    else:
        for s in stats:
            cat = categorize_file(s["path"])
            if cat not in cat_deltas:
                cat_deltas[cat] = {"adds": 0, "dels": 0}
            cat_deltas[cat]["adds"] += s["adds"]
            cat_deltas[cat]["dels"] += s["dels"]
            
            _, ext = os.path.splitext(s["path"])
            ext = ext.lower() or "(no_ext)"
            
            if ext not in ext_deltas:
                ext_deltas[ext] = {"adds": 0, "dels": 0}
            ext_deltas[ext]["adds"] += s["adds"]
            ext_deltas[ext]["dels"] += s["dels"]

    if not cat_deltas:
        cat_deltas["Core Libs"] = {"adds": 0, "dels": 0}

    dt_utc = datetime.fromtimestamp(meta["author_ts"], timezone.utc)
    
    # Author Domain
    domain = meta["author_email"].split("@")[-1].lower() if "@" in meta["author_email"] else "unknown"

    for category, metrics in cat_deltas.items():
        record = {
            "hash": meta["hash"],
            "date_utc": dt_utc.replace(tzinfo=None), # Naive for Parquet
            "year": dt_utc.year,
            "author_name": meta["author_name"],
            "author_email": meta["author_email"].lower(),
            "author_domain": domain,
            "is_merge": is_merge,
            "additions": metrics["adds"],
            "deletions": metrics["dels"],
            "category": category,
            "extensions_json": json.dumps(ext_deltas)
        }
        commits_list.append(record)

def scan_repository(repo_path):
    """Scans HEAD to count files/loc per category for baseline dashboard stats."""
    print("Scanning repository structure at HEAD...")
    stats = {}
    total_files = 0
    total_loc = 0
    
    for root, _, files in os.walk(repo_path):
        if ".git" in root: continue
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, repo_path)
            cat = categorize_file(rel_path)
            _, ext = os.path.splitext(file)
            ext = ext.lower() or "(no_ext)"
            
            loc = 0
            try:
                with open(full_path, 'rb') as f:
                    for _ in f: loc += 1
            except: pass
                
            if cat not in stats:
                stats[cat] = {"files": 0, "loc": 0, "languages": {}}
            
            stats[cat]["files"] += 1
            stats[cat]["loc"] += loc
            if ext not in stats[cat]["languages"]:
                stats[cat]["languages"][ext] = {"files": 0, "loc": 0}
            stats[cat]["languages"][ext]["files"] += 1
            stats[cat]["languages"][ext]["loc"] += loc
            
            total_files += 1
            total_loc += 1
            
    print(f"Scanned {total_files} files, {total_loc} lines.")
    meta_path = os.path.join(os.path.dirname(OUTPUT_PATH), "category_metadata.json")
    with open(meta_path, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"Metadata saved to {meta_path}")

def main():
    print("--- Bitcoin Core Ingestion (New Architecture) ---")
    if not os.path.exists(REPO_PATH):
        print(f"Error: Bitcoin Core repo not found at {REPO_PATH}")
        return

    process = get_git_log(REPO_PATH)
    commits = parse_log(process)
    print(f"Parsed {len(commits)} commit-category slices.")
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    pd.DataFrame(commits).to_parquet(OUTPUT_PATH, index=False)
    print(f"Saved to {OUTPUT_PATH}")
    
    msg_process = get_git_log_with_messages(REPO_PATH)
    messages = parse_messages(msg_process)
    pd.DataFrame(messages).to_parquet(MESSAGES_OUTPUT_PATH, index=False)
    print(f"Saved {len(messages)} messages to {MESSAGES_OUTPUT_PATH}")
    
    scan_repository(REPO_PATH)

if __name__ == "__main__":
    main()
