import subprocess
import pandas as pd
import re
from datetime import datetime, timezone, timedelta
import os
import sys
import json

REPOSITORIES = {
    "bitcoin/bitcoin": "data/sources/bitcoin",
    "bitcoin-core/secp256k1": "data/sources/secp256k1",
    "bitcoin-core/gui": "data/sources/gui",
    "bitcoin-core/guix.sigs": "data/sources/guix.sigs",
    "bitcoin-core/qa-assets": "data/sources/qa-assets",
    "bitcoin-core/HWI": "data/sources/HWI"
}
OUTPUT_PATH = "data/raw/core_commits.parquet"
MESSAGES_OUTPUT_PATH = "data/raw/core_messages.parquet"  # Standardized raw location
STATE_PATH = "data/state.json"


def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_PATH, 'w') as f:
        json.dump(state, f, indent=2)


# --- Categorization Logic ---
# Category names match expertise domain names exactly (see metadata/expertise_domains.json).
# Rule order matters: more-specific rules must appear before broader ones that share paths.
CATEGORY_RULES = {
    # ── Cross-cutting quality (evaluated before subsystem rules) ──────────────
    "Tests": [
        r"/test/", r"/fuzz/", r"/bench/",
        r"src/test/", r"test/", r"src/bench/"
    ],
    "Build & CI": [
        r"Makefile", r"ci/", r"\.github/", r"build_msvc", r"configure\.ac",
        r"CMakeLists\.txt", r"depends/", r"share/"
    ],
    "Documentation": [r"doc/", r".*\.md$", r".*\.txt$", r".*\.rst$"],

    # ── Script interpreter and opcodes ─────────────────────────────────────────
    # Intentionally before Consensus: src/script/ is the opcode/covenant surface,
    # distinct from chain-validation logic in src/consensus/.
    "Script": [
        r"src/script/"
    ],

    # ── Mempool and fee policy ─────────────────────────────────────────────────
    # Standardness rules, fee estimation, RBF, dust thresholds.
    # src/policy/ is policy (not consensus): move it out of Consensus bucket.
    "Mempool": [
        r"src/txmempool\.", r"src/policy/"
    ],

    # ── Mining — block assembly and template generation ────────────────────────
    "Mining": [
        r"src/mining/"
    ],

    # ── Consensus rules ────────────────────────────────────────────────────────
    # Chain state, validation, PoW, activation logic.
    # src/script/ and src/policy/ are carved out above.
    "Consensus": [
        r"src/consensus/", r"src/kernel/", r"src/primitives/",
        r"src/chain", r"src/coins", r"src/pow", r"src/validation\.",
        r"src/deploymentstatus", r"src/versionbits"
    ],

    # ── P2P networking ─────────────────────────────────────────────────────────
    "P2P Network": [r"src/net", r"src/protocol", r"src/addrman"],

    # ── Wallet ─────────────────────────────────────────────────────────────────
    "Wallet": [r"src/wallet/", r"src/interfaces/"],

    # ── Node and RPC interfaces ────────────────────────────────────────────────
    # src/txmempool. is carved out above into Mempool.
    "Node & RPC": [
        r"src/node/", r"src/rpc/", r"src/index/", r"src/zmq/",
        r"src/init\.", r"src/bitcoind\.", r"src/bitcoin-cli\."
    ],

    # ── GUI ────────────────────────────────────────────────────────────────────
    "GUI": [r"src/qt/", r"src/forms/"],

    # ── Persistence ────────────────────────────────────────────────────────────
    "Database": [r"src/leveldb/", r"src/crc32c/", r"src/dbwrapper\."],

    # ── Cryptographic primitives ───────────────────────────────────────────────
    "Cryptography": [r"src/crypto/", r"src/secp256k1/", r"src/minisketch/"],

    # ── Shared utilities ───────────────────────────────────────────────────────
    "Utilities": [
        r"src/util/", r"src/support/", r"src/common/",
        r"src/univalue/", r"src/compat/", r"src/ipc/"
    ]
}

def get_git_log(repo_path):
    """
    Extracts raw git log with specific formatting.
    Format: %H (hash) | %at (author_ts) | %an (author_name) | %ae (author_email) | %cn (committer_name) | %ce (committer_email) | %ct (commiter_ts) | %P (parents) | %s (subject)
    Followed by numstat.
    """

    cmd = [
        "git",
        "-C", repo_path,
        "log",
        "HEAD",
        "--format=COMMIT_Start^|^%H^|^%at^|^%an^|^%ae^|^%cn^|^%ce^|^%ct^|^%P^|^%ai^|^%s",
        # "--numstat", # Temporarily disable numstat to isolate the issue? No, keep it.
        "--numstat"
    ]
    
    # Increase buffer size and ensure text mode
    print(f"Running command: {' '.join(cmd)}")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, errors='replace', bufsize=10*1024*1024)
    
    # We yield stdout line by line, but if it finishes, we check stderr
    return process

def get_git_log_with_messages(repo_path):
    """
    NEW: Extracts git log with full commit body for ACK/reviewer parsing.
    This is a separate pass to avoid complicating the main numstat parsing.
    
    Format: hash, subject, body (includes trailers like ACK, Tested-by, etc.)
    
    NOTE: Future Enhancement - Consider hybrid approach with GitHub PR API
    for more complete reviewer data (includes non-merged PRs, review comments).
    See: https://docs.github.com/en/rest/pulls/reviews
    """
    cmd = [
        "git",
        "-C", repo_path,
        "log",
        "HEAD",
        "--format=MESSAGE_START^|^%H^|^%s^|^%b^|^MESSAGE_END",
    ]
    
    print(f"Extracting commit messages for reviewer parsing...")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, errors='replace', bufsize=10*1024*1024)
    
    return process

def parse_log(process, repo_name):
    stream = process.stdout
    commits = []
    seen_hashes = set()
    
    # Current Commit Buffer
    curr_meta = None
    curr_stats = []
    
    for line in stream:
        line = line.strip()
        if not line:
            continue
            
        if line.startswith("COMMIT_Start^|^"):
            # If previous commit exists, save it
            if curr_meta:
                # Deduplication check
                if curr_meta["hash"] not in seen_hashes:
                    process_commit(curr_meta, curr_stats, commits, repo_name)
                    seen_hashes.add(curr_meta["hash"])
            
            # Start new commit
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
            # Parse numstat line: "added  deleted  filepath"
            # Git numstat can return '-' for binary files
            stat_parts = line.split(maxsplit=2)
            if len(stat_parts) == 3:
                adds = stat_parts[0]
                dels = stat_parts[1]
                path = stat_parts[2]
                
                # Handle binary files ('-')
                if adds == '-': adds = 0
                if dels == '-': dels = 0
                
                curr_stats.append({
                    "adds": int(adds),
                    "dels": int(dels),
                    "path": path
                })
    
    # Process last commit
    if curr_meta and curr_meta["hash"] not in seen_hashes:
        process_commit(curr_meta, curr_stats, commits, repo_name)
        
    # Check for errors
    stderr = process.stderr.read()
    if process.wait() != 0:
        print(f"Git command failed: {stderr}")
    elif stderr:
        # Git log writes to stderr sometimes even on success (e.g. warnings)
        print(f"Git log stderr (warning): {stderr[:200]}...")
        
    return commits

def parse_messages(process, repo_name):
    """
    Parse commit messages to extract hash, subject, and body.
    Body contains ACK/NACK trailers we'll parse later.
    """
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
            # Save previous message if exists
            if current_hash and current_hash not in seen_hashes:
                messages.append({
                    "hash": current_hash,
                    "repository_name": repo_name,
                    "subject": current_subject,
                    "body": "\n".join(current_body_lines)
                })
                seen_hashes.add(current_hash)
            
            # Parse new message header
            parts = line.split("^|^")
            current_hash = parts[1] if len(parts) > 1 else None
            current_subject = parts[2] if len(parts) > 2 else ""
            # Body starts after subject, may span multiple lines until MESSAGE_END
            body_start = parts[3] if len(parts) > 3 else ""
            current_body_lines = [body_start] if body_start else []
            in_message = True
            
        elif "^|^MESSAGE_END" in line or line.strip() == "MESSAGE_END":
            # End of this message's body
            in_message = False
            
        elif in_message and current_hash:
            # Continuation of body
            current_body_lines.append(line)
    
    # Save last message
    if current_hash and current_hash not in seen_hashes:
        messages.append({
            "hash": current_hash,
            "repository_name": repo_name,
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
    return "Utilities"  # fallback: unrecognised path → shared utilities bucket

def process_commit(meta, stats, commits_list, repo_name):
    # Base Stats (Total for the commit)
    total_adds = sum(x["adds"] for x in stats)
    total_dels = sum(x["dels"] for x in stats)
    files_count = len(stats)
    
    # Categorization Stats
    is_merge = len(meta["parents"].split()) > 1
    cat_deltas = {}
    ext_deltas = {}

    if is_merge:
        # Handle Merges specially for authorship attribution
        # To avoid double-counting code authored in PR commits (X, Y) 
        # which are already on the master branch, we attribute 0 lines 
        # to the merge commit (Z) itself, but keep the record so 
        # the maintainer gets the "Commit Count" credit.
        cat_deltas = {"Merge": {"adds": 0, "dels": 0}}
    else:
        # Categorization logic for non-merge commits
        # Hardcode ecosystem repos to their native domains
        eco_map = {
            "bitcoin-core/HWI": "Wallet",
            "bitcoin-core/qa-assets": "Tests",
            "bitcoin-core/guix.sigs": "Build & CI"
        }
        repo_override = eco_map.get(repo_name)

        for s in stats:
            # Category
            cat = repo_override if repo_override else categorize_file(s["path"])
            if cat not in cat_deltas:
                cat_deltas[cat] = {"adds": 0, "dels": 0}
            cat_deltas[cat]["adds"] += s["adds"]
            cat_deltas[cat]["dels"] += s["dels"]
            
            # Extension
            _, ext = os.path.splitext(s["path"])
            ext = ext.lower()
            if not ext:
                ext = "(no_ext)"
            
            if ext not in ext_deltas:
                ext_deltas[ext] = {"adds": 0, "dels": 0}
            ext_deltas[ext]["adds"] += s["adds"]
            ext_deltas[ext]["dels"] += s["dels"]

    # If no stats (empty commit), assign fallback category with 0 stats
    if not cat_deltas:
        cat_deltas["Utilities"] = {"adds": 0, "dels": 0}

    dt_utc = datetime.fromtimestamp(meta["author_ts"], timezone.utc)
    
    # Extract timezone from %ai string...
    tz_str = meta["timezone"] 
    tz_offset_minutes = 0
    try:
        if tz_str:
            offset_token = tz_str.split()[-1]
            if len(offset_token) == 5 and (offset_token.startswith('+') or offset_token.startswith('-')):
                sign = 1 if offset_token[0] == '+' else -1
                hours = int(offset_token[1:3])
                minutes = int(offset_token[3:5])
                tz_offset_minutes = sign * (hours * 60 + minutes)
    except:
        pass

    # Author Domain
    domain = meta["author_email"].split("@")[-1].lower() if "@" in meta["author_email"] else "unknown"

    # Explode by Category
    # If a commit touches multiple categories, we create multiple rows.
    # Each row shares the same hash, date, author info.
    # But 'category' differs, and 'additions/deletions' refer to THAT category's churn.
    # We ALSO keep 'total_additions' for the whole commit, for context? 
    # Or should 'additions' be the category-specific additions? 
    # Yes, for detailed analysis, additions should be specific.
    # But 'files_count'? Specific or total? Specific makes sense.
    
    # Actually, let's keep it simple: 
    # 'category' is the specific category.
    # 'additions' is the specific additions for that category.
    
    for category, metrics in cat_deltas.items():
        record = {
            "hash": meta["hash"],
            "repository_name": repo_name,
            "date_utc": dt_utc,
            "year": dt_utc.year,
            "month": dt_utc.month,
            "day_of_week": dt_utc.weekday(), 
            "hour_utc": dt_utc.hour,
            "timezone_offset_minutes": tz_offset_minutes,
            
            # By default use author identity
            "author_name": meta["committer_name"] if is_merge or "merge" in meta["author_name"].lower() else meta["author_name"],
            "author_email": meta["committer_email"].lower() if is_merge or "merge" in meta["author_name"].lower() else meta["author_email"].lower(),
            "author_domain": domain,
            
            "committer_name": meta["committer_name"],
            "committer_email": meta["committer_email"].lower(),
            
            "is_merge": len(meta["parents"].split()) > 1,
            "parents": meta["parents"],
            
            # Specific to this category-slice
            "additions": metrics["adds"],
            "deletions": metrics["dels"],
            
            # Total context (useful for weighting?)
            "commit_total_adds": total_adds,
            "commit_total_dels": total_dels,
            
            "category": category, # RENAMED from primary_category
            
            "extensions_json": str(ext_deltas) 
        }
        commits_list.append(record)

def main():
    state = load_state()
    all_commits = []
    all_messages = []
    
    for repo_name, repo_path in REPOSITORIES.items():
        if not os.path.exists(repo_path):
            print(f"Error: Repo not found at {repo_path}")
            continue

        if not os.path.isdir(os.path.join(repo_path, '.git')):
            print(f"Error: {repo_path} exists but is not a valid git repository (no .git dir).")
            continue

        # State checkpoint
        latest_commit = subprocess.run(
            ["git", "-C", repo_path, "rev-parse", "HEAD"], capture_output=True, text=True
        ).stdout.strip()
        
        # We will parse unconditionally for now to ensure all repos are fully ingested during upgrade,
        # but keep state updated for future incremental passes
        print(f"\nProcessing {repo_name}...")
        
        print(f"  Reading git log...")
        process = get_git_log(repo_path)
        commits = parse_log(process, repo_name)
        print(f"  Parsed {len(commits)} commits.")
        
        all_commits.extend(commits)
        
        print(f"  Extracting commit messages for reviewer analysis...")
        msg_process = get_git_log_with_messages(repo_path)
        messages = parse_messages(msg_process, repo_name)
        print(f"  Extracted {len(messages)} commit messages.")
        
        all_messages.extend(messages)
        
        state.setdefault("core_repos", {})[repo_name] = {
            "latest_commit": latest_commit,
            "total_commits": len(commits),
            "latest_update": datetime.utcnow().isoformat()
        }
        
    if not all_commits:
        print("Warning: No commits parsed across any repository.")
        empty_cols = ['hash', 'repository_name', 'author_ts', 'author_name', 'author_email', 'committer_name',
                      'committer_email', 'committer_ts', 'parents', 'date_utc', 'subject',
                      'is_merge', 'file_path', 'additions', 'deletions', 'category', 'extensions_json']
        df = pd.DataFrame(columns=empty_cols)
    else:
        df = pd.DataFrame(all_commits)

    # Save commits
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nSaved {len(df)} total commit chunks to {OUTPUT_PATH}")
    
    messages_df = pd.DataFrame(all_messages)
    messages_df.to_parquet(MESSAGES_OUTPUT_PATH, index=False)
    print(f"Saved {len(messages_df)} total messages to {MESSAGES_OUTPUT_PATH}")
    
    # --- Static Analysis ---
    for repo_name, repo_path in REPOSITORIES.items():
        if os.path.exists(repo_path):
            scan_repository(repo_path, repo_name)

    save_state(state)

def scan_repository(repo_path, repo_name):
    """
    Scans the current HEAD of the repo to count files, lines, and languages per category.
    Saves to data/category_metadata.json
    """
    print("Scanning repository structure...")
    
    stats = {} 
    # Structure: { Category: { files: 0, loc: 0, languages: { ext: { files: 0, loc: 0 } } } }
    
    total_files = 0
    total_loc = 0
    
    for root, _, files in os.walk(repo_path):
        if ".git" in root: continue
        
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, repo_path)
            
            # Categorize
            cat = categorize_file(rel_path)
            
            # Extension
            _, ext = os.path.splitext(file)
            ext = ext.lower()
            if not ext: ext = "(no_ext)"
            
            # Lines of Code (simple line count, ignore errors for binary)
            loc = 0
            try:
                # 'rb' allows us to count newlines without decoding issues
                with open(full_path, 'rb') as f:
                    for _ in f: loc += 1
            except:
                pass
                
            # Aggregate
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
            
    print(f"[{repo_name}] Scanned {total_files} files, {total_loc} lines.")
    
    # Save Artifact
    meta_path = f"data/enriched/{repo_name.replace('/', '_')}_metadata.json"
    os.makedirs("data/enriched", exist_ok=True)
    with open(meta_path, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"[{repo_name}] Saved Metadata to {meta_path}")

if __name__ == "__main__":
    main()
