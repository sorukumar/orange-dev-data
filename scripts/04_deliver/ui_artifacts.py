import pandas as pd
import json
import os
import numpy as np
import shutil
from datetime import datetime

# --- Configuration ---
UNIFIED_INPUT = "data/enriched/contributors_unified.parquet"
OUTPUT_DIR = "output/shared/contributors" # Shared Source of Truth for all apps
REGISTRY_FILE = os.path.join(OUTPUT_DIR, "registry_index.json")
PROFILES_DIR = os.path.join(OUTPUT_DIR, "profiles")

# Enriched files produced by 02_process/ scripts — consumed here for profile shards
COMMIT_HISTORY_PATH    = "data/enriched/contributor_commit_history.json"
CONTRIBUTOR_BIPS_PATH  = "data/enriched/contributor_bips.json"
BOOKMARKS_PATH         = "data/enriched/contributor_message_bookmarks.json"
SOCIAL_HISTORY_PATH    = "data/enriched/contributor_social_history.json"

def deliver():
    print("Delivering UI artifacts...")
    if not os.path.exists(UNIFIED_INPUT):
        print(f"Error: {UNIFIED_INPUT} not found.")
        return
        
    df = pd.read_parquet(UNIFIED_INPUT)
    
    # Pre-process columns to handle numpy/datetime objects not serializable by default json
    def clean_object(obj):
        if obj is None:
            return None
        if obj is pd.NaT or isinstance(obj, type(pd.NaT)):
            return None
        if isinstance(obj, str) and obj == 'NaT':
            return None
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, (float, np.floating)):
            return float(obj) if not np.isnan(obj) else None
        if pd.isna(obj):
            return None
        if isinstance(obj, dict):
            return {k: clean_object(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [clean_object(i) for i in obj]
        return obj

    from datetime import datetime
    
    # Convert dataframe to list of dicts with serialization-friendly types
    records = []
    for _, row in df.iterrows():
        rec = {}
        for col in df.columns:
            rec[col] = clean_object(row[col])
        records.append(rec)

    # --- Load enriched per-contributor data ---
    print("Loading enriched per-contributor data files...")

    commit_history: dict = {}
    if os.path.exists(COMMIT_HISTORY_PATH):
        with open(COMMIT_HISTORY_PATH) as f:
            commit_history = json.load(f)
        print(f"  commit_history loaded for {len(commit_history)} contributors")
    else:
        print(f"  WARNING: {COMMIT_HISTORY_PATH} not found — commit_history will be omitted from shards")

    contributor_bips: dict = {}
    if os.path.exists(CONTRIBUTOR_BIPS_PATH):
        with open(CONTRIBUTOR_BIPS_PATH) as f:
            contributor_bips = json.load(f)
        print(f"  contributor_bips loaded for {len(contributor_bips)} contributors")
    else:
        print(f"  WARNING: {CONTRIBUTOR_BIPS_PATH} not found — bip_list will be omitted from shards")

    bookmarks: dict = {}
    if os.path.exists(BOOKMARKS_PATH):
        with open(BOOKMARKS_PATH) as f:
            bookmarks = json.load(f)
        print(f"  message_bookmarks loaded for {len(bookmarks)} contributors")
    else:
        print(f"  WARNING: {BOOKMARKS_PATH} not found — message bookmarks will be omitted from shards")

    social_history: dict = {}
    if os.path.exists(SOCIAL_HISTORY_PATH):
        with open(SOCIAL_HISTORY_PATH) as f:
            social_history = json.load(f)
        print(f"  social_history loaded for {len(social_history)} contributors")
    else:
        print(f"  WARNING: {SOCIAL_HISTORY_PATH} not found — social_history will be omitted from shards")

    # 1. Prepare Sharded Profiles (Deep Dive) first to know who gets a file
    # FILTER: authored_commits >= 10 OR bips_authored > 0
    # This covers all meaningful code contributors and all BIP authors regardless
    # of their hybrid_score rank.
    print("Cleaning profiles directory...")
    if os.path.exists(PROFILES_DIR):
        shutil.rmtree(PROFILES_DIR)
    os.makedirs(PROFILES_DIR, exist_ok=True)
    
    print("Generating profiles and tracking filenames...")
    sharded_count = 0
    id_to_filename = {}

    def qualifies_for_shard(rec: dict) -> bool:
        authored = rec.get('authored_commits') or 0
        bips = rec.get('bips_authored') or 0
        try:
            authored = float(authored)
        except (TypeError, ValueError):
            authored = 0
        try:
            bips = float(bips)
        except (TypeError, ValueError):
            bips = 0
        return authored >= 10 or bips > 0

    for rec in records:
        if not qualifies_for_shard(rec):
            continue

        rec['is_top_50'] = False  # legacy field kept for compatibility

        # Add links to record
        login = rec.get('github_login_final') or (rec.get('github', {}).get('login') if isinstance(rec.get('github'), dict) else None)
        if login:
            rec['github_url'] = f"https://github.com/{login}"
        
        delving_user = rec.get('delving_username_final') or rec.get('delving_username')
        if delving_user:
            rec['delving_url'] = f"https://delvingbitcoin.org/u/{delving_user}"

        # Attach enriched profile data (keyed by canonical_id / uuid)
        cid = rec.get('uuid') or ''
        if cid and cid in commit_history:
            rec['commit_history'] = commit_history[cid]
        if cid and cid in contributor_bips:
            rec['bip_list'] = contributor_bips[cid]
        if cid and cid in bookmarks:
            bm = bookmarks[cid]
            if 'first_message' in bm:
                rec['first_message'] = bm['first_message']
            if 'last_message' in bm:
                rec['last_message'] = bm['last_message']
        if cid and cid in social_history:
            rec['social_history'] = social_history[cid]

        # UUID is present in unified
        file_id = rec.get('uuid', str(rec.get('id')))
        filename = f"{file_id}.json"
        profile_path = os.path.join(PROFILES_DIR, filename)
        
        with open(profile_path, 'w') as f:
            json.dump(rec, f, indent=2)
            
        if 'uuid' in rec:
            id_to_filename[rec['uuid']] = filename
        if 'id' in rec:
            id_to_filename[rec['id']] = filename
        sharded_count += 1
            
    print(f"Sharded {sharded_count} high-signal profiles to {PROFILES_DIR}")

    # 2. Prepare Registry (Compact for Table)
    registry_cols = [
        'uuid', 'id', 'display_name', 'github', 'roles', 'is_top_50',
        'dev_type',
        'total_commits', 'authored_commits', 'merge_commits', 'prs_authored', 'reviews_count', 'hybrid_score', 'bips_authored', 'review_reciprocity',
        'first_seen', 'last_seen', 'global_first_active', 'global_last_active',
        'first_commit', 'last_commit', 'first_active', 'last_active',
        'technical_focus', 'avg_approval_latency_days',
        'ml_threads', 'delving_threads', 'ml_responses', 'delving_responses', 'threads_started', 'replies_sent'
    ]
    
    registry_list = []
    for rec in records:
        entry = {k: rec[k] for k in registry_cols if k in rec}
        # Add quick links to registry too
        login = rec.get('github_login_final')
        if login:
            entry['github_url'] = f"https://github.com/{login}"
            # Update the github object for legacy compatibility in registry
            if 'github' not in entry or not isinstance(entry['github'], dict):
                entry['github'] = {"login": login, "location": "Undisclosed"}
            else:
                entry['github']['login'] = login
        
        delving_user = rec.get('delving_username_final')
        if delving_user:
            entry['delving_url'] = f"https://delvingbitcoin.org/u/{delving_user}"
            
        # Add profile filename if it exists
        if 'uuid' in rec and rec['uuid'] in id_to_filename:
            entry['profile_filename'] = id_to_filename[rec['uuid']]
        elif 'id' in rec and rec['id'] in id_to_filename:
            entry['profile_filename'] = id_to_filename[rec['id']]
            
        registry_list.append(entry)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(REGISTRY_FILE, 'w') as f:
        json.dump({
            "metadata": {
                "count": len(registry_list),
                "generated_at": datetime.now().isoformat(),
                "sharded_count": sharded_count
            },
            "contributors": registry_list
        }, f, indent=2)
    # Final save confirmation
    print(f"Registry and profiles saved to {OUTPUT_DIR}/")

if __name__ == "__main__":
    deliver()
