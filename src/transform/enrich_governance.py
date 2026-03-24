import pandas as pd
import numpy as np
import re
import os
import json
from datetime import datetime
import sys

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import WORK_DIR, TRACKER_DIR

# --- Configuration (Centralized via paths.py) ---
BIPS_PATH = os.path.join(WORK_DIR, "bips.parquet")
SOCIAL_PATH = os.path.join(WORK_DIR, "social", "combined.parquet")
COMMITS_PATH = os.path.join(WORK_DIR, "core", "commits.parquet")
COMMIT_MSGS_PATH = os.path.join(WORK_DIR, "core", "commit_messages.parquet")
OUTPUT_BIPS_ENRICHED = os.path.join(WORK_DIR, "bips_enriched.parquet")
OUTPUT_THEMES_JSON = os.path.join(TRACKER_DIR, "themes.json")

# --- Theme Definitions (The "First Pass" Taxonomy) ---
THEMES = {
    "Consensus & Soft Forks": [r"segwit", r"taproot", r"soft fork", r"hard fork", r"consensus", r"witness", r"bip 141", r"bip 341", r"covenants"],
    "Privacy": [r"privacy", r"coinjoin", r"stealth", r"confidential", r"tor", r"i2p", r"p2pkh", r"p2tr"],
    "Scaling & Lightning": [r"lightning", r"layer 2", r"channels", r"micropayment", r"scaling", r"compression", r"compact block"],
    "P2P Network": [r"p2p", r"protocol", r"handshake", r"relay", r"mempool", r"addrman", r"discovery"],
    "Wallet & Keys": [r"wallet", r"descriptor", r"hd wallet", r"mnemonic", r"bip 32", r"bip 39", r"bip 44", r"multisig", r"miniscript"],
    "Script & Smart Contracts": [r"script", r"opcode", r"cltv", r"csv", r"miniscript", r"tapscript", r"sighash"],
    "Mining": [r"mining", r"stratum", r"block template", r"fee", r"hashrate", r"pow", r"asic"]
}

def categorize(text):
    if not text: return "Other"
    text = text.lower()
    for theme, patterns in THEMES.items():
        for p in patterns:
            if re.search(p, text):
                return theme
    return "Other"

def main():
    print("--- Forensic Enrichment (New Architecture) ---")
    
    if not os.path.exists(BIPS_PATH):
        print(f"Error: {BIPS_PATH} not found. Run BIP ingestion first.")
        return
        
    # 1. Load Datasets
    print("Loading datasets...")
    df_bips = pd.read_parquet(BIPS_PATH)
    
    # Optional social data for enrichment
    if os.path.exists(SOCIAL_PATH):
        df_social = pd.read_parquet(SOCIAL_PATH)
        df_social['date'] = pd.to_datetime(df_social['date'], utc=True)
        # Link BIPs to Social Discussion
        print("Linking BIPs to social discussions...")
        bip_patterns = {}
        for bip_id in df_bips['bip_id'].unique():
            if bip_id.isdigit():
                padded = bip_id.zfill(4)
                short = str(int(bip_id))
                pattern = re.compile(rf"\bBIP[- ]?({short}|{padded})\b", re.IGNORECASE)
            else:
                pattern = re.compile(rf"\b{re.escape(bip_id)}\b", re.IGNORECASE)
            bip_patterns[bip_id] = pattern

        social_subjects = df_social['subject'].fillna("").tolist()
        mentions_count = []
        for bip_id in df_bips['bip_id']:
            pattern = bip_patterns[bip_id]
            count = sum(1 for s in social_subjects if pattern.search(s))
            mentions_count.append(count)
        df_bips['social_mention_count'] = mentions_count
        df_social['theme'] = df_social['subject'].apply(categorize)
    else:
        print("Warning: Social data not found. Mention counts will be set to 0.")
        df_bips['social_mention_count'] = 0
        df_social = pd.DataFrame(columns=['theme'])

    # 2. Map BIPs to Themes
    print("Categorizing BIPs into themes...")
    df_bips['theme'] = df_bips.apply(lambda row: categorize(f"{row['title']} {row['layer']}"), axis=1)

    # 3. Link BIPs to Code (Commits)
    if os.path.exists(COMMIT_MSGS_PATH):
        print("Linking BIPs to repository commits...")
        df_msgs = pd.read_parquet(COMMIT_MSGS_PATH)
        df_msgs['subject_body'] = (df_msgs['subject'].fillna("") + " " + df_msgs['body'].fillna("")).str.lower()
        
        code_mentions = []
        for bip_id in df_bips['bip_id']:
            pattern = bip_patterns.get(bip_id)
            if not pattern:
                if bip_id.isdigit():
                    padded = bip_id.zfill(4)
                    short = str(int(bip_id))
                    pattern = re.compile(rf"\bBIP[- ]?({short}|{padded})\b", re.IGNORECASE)
                else:
                    pattern = re.compile(rf"\b{re.escape(bip_id)}\b", re.IGNORECASE)
            
            count = sum(1 for msg in df_msgs['subject_body'] if pattern.search(msg))
            code_mentions.append(count)
        df_bips['code_mention_count'] = code_mentions
    else:
        print("Warning: Commit messages not found. Code links will be set to 0.")
        df_bips['code_mention_count'] = 0

    # 4. Calculate "Maturity Score"
    print("Calculating Maturity Scores...")
    rev_max = df_bips['revision_count'].max() or 1
    soc_max = df_bips['social_mention_count'].max() or 1
    
    df_bips['maturity_score'] = (
        (df_bips['revision_count'] / rev_max * 0.4) + 
        (df_bips['social_mention_count'] / soc_max * 0.6)
    ).round(2)

    # 5. Save Enriched Artifacts
    print(f"Saving enriched BIPs to {OUTPUT_BIPS_ENRICHED}...")
    os.makedirs(os.path.dirname(OUTPUT_BIPS_ENRICHED), exist_ok=True)
    df_bips.to_parquet(OUTPUT_BIPS_ENRICHED, index=False)
    
    # Save Theme Stats for UI
    os.makedirs(os.path.dirname(OUTPUT_THEMES_JSON), exist_ok=True)
    theme_stats = df_bips['theme'].value_counts().to_dict()
    with open(OUTPUT_THEMES_JSON, 'w') as f:
        json.dump({
            "bip_themes": theme_stats,
            "social_themes": df_social['theme'].value_counts().to_dict(),
            "last_updated": datetime.now().isoformat()
        }, f, indent=2)

    print("\n--- Enrichment Complete ---")
    print(f"BIPs with code links: {len(df_bips[df_bips['code_mention_count'] > 0])}")
    print(f"Top BIPs by Maturity Score:")
    print(df_bips.sort_values('maturity_score', ascending=False)[['bip_id', 'title', 'theme', 'maturity_score']].head(5))

if __name__ == "__main__":
    main()
