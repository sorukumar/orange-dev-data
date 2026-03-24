#!/usr/bin/env python3
import pandas as pd
import re
import os
import json
import sys

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import WORK_DIR

# --- Configuration (Centralized via paths.py) ---
INPUT_PARQUET = os.path.join(WORK_DIR, "social", "combined.parquet")
OUTPUT_PARQUET = os.path.join(WORK_DIR, "social", "combined_categorized.parquet")

# =====================================================================
# CATEGORY DEFINITIONS
# =====================================================================
_CATEGORY_DEFS: dict = {
    "soft-fork-activation": {
        "desc": "Soft fork activation mechanisms (BIP 9, BIP 8, Speedy Trial, UASF, flag day)",
        "keywords": ["soft fork", "softfork", "uasf", "user activated", "speedy trial", "flag day", "lot=true", "lot=false", "version bits", "signaling", "bip148", "bip149", "bip91", "activation"],
        "patterns": [r"\b(?:soft[\s\-]?fork)\s+activation\b", r"\bspeedy\s+trial\b", r"\bflag[\s\-]?day\b", r"\buser[\s\-]?activated\b", r"\bversion[\s\-]?bits\b", r"\bsignaling\b.*(?:threshold|miner|block)"],
        "weight": 70,
        "bips": ["8", "9", "91", "135", "148", "149", "343"],
    },
    "hard-fork-block-size": {
        "desc": "Block size debate & hard fork proposals (2015-2017 era and beyond)",
        "keywords": ["block size", "blocksize", "block weight", "2mb", "8mb", "20mb", "segwit2x", "bitcoin xt", "bitcoin classic", "bitcoin unlimited", "bip100", "bip101", "bip102", "bip103", "bip109", "new york agreement", "hong kong agreement"],
        "patterns": [r"\bblock[\s\-]?size\b", r"\bblock[\s\-]?weight\b", r"\b(?:segwit)?2x\b", r"\bbitcoin[\s\-]?(?:xt|classic|unlimited)\b", r"\b(?:20|2|8)\s*mb\b", r"\bhard[\s\-]?fork\b.*\b(?:size|block|capacity|increase)\b"],
        "weight": 75,
        "bips": ["100", "101", "102", "103", "104", "105", "106", "107", "109"],
    },
    "consensus-cleanup": {
        "desc": "Great Consensus Cleanup & related consensus-level fixes",
        "keywords": ["consensus cleanup", "great consensus cleanup", "timewarp", "64-byte transaction", "duplicate transaction", "merkle tree vulnerability"],
        "patterns": [r"\bconsensus\s+cleanup\b", r"\btimewarp\b", r"\b64[\s\-]?byte\s+transaction\b"],
        "weight": 72,
        "bips": ["30", "53", "54"],
    },
    "segwit": {
        "desc": "Segregated Witness design, deployment, and consequences",
        "keywords": ["segwit", "segregated witness", "witness program", "witness version", "bech32", "malleability", "transaction malleability", "anyone-can-spend"],
        "patterns": [r"\bseg[\s\-]?wit\b", r"\bsegregated\s+witness\b", r"\bbech32(?:m)?\b", r"\bwitness\s+(?:program|version|discount)\b"],
        "weight": 65,
        "bips": ["141", "142", "143", "144", "145", "147", "148", "149", "173", "350"],
    },
    "taproot": {
        "desc": "Taproot, Schnorr signatures, Tapscript",
        "keywords": ["taproot", "schnorr", "tapscript", "bip340", "bip341", "bip342", "mast", "merkelized abstract syntax tree", "key path spend", "script path spend", "annex"],
        "patterns": [r"\btaproot\b", r"\bschnorr\b", r"\btapscript\b", r"\bmast\b", r"\bkey[\s\-]?path\b", r"\bscript[\s\-]?path\b"],
        "weight": 68,
        "bips": ["114", "340", "341", "342", "343", "386"],
    },
    "covenants": {
        "desc": "Covenant proposals: CTV, OP_CAT, OP_VAULT, TXHASH, APO, CSFS",
        "keywords": ["covenant", "op_checktemplateverify", "op_ctv", "checktemplateverify", "bip119", "op_cat", "op_vault", "op_txhash", "anyprevout", "sighash_anyprevout", "checksigfromstack", "csfs", "op_checksigfromstackverify", "op_internalkey", "op_paircommit", "op_checkcontractverify", "op_ccv", "introspection", "lnhance", "graftleaf", "op_expire"],
        "patterns": [r"\bcovenant[s]?\b", r"\bop[\s_]c(?:tv|at|cv)\b", r"\bop[\s_]vault\b", r"\bop[\s_]txhash\b", r"\bop[\s_]checksigfromstack(?:verify)?\b", r"\bop[\s_]checktemplateverify\b", r"\bop[\s_]internalkey\b", r"\bop[\s_]paircommit\b", r"\bop[\s_]checkcontractverify\b", r"\banyprevout\b", r"\bcsfs\b", r"\blnhance\b", r"\bgraftleaf\b"],
        "weight": 80,
        "bips": ["118", "119", "345", "346", "347", "348", "349", "443"],
    },
    "lightning": {
        "desc": "Lightning Network: channels, HTLCs, routing, LN-Symmetry",
        "keywords": ["lightning", "ln ", "htlc", "payment channel", "channel capacity", "routing", "eltoo", "ln-symmetry", "watchtower", "bolt11", "bolt12", "bolt ", "lsp ", "lightning service provider", "submarine swap", "splicing", "onion message", "onion routing", "trampoline", "blinded path", "channel jamming", "channel depletion"],
        "patterns": [r"\blightning\s+(?:network|channel|payment|node|invoice|wallet)\b", r"\bhtlc\b", r"\bln[\s\-]symmetry\b", r"\beltoo\b", r"\bbolt[\s\-]?(?:11|12)\b", r"\bpayment\s+channel[s]?\b", r"\bsplicing\b", r"\bchannel\s+(?:jamming|depletion|capacity|open|close|factory)\b", r"\bsubmarine\s+swap\b", r"\bwatchtower\b", r"\bonion\s+(?:message|routing)\b"],
        "weight": 60,
        "bips": [],
    },
    "privacy": {
        "desc": "Privacy: CoinJoin, PayJoin, CoinSwap, Confidential Transactions",
        "keywords": ["coinjoin", "payjoin", "coinswap", "bustapay", "confidential transaction", "mixer", "mixing", "dandelion", "privacy", "fungibility", "stealth address", "reusable payment code", "bip47", "paynym"],
        "patterns": [r"\bcoin[\s\-]?join\b", r"\bpay[\s\-]?join\b", r"\bcoin[\s\-]?swap\b", r"\bdandelion\b", r"\bstealth\s+address\b", r"\breusable\s+payment\s+code\b", r"\bprivacy\b", r"\bfungib(?:ility|le)\b"],
        "weight": 62,
        "bips": ["47", "78", "79", "126", "156"],
    },
    "mining": {
        "desc": "Mining: PoW, ASICs, pools, block templates, Stratum",
        "keywords": ["mining", "miner", "hashrate", "proof of work", "asicboost", "selfish mining", "block template", "getblocktemplate", "stratum", "mining pool", "fee sniping", "coinbase transaction", "nonce", "braidpool", "radpool", "ocean pool"],
        "patterns": [r"\bmining\b(?!\s+(?:data|the))", r"\bminer[s]?\b", r"\bhashrate\b", r"\basicboost\b", r"\bselfish\s+mining\b", r"\bstratum\s+v?[12]?\b", r"\bblock\s+template\b", r"\bgetblocktemplate\b", r"\bmining\s+pool\b"],
        "weight": 55,
        "bips": ["22", "23", "34", "42", "52", "310", "320"],
    },
    "mempool-fees": {
        "desc": "Mempool policy, RBF, CPFP, package relay, cluster mempool",
        "keywords": ["mempool", "replace by fee", "rbf", "full rbf", "cpfp", "child pays for parent", "fee estimation", "fee rate", "feerate", "package relay", "cluster mempool", "linearization", "pinning", "v3 transaction", "truc", "ephemeral anchor", "p2a", "ancestor package", "mempool policy", "relay policy"],
        "patterns": [r"\bmempool\b", r"\breplace[\s\-]?by[\s\-]?fee\b", r"\b(?:full[\s\-]?)?rbf\b", r"\bcpfp\b", r"\bfee\s+(?:estimation|estimator|rate|bump|snip)\b", r"\bpackage\s+relay\b", r"\bcluster\s+mempool\b", r"\blinearization\b", r"\b(?:tx|transaction)\s+pinning\b", r"\bv3\s+transaction[s]?\b", r"\btruc\b", r"\bephemeral\s+anchor[s]?\b", r"\bp2a\b", r"\bdust\s+(?:limit|threshold|attack)\b"],
        "weight": 58,
        "bips": ["125", "133", "331", "431", "433"],
    }
}

# Compile patterns once
for cat in _CATEGORY_DEFS.values():
    cat["_compiled"] = [re.compile(p, re.IGNORECASE) for p in cat.get("patterns", [])]

def identify_bips(text):
    if not text: return []
    matches = re.findall(rf"\bBIP[- ]?(\d+)\b", text, re.IGNORECASE)
    return sorted(list(set(matches)))

def categorize_thread(text, bips=[]):
    if not text: return "Other", [], 0.0
    text = text.lower()
    matches = []
    
    for name, defs in _CATEGORY_DEFS.items():
        score = 0
        # 1. BIP Match (Strongest)
        if any(b in defs.get("bips", []) for b in bips):
            score += 100
            
        # 2. Keyword Match
        for kw in defs.get("keywords", []):
            if kw in text:
                score += 10
                break
        
        # 3. Regex Match
        for pattern in defs.get("_compiled", []):
            if pattern.search(text):
                score += 20
                break
        
        if score > 0:
            matches.append((name, score + defs.get("weight", 0)))
            
    if not matches:
        return "Other", [], 0.0
    
    matches.sort(key=lambda x: x[1], reverse=True)
    best_cat = matches[0][0]
    all_cats = [m[0] for m in matches]
    confidence = min(1.0, matches[0][1] / 150.0)
    
    return best_cat, all_cats, round(confidence, 2)

def main():
    print("--- Thread Categorization (New Architecture) ---")
    if not os.path.exists(INPUT_PARQUET):
        print(f"Error: {INPUT_PARQUET} not found.")
        return

    df = pd.read_parquet(INPUT_PARQUET)
    print(f"Loaded {len(df)} messages. Grouping by thread...")
    
    # Simple strategy: categorize based on subject and first 500 chars of body
    df['bip_refs'] = df['subject'].fillna("").apply(identify_bips)
    
    # Pre-calculated categories
    results = []
    print("Running taxonomy engine...")
    for idx, row in df.iterrows():
        cat, all_cats, conf = categorize_thread(f"{row['subject']} {row['body_snippet']}", row['bip_refs'])
        results.append({
            "primary_category": cat,
            "all_categories": all_cats,
            "category_confidence": conf
        })
        if idx > 0 and idx % 10000 == 0:
            print(f"  Categorized {idx} messages...")

    res_df = pd.DataFrame(results)
    df = pd.concat([df, res_df], axis=1)
    
    os.makedirs(os.path.dirname(OUTPUT_PARQUET), exist_ok=True)
    df.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"Saved categorized social data to {OUTPUT_PARQUET}")
    
    print("\nCategory Distribution (Top 10):")
    print(df['primary_category'].value_counts().head(10))

if __name__ == "__main__":
    main()
