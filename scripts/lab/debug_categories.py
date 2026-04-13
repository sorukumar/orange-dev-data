import os
import re
import subprocess
from collections import Counter
import sys

# Define path relative to where script is run
REPO_PATH = "raw_data/bitcoin"

# --- Categorization Logic (Copied from ingest.py) ---
CATEGORY_RULES = {
    "Consensus": [r"src/consensus/", r"src/script/", r"src/primitives/", r"src/chain", r"src/coins", r"src/pow"],
    "MemPool/Policy": [r"src/policy/"],
    "Net/P2P": [r"src/net", r"src/protocol", r"src/addrman"],
    "Wallet": [r"src/wallet/"],
    "UI/Qt": [r"src/qt/", r"src/forms/"],
    "Crypto": [r"src/crypto/", r"src/secp256k1/"],
    "Utils": [r"src/util/", r"src/support/", r"src/common/"],
    "Tests": [r"src/test/", r"test/"],
    "Docs": [r"doc/", r".*\.md$"],
    "Build/CI": [r"Makefile", r"ci/", r"\.github/", r"build_msvc", r"configure\.ac", r"CMakeLists\.txt"],
    # 'Core Libs' is the fallback
}

def categorize_file(path):
    for category, regexes in CATEGORY_RULES.items():
        for pattern in regexes:
            if re.search(pattern, path, re.IGNORECASE):
                return category
    return "Core Libs"

def main():
    if not os.path.exists(REPO_PATH):
        print(f"Repo not found at {REPO_PATH}")
        return

    print("Getting list of files...")
    try:
        # Get all files controlled by git
        cmd = ["git", "-C", REPO_PATH, "ls-files"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        files = result.stdout.splitlines()
    except subprocess.CalledProcessError as e:
        print(f"Git error: {e}")
        return

    print(f"Analyzing {len(files)} files...")
    
    counts = Counter()
    fallback_files = []

    for f in files:
        cat = categorize_file(f)
        counts[cat] += 1
        if cat == "Core Libs":
            fallback_files.append(f)
            
    print("\n--- Category Distribution (by File Count) ---")
    for cat, count in counts.most_common():
        print(f"{cat}: {count} ({count/len(files)*100:.1f}%)")

    print("\n--- Top Folders in 'Core Libs' ---")
    # Extract directory of fallback files to see patterns
    fallback_dirs = []
    for f in fallback_files:
        d = os.path.dirname(f)
        if not d: d = "root"
        fallback_dirs.append(d)
    
    dir_counts = Counter(fallback_dirs)
    for d, count in dir_counts.most_common(20):
        print(f"{d}: {count}")

    print("\n--- Top Files in 'Core Libs' (Root src) ---")
    # Show src/*.cpp files that are missing
    root_src = [f for f in fallback_files if os.path.dirname(f) == "src"]
    for f in root_src[:20]:
        print(f)

if __name__ == "__main__":
    main()
