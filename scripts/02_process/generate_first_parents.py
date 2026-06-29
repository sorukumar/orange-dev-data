import subprocess
import os
import json

REPOSITORIES = {
    "bitcoin/bitcoin": "data/sources/bitcoin",
    "bitcoin-core/secp256k1": "data/sources/secp256k1",
    "bitcoin-core/gui": "data/sources/gui",
    "bitcoin-core/guix.sigs": "data/sources/guix.sigs",
    "bitcoin-core/qa-assets": "data/sources/qa-assets",
    "bitcoin-core/HWI": "data/sources/HWI"
}

def get_first_parent_commits():
    all_hashes = set()
    for repo_name, repo_path in REPOSITORIES.items():
        if not os.path.exists(repo_path):
            continue
        try:
            cmd = ["git", "-C", repo_path, "log", "HEAD", "--first-parent", "--format=%H"]
            res = subprocess.run(cmd, capture_output=True, text=True, check=True)
            hashes = res.stdout.splitlines()
            all_hashes.update(hashes)
            print(f"Got {len(hashes)} first-parent commits for {repo_name}")
        except Exception as e:
            print(f"Error on {repo_name}: {e}")
            
    out_path = "metadata/first_parent_merges.json"
    os.makedirs("metadata", exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(list(all_hashes), f)
    print(f"Saved {len(all_hashes)} total first-parent hashes to {out_path}")

if __name__ == "__main__":
    get_first_parent_commits()
