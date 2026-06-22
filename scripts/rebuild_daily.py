import os
import subprocess
import sys

def load_env():
    """Load environment variables from .env file"""
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value
        print("✅ Loaded .env file")

def run(command, cwd=None):
    """Execute a system command and check for errors"""
    if command.startswith("python3 "):
        command = command.replace("python3 ", f'"{sys.executable}" ', 1)
        
    print(f"\n--- Running: {command} ---")
    result = subprocess.run(command, shell=True, cwd=cwd)
    if result.returncode != 0:
        if "git pull" in command:
             print("⚠️  Git pull failed. Continuing with current local data.")
             return True
        print(f"⚠️  Command failed with exit code {result.returncode}")
        return False
    return True

def main():
    print("🚀 Starting DAILY Automated Pipeline (Fast/Incremental)...")
    
    load_env()
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    
    for folder in ["data/enriched", "data/cache", "data/raw", "data/sources", "output/tracker", "output/network"]:
        os.makedirs(os.path.join(root_dir, folder), exist_ok=True)

    # PHASE 0: Fresh Sync
    print("\n--- PHASE 0: Raw Data Sync ---")
    run("git -C data/sources/bitcoin pull origin master", cwd=root_dir)
    run("git -C data/sources/bips pull origin master", cwd=root_dir)
    run("git -C data/sources/delving pull origin master", cwd=root_dir)
    run("git -C data/sources/mailing_list/shard_0 fetch origin", cwd=root_dir)
    run("git -C data/sources/secp256k1 pull origin master", cwd=root_dir)
    run("git -C data/sources/gui pull origin master", cwd=root_dir)
    run("git -C data/sources/guix.sigs pull origin main", cwd=root_dir)
    run("git -C data/sources/qa-assets pull origin main", cwd=root_dir)
    run("git -C data/sources/HWI pull origin master", cwd=root_dir)

    # PHASE 1: Extraction (Source -> Raw)
    print("\n--- PHASE 1: Extraction (Raw Staging) ---")
    run("python3 scripts/01_ingest/core.py", cwd=root_dir)
    run("python3 scripts/01_ingest/bips.py", cwd=root_dir)
    run("python3 scripts/01_ingest/bips_metadata.py", cwd=root_dir)  # BIPs GitHub PR + review events
    run("python3 scripts/01_ingest/delving.py", cwd=root_dir)
    run("python3 scripts/01_ingest/mailing_list.py", cwd=root_dir)
    run("python3 scripts/01_ingest/github_metadata.py", cwd=root_dir)

    # PHASE 2: Convergence (Raw -> Enriched)
    print("\n--- PHASE 2: Convergence (Enrichment) ---")
    run("python3 scripts/identity/build_identities.py", cwd=root_dir)
    run("python3 scripts/identity/restamp_social_ids.py", cwd=root_dir)
    run("python3 scripts/identity/restamp_delving_ids.py", cwd=root_dir)
    run("python3 scripts/02_process/resolve_commits.py", cwd=root_dir)
    run("python3 scripts/02_process/reviews.py", cwd=root_dir)
    run("python3 scripts/02_process/github_social.py", cwd=root_dir)
    run("python3 scripts/02_process/enrich_prs.py", cwd=root_dir)
    run("python3 scripts/02_process/enrich_reviews.py", cwd=root_dir)
    run("python3 scripts/02_process/merge_social.py", cwd=root_dir)
    run("python3 scripts/02_process/governance.py", cwd=root_dir)
    
    run("python3 scripts/04_deliver/badges.py", cwd=root_dir)

    # PHASE 3: Intelligence (Skipped in daily, except lightweight influence)
    print("\n--- PHASE 3: Intelligence (Light) ---")
    run("python3 scripts/03_analyze/review_metrics.py", cwd=root_dir)
    run("python3 scripts/03_analyze/expertise.py", cwd=root_dir)
    run("python3 scripts/03_analyze/influence.py", cwd=root_dir)
    run("python3 scripts/03_analyze/unify_contributors.py", cwd=root_dir)

    # PHASE 4: Delivery (Enriched -> Output)
    print("\n--- PHASE 4: Artifact Generation ---")
    run("python3 scripts/04_deliver/registry.py", cwd=root_dir)
    run("python3 scripts/04_deliver/tracker_artifacts.py", cwd=root_dir)
    run("python3 scripts/04_deliver/ui_artifacts.py", cwd=root_dir)
    run("python3 scripts/04_deliver/ecosystem_summary.py", cwd=root_dir)
    run("python3 scripts/04_deliver/discussions_pulse.py", cwd=root_dir)
    run("python3 scripts/04_deliver/network_home_snapshot.py", cwd=root_dir)
    
    print("\n✨ DAILY PIPELINE COMPLETE!")

if __name__ == "__main__":
    main()
