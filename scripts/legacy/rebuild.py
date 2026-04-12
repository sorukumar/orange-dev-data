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
    print(f"\n--- Running: {command} ---")
    result = subprocess.run(command, shell=True, cwd=cwd)
    if result.returncode != 0:
        # We don't want a git pull failure to stop the whole pipeline if we already have data
        if "git pull" in command:
             print("⚠️  Git pull failed (maybe no internet?). Continuing with current local data.")
             return True
        print(f"⚠️  Command failed with exit code {result.returncode}")
        return False
    return True

def main():
    print("🚀 Starting AUTOMATED Bitcoin R&D Pipeline (orange-dev-data)...")
    
    # Initialization
    load_env()
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    
    # Ensure folders exist
    for folder in ["core", "viz", "governance", "cache", "network", "raw"]:
        os.makedirs(os.path.join(root_dir, "data", folder), exist_ok=True)

    # PHASE 0: Fresh Sync (Update all raw source repositories)
    print("\n--- PHASE 0: Raw Data Sync ---")
    # Pull latest from Bitcoin Core
    run("git -C raw_data/bitcoin pull origin master", cwd=root_dir)
    # Pull latest BIPs
    run("git -C raw_data/bips_repo pull origin master", cwd=root_dir)
    # Pull latest Delving
    run("git -C raw_data/delving pull origin master", cwd=root_dir)

    # PHASE 1: Bitcoin Core Analysis
    print("\n--- PHASE 1: Bitcoin Core Analysis ---")
    run("python3 scripts/core/ingest.py", cwd=root_dir)
    run("python3 scripts/core/social.py", cwd=root_dir)
    run("python3 scripts/core/enrich.py", cwd=root_dir) # Incremental (uses cache)
    run("python3 scripts/extract_reviewers.py", cwd=root_dir)
    run("python3 scripts/core/process.py", cwd=root_dir)
    run("python3 scripts/generate_regional_evolution.py", cwd=root_dir)

    # PHASE 2: Social & Governance Ingestion
    print("\n--- PHASE 2: Social & Governance Analysis ---")
    run("python3 scripts/ingest_bips.py", cwd=root_dir)
    run("python3 scripts/ingest_delving.py", cwd=root_dir)
    run("python3 scripts/ingest_mailing_list.py", cwd=root_dir) # Incremental (uses state.json)
    
    # PHASE 2.5: Merging & Enrichment
    print("\n--- PHASE 2.5: Merging & Enrichment ---")
    run("python3 scripts/enrich_governance.py", cwd=root_dir) 
    run("python3 scripts/merge_data.py", cwd=root_dir)

    # PHASE 3: Advanced Network Intelligence
    print("\n--- PHASE 3: Network Graph & Expertise Mapping ---")
    run("python3 scripts/categorize_threads.py", cwd=root_dir) # Fast if cache is up to date
    run("python3 scripts/influence_hubs.py", cwd=root_dir)
    run("python3 scripts/map_expertise.py", cwd=root_dir)

    # PHASE 4: UI Artifact Generation
    run("python3 scripts/generate_ui_artifacts.py", cwd=root_dir)
    
    print("\n✨ UNIFIED PIPELINE COMPLETE!")
    print("Everything is up to date. Push the 'data/' folder to GitHub to sync your dashboards.")

if __name__ == "__main__":
    main()
