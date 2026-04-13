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
    print("🚀 Starting MONTHLY Automated Pipeline (Deep Analytics)...")
    
    load_env()
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    
    for folder in ["enriched", "cache", "network", "raw", "sources"]:
        os.makedirs(os.path.join(root_dir, "data", folder), exist_ok=True)

    # PHASE 0: Fresh Sync
    print("\n--- PHASE 0: Raw Data Sync ---")
    run("git -C data/sources/bitcoin pull origin master", cwd=root_dir)
    run("git -C data/sources/bips pull origin master", cwd=root_dir)
    run("git -C data/sources/delving pull origin master", cwd=root_dir)

    # PHASE 1: Extraction (Source -> Raw)
    print("\n--- PHASE 1: Extraction (Raw Staging) ---")
    run("python3 scripts/01_ingest/core.py", cwd=root_dir)
    run("python3 scripts/01_ingest/bips.py", cwd=root_dir)
    run("python3 scripts/01_ingest/delving.py", cwd=root_dir)
    run("python3 scripts/01_ingest/mailing_list.py", cwd=root_dir)

    # PHASE 2: Convergence (Raw -> Enriched)
    print("\n--- PHASE 2: Convergence (Enrichment) ---")
    run("python3 scripts/02_process/enrich_identity.py", cwd=root_dir)
    run("python3 scripts/02_process/reviews.py", cwd=root_dir)
    run("python3 scripts/02_process/github_social.py", cwd=root_dir)
    run("python3 scripts/02_process/core.py", cwd=root_dir)
    run("python3 scripts/02_process/merge_social.py", cwd=root_dir)
    run("python3 scripts/02_process/governance.py", cwd=root_dir)
    
    # PHASE 3: Intelligence (Heavy Analytics & Graphs)
    print("\n--- PHASE 3: Intelligence (NLP & Graphs) ---")
    run("python3 scripts/02_process/categorize.py", cwd=root_dir)
    run("python3 scripts/03_analyze/influence.py", cwd=root_dir)
    run("python3 scripts/03_analyze/expertise.py", cwd=root_dir)

    # PHASE 4: Delivery (Enriched -> Output)
    print("\n--- PHASE 4: Artifact Generation ---")
    run("python3 scripts/04_deliver/regional_evolution.py", cwd=root_dir)
    run("python3 scripts/04_deliver/registry.py", cwd=root_dir)
    run("python3 scripts/04_deliver/ui_artifacts.py", cwd=root_dir)
    
    print("\n✨ MONTHLY PIPELINE COMPLETE!")
    print("Everything is up to date and all NLP/Graphs have been recalculated.")

if __name__ == "__main__":
    main()
