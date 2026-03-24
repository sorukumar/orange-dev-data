#!/usr/bin/env python3
import os
import subprocess
import sys

def load_env():
    """Load environment variables from .env file"""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value
        print("✅ Loaded .env file")

def run(command, cwd=None):
    """Execute a python script in the src/ folder"""
    print(f"\n🚀 Running: {command}...")
    result = subprocess.run(command, shell=True, cwd=cwd)
    if result.returncode != 0:
        print(f"⚠️  Command failed with exit code {result.returncode}")
        return False
    return True

def main():
    print("--- Orange Dev Data Pipeline Master (v2) ---")
    load_env()
    root_dir = os.path.dirname(os.path.abspath(__file__))
    
    # --- PHASE 0: Raw Data Sync ---
    print("\n[PHASE 0] Data Sync & Ingestion")
    run("python3 src/ingest/bitcoin_repo.py", cwd=root_dir)
    run("python3 src/ingest/github_social.py", cwd=root_dir)
    run("python3 src/ingest/bips.py", cwd=root_dir)
    run("python3 src/ingest/social_delving.py", cwd=root_dir)
    run("python3 src/ingest/social_mailing_list.py", cwd=root_dir)
    
    # --- PHASE 1: Transformation & Normalization ---
    print("\n[PHASE 1] Transformation & Enrichment")
    run("python3 src/transform/enrich_profiles.py", cwd=root_dir)
    run("python3 src/transform/merge_social.py", cwd=root_dir)
    run("python3 src/transform/categorize_threads.py", cwd=root_dir)
    run("python3 src/transform/extract_reviewers.py", cwd=root_dir)
    run("python3 src/transform/enrich_governance.py", cwd=root_dir)
    
    # --- PHASE 2: Core Analytics ---
    print("\n[PHASE 2] High-Signal Analytics")
    run("python3 src/analyze/core_metrics.py", cwd=root_dir)
    run("python3 src/analyze/regional_evolution.py", cwd=root_dir)
    run("python3 src/analyze/maintainer_footprint.py", cwd=root_dir)
    run("python3 src/analyze/influence_hubs.py", cwd=root_dir)
    run("python3 src/analyze/map_expertise.py", cwd=root_dir)
    run("python3 src/analyze/social_summary.py", cwd=root_dir)
    
    # --- PHASE 3: Product Export ---
    print("\n[PHASE 3] Showroom Export (Product Generation)")
    run("python3 src/export/tracker_ui.py", cwd=root_dir)
    
    print("\n✨ PIPELINE COMPLETE!")
    print("Standalone data products available in: output/shared/")
    print("App-specific artifacts available in: output/tracker/ and output/network/")

if __name__ == "__main__":
    main()
