
import os
import subprocess
import sys

def load_env():
    env_path = os.path.join(os.getcwd(), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value
        print("✅ Loaded .env file")

def run(command):
    print(f"\n--- Running: {command} ---")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        print(f"Error: Command failed with exit code {result.returncode}")
        # sys.exit(result.returncode) # Don't exit, try to continue

def main():
    print("🚀 Starting Bitcoin Dev Tracker Rebuild Workflow...")
    
    # Load .env file
    load_env()
    
    # Check for GITHUB_TOKEN
    if not os.environ.get("GITHUB_TOKEN"):
        print("⚠️  Warning: GITHUB_TOKEN environment variable is not set.")
        print("   Metadata enrichment will be limited to local legacy data.")
    
    # 1. Enrich/Refresh Metadata
    run("python code/core/enrich.py")
    
    # 1.5 Extract Reviewers (needed for process.py)
    run("python analysis/extract_reviewers.py")
    
    # 2. Main Processing Pipeline (generates most stats_*.json)
    run("python code/core/process.py")
    
    # 3. Specific Insight Generators
    run("python analysis/generate_regional_evolution.py")
    
    print("\n✨ Rebuild complete! Charts and data have been updated.")

if __name__ == "__main__":
    main()
