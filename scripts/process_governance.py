import subprocess
import os
import sys

# List of scripts to run in order
GOVERNANCE_SCRIPTS = [
    "code/governance/ingest_bips.py",
    "code/governance/ingest_delving.py",
    "code/governance/ingest_mailing_list.py",
    "code/governance/process_social.py",
    "code/governance/enrich_governance.py",
    "code/governance/map_expertise.py",
    "code/governance/generate_ui_artifacts.py"
]

def run_governance_pipeline():
    print("🚀 Starting Orange Dev Tracker: Governance Pipeline")
    print("================================================")
    
    for script in GOVERNANCE_SCRIPTS:
        print(f"\n▶️ Running: {script}")
        result = subprocess.run([sys.executable, script], capture_output=False)
        if result.returncode != 0:
            print(f"❌ Error in {script}. Pipeline halted.")
            sys.exit(1)
            
    print("\n✅ Pipeline Complete! All artifacts refreshed in data/governance/")
    print("Check lab/bips/ for the result.")

if __name__ == "__main__":
    run_governance_pipeline()
