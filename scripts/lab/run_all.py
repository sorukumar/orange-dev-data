#!/usr/bin/env python3
"""
Smart Ingestion Workflow for Bitcoin Social Data

This script orchestrates the ingestion of data from multiple sources:
- Bitcoin-dev mailing list (from Git repository)
- Delving Bitcoin forum (via API)
- Bitcoin Core mailing list (from Gnusha public-inbox)

Each source is ingested separately for independent refresh and validation,
then merged into a combined dataset.

Usage:
    python scripts/ingest/run_all.py
"""

import subprocess
import sys
import os
from pathlib import Path

# Define the scripts to run in order
INGEST_SCRIPTS = [
    "scripts/ingest/ingest_mailing_list.py",
    "scripts/ingest/ingest_delving.py",
    "scripts/ingest/ingest_bips.py",
]

MERGE_SCRIPT = "scripts/ingest/merge_data.py"

def run_script(script_path):
    """Run a Python script and return success status."""
    print(f"Running {script_path}...")
    try:
        result = subprocess.run([sys.executable, script_path], 
                              capture_output=True, text=True, cwd=os.getcwd())
        if result.returncode == 0:
            print(f"✓ {script_path} completed successfully")
            return True
        else:
            print(f"✗ {script_path} failed with return code {result.returncode}")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            return False
    except Exception as e:
        print(f"✗ Error running {script_path}: {e}")
        return False

def validate_data():
    """Basic validation of ingested data."""
    data_dir = Path("data/raw")
    required_files = [
        "social_mailing_list.parquet",
        "social_delving.parquet",
        "social_combined.parquet"
    ]
    
    print("Validating data files...")
    all_present = True
    for file in required_files:
        path = data_dir / file
        if path.exists():
            size = path.stat().st_size
            print(f"✓ {file} exists ({size} bytes)")
        else:
            print(f"✗ {file} missing")
            all_present = False
    
    return all_present

def detailed_validate():
    """Detailed validation of each data source."""
    import pandas as pd
    from pathlib import Path
    
    data_dir = Path("data/raw")
    sources = {
        "social_mailing_list.parquet": "Bitcoin-dev Mailing List",
        "social_delving.parquet": "Delving Bitcoin Forum",
        "social_combined.parquet": "Combined Social Data"
    }
    
    print("Detailed Data Validation:")
    print("=" * 50)
    
    for file, name in sources.items():
        path = data_dir / file
        if not path.exists():
            print(f"✗ {name}: File missing")
            continue
        
        try:
            df = pd.read_parquet(path)
            total = len(df)
            if total == 0:
                print(f"✗ {name}: Empty file")
                continue
            
            first_date = df['date'].min()
            last_date = df['date'].max()
            year_counts = df['date'].dt.year.value_counts().sort_index()
            
            print(f"\n✓ {name}:")
            print(f"  Total messages: {total}")
            print(f"  Date range: {first_date} to {last_date}")
            print("  Messages by year:")
            for year, count in year_counts.items():
                print(f"    {year}: {count}")
                
        except Exception as e:
            print(f"✗ {name}: Error reading file - {e}")
    
    return True

def main():
    print("Starting Smart Bitcoin Social Data Ingestion Workflow")
    print("=" * 60)
    
    # Run ingestion scripts
    print("Phase 1: Ingesting data from individual sources...")
    for script in INGEST_SCRIPTS:
        if not run_script(script):
            print(f"Stopping workflow due to failure in {script}")
            sys.exit(1)
    
    # Run merge script
    print("\nPhase 2: Merging data sources...")
    if not run_script(MERGE_SCRIPT):
        print("Stopping workflow due to failure in merge")
        sys.exit(1)
    
    # Validate
    print("\nPhase 3: Validating final data...")
    if validate_data():
        detailed_validate()
        print("\n✓ Workflow completed successfully!")
        print("All data sources ingested, merged, and validated.")
    else:
        print("\n✗ Validation failed - some data files are missing")
        sys.exit(1)

if __name__ == "__main__":
    main()