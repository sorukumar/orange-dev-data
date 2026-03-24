import pandas as pd
import json
import os
import sys

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import WORK_DIR, ID_PATH

# --- Configuration (Centralized via paths.py) ---
INPUT_PATH = os.path.join(WORK_DIR, "social", "combined.parquet")
MAPPING_PATH = ID_PATH

def main():
    print("--- Social Data Summary & Coverage Analysis ---")
    if not os.path.exists(INPUT_PATH):
        print(f"Error: {INPUT_PATH} not found.")
        return

    df = pd.read_parquet(INPUT_PATH)
    print(f"Total Unified Social Records: {len(df)}")
    
    unique_authors = df['canonical_id'].nunique()
    print(f"Unique Social Humans detected: {unique_authors}")
    
    # Check coverage against Whitelist
    if os.path.exists(MAPPING_PATH):
        with open(MAPPING_PATH, 'r') as f:
            mapping_data = json.load(f)
        
        known_canonical_names = {entry['canonical_name'] for entry in mapping_data.get('aliases', [])}
        
        post_counts = df['canonical_id'].value_counts()
        unmapped_counts = post_counts[~post_counts.index.isin(known_canonical_names)]
        mapped_count = len(post_counts[post_counts.index.isin(known_canonical_names)])
        
        print(f"Mapped Humans: {mapped_count} ({mapped_count/unique_authors*100:.1f}%)")
        print(f"Unmapped Humans with activity: {len(unmapped_counts)}")
        
        if not unmapped_counts.empty:
            print("\nHigh-Impact Potential Whitelist additions (Top Unmapped):")
            print(unmapped_counts.head(10))
    else:
        print("Warning: Identity mapping file not found for coverage analysis.")

if __name__ == "__main__":
    main()
