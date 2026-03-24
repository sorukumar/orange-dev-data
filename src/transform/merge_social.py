import pandas as pd
import os
import sys

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import WORK_DIR, SHARED_DIR

# --- Configuration (Centralized via paths.py) ---
DELVING_PATH = os.path.join(WORK_DIR, "social", "delving.parquet")
MAILING_LIST_PATH = os.path.join(WORK_DIR, "social", "mailing_list.parquet")
WORK_OUTPUT = os.path.join(WORK_DIR, "social", "combined.parquet")
SHARED_OUTPUT = os.path.join(SHARED_DIR, "social_threads.parquet")

def main():
    print("--- Social Data Merge (New Architecture) ---")
    dfs = []
    
    if os.path.exists(DELVING_PATH):
        print(f"Loading Delving data from {DELVING_PATH}...")
        df_delving = pd.read_parquet(DELVING_PATH)
        # Ensure date is TZ-naive for consistency
        if df_delving['date'].dt.tz is not None:
            df_delving['date'] = df_delving['date'].dt.tz_localize(None)
        dfs.append(df_delving)
        
    if os.path.exists(MAILING_LIST_PATH):
        print(f"Loading Mailing List data from {MAILING_LIST_PATH}...")
        df_ml = pd.read_parquet(MAILING_LIST_PATH)
        
        # Link generation for Mailing List (Public-Inbox)
        def clean_mid(mid):
            if not mid: return None
            return str(mid).strip('<>')
            
        df_ml['link'] = df_ml['message_id'].apply(lambda x: f"https://gnusha.org/pi/bitcoindev/{clean_mid(x)}" if x else None)
        
        # Ensure date is TZ-naive
        if df_ml['date'].dt.tz is not None:
            df_ml['date'] = df_ml['date'].dt.tz_localize(None)
        dfs.append(df_ml)
        
    if not dfs:
        print("No social data found to merge.")
        return
        
    print("Merging source streams...")
    df_combined = pd.concat(dfs, ignore_index=True)
    df_combined = df_combined.sort_values('date', ascending=False)
    
    # Deduplicate if necessary (e.g. cross-posted threads)
    df_combined = df_combined.drop_duplicates(subset=['message_id'])
    
    # Save to Internal Work location
    os.makedirs(os.path.dirname(WORK_OUTPUT), exist_ok=True)
    df_combined.to_parquet(WORK_OUTPUT, index=False)
    print(f"Saved Work artifact to {WORK_OUTPUT}")

    # Export to Public Shared location
    os.makedirs(os.path.dirname(SHARED_OUTPUT), exist_ok=True)
    df_combined.to_parquet(SHARED_OUTPUT, index=False)
    print(f"Exported Shared artifact: {SHARED_OUTPUT}")
    
    print(f"\nTotal Unified Records: {len(df_combined)}")
    print("\nSocial Activity by Source:")
    print(df_combined['source'].value_counts())

if __name__ == "__main__":
    main()
