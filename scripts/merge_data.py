import pandas as pd
import os

# --- Configuration ---
DELVING_PATH = "data/raw/social_delving.parquet"
MAILING_LIST_PATH = "data/raw/social_mailing_list.parquet"
OUTPUT_PARQUET = "data/raw/social_combined.parquet"

def main():
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
        
        # Add link for mailing list
        def clean_mid(mid):
            if not mid: return None
            return mid.strip('<>')
            
        df_ml['link'] = df_ml['message_id'].apply(lambda x: f"https://gnusha.org/pi/bitcoindev/{clean_mid(x)}" if x else None)
        
        # Ensure date is TZ-naive
        if df_ml['date'].dt.tz is not None:
            df_ml['date'] = df_ml['date'].dt.tz_localize(None)
            
        dfs.append(df_ml)
        
    if not dfs:
        print("No social data found to merge.")
        return
        
    print("Merging social data...")
    df_combined = pd.concat(dfs, ignore_index=True)
    
    # Sort by date
    df_combined = df_combined.sort_values('date', ascending=False)
    
    # Save combined artifact
    os.makedirs(os.path.dirname(OUTPUT_PARQUET), exist_ok=True)
    df_combined.to_parquet(OUTPUT_PARQUET, index=False)
    
    print(f"Total social records: {len(df_combined)}")
    print(f"Saved to {OUTPUT_PARQUET}")
    
    # Print status summary
    print("\nSocial Activity by Source:")
    print(df_combined['source'].value_counts())
    
    print("\nTop 5 Contributors (Governance):")
    print(df_combined['canonical_id'].value_counts().head(5))

if __name__ == "__main__":
    main()
