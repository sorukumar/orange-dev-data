import os
import pandas as pd

def check_dates():
    raw_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    print("=== LATEST DATES FROM RAW PARQUET FILES ===")
    if not os.path.exists(raw_dir):
        print(f"Raw directory not found: {raw_dir}")
        return

    for fname in os.listdir(raw_dir):
        if not fname.endswith(".parquet"):
            continue
        
        fpath = os.path.join(raw_dir, fname)
        try:
            df = pd.read_parquet(fpath)
            
            # Find candidate date columns
            date_cols = [c for c in df.columns if 'date' in c.lower() or 'time' in c.lower() or 'at' in c.lower()]
            
            print(f"\n{fname} (Total Rows: {len(df):,})")
            for c in date_cols:
                try:
                    # Convert to datetime if it's not already, errors='coerce' to ignore non-date strings
                    df_date = pd.to_datetime(df[c], errors='coerce', utc=True)
                    if df_date.notna().any():
                        max_date = df_date.max()
                        print(f"  - Max {c}: {max_date}")
                except Exception as e:
                    pass
        except Exception as e:
            print(f"Error reading {fname}: {e}")

if __name__ == "__main__":
    check_dates()
