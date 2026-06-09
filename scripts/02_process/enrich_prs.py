import pandas as pd
import os
import sys
sys.path.append(os.getcwd())
from scripts.utils.identity import resolver

RAW_PRS = "data/raw/github_pr_metadata.parquet"
OUTPUT_PRS = "data/enriched/enriched_prs.parquet"

def enrich_prs():
    if not os.path.exists(RAW_PRS):
        print(f"File not found: {RAW_PRS}. Skipping PR enrichment.")
        return

    print("Loading raw PRs...")
    df_prs = pd.read_parquet(RAW_PRS)
    
    if 'author' not in df_prs.columns:
        print("Error: 'author' column not found in raw PRs.")
        return
        
    print("Joining PR authors with UUIDs using central identity resolver...")
    df_prs['uuid'] = df_prs['author'].apply(lambda x: resolver.resolve_github(x) if pd.notna(x) else None)
    df_enriched = df_prs
    
    os.makedirs(os.path.dirname(OUTPUT_PRS), exist_ok=True)
    df_enriched.to_parquet(OUTPUT_PRS, index=False)
    
    matched = df_enriched['uuid'].notna().sum()
    total = len(df_enriched)
    if total > 0:
        print(f"Saved {total} enriched PRs to {OUTPUT_PRS}")
        print(f"Matched {matched} PRs ({matched/total*100:.1f}%) to unified contributors.")
    else:
        print(f"Saved {total} enriched PRs to {OUTPUT_PRS}")

if __name__ == "__main__":
    enrich_prs()
