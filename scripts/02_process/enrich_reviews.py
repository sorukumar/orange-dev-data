import pandas as pd
import os
import sys
sys.path.append(os.getcwd())
from scripts.utils.identity import resolver

RAW_REVIEWS = "data/raw/github_review_events.parquet"
OUTPUT_REVIEWS = "data/enriched/enriched_reviews.parquet"

def enrich_reviews():
    if not os.path.exists(RAW_REVIEWS):
        print(f"File not found: {RAW_REVIEWS}. Skipping reviews enrichment.")
        return

    print("Loading raw reviews...")
    df_reviews = pd.read_parquet(RAW_REVIEWS)
    
    if 'user' not in df_reviews.columns:
        print("Error: 'user' column not found in raw reviews.")
        return
        
    print("Joining review authors with UUIDs using central identity resolver...")
    df_reviews['uuid'] = df_reviews['user'].apply(lambda x: resolver.resolve_github(x) if pd.notna(x) else None)
    df_enriched = df_reviews
    
    os.makedirs(os.path.dirname(OUTPUT_REVIEWS), exist_ok=True)
    df_enriched.to_parquet(OUTPUT_REVIEWS, index=False)
    
    matched = df_enriched['uuid'].notna().sum()
    total = len(df_enriched)
    if total > 0:
        print(f"Saved {total} enriched reviews to {OUTPUT_REVIEWS}")
        print(f"Matched {matched} reviews ({matched/total*100:.1f}%) to unified contributors.")
    else:
        print(f"Saved {total} enriched reviews to {OUTPUT_REVIEWS}")

if __name__ == "__main__":
    enrich_reviews()
