import pandas as pd
import numpy as np
import json
import os
import sys

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import WORK_DIR, TRACKER_DIR, SHARED_DIR

# --- Configuration (Centralized via paths.py) ---
BIPS_ENRICHED_PATH = os.path.join(WORK_DIR, "bips_enriched.parquet")
OUTPUT_DIR = TRACKER_DIR
SHARED_BIPS_PATH = os.path.join(SHARED_DIR, "bips_enriched.parquet")

def main():
    print("--- Tracker UI Artifact Generation (New Architecture) ---")
    if not os.path.exists(BIPS_ENRICHED_PATH):
        print(f"Error: {BIPS_ENRICHED_PATH} not found. Run enrichment first.")
        return

    print("Generating UI artifacts from enriched data...")
    df_bips = pd.read_parquet(BIPS_ENRICHED_PATH)
    
    # 1. Stats UI (Global KPIs)
    stats = {
        "total_bips": len(df_bips),
        "final_active_bips": len(df_bips[df_bips['status'].isin(['Final', 'Active', 'Deployed'])]),
        "social_mentions": int(df_bips['social_mention_count'].sum()) if 'social_mention_count' in df_bips else 0,
        "total_revisions": int(df_bips['revision_count'].sum()) if 'revision_count' in df_bips else 0,
        "last_updated": pd.Timestamp.now().isoformat()
    }
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, "stats_ui.json"), 'w') as f:
        json.dump(stats, f, indent=2)

    # 2. Themes UI (Charts)
    theme_counts = df_bips['theme'].value_counts().to_dict()
    theme_mentions = df_bips.groupby('theme')['social_mention_count'].sum().to_dict() if 'social_mention_count' in df_bips else {}
    
    themes_ui = {
        "bip_counts": [{"name": k, "value": v} for k, v in theme_counts.items()],
        "social_mentions": [{"name": k, "value": int(v)} for k, v in theme_mentions.items()]
    }
    with open(os.path.join(OUTPUT_DIR, "themes_ui.json"), 'w') as f:
        json.dump(themes_ui, f, indent=2)

    # 3. Funnel UI (Consensus Stages)
    status_counts = df_bips['status'].value_counts().to_dict()
    funnel_data = [{"name": k, "value": v} for k, v in status_counts.items()]
    with open(os.path.join(OUTPUT_DIR, "funnel_ui.json"), 'w') as f:
        json.dump(funnel_data, f, indent=2)

    # 4. BIPs UI (The Table)
    cols = ['bip_id', 'title', 'status', 'theme', 'maturity_score', 
            'social_mention_count', 'revision_count', 'code_mention_count', 'author_names']
    # filter for existing columns
    cols = [c for c in cols if c in df_bips.columns]
    
    df_ui = df_bips[cols].sort_values('maturity_score', ascending=False).copy()
    
    def clean_authors(val):
        if isinstance(val, (list, pd.Series, np.ndarray)):
            return ", ".join([str(x) for x in val])
        return str(val)
        
    if 'author_names' in df_ui.columns:
        df_ui['authors'] = df_ui['author_names'].apply(clean_authors)
        df_ui = df_ui.drop('author_names', axis=1)
    
    bips_ui = df_ui.to_dict(orient='records')
    with open(os.path.join(OUTPUT_DIR, "bips_ui.json"), 'w') as f:
        json.dump(bips_ui, f, indent=2)

    # 5. Export to Public Shared location
    os.makedirs(os.path.dirname(SHARED_BIPS_PATH), exist_ok=True)
    df_bips.to_parquet(SHARED_BIPS_PATH, index=False)
    print(f"Exported Shared artifact: {SHARED_BIPS_PATH}")

    print(f"UI artifacts generated successfully in {OUTPUT_DIR}/")

if __name__ == "__main__":
    main()
