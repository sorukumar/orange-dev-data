import pandas as pd
import numpy as np
import json
import os

# --- Configuration ---
BIPS_ENRICHED_PATH = "data/governance/bips_enriched.parquet"
SOCIAL_PATH = "data/governance/social.parquet"
OUTPUT_DIR = "data/governance"

def main():
    if not os.path.exists(BIPS_ENRICHED_PATH):
        print(f"Error: {BIPS_ENRICHED_PATH} not found. Run enrichment script first.")
        return

    print("Generating UI artifacts from enriched data...")
    df_bips = pd.read_parquet(BIPS_ENRICHED_PATH)
    
    # 1. Stats UI (Global KPIs)
    stats = {
        "total_bips": len(df_bips),
        "final_active_bips": len(df_bips[df_bips['status'].isin(['Final', 'Active', 'Deployed'])]),
        "social_mentions": int(df_bips['social_mention_count'].sum()),
        "total_revisions": int(df_bips['revision_count'].sum()),
        "last_updated": pd.Timestamp.now().isoformat()
    }
    with open(os.path.join(OUTPUT_DIR, "stats_ui.json"), 'w') as f:
        json.dump(stats, f, indent=2)

    # 2. Themes UI (Charts)
    # Prepare data for a treemap or pie chart
    theme_counts = df_bips['theme'].value_counts().to_dict()
    theme_mentions = df_bips.groupby('theme')['social_mention_count'].sum().to_dict()
    
    themes_ui = {
        "bip_counts": [{"name": k, "value": v} for k, v in theme_counts.items()],
        "social_mentions": [{"name": k, "value": int(v)} for k, v in theme_mentions.items()]
    }
    with open(os.path.join(OUTPUT_DIR, "themes_ui.json"), 'w') as f:
        json.dump(themes_ui, f, indent=2)

    # 3. Funnel UI (Consensus Stages)
    # Map raw statuses to high-level funnel stages if needed, or just use raw
    status_counts = df_bips['status'].value_counts().to_dict()
    funnel_data = [{"name": k, "value": v} for k, v in status_counts.items()]
    with open(os.path.join(OUTPUT_DIR, "funnel_ui.json"), 'w') as f:
        json.dump(funnel_data, f, indent=2)

    # 4. BIPs UI (The Table)
    # Select high-signal columns and convert to list of dicts
    # Sort by maturity score descending
    df_ui = df_bips[[
        'bip_id', 'title', 'status', 'theme', 'maturity_score', 
        'social_mention_count', 'revision_count', 'code_mention_count',
        'author_names'
    ]].sort_values('maturity_score', ascending=False).copy()
    
    # Handle list of names (ensure they are actual lists and not numpy arrays)
    def clean_authors(val):
        if isinstance(val, (list, pd.Series, np.ndarray)):
            return ", ".join([str(x) for x in val])
        return str(val)
        
    df_ui['authors'] = df_ui['author_names'].apply(clean_authors)
    df_ui = df_ui.drop('author_names', axis=1)
    
    bips_ui = df_ui.to_dict(orient='records')
    with open(os.path.join(OUTPUT_DIR, "bips_ui.json"), 'w') as f:
        json.dump(bips_ui, f, indent=2)

    print("UI artifacts generated successfully in data/governance/")

if __name__ == "__main__":
    main()
