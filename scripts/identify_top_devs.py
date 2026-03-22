import json
import pandas as pd
import os

# Paths
rich_path = "/Users/saurabhkumar/Desktop/Work/github/orange-dev-tracker/data/core/contributors_rich.json"
enriched_path = "/Users/saurabhkumar/Desktop/Work/github/orange-dev-tracker/data/core/contributors_enriched.parquet"
sponsors_path = "/Users/saurabhkumar/Desktop/Work/github/orange-dev-tracker/data/cache/sponsors_lookup.json"

def identify_targets():
    if not os.path.exists(rich_path):
        print(f"File not found: {rich_path}")
        return

    with open(rich_path, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # 1. Active in last 3 years (2024, 2025, 2026)
    active_mask = df['last_active_year'] >= 2024
    active_devs = df[active_mask]

    # 2. Top 25 contributors overall
    top_25 = df.sort_values('total_commits', ascending=False).head(25)

    # Merge targets
    targets = pd.concat([active_devs, top_25]).drop_duplicates(subset=['name'])

    # Load current sponsors to exclude if already classified (optional)
    # Actually, we want to REVIEW them, so we'll keep them but mark them.
    with open(sponsors_path, "r") as f:
        sponsors_info = json.load(f)
    
    sponsored_names = {s['canonical_name'] for s in sponsors_info.get("sponsored_developers", [])}
    
    targets['already_sponsored'] = targets['name'].apply(lambda x: x in sponsored_names)

    # Filter columns for cleaner research
    cols = ['name', 'login', 'total_commits', 'last_active_year', 'primary_category', 'location', 'company', 'already_sponsored']
    final_targets = targets[cols].sort_values(['already_sponsored', 'total_commits'], ascending=[True, False])

    output_path = "/Users/saurabhkumar/Desktop/Work/github/orange-dev-tracker/analysis/research_targets.csv"
    final_targets.to_csv(output_path, index=False)
    
    print(f"Identified {len(final_targets)} targets for sponsorship research.")
    print(f"Draft saved to {output_path}")
    
    # Print the top 10 unsponsored targets for immediate review
    print("\nTop Unsponsored Targets:")
    print(final_targets[~final_targets['already_sponsored']].head(20).to_string())

if __name__ == "__main__":
    identify_targets()
