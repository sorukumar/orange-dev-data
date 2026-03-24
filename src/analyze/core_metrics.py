import pandas as pd
import json
import os
import sys
import numpy as np
from datetime import datetime
import sys

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import WORK_DIR, TRACKER_DIR
from src.core.identity import Consolidator
from src.core.lookup import MaintainerLookup, SponsorLookup

# --- Configuration (Centralized via paths.py) ---
COMMITS_FILE = os.path.join(WORK_DIR, "core", "commits.parquet")
OUTPUT_DIR = TRACKER_DIR

# --- Analytics Logic ---
class MetricGenerators:
    @staticmethod
    def generate_vital_signs(commits):
        print("Generating Vital Signs (Digital Pulse)...")
        maintainers = MaintainerLookup.load()
        
        # 1. Unique Contributors (Canonical)
        unique_contributors = commits['canonical_id'].nunique()
        total_commits = commits['hash'].nunique()
        
        # 2. Maintainers (Whitelisted)
        all_maintainers = MaintainerLookup.get_all()
        active_maintainers = [m for m in all_maintainers if m.get("status") == "active"]
        
        # 3. Codebase Size (Net LOC)
        net_lines = int(commits['additions'].sum() - commits['deletions'].sum())
        
        data = {
            "unique_contributors": int(unique_contributors),
            "active_maintainers": len(active_maintainers),
            "total_maintainers": len(all_maintainers),
            "total_commits": int(total_commits),
            "net_loc": net_lines,
            "generated_at": datetime.now().isoformat()
        }
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(os.path.join(OUTPUT_DIR, "dashboard_vital_signs.json"), "w") as f:
            json.dump(data, f, indent=2)

    @staticmethod
    def generate_snapshots(commits):
        print("Generating Work Distribution Snapshots...")
        
        # 1. Work Distribution (Commits by Area)
        # Handle fractional attribution if a hash has multiple categories
        commit_counts = commits.groupby('category')['hash'].nunique().sort_values(ascending=False).to_dict()
        
        work_data = [{"name": k, "value": v} for k, v in commit_counts.items()]
        with open(os.path.join(OUTPUT_DIR, "stats_work_distribution.json"), "w") as f:
            json.dump({"data": work_data}, f, indent=2)
            
        # 2. Tech Stack (Net Lines by Area)
        vol_data = commits.groupby('category').apply(lambda x: int(x['additions'].sum() - x['deletions'].sum())).to_dict()
        vol_list = [{"name": k, "value": v} for k, v in vol_data.items() if v > 0]
        with open(os.path.join(OUTPUT_DIR, "stats_code_volume.json"), "w") as f:
            json.dump({"data": vol_list}, f, indent=2)

    @staticmethod
    def generate_category_evolution(commits):
        print("Generating Category Evolution (Temporal Trends)...")
        commits['year'] = pd.to_datetime(commits['date_utc']).dt.year
        
        pivot = commits.groupby(['year', 'category'])['hash'].nunique().unstack(fill_value=0)
        
        categories = pivot.columns.tolist()
        evolution = {
            "xAxis": [str(y) for y in pivot.index.tolist()],
            "series": []
        }
        for cat in categories:
            evolution["series"].append({
                "name": cat,
                "type": "line",
                "stack": "Total",
                "areaStyle": {},
                "data": pivot[cat].tolist()
            })
            
        with open(os.path.join(OUTPUT_DIR, "stats_category_evolution.json"), "w") as f:
            json.dump(evolution, f, indent=2)

    @staticmethod
    def generate_contributor_growth(commits):
        print("Generating Contributor Growth (Retention Trends)...")
        commits['year'] = pd.to_datetime(commits['date_utc']).dt.year
        
        # First seen year (Canonical)
        author_start = commits.groupby('canonical_id')['year'].min().reset_index().rename(columns={'year': 'start_year'})
        df = commits.merge(author_start, on='canonical_id')
        
        years = sorted(df['year'].unique())
        new_counts = []
        vet_counts = []
        
        for y in years:
            active = df[df['year'] == y]
            n_new = active[active['start_year'] == y]['canonical_id'].nunique()
            n_vet = active[active['start_year'] < y]['canonical_id'].nunique()
            new_counts.append(n_new)
            vet_counts.append(n_vet)
            
        growth = {
            "xAxis": [str(y) for y in years],
            "series": [
                {"name": "New Contributors", "type": "bar", "stack": "total", "data": new_counts},
                {"name": "Veterans", "type": "bar", "stack": "total", "data": vet_counts}
            ]
        }
        with open(os.path.join(OUTPUT_DIR, "stats_contributor_growth.json"), "w") as f:
            json.dump(growth, f, indent=2)

def main():
    print("--- Core Analytics Engine (New Architecture) ---")
    if not os.path.exists(COMMITS_FILE):
        print(f"Error: {COMMITS_FILE} not found. Run repository ingestion first.")
        return

    # 1. Load data
    df = pd.read_parquet(COMMITS_FILE)
    
    # 2. Resolve Identities (Canonicalization)
    df = Consolidator.normalize(df)
    
    # 3. Generate Metrics
    MetricGenerators.generate_vital_signs(df)
    MetricGenerators.generate_snapshots(df)
    MetricGenerators.generate_category_evolution(df)
    MetricGenerators.generate_contributor_growth(df)
    
    # 4. Save intermediate rich dataset
    rich_path = os.path.join(WORK_DIR, "core", "contributors_rich.json")
    # (Simplified for now, just the counts per author)
    # This is needed by regional_evolution.py
    author_summary = df.groupby('canonical_id').agg({
        'year': 'min', # cohort_year
        'canonical_name': 'first',
        'author_name': 'first', # fallback
        'author_email': 'first' # needed for location lookup if we had it
    }).rename(columns={'year': 'cohort_year'}).reset_index()
    
    # In a full run, we would merge location data here.
    # For now, let's just make sure cohort_year is there.
    author_summary.to_json(rich_path, orient='records', indent=2)
    print(f"Saved Author Summary for Geo: {rich_path}")

if __name__ == "__main__":
    main()
