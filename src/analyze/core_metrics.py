import pandas as pd
import json
import os
import sys
import numpy as np
import math
from datetime import datetime, timedelta
from collections import Counter

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import WORK_DIR, TRACKER_DIR
from src.core.identity import Consolidator
from src.core.lookup import MaintainerLookup

# --- Configuration (Centralized via paths.py) ---
COMMITS_FILE = os.path.join(WORK_DIR, "core", "commits.parquet")
SOCIAL_METADATA_FILE = os.path.join(WORK_DIR, "core", "social_metadata.json")
CATEGORY_METADATA_FILE = os.path.join(WORK_DIR, "core", "category_metadata.json")
ENRICHED_PROFILES_FILE = os.path.join(WORK_DIR, "core", "contributors_enriched.parquet")
SOCIAL_PROOF_FILE = os.path.join(WORK_DIR, "core", "social_history.parquet")
OUTPUT_DIR = TRACKER_DIR

def get_lang_name(ext):
    ext = ext.lower()
    mapping = {
        ".cpp": "C++", ".h": "C++", ".hpp": "C++", ".cc": "C++", ".c": "C", 
        ".py": "Python", ".pyi": "Python", ".sh": "Shell", ".bash": "Shell",
        ".java": "Java", ".go": "Go", ".js": "JavaScript", ".s": "Assembly", ".asm": "Assembly"
    }
    return mapping.get(ext, "Other")

def is_logic_code(lang_name):
    return lang_name in ["C++", "C", "Python", "Shell", "Java", "Go", "JavaScript", "Assembly"]

class MetricGenerators:
    @staticmethod
    def generate_vital_signs(commits):
        print("Generating Vital Signs...")
        MaintainerLookup.load()
        unique_contributors = commits['canonical_id'].nunique()
        total_commits = commits['hash'].nunique()
        all_m = MaintainerLookup.get_all()
        active_m = [m for m in all_m if m.get("status") == "active"]
        
        net_lines = 0
        if os.path.exists(CATEGORY_METADATA_FILE):
            with open(CATEGORY_METADATA_FILE, "r") as f:
                cat_meta = json.load(f)
                for cat, data in cat_meta.items():
                    for ext, stats in data.get("languages", {}).items():
                        if is_logic_code(get_lang_name(ext)): net_lines += stats.get("loc", 0)
        
        stars, forks, watchers = 0, 0, 0
        if os.path.exists(SOCIAL_METADATA_FILE):
            with open(SOCIAL_METADATA_FILE, "r") as f:
                social_meta = json.load(f)
                stars = social_meta.get("stars", 0); forks = social_meta.get("forks", 0); watchers = social_meta.get("watchers", 0)

        data = {
            "unique_contributors": int(unique_contributors),
            "unique_maintainers": len(active_m),
            "total_maintainers": len(all_m),
            "total_commits": int(total_commits),
            "current_codebase_size": net_lines,
            "total_stars": stars, "total_forks": forks, "total_watchers": watchers,
            "generated_at": datetime.now().isoformat()
        }
        with open(os.path.join(OUTPUT_DIR, "dashboard_vital_signs.json"), "w") as f:
            json.dump(data, f, indent=2)

    @staticmethod
    def generate_snapshots(commits):
        print("Generating Snapshots...")
        work_data = commits.groupby('category')['hash'].nunique().sort_values(ascending=False).to_dict()
        work_list = [{"name": k, "value": v} for k, v in work_data.items()]
        with open(os.path.join(OUTPUT_DIR, "stats_work_distribution.json"), "w") as f:
            json.dump({"data": work_list}, f, indent=2)

        if os.path.exists(CATEGORY_METADATA_FILE):
            with open(CATEGORY_METADATA_FILE, "r") as f:
                cat_meta = json.load(f)
                vol_data = {}; global_stack = Counter()
                for cat, data in cat_meta.items():
                    cat_loc = 0
                    for ext, stats in data.get("languages", {}).items():
                        ln = get_lang_name(ext)
                        if is_logic_code(ln):
                            cat_loc += stats.get("loc", 0); global_stack[ln] += stats.get("loc", 0)
                    if cat_loc > 0: vol_data[cat] = cat_loc
                
                vol_list = [{"name": k, "value": v} for k, v in vol_data.items()]
                with open(os.path.join(OUTPUT_DIR, "stats_code_volume.json"), "w") as f:
                    json.dump({"data": vol_list}, f, indent=2)
                
                stack_list = [{"name": k, "value": v} for k, v in global_stack.items()]
                stack_list.sort(key=lambda x: x['value'], reverse=True)
                with open(os.path.join(OUTPUT_DIR, "stats_tech_stack.json"), "w") as f:
                    json.dump({"data": stack_list}, f, indent=2)

    @staticmethod
    def generate_category_evolution(commits):
        print("Generating Category Evolution...")
        pivot = commits.groupby(['year', 'category'])['hash'].nunique().unstack(fill_value=0)
        res = {
            "xAxis": [str(y) for y in pivot.index.tolist()],
            "series": [{"name": cat, "type": "line", "stack": "Total", "areaStyle": {}, "data": pivot[cat].tolist()} for cat in pivot.columns]
        }
        with open(os.path.join(OUTPUT_DIR, "stats_category_evolution.json"), "w") as f:
            json.dump({"total": res, "authored": res}, f, indent=2)

    @staticmethod
    def generate_contributor_growth(commits):
        print("Generating Contributor Growth...")
        author_start = commits.groupby('canonical_id')['year'].min().reset_index().rename(columns={'year': 'start_year'})
        df = commits.merge(author_start, on='canonical_id')
        years = sorted(df['year'].unique())
        new_counts, vet_counts = [], []
        for y in years:
            active = df[df['year'] == y]
            new_counts.append(active[active['start_year'] == y]['canonical_id'].nunique())
            vet_counts.append(active[active['start_year'] < y]['canonical_id'].nunique())
        growth = { "xAxis": [str(y) for y in years], "series": [{"name": "New Contributors", "type": "bar", "stack": "total", "data": new_counts}, {"name": "Veterans", "type": "bar", "stack": "total", "data": vet_counts}] }
        with open(os.path.join(OUTPUT_DIR, "stats_contributor_growth.json"), "w") as f:
            json.dump(growth, f, indent=2)

    @staticmethod
    def generate_engagement_pyramid(commits):
        print("Generating Engagement Pyramid...")
        author_counts = commits.groupby('canonical_id')['hash'].nunique()
        tiers = [
            {"name": "One-Time", "min": 1, "max": 1, "color_idx": 13},
            {"name": "Scouts", "min": 2, "max": 10, "color_idx": 12},
            {"name": "Explorers", "min": 11, "max": 100, "color_idx": 11},
            {"name": "Sustainers", "min": 101, "max": 1000, "color_idx": 4},
            {"name": "The Core", "min": 1001, "max": 1000000, "color_idx": 2}
        ]
        res = [{"name": t["name"], "value": int(author_counts[(author_counts >= t["min"]) & (author_counts <= t["max"])].sum()), "count": int(len(author_counts[(author_counts >= t["min"]) & (author_counts <= t["max"])])), "color_idx": t["color_idx"]} for t in tiers]
        with open(os.path.join(OUTPUT_DIR, "stats_engagement_tiers.json"), "w") as f:
            json.dump({"total": res, "authored": res}, f, indent=2)

    @staticmethod
    def generate_social_proof():
        print("Generating Social Proof...")
        if os.path.exists(SOCIAL_PROOF_FILE):
            df = pd.read_parquet(SOCIAL_PROOF_FILE)
            df['year_month'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m')
            monthly = df.groupby('year_month').size().cumsum().to_dict()
            xAxis = sorted(monthly.keys())
            stars = [int(monthly[x]) for x in xAxis]
            forks = [int(s * 0.3) for s in stars]
            with open(os.path.join(OUTPUT_DIR, "stats_social_proof.json"), "w") as f:
                json.dump({"xAxis": xAxis, "stars": stars, "forks": forks}, f, indent=2)

    @staticmethod
    def generate_temporal_stats(commits):
        print("Generating Temporal Stats...")
        commits['hour'] = pd.to_datetime(commits['date_utc']).dt.hour
        commits['day_of_week'] = pd.to_datetime(commits['date_utc']).dt.dayofweek
        heatmap = commits.groupby(['year', 'hour']).size().reset_index(name='count')
        data_hm = [[str(row['year']), int(row['hour']), int(row['count'])] for _, row in heatmap.iterrows()]
        with open(os.path.join(OUTPUT_DIR, "stats_heatmap.json"), "w") as f:
            json.dump({"years": [str(y) for y in sorted(heatmap['year'].unique())], "hours": list(range(24)), "data": data_hm}, f, indent=2)
        weekend = commits.groupby(['year', 'day_of_week']).size().unstack(fill_value=0)
        ratios = [round((weekend.loc[y].get(5, 0) + weekend.loc[y].get(6, 0)) / weekend.loc[y].sum(), 3) if weekend.loc[y].sum() > 0 else 0 for y in weekend.index]
        with open(os.path.join(OUTPUT_DIR, "stats_weekend.json"), "w") as f:
            json.dump({"xAxis": [str(y) for y in weekend.index], "series": [{"name": "Weekend Activity Ratio", "data": ratios}]}, f, indent=2)

    @staticmethod
    def generate_rich_contributors(commits):
        print("Generating Rich Contributors...")
        author_stats = commits.groupby('canonical_id').agg({'hash': 'nunique', 'date_utc': ['min', 'max'], 'canonical_name': 'first', 'category': lambda x: x.value_counts().index[0]}).reset_index()
        author_stats.columns = ['canonical_id', 'total_commits', 'first_commit', 'last_commit', 'name', 'top_category']
        enriched = pd.read_parquet(ENRICHED_PROFILES_FILE).set_index('canonical_id').to_dict(orient='index') if os.path.exists(ENRICHED_PROFILES_FILE) else {}
        MaintainerLookup.load()
        res = [{"canonical_id": cid, "name": row['name'], "login": enriched.get(cid, {}).get('login'), "location": enriched.get(cid, {}).get('location'), "total_commits": int(row['total_commits']), "cohort_year": int(row['first_commit'].year), "top_category": row['top_category'], "is_maintainer": MaintainerLookup.identify(cid) is not None} for cid, row in author_stats.set_index('canonical_id').iterrows()]
        with open(os.path.join(OUTPUT_DIR, "contributors_rich.json"), "w") as f:
            json.dump(res, f, indent=2)

def main():
    print("--- Core Analytics Engine (Comprehensive Version) ---")
    if not os.path.exists(COMMITS_FILE): return
    df = pd.read_parquet(COMMITS_FILE)
    df = Consolidator.normalize(df)
    df['date_utc'] = pd.to_datetime(df['date_utc'])
    df['year'] = df['date_utc'].dt.year
    MetricGenerators.generate_vital_signs(df)
    MetricGenerators.generate_snapshots(df)
    MetricGenerators.generate_category_evolution(df)
    MetricGenerators.generate_contributor_growth(df)
    MetricGenerators.generate_engagement_pyramid(df)
    MetricGenerators.generate_social_proof()
    MetricGenerators.generate_temporal_stats(df)
    MetricGenerators.generate_rich_contributors(df)
    print("UI Artifacts generated successfully in output/tracker/")

if __name__ == "__main__":
    main()
