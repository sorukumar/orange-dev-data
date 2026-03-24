import pandas as pd
import json
import os
import sys
import numpy as np
import math
import re
from datetime import datetime, timezone
from collections import Counter

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import WORK_DIR, TRACKER_DIR
from src.core.identity import Consolidator
from src.core.lookup import MaintainerLookup, SponsorLookup

# --- Configuration (Centralized via paths.py) ---
COMMITS_FILE = os.path.join(WORK_DIR, "core", "commits.parquet")
SOCIAL_METADATA_FILE = os.path.join(WORK_DIR, "core", "social_metadata.json")
CATEGORY_METADATA_FILE = os.path.join(WORK_DIR, "core", "category_metadata.json")
ENRICHED_PROFILES_FILE = os.path.join(WORK_DIR, "core", "contributors_enriched.parquet")
SOCIAL_PROOF_FILE = os.path.join(WORK_DIR, "core", "social_history.parquet")
OUTPUT_DIR = TRACKER_DIR

# --- Radar Mapping ---
RISK_WEIGHTS = {
    "Consensus (Domain Logic)": 50, "Cryptography (Primitives)": 50, "Core Libs": 50,
    "P2P Network (Infrastructure)": 40, "Database (Persistence)": 30, "Utilities (Shared Libs)": 30,
    "Node & RPC (App/Interface)": 10, "GUI (Presentation Layer)": 10, "Wallet (Client App)": 20,
    "Tests (QA)": 5, "Build & CI (DevOps)": 5, "Documentation": 1
}

RADAR_AXES = {
    "Security": ["Consensus (Domain Logic)", "Cryptography (Primitives)", "Core Libs"],
    "Resilience": ["P2P Network (Infrastructure)", "Database (Persistence)", "Utilities (Shared Libs)"],
    "Usability": ["GUI (Presentation Layer)", "Node & RPC (App/Interface)", "Wallet (Client App)"],
    "Quality": ["Tests (QA)", "Build & CI (DevOps)"],
    "Education": ["Documentation"]
}

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
        MaintainerLookup.load(); unique_contributors = commits['canonical_id'].nunique(); total_commits = commits['hash'].nunique()
        all_m = MaintainerLookup.get_all(); active_m = [m for m in all_m if m.get("status") == "active"]
        
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
            "unique_contributors": int(unique_contributors), "unique_maintainers": len(active_m), "total_maintainers": len(all_m),
            "total_commits": int(total_commits), "current_codebase_size": net_lines, "total_stars": stars, "total_forks": forks, "total_watchers": watchers,
            "generated_at": datetime.now().isoformat()
        }
        with open(os.path.join(OUTPUT_DIR, "dashboard_vital_signs.json"), "w") as f:
            json.dump(data, f, indent=2)

    @staticmethod
    def generate_snapshots(commits):
        print("Generating Snapshots...")
        work_data = commits.groupby('category')['hash'].nunique().sort_values(ascending=False).to_dict()
        with open(os.path.join(OUTPUT_DIR, "stats_work_distribution.json"), "w") as f:
            json.dump({"data": [{"name": k, "value": v} for k, v in work_data.items()]}, f, indent=2)

        if os.path.exists(CATEGORY_METADATA_FILE):
            with open(CATEGORY_METADATA_FILE, "r") as f:
                cat_meta = json.load(f)
                vol_data = {}; global_stack = Counter()
                for cat, data in cat_meta.items():
                    cat_loc = 0
                    for ext, stats in data.get("languages", {}).items():
                        ln = get_lang_name(ext); 
                        if is_logic_code(ln): cat_loc += stats.get("loc", 0); global_stack[ln] += stats.get("loc", 0)
                    if cat_loc > 0: vol_data[cat] = cat_loc
                with open(os.path.join(OUTPUT_DIR, "stats_code_volume.json"), "w") as f:
                    json.dump({"data": [{"name": k, "value": v} for k, v in vol_data.items()]}, f, indent=2)
                stack_list = sorted([{"name": k, "value": v} for k, v in global_stack.items()], key=lambda x: x['value'], reverse=True)
                with open(os.path.join(OUTPUT_DIR, "stats_tech_stack.json"), "w") as f:
                    json.dump({"data": stack_list}, f, indent=2)

    @staticmethod
    def generate_category_evolution(commits):
        print("Generating Category Evolution...")
        pivot = commits.groupby(['year', 'category'])['hash'].nunique().unstack(fill_value=0)
        res = { "xAxis": [str(y) for y in pivot.index.tolist()], "series": [{"name": cat, "type": "line", "stack": "Total", "areaStyle": {}, "data": pivot[cat].tolist()} for cat in pivot.columns] }
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
        print("Generating Engagement Pyramid (3-Tier Model)...")
        def calculate_tiers(counts):
            n = len(counts)
            if n == 0: return []
            counts = counts.sort_values(ascending=False); i1 = int(np.ceil(n * 0.01)); i20 = int(np.ceil(n * 0.20))
            return [
                {"name": "👑 The Core (Top 1%)", "value": int(counts.iloc[0:i1].sum()), "count": i1, "color_idx": 4},
                {"name": "⭐ The Contributors (Top 20%)", "value": int(counts.iloc[i1:i20].sum()), "count": i20 - i1, "color_idx": 5},
                {"name": "🌱 The Prospects (Bottom 80%)", "value": int(counts.iloc[i20:].sum()), "count": n - i20, "color_idx": 12}
            ]
        c_total = commits.groupby('canonical_id')['hash'].nunique()
        c_auth = commits[commits['category'] != 'Merge'].groupby('canonical_id')['hash'].nunique()
        with open(os.path.join(OUTPUT_DIR, "stats_engagement_tiers.json"), "w") as f:
            json.dump({"total": calculate_tiers(c_total), "authored": calculate_tiers(c_auth)}, f, indent=2)

    @staticmethod
    def generate_social_proof():
        print("Generating Social Proof...")
        xAxis, stars, forks = [], [], []
        if os.path.exists(SOCIAL_PROOF_FILE):
            df = pd.read_parquet(SOCIAL_PROOF_FILE); df['year_month'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m')
            monthly = df.groupby('year_month').size().cumsum().to_dict(); xAxis = sorted(monthly.keys()); stars = [int(monthly[x]) for x in xAxis]; forks = [int(s * 0.3) for s in stars]
        with open(os.path.join(OUTPUT_DIR, "stats_social_proof.json"), "w") as f:
            json.dump({"xAxis": xAxis, "stars": stars, "forks": forks}, f, indent=2)

    @staticmethod
    def generate_temporal_stats(commits):
        print("Generating Temporal Stats...")
        commits['hour'] = pd.to_datetime(commits['date_utc']).dt.hour; heatmap = commits.groupby(['year', 'hour']).size().reset_index(name='count')
        data_hm = [[str(row['year']), int(row['hour']), int(row['count'])] for _, row in heatmap.iterrows()]
        with open(os.path.join(OUTPUT_DIR, "stats_heatmap.json"), "w") as f:
            json.dump({"years": [str(y) for y in sorted(heatmap['year'].unique())], "hours": list(range(24)), "data": data_hm}, f, indent=2)
        weekend = commits.groupby(['year', pd.to_datetime(commits['date_utc']).dt.dayofweek]).size().unstack(fill_value=0)
        ratios = [round((weekend.loc[y].get(5, 0) + weekend.loc[y].get(6, 0)) / weekend.loc[y].sum(), 3) if weekend.loc[y].sum() > 0 else 0 for y in weekend.index]
        with open(os.path.join(OUTPUT_DIR, "stats_weekend.json"), "w") as f:
            json.dump({"xAxis": [str(y) for y in weekend.index], "series": [{"name": "Weekend Activity Ratio", "data": ratios}]}, f, indent=2)

    @staticmethod
    def generate_rich_contributors(commits):
        print("Generating High-Fidelity Rich Contributors (Galaxy Data)...")
        # 1. Base Stats
        g1 = commits.groupby('canonical_id').agg({
            'year': ['min', 'max', 'nunique'],
            'additions': 'sum',
            'hash': 'nunique',
            'canonical_name': 'first'
        })
        g1.columns = ['start_year', 'end_year', 'tenure', 'lines_added', 'total_commits', 'name']
        
        # 2. Authored vs Merge
        g1['merge_commits'] = commits[commits['category'] == 'Merge'].groupby('canonical_id')['hash'].nunique().reindex(g1.index, fill_value=0)
        g1['authored_commits'] = commits[commits['category'] != 'Merge'].groupby('canonical_id')['hash'].nunique().reindex(g1.index, fill_value=0)
        
        # 3. Percentiles
        g1['percentile'] = g1['total_commits'].rank(pct=True)
        
        # 4. Fractional Weighting for Radar & History
        # We need to know how many categories each commit has to divide weight
        commit_cat_counts = commits.groupby('hash')['category'].nunique().to_dict()
        commits['weight'] = commits['hash'].map(commit_cat_counts).apply(lambda x: 1.0/x if x > 0 else 1.0)
        
        # 5. History Map: {cid: {year: {cat: weight}}}
        print("  Calculating temporal authorship history...")
        hist_agg = commits.groupby(['canonical_id', 'year', 'category'])['weight'].sum().unstack(fill_value=0)
        history_map = {}
        for (cid, year), row in hist_agg.iterrows():
            if cid not in history_map: history_map[cid] = {}
            history_map[cid][int(year)] = {k: round(v, 2) for k, v in row.to_dict().items() if v > 0}

        # 6. Radar Profiles
        print("  Calculating technical radar profiles...")
        commits['risk_val'] = commits['category'].map(RISK_WEIGHTS).fillna(1)
        commits['weighted_score'] = commits['weight'] * commits['risk_val']
        cat_scores = commits.groupby(['canonical_id', 'category'])['weighted_score'].sum().unstack(fill_value=0)
        
        radar_profiles = {}
        for cid, row in cat_scores.iterrows():
            profile = {}
            for axis, cats in RADAR_AXES.items():
                profile[axis] = round(sum(row.get(c, 0) for c in cats), 2)
            radar_profiles[cid] = profile

        # 7. Final Assembly
        enriched = pd.read_parquet(ENRICHED_PROFILES_FILE).set_index('canonical_id').to_dict(orient='index') if os.path.exists(ENRICHED_PROFILES_FILE) else {}
        MaintainerLookup.load()
        
        res = []
        tot_sys_commits = commits['hash'].nunique()
        for cid, row in g1.iterrows():
            e = enriched.get(cid, {})
            m = MaintainerLookup.identify(cid)
            
            # Focus Areas
            focus_data = commits[commits['canonical_id'] == cid].groupby('category')['weight'].sum().to_dict()
            
            res.append({
                "canonical_id": cid, "name": row['name'], "login": e.get('login'), "location": e.get('location'),
                "total_commits": int(row['total_commits']), "authored_commits": int(row['authored_commits']), "merge_commits": int(row['merge_commits']),
                "cohort_year": int(row['start_year']), "last_active_year": int(row['end_year']),
                "impact": int(row['lines_added']), "tenure": int(row['tenure']), "span": f"{int(row['start_year'])}-{int(row['end_year'])}",
                "contribution_pct": round((row['total_commits'] / tot_sys_commits) * 100, 4),
                "percentile_raw": round(row['percentile'] * 100, 1),
                "is_maintainer": m is not None, "maintainer_status": m.get("status") if m else None,
                "focus_areas": {k: round(v, 2) for k, v in focus_data.items() if v > 0},
                "history": history_map.get(cid, {}),
                "radar_profile": radar_profiles.get(cid, {})
            })
            
        with open(os.path.join(OUTPUT_DIR, "contributors_rich.json"), "w") as f:
            json.dump(res, f, indent=2)

def main():
    print("--- Core Analytics Engine (Comprehensive Multi-Tab Parity) ---")
    if not os.path.exists(COMMITS_FILE): return
    df = pd.read_parquet(COMMITS_FILE); df = Consolidator.normalize(df)
    df['date_utc'] = pd.to_datetime(df['date_utc']); df['year'] = df['date_utc'].dt.year
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
