import pandas as pd
import json
import os
import ast
import numpy as np
import re
from datetime import datetime, timedelta
import clean
import footprint


# --- Configuration ---
class Config:
    DATA_DIR = "data/core"
    CACHE_DIR = "data/cache"
    OUTPUT_DIR = "data/core"
    
    COMMITS_FILE = f"{DATA_DIR}/commits.parquet"
    SOCIAL_FILE = f"{DATA_DIR}/social_history.parquet"
    METADATA_FILE = f"{DATA_DIR}/social_metadata.json"
    ENRICHED_FILE = f"{DATA_DIR}/contributors_enriched.parquet"
    
    MAINTAINERS_FILE = "lookups/maintainers_lookup.json"
    SPONSORS_FILE = "lookups/sponsors_lookup.json"

    # Map output filenames
    FILES = {
        "vital_signs": f"{OUTPUT_DIR}/dashboard_vital_signs.json",
        "snapshot_work": f"{OUTPUT_DIR}/stats_work_distribution.json",
        "snapshot_volume": f"{OUTPUT_DIR}/stats_code_volume.json",
        "snapshot_stack": f"{OUTPUT_DIR}/stats_tech_stack.json",
        "trend_category": f"{OUTPUT_DIR}/stats_category_evolution.json",
        "trend_growth": f"{OUTPUT_DIR}/stats_contributor_growth.json",
        "trend_maintainers": f"{OUTPUT_DIR}/stats_maintainers.json",
        "trend_social": f"{OUTPUT_DIR}/stats_social_proof.json",
        "trend_corporate": f"{OUTPUT_DIR}/stats_corporate.json",
        "meta_heatmap": f"{OUTPUT_DIR}/stats_heatmap.json",
        "meta_weekend": f"{OUTPUT_DIR}/stats_weekend.json",
        "meta_geography": f"{OUTPUT_DIR}/stats_geography.json",
        "contributors_rich": f"{OUTPUT_DIR}/contributors_rich.json"
    }

# --- Maintainer Lookup ---
class MaintainerLookup:
    """
    Uses a curated whitelist of known Bitcoin Core maintainers.
    Identity is from the whitelist, activity is calculated from git logs.
    """
    _instance = None
    _email_to_id = {}
    _id_to_record = {}
    _maintainers = []
    
    @classmethod
    def load(cls):
        if cls._instance is not None:
            return cls._instance
        
        cls._instance = cls()
        
        if not os.path.exists(Config.MAINTAINERS_FILE):
            print(f"Warning: {Config.MAINTAINERS_FILE} not found. Maintainer detection will be degraded.")
            return cls._instance
        
        with open(Config.MAINTAINERS_FILE, "r") as f:
            data = json.load(f)
        
        cls._maintainers = data.get("maintainers", [])
        
        # Build lookups
        for m in cls._maintainers:
            cls._id_to_record[m["id"]] = m
            for email in m.get("emails", []):
                cls._email_to_id[email.lower()] = m["id"]
        
        print(f"Loaded {len(cls._maintainers)} maintainers with {len(cls._email_to_id)} email aliases.")
        return cls._instance
    
    @classmethod
    def identify_maintainer(cls, email):
        """Returns maintainer record if email belongs to a known maintainer, else None."""
        mid = cls._email_to_id.get(email.lower() if email else "")
        return cls._id_to_record.get(mid) if mid else None

    @classmethod
    def is_maintainer(cls, email):
        """Check if email belongs to a known maintainer."""
        return (email.lower() if email else "") in cls._email_to_id
    
    @classmethod
    def get_all_maintainers(cls):
        """Returns list of all maintainer records."""
        return cls._maintainers

    @classmethod
    def get_status(cls, email):
        """Returns status (active, emeritus, historical) from whitelist if known."""
        m = cls.identify_maintainer(email)
        return m.get("status") if m else None

    @classmethod
    def count_whitelist(cls):
        """Total unique maintainers in the whitelist."""
        return len(cls._maintainers)

# --- Sponsor Lookup ---
class SponsorLookup:
    """
    Uses a curated list of known Bitcoin Core sponsors and their funded developers.
    Provides accurate corporate/sponsored classification vs. naive domain heuristics.
    """
    _instance = None
    _email_to_sponsor = {}  # email -> sponsor_id
    _sponsors = {}          # sponsor_id -> sponsor info
    _rules = {}             # classification_rules
    
    @classmethod
    def load(cls):
        if cls._instance is not None:
            return cls._instance
        
        cls._instance = cls()
        
        if not os.path.exists(Config.SPONSORS_FILE):
            print(f"Warning: {Config.SPONSORS_FILE} not found. Using fallback heuristics.")
            return cls._instance
        
        with open(Config.SPONSORS_FILE, "r") as f:
            data = json.load(f)
        
        # Load sponsors
        for s in data.get("sponsors", []):
            cls._sponsors[s["id"]] = s
        
        # Build email -> sponsor lookup from sponsored_developers
        for dev in data.get("sponsored_developers", []):
            sponsor_id = dev.get("sponsor_id")
            for email in dev.get("emails", []):
                cls._email_to_sponsor[email.lower()] = sponsor_id
        
        # Load fallback rules
        cls._rules = data.get("classification_rules", {})
        
        print(f"Loaded {len(cls._sponsors)} sponsors, {len(cls._email_to_sponsor)} sponsored developer emails.")
        return cls._instance
    
    @classmethod
    def classify(cls, email, canonical_name=None, enrich_company=None):
        """
        Classifies an author as 'Sponsored', 'Corporate', or 'Personal'.
        
        Priority:
        1. Known sponsored developer (email match) → 'Sponsored'
        2. Enriched company field exists → 'Corporate'
        3. Corporate domain (chaincode.com, etc.) → 'Corporate'
        4. Personal domain (gmail, etc.) → 'Personal'
        5. Unknown custom domain → 'Unknown' (conservative)
        """
        email_lower = email.lower() if email else ""
        domain = email_lower.split('@')[-1] if '@' in email_lower else ""
        
        # 1. Check if known sponsored developer
        if email_lower in cls._email_to_sponsor:
            return "Sponsored"
        
        # 2. Check enriched company field
        if enrich_company and isinstance(enrich_company, str) and len(enrich_company.strip()) > 1:
            return "Corporate"
        
        # 3. Check corporate domains from rules
        corporate_domains = cls._rules.get("corporate_domains", [])
        if domain in corporate_domains:
            return "Sponsored"  # Known Bitcoin sponsor domain
        
        # 4. Check academic domains (treat as Corporate/Institutional)
        academic_domains = cls._rules.get("academic_domains", [])
        if domain in academic_domains:
            return "Corporate"  # Academic institution
        
        # 5. Check personal domains
        personal_domains = cls._rules.get("personal_domains", [])
        if domain in personal_domains:
            return "Personal"
        
        # 6. Unknown domain - be conservative, mark as Personal
        # (Previously we assumed custom domain = Corporate, which caused false positives)
        return "Personal"
    
    @classmethod
    def get_sponsor_name(cls, email):
        """Returns sponsor name if email belongs to a sponsored developer."""
        email_lower = email.lower() if email else ""
        sponsor_id = cls._email_to_sponsor.get(email_lower)
        if sponsor_id and sponsor_id in cls._sponsors:
            return cls._sponsors[sponsor_id].get("name")
        return None


# --- Data Factory ---
class DataFactory:
    @staticmethod
    def load():
        print("Loading data...")
        if not os.path.exists(Config.COMMITS_FILE):
            raise FileNotFoundError(f"Missing {Config.COMMITS_FILE}")
            
        commits = pd.read_parquet(Config.COMMITS_FILE)
        
        # Safe load social
        try:
            social = pd.read_parquet(Config.SOCIAL_FILE)
            if 'date' in social.columns:
                 social['date'] = pd.to_datetime(social['date'])
        except:
            social = pd.DataFrame(columns=["date", "type"])
            
        return commits, social

    @staticmethod
    def normalize_data(commits):
        return clean.Consolidator.normalize(commits)


# --- Helpers ---
class CodeClassifier:
    @staticmethod
    def get_lang_name(ext):
        ext = ext.lower()
        # Non-Code / Excluded
        if ext in ['.ts', '.xlf']: return "Qt Translation"
        if ext in ['.ui', '.qrc']: return "Qt UI"
        if ext in ['.json', '.hex', '.raw', '.csv', '.xml']: return "Data"
        if ext in ['.cmake', '.m4', '.in', '.am', '.ac', '.mk', '.dockerfile', '.supp', '.patch', '.rs']: return "Build System"
        if ext in ['.yml', '.yaml', '.conf', '.cfg', '.ini', '.toml', '.lock']: return "Config"
        if ext in ['.png', '.svg', '.ico', '.jpg', '.bmp', '.xpm', '.icns', '.ttf']: return "Assets"
        if 'makefile' in ext: return "Build System"
        
        # Code
        mapping = {
            ".cpp": "C++", ".h": "C++", ".hpp": "C++", ".cc": "C++", ".c": "C", 
            ".py": "Python", ".pyi": "Python",
            ".md": "Markdown", ".txt": "Text", ".rst": "Documentation",
            ".sh": "Shell", ".bash": "Shell",
            ".java": "Java", ".go": "Go", ".js": "JavaScript", 
            ".html": "Web", ".css": "Web",
            ".s": "Assembly", ".asm": "Assembly"
        }
        return mapping.get(ext, "Unknown")

    @staticmethod
    def is_logic_code(lang_name):
        # We exclude Markdown/Text/Docs, Build, Data, Config, Assets, Translation, UI
        # Logic = The core programming languages
        allowed = {
            "C++", "C", "Python", "Shell", "Java", "Go", 
            "JavaScript", "TypeScript", "Web", "Assembly"
        }
        return lang_name in allowed

# --- Metric Generators ---
class MetricGenerators:
    
    @staticmethod
    def generate_vital_signs(commits, social):
        """
        KPI Cards: Contributors, Maintainers, Codebase Size, Stars/Forks
        """
        print("Generating Vital Signs...")
        
        # Load maintainer lookup
        MaintainerLookup.load()
        
        # 1. Unique Contributors (Canonical)
        unique_contributors = commits['canonical_id'].nunique()
        
        # 1.5 Total Commits (Simulated SHA count if available, else row count)
        total_commits = commits['hash'].nunique()

        # 2. Maintainers - Data Driven
        # Identification via whitelist, Activity via Commit Logs.
        
        # Tag maintainer actions:
        # A) Merge commits by known maintainers
        # B) Early commits (pre-2012) by historical maintainers (Satoshi era)
        commits['is_maintainer_action'] = False
        
        # Case A: Merge commits (Committer is the maintainer)
        merge_mask = (commits['is_merge'] == True)
        commits.loc[merge_mask, 'is_maintainer_action'] = commits.loc[merge_mask, 'committer_email'].apply(MaintainerLookup.is_maintainer)
        
        # Case B: Early Era handling (Pre-2012)
        # Using author_email as early devs often committed their own work directly.
        early_mask = (commits['date_utc'].dt.year < 2012)
        historical_ids = [m['id'] for m in MaintainerLookup.get_all_maintainers() if m.get('status') == 'historical']
        
        def is_historical_maintainer(email):
            m = MaintainerLookup.identify_maintainer(email)
            return m['id'] in historical_ids if m else False
            
        commits.loc[early_mask, 'is_maintainer_action'] = commits.loc[early_mask, 'is_maintainer_action'] | commits.loc[early_mask, 'author_email'].apply(is_historical_maintainer)

        # Calculate Statistics
        # We use the Whitelist for the Headline KPI counts to ensure recent appointees (Authorized) 
        # are included even if they haven't merged yet.
        unique_maintainers_total = MaintainerLookup.count_whitelist()
        unique_maintainers_active = len([m for m in MaintainerLookup.get_all_maintainers() if m.get("status") == "active"])
        
        # 3. Current Codebase Size (True Static LOC)
        # We prefer the actual scan data over historical net churn
        meta_path = os.path.join(Config.DATA_DIR, "category_metadata.json")
        net_lines = 0
        
        if os.path.exists(meta_path):
             try:
                 with open(meta_path, "r") as f:
                     meta_scan = json.load(f)
                     # Sum ONLY Logic Code
                     for cat_data in meta_scan.values():
                         for ext, stats in cat_data.get("languages", {}).items():
                             lang = CodeClassifier.get_lang_name(ext)
                             if CodeClassifier.is_logic_code(lang):
                                 net_lines += stats.get("loc", 0)
             except: 
                 pass
        
        # Fallback to Churn if Metadata missing
        if net_lines == 0:
            total_adds = commits['additions'].sum()
            total_dels = commits['deletions'].sum()
            net_lines = int(total_adds - total_dels)
        
        
        # 4. Social Stats (Stars, Forks, Watchers)
        # Try metadata first (Realtime totals), else fallback to history
        stars = 0
        forks = 0
        watchers = 0
        
        # Load metadata
        if os.path.exists(Config.METADATA_FILE):
             try:
                 with open(Config.METADATA_FILE, "r") as f:
                     meta = json.load(f)
                     stars = int(meta.get("stars", 0))
                     forks = int(meta.get("forks", 0))
                     watchers = int(meta.get("watchers", 0))
             except: pass
        
        # Fallback if 0 (and history exists)
        if stars == 0 and not social.empty:
            social_counts = social['type'].value_counts()
            stars = int(social_counts.get('star', 0))
            forks = int(social_counts.get('fork', 0))
            
        data = {
            "unique_contributors": int(unique_contributors),
            "unique_maintainers": int(unique_maintainers_active),
            "total_maintainers": int(unique_maintainers_total),
            "total_commits": int(total_commits),
            "current_codebase_size": net_lines,
            "total_stars": stars,
            "total_forks": forks,
            "total_watchers": watchers,
            "generated_at": datetime.now().strftime("%Y-%m-%d")
        }
        
        with open(Config.FILES["vital_signs"], "w") as f:
            json.dump(data, f)

    @staticmethod
    def generate_snapshots(commits):
        """
        Snapshots: Work (Commits), Volume (Net Lines), Stack (Language)
        AND Rich Category Details
        """
        print("Generating Snapshots...")
        
        # Deduplication is now handled by ingest.py (clean (hash, category) pairs).
        # We assume 'commits' has one row per (hash, category) where applicable.
        
        # Load Static Metadata (Files, LOC)
        meta_path = os.path.join(Config.DATA_DIR, "category_metadata.json")
        static_meta = {}
        if os.path.exists(meta_path):
             with open(meta_path, "r") as f:
                 static_meta = json.load(f)
        
        # --- 1. Category Rich Stats ---
        # Commits Total
        # Filter to 2025 for consistency with Snapshot label
        # Filter to end of previous month (handled globally now)
        commits_filtered = commits.copy()
        
        # Count unique hashes per category (A commit can count for multiple categories)
        # ingest now guarantees clean rows, so we rely on 'category' column
        cat_counts = commits_filtered.groupby('category')['hash'].nunique()
        
        # Commits Last 5 Years
        max_date = commits['date_utc'].max()
        cutoff_date = max_date - pd.Timedelta(days=5*365)
        recent_commits = commits[commits['date_utc'] > cutoff_date]
        cat_recent = recent_commits.groupby('category')['hash'].nunique()
        
        # Last Commit Year
        cat_last_year = commits.groupby('category')['year'].max()
        
        # Aggregating
        rich_data = []
        all_cats = set(cat_counts.index) | set(static_meta.keys())
        
        for cat in all_cats:
            # Static
            s = static_meta.get(cat, {})
            # files = s.get("files", 0) # OLD: Raw total
            # loc = s.get("loc", 0) # OLD: Raw total
            
            # NEW: Filtered Total (Logic Only)
            filtered_files = 0
            filtered_loc = 0
            langs = s.get("languages", {})
            
            for ext, l_stats in langs.items():
                lang_name = CodeClassifier.get_lang_name(ext)
                if CodeClassifier.is_logic_code(lang_name):
                    filtered_files += l_stats.get("files", 0)
                    filtered_loc += l_stats.get("loc", 0)
            
            # Dynamic
            total_c = int(cat_counts.get(cat, 0))
            recent_c = int(cat_recent.get(cat, 0))
            last_y = int(cat_last_year.get(cat, 0))
            
            # Language breakdown calculations
            rich_data.append({
                "name": cat,
                "files": filtered_files, # Updated
                "loc": filtered_loc,     # Updated
                "commits_total": total_c,
                "commits_last_5y": recent_c,
                "last_year": last_y,
                "languages": langs 
            })
            
        with open(os.path.join(Config.OUTPUT_DIR, "stats_category_details.json"), "w") as f:
            json.dump({"data": rich_data}, f)

        # --- Legacy Snapshot Support ---
        
        # 1. Work Distribution (Commits by Category)
        work_data = [{"name": d["name"], "value": d["commits_total"]} for d in rich_data if d["commits_total"] > 0]
        work_data.sort(key=lambda x: x["value"], reverse=True)
        with open(Config.FILES["snapshot_work"], "w") as f:
            json.dump({"data": work_data}, f)
            
        # 2. Code Volume (Net Lines by Category)
        vol_data = [{"name": d["name"], "value": d["loc"]} for d in rich_data if d["loc"] > 0]
        vol_data.sort(key=lambda x: x["value"], reverse=True)
        with open(Config.FILES["snapshot_volume"], "w") as f:
            json.dump({"data": vol_data}, f)
            
        # 3. Tech Stack (Global Languages)
        # Aggregate static language stats from all categories
        global_langs = {}
        for cat_stats in rich_data:
            for ext, metrics in cat_stats.get("languages", {}).items():
                if ext not in global_langs: global_langs[ext] = 0
                global_langs[ext] += metrics["loc"]
        
        final_stack = {}
        for ext, count in global_langs.items():
            name = CodeClassifier.get_lang_name(ext)
            if CodeClassifier.is_logic_code(name):
                final_stack[name] = final_stack.get(name, 0) + count

        # Use LOC to determine the "Main" languages for the whole site
        sorted_by_loc = sorted([{"name": k, "value": v} for k, v in final_stack.items() if v > 0], key=lambda x: x['value'], reverse=True)
        top_5_names = [x['name'] for x in sorted_by_loc[:5]]
        
        # Build snapshot_stack output
        stack_out = sorted_by_loc[:5]
        remaining = sorted_by_loc[5:]
        if remaining:
            other_val = sum(r['value'] for r in remaining)
            def fmt_loc(n):
                if n >= 1000: return f"{n/1000:.1f}k LOC".replace(".0k", "k")
                return f"{n} LOC"
            other_names = ", ".join([f"{r['name']} ({fmt_loc(r['value'])})" for r in remaining])
            stack_out.append({"name": "Other", "value": other_val, "details": other_names})
            
        with open(Config.FILES["snapshot_stack"], "w") as f:
            json.dump({"data": stack_out, "metadata": {"top_languages": top_5_names}}, f)

    @staticmethod
    def generate_category_evolution(commits):
        print("Generating Category Evolution...")
        
        # Annual Aggregation helper
        def get_evolution_data(df):
            df['year'] = df['date_utc'].dt.year
            pivot = df.groupby(['year', 'category'])['hash'].nunique().unstack(fill_value=0)
            
            # Auto-cutoff handles future years
            
            categories = pivot.columns.tolist()
            res = {
                "categories": categories,
                "xAxis": [str(y) for y in pivot.index.tolist()],
                "series": []
            }
            for cat in categories:
                res["series"].append({
                    "name": cat,
                    "type": "line",
                    "stack": "Total",
                    "areaStyle": {},
                    "emphasis": {"focus": "series"},
                    "data": pivot[cat].tolist()
                })
            return res

        # 1. Total Activity
        print("  Generating Total view...")
        data_total = get_evolution_data(commits.copy())
        
        # 2. Authored Work (No 'Merge')
        print("  Generating Authored view...")
        commits_authored = commits[commits['category'] != 'Merge'].copy()
        data_authored = get_evolution_data(commits_authored)
        
        output = {
            "total": data_total,
            "authored": data_authored
        }

        with open(Config.FILES["trend_category"], "w") as f:
            json.dump(output, f)
            
    @staticmethod
    def generate_contributor_growth(commits):
        print("Generating Contributor Growth...")
        # First seen year (Canonical)
        author_start = commits.groupby('canonical_id')['year'].min().reset_index().rename(columns={'year': 'start_year'})
        commits_merged = commits.merge(author_start, on='canonical_id')
        
        years = sorted(commits['year'].unique())

        new_counts = []
        vet_counts = []
        
        # Calculate per year (distinct people)
        for y in years:
            active = commits_merged[commits_merged['year'] == y]
            if active.empty:
                new_counts.append(0)
                vet_counts.append(0)
                continue
                
            # New if start_year == y
            # Vet if start_year < y
            
            # Get unique authors active this year and their start years
            # Distinct pairs (canonical_id, start_year)
            unique_active = active[['canonical_id', 'start_year']].drop_duplicates()
            
            n_new = len(unique_active[unique_active['start_year'] == y])
            n_vet = len(unique_active[unique_active['start_year'] < y])
            
            new_counts.append(n_new)
            vet_counts.append(n_vet)
            
        data = {
            "xAxis": [str(y) for y in years],
            "series": [
                {"name": "New Contributors", "type": "bar", "stack": "total", "data": new_counts},
                {"name": "Veterans", "type": "bar", "stack": "total", "data": vet_counts}
            ]
        }
        with open(Config.FILES["trend_growth"], "w") as f:
            json.dump(data, f)
            
    @staticmethod
    def generate_contributor_landscape(commits):
        """
        Interactive Bubble Chart Data
        """
        print("Generating Contributor Landscape...")
        
        if os.path.exists(Config.ENRICHED_FILE):
             enriched_df = pd.read_parquet(Config.ENRICHED_FILE)
             # Map canonical_id -> Enriched Data
             enrich_map = enriched_df.set_index('canonical_id').to_dict(orient='index')
        else:
             enrich_map = {}
        
        # Group by Author
        # metrics: start_year, end_year, active_years_count, total_commits, total_lines_added, primary_cat
        
        # 1. Basic Stats
        # We need a custom aggregation
        
        # Flatten categories per author for "Focus Areas"
        # We can't do this easily with just the primary_category column unless we trust it.
        # "Focus Areas: % breakdown of commits by Category"
        # Yes, we can aggregate the primary_category column for each author.
        
        # Group 1: Time & Volume
        g1 = commits.groupby('canonical_id').agg({
            'year': ['min', 'max', 'nunique'], # Start, End, Tenure (Active Years)
            'additions': 'sum', # Impact
            'hash': 'nunique', # Total Commits (Unique SHAs)
            'canonical_name': 'first' # Name
        })
        g1.columns = ['start_year', 'end_year', 'tenure', 'lines_added', 'total_commits', 'name']
        
        # Add Authored vs Merge breakdown
        g1['merge_commits'] = commits[commits['category'] == 'Merge'].groupby('canonical_id')['hash'].nunique()
        g1['authored_commits'] = commits[commits['category'] != 'Merge'].groupby('canonical_id')['hash'].nunique()
        g1['merge_commits'] = g1['merge_commits'].fillna(0).astype(int)
        g1['authored_commits'] = g1['authored_commits'].fillna(0).astype(int)
        
        # Rename 'commits' to 'total_commits' above and use it consistently
        df = g1.copy()
        
        # Group 2: Focus / Category
        # Most frequent category
        def get_top_cat(x):
            return x.value_counts().index[0] if not x.empty else "Unknown"
        
        # Logic for tooltips: % breakdown
        # Pivot: Author x Category -> Count
        cat_pivot = pd.crosstab(commits['canonical_id'], commits['category'])
        # Normalize to percentage
        cat_pct = cat_pivot.div(cat_pivot.sum(axis=1), axis=0).round(2)
        
        # Merge
        df = g1.join(cat_pct) # Adds category columns
        
        # --- NEW: Risk & Radar Profile ---
        # 1. Define Weights & Mapping
        RISK_WEIGHTS = {
            "Consensus (Domain Logic)": 50,
            "Cryptography (Primitives)": 50,
            "Core Libs": 50,
            "P2P Network (Infrastructure)": 40,
            "Database (Persistence)": 30,
            "Utilities (Shared Libs)": 30,
            "Node & RPC (App/Interface)": 10,
            "GUI (Presentation Layer)": 10,
            "Wallet (Client App)": 20,
            "Tests (QA)": 5,
            "Build & CI (DevOps)": 5,
            "Documentation": 1
        }
        
        RADAR_AXES = {
            "Security": ["Consensus (Domain Logic)", "Cryptography (Primitives)", "Core Libs"],
            "Resilience": ["P2P Network (Infrastructure)", "Database (Persistence)", "Utilities (Shared Libs)"],
            "Usability": ["GUI (Presentation Layer)", "Node & RPC (App/Interface)", "Wallet (Client App)"],
            "Quality": ["Tests (QA)", "Build & CI (DevOps)"],
            "Education": ["Documentation"]
        }
        
        # 2. Calculate Aggregated Risk Score per Author
        # SENIOR ENGINEER NOTE: Merge commits are excluded from RADAR_AXES below.
        # This keeps the "Impact Profile" focused on technical authorship rather than
        # administrative integration work.
        # Formula: Sum(Commit_Weight * Category_Risk)
        # We need to iterate carefully. We have 'commits_w' from Fractional Attribution step below.
        # But we haven't computed commits_w yet in this flow (it's in the next block). 
        # So I will move the Fractional Attribution block UP or compute it here.
        
        # --- NEW: Fractional Attribution (History by Year & Category) ---
        # 1. Calculate how many categories each commit touches (N)
        # Calculate N: distinct categories per hash
        commit_cats = commits.groupby('hash')['category'].nunique().reset_index().rename(columns={'category': 'n_cats'})
        # Merge N back to commits
        commits_w = commits.merge(commit_cats, on='hash')
        # Calculate Weight: 1/N
        commits_w['weight'] = 1.0 / commits_w['n_cats']
        
        # Calculate Risk Score
        # Map category to weight
        commits_w['risk_val'] = commits_w['category'].map(RISK_WEIGHTS).fillna(1)
        # Commit Risk Score = Weight * Risk_Val
        commits_w['commit_score'] = commits_w['weight'] * commits_w['risk_val']
        
        # Aggregates by ID
        risk_agg = commits_w.groupby('canonical_id')['commit_score'].sum()
        df['risk_score'] = df.index.map(risk_agg).fillna(0)
        
        # Calculate Radar Profile (Score per Axis)
        # We need Score per Category per ID
        cat_scores = commits_w.groupby(['canonical_id', 'category'])['commit_score'].sum().unstack(fill_value=0)
        
        radar_profiles = {}
        for cid, row in cat_scores.iterrows():
            profile = {}
            for axis, cats in RADAR_AXES.items():
                # Sum scores of categories belonging to this axis
                val = 0
                for c in cats:
                    if c in row: val += row[c]
                profile[axis] = round(val, 2)
            radar_profiles[cid] = profile

        # Now continue with History agg...
        # Now agg: Group by Canonical ID, Year, Category -> Sum of Weights
        hist = commits_w.groupby(['canonical_id', 'year', 'category'])['weight'].sum().unstack(fill_value=0)

        
        # hist is MultiIndex (canonical_id, year) with columns=Categories, values=Fractional Sums
        
        # Convert to dictionary map: { canonical_id: { year: { cat: count, ... }, ... } }
        history_map = {}
        
        hist_reset = hist.reset_index()
        cat_cols = [c for c in hist_reset.columns if c not in ['canonical_id', 'year']]
        
        for _, row in hist_reset.iterrows():
            cid = row['canonical_id']
            y = int(row['year'])
            
            if cid not in history_map: history_map[cid] = {}
            
            stats = {}
            for c in cat_cols:
                val = float(row[c])
                if val > 0:
                    stats[c] = round(val, 2) # Round to 2 decimals
            
            if stats:
                history_map[cid][y] = stats

        # --- ENRICHMENT METRICS ---
        total_project_commits = commits.shape[0]
        
        # Sort by total_commits for ranking
        df = df.sort_values('total_commits', ascending=False)
        
        # Calculate Ranks and Percentiles
        df['rank'] = df['total_commits'].rank(ascending=False, method='min')
        df['percentile'] = df['total_commits'].rank(pct=True) # 0 to 1, higher is better (more commits)
        
        def get_rank_label(row, total_authors):
            # Top 1% or Top 5 is "Legend"
            pct = row['percentile']
            comm = row['total_commits']
            
            if pct > 0.99: return "👑 The Core" # Top 1%
            if pct > 0.90: return "⭐ The Regulars" # Top 10%
            if pct > 0.75: return "⚒️ The Sustainers" # Top 25%
            if pct > 0.50: return "🔭 The Explorers" # Top 50%
            return "🧱 The Scouts" # Bottom 50%

        # Prepare maintainer lookup map for the loop
        maintainer_info_map = {}
        maintainer_ids = commits[commits['is_maintainer_action'] == True]['canonical_id'].unique()
        for cid in maintainer_ids:
             # Find any email associated with this CID that is in the whitelist
             cid_emails = commits[commits['canonical_id'] == cid]['committer_email'].unique()
             for email in cid_emails:
                  m = MaintainerLookup.identify_maintainer(email)
                  if m:
                       maintainer_info_map[cid] = m
                       break

        # Build JSON list
        output_list = []
        possible_cats = list(cat_pivot.columns)
        total_authors = len(df)
        
        for cid, row in df.iterrows():
             # Determine primary (max pct)
             # row[possible_cats] are the %s
             focus_map = row[possible_cats].to_dict()
             # Filter 0s
             focus_map = {k:v for k,v in focus_map.items() if v > 0}
             
             primary = max(focus_map, key=focus_map.get) if focus_map else "None"
             
             # Enrichment
             enrich_data = enrich_map.get(cid, {})
             login = enrich_data.get('login')
             company = enrich_data.get('company')
             location = enrich_data.get('location')
             
             # Metrics
             contribution_pct = (row['total_commits'] / total_project_commits) * 100
             rank_label = get_rank_label(row, total_authors)
             
             # Maintainer info
             m_info = maintainer_info_map.get(cid)
             
             output_list.append({
                 "id": str(cid), 
                 "name": row['name'],
                 "login": login,
                 "company": company,
                 "location": location,
                 "is_maintainer": m_info is not None,
                 "maintainer_status": m_info.get("status") if m_info else None,
                 "cohort_year": int(row['start_year']),
                 "last_active_year": int(row['end_year']),
                 "total_commits": int(row['total_commits']),
                 "authored_commits": int(row['authored_commits']),
                 "merge_commits": int(row['merge_commits']),
                 "impact": int(row['lines_added']),
                 "primary_category": primary,
                 "span": f"{int(row['start_year'])}-{int(row['end_year'])}",
                 "tenure": int(row['tenure']),
                 "focus_areas": focus_map,
                 "contribution_pct": round(contribution_pct, 4),
                 "rank_label": rank_label,
                 "percentile_raw": round(row['percentile'] * 100, 1), # e.g. 99.5
                 "history": history_map.get(cid, {}),
                 "risk_score": int(row['risk_score']),
                 "radar_profile": radar_profiles.get(cid, {})
             })
             
        with open(Config.FILES["contributors_rich"], "w") as f:
            json.dump(output_list, f)

    @staticmethod
    def generate_common_metrics(commits):
        # Maintainers, Heatmap, Weekend
        print("Generating Common Metrics...")
        
        # Load maintainer lookup
        MaintainerLookup.load()
        
        # Maintainers Trend - Data Driven
        # Count unique maintainers active in rolling windows
        
        dates = []
        counts = []
        
        # Ensure 'is_maintainer_action' is calculated (it should be from Vital Signs step, but just in case)
        if 'is_maintainer_action' not in commits.columns:
             # Basic detection if not already present
             commits['is_maintainer_action'] = (commits['is_merge'] == True) & (commits['committer_email'].apply(MaintainerLookup.is_maintainer))
        
        maintainer_commits = commits[commits['is_maintainer_action'] == True].copy()
        maintainer_commits['date'] = maintainer_commits['date_utc']
        maintainer_commits = maintainer_commits.set_index('date').sort_index()
        
        periods = pd.date_range(start=commits['date_utc'].min(), end=commits['date_utc'].max(), freq='M')
        
        for p in periods:
            # Rolling 12-month window
            start_date = p - pd.DateOffset(months=12)
            mask = (maintainer_commits.index > start_date) & (maintainer_commits.index <= p)
            n = maintainer_commits.loc[mask, 'canonical_id'].nunique()
            
            # Special case for very early years to avoid 0s if data is sparse
            if p.year < 2011 and n == 0:
                 # Be more lenient for Satoshi era
                 mask_lenient = (maintainer_commits.index <= p)
                 n = maintainer_commits.loc[mask_lenient, 'canonical_id'].nunique()

            dates.append(p.strftime("%Y-%m"))
            counts.append(int(n))
            
        with open(Config.FILES["trend_maintainers"], "w") as f:
            json.dump({"xAxis": dates, "series": [{"name": "Active Maintainers", "type": "line", "step": "start", "data": counts}]}, f)
            
        # Heatmap
        heatmap = []
        years = sorted(commits['year'].unique())
        for i, y in enumerate(years):
            subset = commits[commits['year'] == y]
            counts = subset.groupby('hour_utc').size()
            for h in range(24):
                val = int(counts.get(h, 0))
                if val > 0: heatmap.append([i, h, val])
        
        with open(Config.FILES["meta_heatmap"], "w") as f:
             json.dump({"years": [str(y) for y in years], "hours": [str(h) for h in range(24)], "data": heatmap}, f)
             
        # Weekend
        ratios = []
        for y in years:
            subset = commits[commits['year'] == y]
            tot = len(subset)
            if tot == 0: 
                ratios.append(0)
                continue
            wk = len(subset[subset['day_of_week'].isin([5,6])])
            ratios.append(round(wk/tot, 3))
            
        with open(Config.FILES["meta_weekend"], "w") as f:
            json.dump({"xAxis": [str(y) for y in years], "series": [{"name": "Weekend %", "type": "line", "data": ratios}]}, f)

    @staticmethod
    def generate_social(social):
        print("Generating Social...")
        if social.empty: return
        
        social = social.set_index('date').sort_index()
        stars = social[social['type'] == 'star'].resample('M').size().cumsum()
        forks = social[social['type'] == 'fork'].resample('M').size().cumsum()
        
        # Load Real Metadata Totals to project the curve
        total_stars = 0
        total_forks = 0
        if os.path.exists(Config.METADATA_FILE):
             try:
                 with open(Config.METADATA_FILE, "r") as f:
                     meta = json.load(f)
                     total_stars = int(meta.get("stars", 0))
                     total_forks = int(meta.get("forks", 0))
             except: pass
        
        # Extrapolate if history is truncated
        if not stars.empty:
            last_date = stars.index[-1]
            # Ensure timezone compatibility
            if last_date.tz is not None:
                now_date = pd.Timestamp.now(tz=last_date.tz)
            else:
                now_date = pd.Timestamp.now()
            
            # If our last history point is old (e.g. 2015) and we have a higher total, interpolate
            if last_date < (now_date - pd.Timedelta(days=365)) and total_stars > stars.iloc[-1]:
                print(f"Extrapolating Stars from {stars.index[-1].date()} ({stars.iloc[-1]}) to {now_date.date()} ({total_stars})")
                # Create a linear range
                # We add monthly points from last_date to now
                extra_dates = pd.date_range(start=last_date + pd.DateOffset(months=1), end=now_date, freq='M')
                if not extra_dates.empty:
                    # Linear interpolation logic
                    start_val = stars.iloc[-1]
                    end_val = total_stars
                    steps = len(extra_dates)
                    step_size = (end_val - start_val) / steps
                    
                    new_vals = [int(start_val + (i+1)*step_size) for i in range(steps)]
                    extra_series = pd.Series(new_vals, index=extra_dates)
                    stars = pd.concat([stars, extra_series])

        if not forks.empty:
            last_date = forks.index[-1]
            if last_date.tz is not None:
                now_date = pd.Timestamp.now(tz=last_date.tz)
            else:
                now_date = pd.Timestamp.now()
                
            if last_date < (now_date - pd.Timedelta(days=365)) and total_forks > forks.iloc[-1]:
                 print(f"Extrapolating Forks from {forks.index[-1].date()} ({forks.iloc[-1]}) to {now_date.date()} ({total_forks})")
                 extra_dates = pd.date_range(start=last_date + pd.DateOffset(months=1), end=now_date, freq='M')
                 if not extra_dates.empty:
                    start_val = forks.iloc[-1]
                    end_val = total_forks
                    steps = len(extra_dates)
                    step_size = (end_val - start_val) / steps
                    new_vals = [int(start_val + (i+1)*step_size) for i in range(steps)]
                    extra_series = pd.Series(new_vals, index=extra_dates)
                    forks = pd.concat([forks, extra_series])

        all_dates = sorted(list(set(stars.index.union(forks.index))))
        stars = stars.reindex(all_dates, method='ffill').fillna(0)
        forks = forks.reindex(all_dates, method='ffill').fillna(0)
        
        data = {
            "xAxis": [d.strftime("%Y-%m") for d in all_dates],
            "stars": stars.tolist(),
            "forks": forks.tolist()
        }
        with open(Config.FILES["trend_social"], "w") as f:
            json.dump(data, f)
            

    @staticmethod
    def generate_corporate_era(commits):
        """
        Generates TWO datasets:
        1. stats_corporate.json - Contributor commits by sponsorship status (% over time)
        2. stats_maintainer_independence.json - Maintainer diversity by sponsor
        """
        print("Generating Corporate Era & Maintainer Independence...")
        
        # Load lookups
        SponsorLookup.load()
        MaintainerLookup.load()
        
        # Load Enriched Data for Company/Email info
        enrich_map = {}
        if os.path.exists(Config.ENRICHED_FILE):
             enriched_df = pd.read_parquet(Config.ENRICHED_FILE)
             enrich_map = enriched_df.set_index('canonical_id').to_dict(orient='index')
        
        commits['year'] = commits['date_utc'].dt.year
        maintainer_commits = commits[commits['is_maintainer_action'] == True]

        # ========================================
        # PART 1: Contributor Commits by Sponsorship
        # ========================================
        
        # Build a map of cid -> classification
        authors = commits[['canonical_id', 'author_email']].drop_duplicates('canonical_id')
        author_types = {}
        
        for _, row in authors.iterrows():
            cid = row['canonical_id']
            email = row['author_email']
            enrich_company = enrich_map.get(cid, {}).get('company')
            classification = SponsorLookup.classify(email, enrich_company=enrich_company)
            
            # Map Sponsored -> Corporate for the chart (simpler 2-way split)
            if classification == "Sponsored":
                author_types[cid] = "Sponsored"
            else:
                author_types[cid] = classification  # "Corporate" or "Personal"
        
        # Apply to commits
        commits['author_type'] = commits['canonical_id'].map(author_types)
        
        # Aggregate by Year - COUNT COMMITS
        stats = commits.groupby(['year', 'author_type']).size().unstack(fill_value=0)
        
        # Normalize to %
        stats_pct = stats.div(stats.sum(axis=1), axis=0).round(4) * 100
        
        years = stats.index.tolist()
        
        # Combine Sponsored + Corporate into one "Sponsored/Corporate" bucket for cleaner chart
        sponsored_pct = stats_pct.get('Sponsored', pd.Series([0]*len(years), index=years)).fillna(0)
        corporate_pct = stats_pct.get('Corporate', pd.Series([0]*len(years), index=years)).fillna(0)
        personal_pct = stats_pct.get('Personal', pd.Series([0]*len(years), index=years)).fillna(0)
        
        combined_corp = (sponsored_pct + corporate_pct).tolist()
        
        data = {
            "title": "Contributor Activity by Sponsorship",
            "subtitle": "% of commits from sponsored vs independent developers",
            "xAxis": [str(y) for y in years],
            "series": [
                {"name": "Independent/Hobbyist", "type": "line", "stack": "Total", "areaStyle": {}, "data": personal_pct.tolist()},
                {"name": "Sponsored/Corporate", "type": "line", "stack": "Total", "areaStyle": {}, "data": combined_corp}
            ]
        }
        
        with open(Config.FILES["trend_corporate"], "w") as f:
            json.dump(data, f)
        
        # ========================================
        # PART 2: Maintainer Independence
        # ========================================
        
        maintainers = MaintainerLookup.get_all_maintainers()
        
        if not maintainers:
            print("  Skipping Maintainer Independence (no maintainer data)")
            return
        
        # Run Footprint Analysis (Modular approach)
        print("Running Maintainer Footprint Analysis...")
        try:
             footprints = footprint.run_footprint_analysis("raw_data/bitcoin", Config.MAINTAINERS_FILE, "data/core/maintainer_footprints.json")
        except Exception as e:
             print(f"  Warning: Footprint analysis failed: {e}")
             footprints = {}

        # For each maintainer, determine their sponsor (if any)
        maintainer_sponsors = []
        
        for m in maintainers:
            m_id = m.get("id")
            m_name = m.get("name", m_id)
            m_status = m.get("status", "unknown")
            emails = m.get("emails", [])
            
            # Check if any email matches a sponsor
            sponsor_name = None
            for email in emails:
                sponsor_name = SponsorLookup.get_sponsor_name(email)
                if sponsor_name:
                    break
            
            # If no sponsor found, check enrichment data
            if not sponsor_name:
                # Try to find canonical_id for this maintainer
                for email in emails:
                    email_lower = email.lower()
                    # Search commits for matching email
                    match = commits[commits['author_email'].str.lower() == email_lower]
                    if not match.empty:
                        cid = match.iloc[0]['canonical_id']
                        company = enrich_map.get(cid, {}).get('company')
                        if company and len(str(company).strip()) > 1:
                            sponsor_name = company
                            break

            # NEW: Calculate active years from commit data
            emails_lower = [e.lower() for e in emails]
            m_actions = maintainer_commits[maintainer_commits['committer_email'].str.lower().isin(emails_lower)]
            
            # For legacy maintainers, also check early author credit (Satoshi era)
            if m_status in ['historical', 'emeritus']:
                 m_early = commits[(commits['date_utc'].dt.year < 2012) & (commits['author_email'].str.lower().isin(emails_lower))]
                 m_actions = pd.concat([m_actions, m_early])
            
            # If still no actions found (e.g. build system maintainers like Cory who push directly/no merges)
            # check their general authorship across the whole history
            if m_actions.empty:
                m_actions = commits[commits['author_email'].str.lower().isin(emails_lower)]

            active_years = sorted(m_actions['year'].unique().tolist()) if not m_actions.empty else []
            merges_count = len(m_actions[m_actions['is_merge'] == True]) if not m_actions.empty else 0
            
            # For showing in the relay race even if no merges (recent appointees or emeritus)
            if not active_years and m_status in ['active', 'emeritus']:
                 active_years = [commits['date_utc'].max().year]
            
            maintainer_entry = {
                "id": m_id,
                "name": m_name,
                "status": m_status,
                "sponsor": sponsor_name if sponsor_name else "Independent",
                "active_years": [int(y) for y in active_years],
                "merges_count": merges_count,
                "merges_active": merges_count > 0 or m_status == 'active',
                "merge_authority": m.get("merge_authority", True) # Default to True for historicals
            }
            
            # Pass through new metadata
            if m.get("role"):
                maintainer_entry["role"] = m["role"]
            if m.get("gpg_fingerprint"):
                maintainer_entry["gpg_fingerprint"] = m["gpg_fingerprint"]
            if "merge_authority" in m:
                maintainer_entry["merge_authority"] = m["merge_authority"]
            if m.get("evidence"):
                maintainer_entry["evidence"] = m["evidence"]
            if m.get("segments"):
                maintainer_entry["segments"] = m["segments"]
            
            # Add Footprint (Top specializations)
            if m_id in footprints:
                maintainer_entry["footprint"] = footprints[m_id].get("top_areas", {})
                
            maintainer_sponsors.append(maintainer_entry)
        
        # Summary: Count by Sponsor
        # Use the whitelist status as primary truth for totals to match Dashboard KPI
        active_maintainers = [m for m in maintainer_sponsors if m["status"] == "active"]
        # For all-time, include everyone in the whitelist/processed list
        all_maintainers = [m for m in maintainer_sponsors]
        
        # Count sponsors for active maintainers
        sponsor_counts_active = {}
        for m in active_maintainers:
            s = m["sponsor"]
            sponsor_counts_active[s] = sponsor_counts_active.get(s, 0) + 1
        
        # Count sponsors for all maintainers (historical)
        sponsor_counts_all = {}
        for m in all_maintainers:
            s = m["sponsor"]
            sponsor_counts_all[s] = sponsor_counts_all.get(s, 0) + 1

        # Format for chart (horizontal bar or pie)
        independence_data = {
            "title": "Maintainer Independence",
            "subtitle": "Who funds the gatekeepers?",
            "active": {
                "total": len(active_maintainers),
                "by_sponsor": [{"name": k, "value": v} for k, v in sorted(sponsor_counts_active.items(), key=lambda x: -x[1])]
            },
            "all_time": {
                "total": len(all_maintainers),
                "by_sponsor": [{"name": k, "value": v} for k, v in sorted(sponsor_counts_all.items(), key=lambda x: -x[1])]
            },
            "maintainers": maintainer_sponsors  # Full list for detailed view
        }
        
        with open(os.path.join(Config.OUTPUT_DIR, "stats_maintainer_independence.json"), "w") as f:
            json.dump(independence_data, f)
        
        print(f"  Generated: {len(active_maintainers)} active maintainers, {len(all_maintainers)} total")

    @staticmethod
    def generate_geography(commits):
        print("Generating Geography...")
        
        # Load Enriched Data
        if not os.path.exists(Config.ENRICHED_FILE):
            print("  Skipping Geography (No enriched data)")
            return

        enriched_df = pd.read_parquet(Config.ENRICHED_FILE)
        
        # We want simple counts of contributors per location
        # Raw location strings are messy ("Berlin", "Berlin, DE", "Germany").
        # MVP: Just take the top clean strings or use timezone as fallback?
        
        def clean_loc(loc):
            if not loc: return None
            loc = str(loc).lower()
            if "united states" in loc or "usa" in loc or "u.s." in loc or "san francisco" in loc or "new york" in loc: return "USA"
            if "germany" in loc or "berlin" in loc: return "Germany"
            if "united kingdom" in loc or "london" in loc: return "UK"
            if "canada" in loc: return "Canada"
            if "china" in loc: return "China"
            if "france" in loc or "paris" in loc: return "France"
            if "australia" in loc: return "Australia"
            if "netherlands" in loc or "amsterdam" in loc: return "Netherlands"
            if "switzerland" in loc or "zurich" in loc: return "Switzerland"
            if "japan" in loc or "tokyo" in loc: return "Japan"
            return None 
            
        locations = enriched_df['location'].apply(clean_loc).dropna()
        counts = locations.value_counts().head(15).reset_index()
        counts.columns = ['name', 'value']
        
        with open(Config.FILES["meta_geography"], "w") as f:
             json.dump({"data": counts.to_dict(orient="records")}, f)

    @staticmethod
    def generate_codebase_stats(commits):
        print("Generating Codebase Stats...")

        # --- 1. Snapshots (from Metadata) ---
        meta_path = os.path.join(Config.DATA_DIR, "category_metadata.json")
        if not os.path.exists(meta_path):
             print("Missing metadata for snapshots.")
             return

        with open(meta_path, "r") as f:
             meta = json.load(f)

        # Aggregations
        files_by_lang = {}
        files_by_cat = []
        
        for cat, stats in meta.items():
            # Files by Cat
            files_by_cat.append({"name": cat, "value": stats.get("files", 0)})
            
            # Files by Lang
            for ext, lstats in stats.get("languages", {}).items():
                name = CodeClassifier.get_lang_name(ext)
                # Only include proper programming languages in the Tech Stack count
                if CodeClassifier.is_logic_code(name):
                    if name not in files_by_lang: files_by_lang[name] = 0
                    files_by_lang[name] += lstats.get("files", 0)

        # Load the "Main Languages" list from snapshot_stack for consistency
        main_langs = []
        try:
            with open(Config.FILES["snapshot_stack"], "r") as f:
                lres = json.load(f)
                main_langs = lres.get("metadata", {}).get("top_languages", [])
        except: pass

        # Files by Lang Snapshot
        langs_final = []
        remaining_langs = []
        for name, val in sorted(files_by_lang.items(), key=lambda x: x[1], reverse=True):
            if name in main_langs:
                langs_final.append({"name": name, "value": val})
            else:
                remaining_langs.append({"name": name, "value": val})
                
        # Sort langs_final to match main_langs order
        langs_final.sort(key=lambda x: main_langs.index(x['name']))
        
        if remaining_langs:
            other_val = sum(r['value'] for r in remaining_langs)
            other_names = ", ".join([f"{r['name']} ({r['value']} files)" for r in remaining_langs])
            langs_final.append({"name": "Other", "value": other_val, "details": other_names})

        snapshot_data = {
            "files_by_cat": sorted(files_by_cat, key=lambda x: x['value'], reverse=True),
            "files_by_lang": langs_final
        }
        
        with open(os.path.join(Config.OUTPUT_DIR, "stats_codebase_snapshots.json"), "w") as f:
            json.dump(snapshot_data, f)
            
        # --- 2. Evolution (Replay Commits) ---
        # We need to track LOC per language over time
        # commits have 'extensions_json' which is "{'.py': {'adds': 1, 'dels': 0}, ...}"
        
        # Sort by date
        df = commits.sort_values('date_utc')
        
        # State: { LangName: CurrentLOC }
        current_state = {}
        
        # We will sample end of every month
        df['month_period'] = df['date_utc'].dt.to_period('M')
        
        monthly_groups = df.groupby('month_period')
        
        history = [] # [{period: '2009-01', languages: {C++: 100, Python: 0...}}]
        
        all_langs = set()
        
        print(f"  Replaying {len(df)} commits for stack evolution...")
        
        for period, group in monthly_groups:
            # Apply all changes in this group
            for _, row in group.iterrows():
                try:
                    # Parse JSON string safety
                    # It relies on single quotes often from python dict string, 
                    # but ast.literal_eval is safer than json.loads for python string repr
                    ext_map = ast.literal_eval(row['extensions_json'])
                    
                    for ext, delta in ext_map.items():
                        lang = CodeClassifier.get_lang_name(ext)
                        
                        # Only track Logic Code evolution?
                        # If we track everything, Qt Translation clutters the chart.
                        # Yes, filter here too.
                        if CodeClassifier.is_logic_code(lang):
                            net = delta['adds'] - delta['dels']
                            current_state[lang] = current_state.get(lang, 0) + net
                            all_langs.add(lang)
                except:
                    continue
            
            # Snapshot state
            # Copy state? No, just store current values
            snapshot = {"period": str(period)}
            snapshot.update(current_state)
            history.append(snapshot)
            
        # Filter for End-of-Year (December Only)
        filtered_history = [
            h for h in history 
            if h['period'].endswith('-12')
        ]
            
        history = filtered_history
            
        # Format for ECharts (Stacked Area)
        # xAxis: periods
        # series: one per lang
        
        # SCALING LOGIC: Normalize to match Static Scan Total (Shared Logic, could be refactored)
        meta_path = os.path.join(Config.DATA_DIR, "category_metadata.json")
        target_loc = 0
        if os.path.exists(meta_path):
             try:
                 with open(meta_path, "r") as f:
                     meta = json.load(f)
                     for c in meta.values():
                         for ext, stats in c.get("languages", {}).items():
                             if CodeClassifier.is_logic_code(CodeClassifier.get_lang_name(ext)):
                                 target_loc += stats.get("loc", 0)
             except: pass
        
        current_hist_total = 0
        if history:
            current_hist_total = sum(v for k,v in history[-1].items() if isinstance(v, (int, float)))
            
        scale_factor = 1.0
        if target_loc > 0 and current_hist_total > 0:
            scale_factor = target_loc / current_hist_total
            print(f"  Scaling Stack Evolution by {scale_factor:.6f} (Hist: {current_hist_total} -> Target: {target_loc})")
            
        periods = [h['period'] for h in history]
        series = []
        
        # Filter noise to avoid cluttering the chart, but keep integrity via Other
        # Use the same main_langs for consistency
        main_langs = []
        try:
            with open(Config.FILES["snapshot_stack"], "r") as f:
                lres = json.load(f)
                main_langs = lres.get("metadata", {}).get("top_languages", [])
        except: pass

        final_vol = history[-1] if history else {}
        # If main_langs not available yet, fall back to top 5
        if not main_langs:
            sorted_langs = sorted(list(all_langs), key=lambda l: final_vol.get(l, 0), reverse=True)
            main_langs = sorted_langs[:5]
            
        top_langs = [l for l in main_langs if l in all_langs]
        other_langs = [l for l in all_langs if l not in top_langs]
        
        # Meta-information for Other series
        other_details = ", ".join(other_langs) if other_langs else ""
        
        # Build Series
        for lang in top_langs:
            data_points = [max(0, h.get(lang, 0)) * scale_factor for h in history] # Clamp and Scale
            series.append({
                "name": lang,
                "type": "line",
                "stack": "Total",
                "areaStyle": {},
                "symbol": "none",
                "data": data_points
            })
            
        # Load static scan for final period filtering
        static_langs = set()
        try:
            with open(Config.FILES["snapshot_stack"], "r") as f:
                sres = json.load(f)
                static_langs = {d['name'] for d in sres['data']}
                # Also include names from details if other
                for d in sres['data']:
                    if d['name'] == 'Other' and 'details' in d:
                        # Extract names from "Name (Count), Name (Count)"
                        import re
                        details_names = re.findall(r'([^,(\s]+) \(', d['details'])
                        static_langs.update(details_names)
        except: pass

        # Other
        if other_langs:
            other_data = []
            detailed_history = []
            for i, h in enumerate(history):
                # For later periods (last 3 years), ensure we only show what's actually in the static scan
                is_recent = (len(history) - i) <= 3 
                active_others = [l for l in other_langs if h.get(l, 0) > 0]
                if is_recent and static_langs:
                    active_others = [l for l in active_others if l in static_langs]
                
                val = sum(h.get(l, 0) for l in other_langs)
                other_data.append(max(0, val) * scale_factor)
                detailed_history.append(", ".join(active_others))
            
            series.append({
                "name": "Other",
                "type": "line",
                "stack": "Total",
                "areaStyle": {},
                "symbol": "none",
                "color": "#444",
                "data": other_data,
                "details": detailed_history
            })
            
        with open(os.path.join(Config.OUTPUT_DIR, "stats_stack_evolution.json"), "w") as f:
            json.dump({"xAxis": periods, "series": series}, f)

    @staticmethod
    def generate_category_history(commits):
        print("  Replaying commits for Category History...")
        df = commits.sort_values('date_utc')
        
        # We need to replay history to get "Lines of Code at Point in Time"
        # We use 'category' as the approximation for the whole commit
        
        # Group by Period (Month)
        # Drop timezone for period conversion to silence warning
        df['month_period'] = df['date_utc'].dt.tz_convert(None).dt.to_period('M')
        
        # Pre-group adds/dels by month/cat for speed
        df['net'] = df['additions'] - df['deletions']
        grouped = df.groupby(['month_period', 'category'])['net'].sum()
        
        min_date = df['date_utc'].min().replace(day=1)
        # Cap at end of 2025
        limit_date = commits['date_utc'].max()
        daterange = pd.period_range(min_date, limit_date, freq='M')
        
        history = []
        current_state = {} # cat -> loc
        all_cats = set()
        
        for period in daterange:
            # Update state with changes in this period
            if period in grouped.index.get_level_values(0):
                # Get changes for this month
                changes = grouped.loc[period]
                for cat, net_change in changes.items():
                    current_state[cat] = current_state.get(cat, 0) + net_change
                    all_cats.add(cat)
            
            # Filter: End of Year (December) Only
            if period.month == 12:
                snapshot = {"period": str(period)}
                snapshot.update(current_state)
                history.append(snapshot)

        # SCALING LOGIC: Normalize to match Static Scan Total
        meta_path = os.path.join(Config.DATA_DIR, "category_metadata.json")
        target_loc = 0
        if os.path.exists(meta_path):
             try:
                 with open(meta_path, "r") as f:
                     meta = json.load(f)
                     for c in meta.values():
                         for ext, stats in c.get("languages", {}).items():
                             if CodeClassifier.is_logic_code(CodeClassifier.get_lang_name(ext)):
                                 target_loc += stats.get("loc", 0)
             except: pass
        
        current_hist_total = 0
        if history:
            # Sum all numeric values in the last snapshot
            current_hist_total = sum(v for k,v in history[-1].items() if isinstance(v, (int, float)))
            
        scale_factor = 1.0
        if target_loc > 0 and current_hist_total > 0:
            scale_factor = target_loc / current_hist_total
            print(f"  Scaling Category History by {scale_factor:.6f} (Hist: {current_hist_total} -> Target: {target_loc})")

        # Format for ECharts
        periods = [h['period'] for h in history]
        series = []
        
        # Sort cats by final volume
        final_vol = history[-1] if history else {}
        sorted_cats = sorted(list(all_cats), key=lambda c: final_vol.get(c, 0), reverse=True)
        
        # Take All Categories (except Merge)
        for cat in sorted_cats:
            if cat == "Merge": continue
            
            # Apply Scale Factor here
            data_points = [max(0, h.get(cat, 0)) * scale_factor for h in history]
            
            series.append({
                "name": cat,
                "type": "line",
                "stack": "Total",
                "areaStyle": {},
                "symbol": "none",
                "data": data_points
            })
            
        with open(os.path.join(Config.OUTPUT_DIR, "stats_category_history.json"), "w") as f:
            json.dump({"xAxis": periods, "series": series}, f)

    @staticmethod
    def generate_churn_metrics(commits):
        print("Generating Churn Metrics...")
        commits_unique = commits[['hash', 'date_utc', 'commit_total_adds', 'commit_total_dels']].drop_duplicates()
        weekly = commits_unique.set_index('date_utc').resample('W').agg({
            'commit_total_adds': 'sum',
            'commit_total_dels': 'sum',
            'hash': 'count'
        }).reset_index()
        
        weekly.columns = ['date', 'additions', 'deletions', 'commit_count']
        weekly['net_change'] = weekly['additions'] - weekly['deletions']
        weekly['churn'] = weekly['additions'] + weekly['deletions']
        
        chart_data = {
            "dates": weekly['date'].dt.strftime('%Y-%m-%d').tolist(),
            "net_change": [int(x) for x in weekly['net_change']],
            "churn": [int(x) for x in weekly['churn']],
            "commit_count": [int(x) for x in weekly['commit_count']]
        }
        
        with open(os.path.join(Config.OUTPUT_DIR, "stats_churn.json"), 'w') as f:
            json.dump(chart_data, f)

    @staticmethod
    def generate_retention_metrics(commits):
        print("Generating Retention Metrics...")
        # A "Regular" is someone with 3+ commits in a single year
        yearly_activity = commits.groupby(['year', 'canonical_id']).size().reset_index(name='commit_count')
        regulars = yearly_activity[yearly_activity['commit_count'] >= 3]
        
        # Arrival Years for each person (First commit ever)
        arrival_years = commits.groupby('canonical_id')['year'].min().reset_index(name='first_year')
        
        years = sorted([int(y) for y in commits['year'].unique()])
        focus_years = [y for y in years if y >= 2018]
        
        # Strategy A: Workforce Retention (Active regulars of that year, regardless of join date)
        workforce_data = []
        for cohort_year in focus_years:
            cohort_ids = set(regulars[regulars['year'] == cohort_year]['canonical_id'])
            if not cohort_ids: continue
                
            counts = []
            for check_year in focus_years:
                if check_year < cohort_year:
                    counts.append(None)
                    continue
                active_now = set(yearly_activity[yearly_activity['year'] == check_year]['canonical_id'])
                still_active = cohort_ids.intersection(active_now)
                counts.append(int(len(still_active)))
            workforce_data.append({"cohort_year": int(cohort_year), "starting_size": int(len(cohort_ids)), "counts": counts})

        # Strategy B: Arrival Loyalty (People who JOINED and were regulars in their first year)
        loyalty_data = []
        for cohort_year in focus_years:
            # People who joined in this year
            joined_in_year = set(arrival_years[arrival_years['first_year'] == cohort_year]['canonical_id'])
            # People who were regulars in this year
            regulars_in_year = set(regulars[regulars['year'] == cohort_year]['canonical_id'])
            # Intersection: The "Incoming Class"
            cohort_ids = joined_in_year.intersection(regulars_in_year)
            
            if not cohort_ids: continue
                
            counts = []
            for check_year in focus_years:
                if check_year < cohort_year:
                    counts.append(None)
                    continue
                active_now = set(yearly_activity[yearly_activity['year'] == check_year]['canonical_id'])
                still_active = cohort_ids.intersection(active_now)
                counts.append(int(len(still_active)))
            loyalty_data.append({"cohort_year": int(cohort_year), "starting_size": int(len(cohort_ids)), "counts": counts})
            
        with open(os.path.join(Config.OUTPUT_DIR, "stats_retention.json"), 'w') as f:
            json.dump({
                "xAxis": [str(y) for y in focus_years], 
                "workforce": workforce_data,
                "loyalty": loyalty_data
            }, f)

    @staticmethod
    def generate_reviewer_metrics():
        print("Generating Reviewer Metrics...")
        REVIEWS_FILE = "data/reviews.parquet"
        ALIASES_FILE = "data/aliases_lookup.json"
        
        if not os.path.exists(REVIEWS_FILE): return

        df = pd.read_parquet(REVIEWS_FILE)
        
        # Identity Resolution
        lookup = {}
        if os.path.exists(ALIASES_FILE):
            with open(ALIASES_FILE, 'r') as f:
                als = json.load(f).get("aliases", [])
                for e in als:
                    name = e["canonical_name"]
                    for a in e.get("aliases", []): lookup[a.lower()] = name
                    for m in e.get("emails", []): lookup[m.lower()] = name
                    lookup[name.lower()] = name

        def canonicalize(name, email):
            if email and email.lower() in lookup: return lookup[email.lower()]
            if name and name.lower() in lookup: return lookup[name.lower()]
            return name

        df = df[df['reviewer_name'].notna() | df['reviewer_email'].notna()]
        df['canonical_name'] = df.apply(lambda row: canonicalize(row['reviewer_name'], row['reviewer_email']), axis=1)
        
        # Whitelist Filter (Only known contributors)
        if os.path.exists("data/contributors_enriched.parquet"):
            c_df = pd.read_parquet("data/contributors_enriched.parquet")
            whitelist = set(c_df['name'].str.lower().tolist())
            df = df[df['canonical_name'].str.lower().isin(whitelist)]

        # Scoring
        exclude_types = ["CO-AUTHORED-BY", "TRAILER"]
        df = df[~df['review_type'].str.upper().isin(exclude_types)]
        
        SCORES = {"ACK": 1.0, "TESTED ACK": 1.5, "TACK": 1.5, "UTACK": 0.5, "NACK": 0.2, "CONCEPT ACK": 0.8, "REVIEWED-BY": 1.2, "TESTED-BY": 1.3, "ACKED-BY": 1.1}
        df['score'] = df['review_type'].str.upper().map(SCORES).fillna(1.0)
        
        summary = df.groupby('canonical_name').agg({'score': 'sum', 'commit_hash': 'nunique', 'review_type': 'count'}).rename(columns={'commit_hash': 'unique_commits', 'review_type': 'total_reviews'})
        top_n = summary.sort_values('score', ascending=False).head(50).reset_index()
        
        output = [{"name": r['canonical_name'], "score": round(float(r['score']), 1), "commits": int(r['unique_commits']), "reviews": int(r['total_reviews'])} for _, r in top_n.iterrows()]
        
        with open(os.path.join(Config.OUTPUT_DIR, "stats_reviewers.json"), 'w') as f:
            json.dump(output, f)

    @staticmethod
    def generate_engagement_tiers(commits):
        print("Generating Engagement Tiers...")
        
        # We need two views: Total Activity vs Authored Work
        
        def calculate_tiers(counts_series):
            # Sort descending
            counts = counts_series.sort_values(ascending=False)
            total_vol = counts.sum()
            n_contributors = len(counts)
            
            if n_contributors == 0: return []
            
            # Thresholds
            i1 = int(np.ceil(n_contributors * 0.01))
            i20 = int(np.ceil(n_contributors * 0.20))
            
            # Slices
            g1 = counts.iloc[0:i1]
            g2 = counts.iloc[i1:i20]
            g3 = counts.iloc[i20:]
            
            # Output Structure
            # [Core, Regulars, Prospects]
            return [
                {
                    "name": "👑 The Core (Top 1%)",
                    "value": int(g1.sum()),
                    "count": int(len(g1)),
                    "color_idx": 4 # GHIBLI_PALETTE[4]
                },
                {
                    "name": "⭐ The Contributors (Top 20%)",
                    "value": int(g2.sum()),
                    "count": int(len(g2)),
                    "color_idx": 5 # GHIBLI_PALETTE[5]
                },
                {
                    "name": "🌱 The Prospects (Bottom 80%)",
                    "value": int(g3.sum()),
                    "count": int(len(g3)),
                    "color_idx": 12 # GHIBLI_PALETTE[12]
                }
            ]

        # 1. Total Commits
        total_counts = commits.groupby('canonical_id')['hash'].nunique()
        tiers_total = calculate_tiers(total_counts)
        
        # 2. Authored Commits (Exclude Merge)
        authored_commits = commits[commits['category'] != 'Merge']
        authored_counts = authored_commits.groupby('canonical_id')['hash'].nunique()
        tiers_authored = calculate_tiers(authored_counts)
        
        output = {
            "total": tiers_total,
            "authored": tiers_authored
        }
        
        with open(os.path.join(Config.OUTPUT_DIR, "stats_engagement_tiers.json"), "w") as f:
            json.dump(output, f)

# --- Orchestrator ---
def main():
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    
    
    commits, social = DataFactory.load()
    
    # --- DYNAMIC CUTOFF LOGIC ---
    # Goal: Have data refreshed monthly. In Feb 2026, we should have data till end of Jan 2026.
    # Use timezone-aware comparison to avoid TypeError
    from datetime import timezone as dt_timezone
    now = datetime.now(dt_timezone.utc)
    # First day of current month (UTC)
    first_day_curr = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # End of previous month
    cutoff_date = first_day_curr - timedelta(seconds=1)
    
    # Filter commits
    commits['date_utc'] = pd.to_datetime(commits['date_utc'], utc=True)
    commits = commits[commits['date_utc'] <= cutoff_date]
    if not social.empty:
        social['date'] = pd.to_datetime(social['date'], utc=True)
        social = social[social['date'] <= cutoff_date]
    
    print(f"Filtering data to cutoff: {cutoff_date.strftime('%Y-%m-%d')}")
    
    # Normalize Identities
    commits = DataFactory.normalize_data(commits)
    
    # Run Generators
    MetricGenerators.generate_vital_signs(commits, social)
    MetricGenerators.generate_snapshots(commits)
    MetricGenerators.generate_contributor_landscape(commits)
    
    MetricGenerators.generate_category_evolution(commits)
    MetricGenerators.generate_contributor_growth(commits)
    MetricGenerators.generate_common_metrics(commits)
    MetricGenerators.generate_social(social)
    
    MetricGenerators.generate_corporate_era(commits)
    MetricGenerators.generate_geography(commits)
    MetricGenerators.generate_codebase_stats(commits)
    MetricGenerators.generate_category_history(commits) # New
    MetricGenerators.generate_engagement_tiers(commits) # New - Pyramid Toggle Support
    
    # Advanced Metrics
    MetricGenerators.generate_churn_metrics(commits)
    MetricGenerators.generate_retention_metrics(commits)
    MetricGenerators.generate_reviewer_metrics()
    
    print("All stats generated successfully.")

if __name__ == "__main__":
    main()
