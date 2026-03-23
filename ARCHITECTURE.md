# Orange Dev Architecture: Data Engine & Frontend Linking

This document outlines the architectural relationship between the repositories in the Orange Dev project. Its goal is to provide context for future development and ensure the "separation of concerns" between data processing and visualization is maintained.

## 🏗️ System Overview

The project is split into a **Data Engine** and multiple **Stateless Frontends**.

1.  **`orange-dev-data` (The Engine)**: Centralized repository for all data ingestion, enrichment, and artifact generation. It is the "Single Source of Truth."
2.  **`orange-dev-tracker` / `orange-dev-network` (The Views)**: These repositories contain only the frontend logic (HTML/D3/ECharts). They are designed to be stateless, fetching processed JSON artifacts from the Data repo's GitHub Pages.
3.  **Lab Work Exceptions**: Experimental scripts and unvalidated data stay within the frontend repositories (e.g., `data/lab/` in the tracker) until they are ready to be integrated into the automated `orange-dev-data` pipeline.

---

## 📂 Data Directory Structure (`data/`)

| Folder | Purpose |
| :--- | :--- |
| `raw/` | Final source Parquet files (Commits, Social, BIPs) ready for aggregation. |
| `core/` | The "Heart" of the project. Contains all dashboard metrics and statistical summaries. |
| `viz/` | Specialized artifacts for complex visualizations (e.g., the Network Graph). |
| `governance/`| Data specifically related to BIPs, themes, and expertise mapping. |
| `cache/` | Persistent local caches to prevent redundant API calls and speed up rebuilds. |
| `raw_archives/`| Bare Git clones of source repositories used for local parsing. |

---

## 📊 Core Data Inventory (`data/core/`)

These files power the **Orange Dev Tracker** dashboards.

### 1. Dashboard & KPIs
*   **`dashboard_vital_signs.json`**: Top-level metrics (Commits, Stars, Total Contributors).
*   **`stats_engagement_tiers.json`**: Data for the "Contributor Pyramid" (Core vs Regular vs Casual).
*   **`stats_contributor_growth.json`**: Historical trend of new developers joining the ecosystem.

### 2. Contributor Deep-Dives
*   **`contributors_rich.json`**: Complete dataset for the **Contributor Galaxy** scatter plot.
*   **`stats_maintainers.json`**: Activity metrics specifically for official Bitcoin Core maintainers.
*   **`stats_maintainer_independence.json`**: Tracks the corporate/funding diversity of the maintainer set.
*   **`maintainer_footprints.json`**: Radar chart data showing focus areas (Security, P2P, etc) for maintainers.

### 3. Engineering Metrics
*   **`stats_churn.json`**: Churn Intensity vs Net Code change (Refactoring vs New Features).
*   **`stats_retention.json`**: Retention Heatmap and survival curves for developer cohorts.
*   **`reviewers_summary.json`**: List of top PR reviewers and their "Review Score".
*   **`stats_code_volume.json`**: Historical growth of the codebase size (LOC).

### 4. Technical Domains & Evolution
*   **`stats_category_evolution.json`**: Domain focus trends (e.g., Mempool growth vs L2 growth).
*   **`stats_category_details.json`**: Metadata for technical area groupings.
*   **`stats_tech_stack.json`**: Programming language distribution and evolution over time.

### 5. Logistics & Geography
*   **`stats_heatmap.json`**: Contribution density by hour and day of week (Punchcard).
*   **`stats_weekend.json`**: Analysis of professional vs hobbyist activity (Weekend %).
*   **`stats_regional_evolution.json`**: The global shift of Bitcoin R&D across continents.

---

## 💾 Cache Inventory (`data/cache/`)

Used only by the processing scripts to maintain state and preserve API rate limits.

*   **`enrichment_cache.json`**: The primary cache for GitHub PR metadata (labels, associations).
*   **`enrichment_cache_remote.json`**: Results from remote API lookups for contributor profiles.
*   **`identified_locations.json`**: Maps location strings (e.g., "SF, CA") to standard region/country names.
*   **`sponsors_lookup.json`**: Working cache of entity-to-developer funding relationships.
*   **`contributors_missing_location.json`**: A tracking list of developers who need manual geolocation research.

---

## ⚙️ Processing Scripts (`scripts/`)

| Script | Purpose |
| :--- | :--- |
| `rebuild.py` | Runs the entire pipeline from end-to-end. |
| `scripts/core/process.py` | The main engine for calculating all `data/core/` metrics. |
| `scripts/influence_hubs.py` | Generates the social-technical **Technical Influence Graph**. |
| `scripts/categorize_threads.py`| Tags mailing list and Delving Bitcoin threads with technical themes. |
| `scripts/generate_ui_artifacts.py`| Packages massive Parquet files into lightweight JSON for the UI. |
