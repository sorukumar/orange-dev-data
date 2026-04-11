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

## ⚙️ Processing Scripts (`scripts/`)

These scripts orchestrate the data pipeline. They transform raw source data (Git, API, Mail) into the JSON files consumed by the UI.

| Script | Purpose | Output / Visualization |
| :--- | :--- | :--- |
| `rebuild.py` | **Master Orchestrator**. Runs the entire pipeline from ingestion to UI artifact generation. | All Data |
| `scripts/core/ingest.py` | Extracts raw commit data from the Bitcoin Core Git repository. | `data/core/commits.parquet` |
| `scripts/core/process.py` | Calculates engineering metrics: Churn, Net Change, and Retention. | `stats_churn.json`, `stats_retention.json` |
| `scripts/ingest_mailing_list.py` | Parses 15+ years of Mailing List Git archives into structured data. | `social_mailing_list.parquet` |
| `scripts/ingest_delving.py` | Fetches the latest research threads from Delving Bitcoin via API. | `social_delving.parquet` |
| `scripts/categorize_threads.py`| Uses keywords and NLP to tag social threads with technical themes (Mempool, L2, etc). | `social_combined_categorized.parquet` |
| `scripts/influence_hubs.py` | Calculates PageRank influence and build the social-technical graph. | **Technical Influence Graph** (Network) |
| `scripts/generate_ui_artifacts.py`| Converts large Parquet files into lightweight, optimized JSON for the browser. | Dashboard KPI Cards |

---

## 🔍 Lookup Tables & Data Intelligence (`lookups/`)

The pipeline relies on curated lookup tables to resolve identities and enrich raw data. These are the "Intelligence" layer of the system.

| File | Purpose | Creation Method | Usage |
| :--- | :--- | :--- | :--- |
| `identity_mappings.json` | **Identity Resolution**. Maps aliases, emails, and handles to a Canonical Name. | Manual research + LLM deduplication. | Unifies IDs in all ingestion scripts. |
| `maintainers_lookup.json`| Defines official maintainer roles and tenure. | Researching repo docs and GitHub permissions. | Identifies "Maintenance" vs "Authored" work. |
| `identified_locations.json`| Maps contributor profiles to geographic regions (Country/Continent). | GitHub API scraping + manual geo-tagging. | Powers geographic evolution charts. |
| `sponsors_lookup.json` | Maps developers to funding entities (Blockstream, Chaincode, etc). | Deep internet research (Linkedin, bios). | Used for Corporate Independence analysis. |
| `enrichment_cache.json` | Caches GitHub API responses (PRs, reviews, labels). | Automated via `scripts/core/enrich.py`. | Prevents API rate-limiting during rebuilds. |

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
