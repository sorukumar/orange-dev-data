# Orange Dev Architecture: Data Engine & Frontend Linking

This document outlines the architectural relationship between the repositories in the Orange Dev project. Its goal is to provide context for future development and ensure the "separation of concerns" between data processing and visualization is maintained.

## 🏗️ System Overview

The project is split into a **Data Engine** and multiple **Stateless Frontends**.

1.  **`orange-dev-data` (The Engine)**: Centralized repository for all data ingestion, enrichment, and artifact generation. It is the "Single Source of Truth."
2.  **`orange-dev-tracker` / `orange-dev-network` (The Views)**: These repositories contain only the frontend logic (HTML/D3/ECharts). They are designed to be stateless, fetching processed JSON artifacts from the Data repo's GitHub Pages.
3.  **Lab Work Exceptions**: Experimental scripts and unvalidated data stay within the frontend repositories (e.g., `data/lab/` in the tracker) until they are ready to be integrated into the automated `orange-dev-data` pipeline.

---

## 📂 Repository Breakdown: `orange-dev-data`

### 1. ⚙️ Processing Scripts (`scripts/`)

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

### 2. 📊 Data Artifacts (`data/`)

These are the files fetched by the frontend applications.

| Path | Primary Consumer | Visualization Target |
| :--- | :--- | :--- |
| `data/viz/network_graph.json` | `orange-dev-network` | The D3 Force-Directed Graph |
| `data/core/dashboard_vital_signs.json` | `orange-dev-tracker` | Main Dashboard KPIs (Commits, Maintainers) |
| `data/core/contributors_rich.json` | `orange-dev-tracker` | The Contributor Galaxy scatter plot |
| `data/core/stats_category_evolution.json` | `orange-dev-tracker` | Category Trends (Area Chart) |
| `data/core/stats_retention.json` | `orange-dev-tracker` | Workforce Retention (Cohort analysis) |
| `data/governance/bips_ui.json` | `orange-dev-tracker` | BIPs Governance Dashboard |
| `data/state.json` | Pipeline Scripts | Tracks shard progress (Internal use only) |

---

## 🔗 Linking Strategy

### Production Link
The frontend repositories use a `DATA_PATH_PREFIX` (defined in `utils.js` or directly in scripts) pointing to:
`https://sorukumar.github.io/orange-dev-data/`

### Local Development
When iterating on the data pipeline, the `data/` folder should be pushed to GitHub to update the dashboards globally. 

Once the numbers are validated, the ingestion logic is moved to `orange-dev-data/scripts` and the data is served from the central engine.

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

### How Lookups are Used
*   **Ingestion Phase**: `identity_mappings.json` is loaded first to ensure that when a developer posts on a mailing list and commits to GitHub, they are counted as the same person.
*   **Processing Phase**: Maintainer and Sponsor lookups are applied to categorize the *intent* and *independence* of the work.
*   **Enrichment Phase**: Location lookups are used to aggregate technical trends by global region.
