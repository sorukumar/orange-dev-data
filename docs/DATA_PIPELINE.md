# Data Pipeline Reference

The data pipeline in `orange-dev-data` is an automated workflow designed to ingest raw data from multiple Bitcoin-related sources, process and enrich it with metadata, and output lightweight JSON artifacts for consumption by frontend dashboards (like `orange-dev-tracker` and `orange-dev-network`).

The pipeline is orchestrated by two primary scripts: `scripts/rebuild_daily.py` and `scripts/rebuild_monthly.py`.

## 1. Master Orchestrators: Daily vs Monthly
- **`rebuild_daily.py` (Fast/Automated):** Invoked by GitHub Actions, this script ensures data directories exist, syncs raw data sources via `git pull`, and sequentially triggers the *lightweight* Python scripts needed to instantly generate UI metrics, skipping heavy algorithms.
- **`rebuild_monthly.py` (Local/Comprehensive):** Executed locally, this script runs the exact same first half, but extends the pipeline to trigger heavy PageRank math, NLP categorizations, and legacy mailing list ingestion.

## 2. The Four Data Sources and Ingestion
The pipeline tracks four distinct data sources. During the ingestion phase, raw text or API responses are transformed into structured `.parquet` files for standardized processing.

1. **Bitcoin Core Repository (`raw_data/bitcoin`)**
   - **Ingested by:** `scripts/core/ingest.py`
   - **What it does:** Scrapes raw git commit history, lines of code changed, and authors.
   - **Output:** `data/core/commits.parquet`

2. **BIPs Repository (`raw_data/bips_repo`)**
   - **Ingested by:** `scripts/ingest_bips.py`
   - **What it does:** Parses the frontmatter and content of Bitcoin Improvement Proposals to track standards progression.
   - **Output:** `data/governance/bips.parquet`

3. **Bitcoin-dev Mailing List**
   - **Ingested by:** `scripts/ingest_mailing_list.py`
   - **What it does:** Incrementally parses thousands of threads from a public-inbox Git archive.
   - **Output:** `data/raw/social_mailing_list.parquet`

4. **Delving Bitcoin (Discourse Research Forum)**
   - **Ingested by:** `scripts/ingest_delving.py`
   - **What it does:** Queries the Delving Bitcoin API to pull deeply technical research threads and comments.
   - **Output:** `data/raw/social_delving.parquet`

## 3. Processing and Enrichment
Once the raw parquets are generated, a series of scripts cleans, merges, and enriches them:

- **Entity & Location Enrichment:** 
  - `scripts/core/enrich.py` and `scripts/enrich_governance.py` map raw git/social handles to canonical identities and pull geolocation attributes using data in the `metadata/` folder.
- **Metric Calculations:** 
  - `scripts/core/process.py` calculates engineering metrics like churn vs. net change, and cohort retention.
  - `scripts/core/social.py` analyzes GitHub connections and reviewer relationships.
- **Social & Governance Merging:** 
  - `scripts/merge_data.py` consolidates mailing list and Delving Bitcoin discussions into a unified social dataset.

## 4. Advanced Network Intelligence
To map the "thought-space" of Bitcoin R&D, structural metadata is generated:
- **`scripts/categorize_threads.py`:** Uses NLP/keywords to tag mailing list and Delving threads with actionable themes (e.g., L2, Mempool, P2P).
- **`scripts/influence_hubs.py`:** Calculates PageRank and constructs the social-technical influence graph linking developers who converse frequently.
- **`scripts/map_expertise.py`:** Aligns the categorized threads with the primary contributors to determine who the deep experts are in specific domains.

## 5. UI Artifact Generation
Finally, the frontend needs lightweight data it can draw instantly in D3 or ECharts.
- **`scripts/generate_ui_artifacts.py`:** Ingests all the heavy `.parquet` data and calculations, summarizes them, and outputs dozens of small `.json` files to `data/core/`, `data/governance/`, and `data/viz/`.

These `.json` artifacts are then deployed to GitHub Pages, where the stateless frontend repos (`orange-dev-tracker` / `orange-dev-network`) perform simple `fetch()` calls to display the dashboard.
