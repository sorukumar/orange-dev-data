# Script Reference

This document serves as an index of the Python scripts found in the `scripts/` directory.

### Orchestration
- **`rebuild_daily.py`**: The computationally lightweight daily master runner. Invoked automatically by GitHub Actions, it updates raw repositories and runs rapid, incremental extraction scripts (bypassing heavy NLP or graph topology scripts) to quickly generate dashboard KPIs.
- **`rebuild_monthly.py`**: The comprehensive master runner. Designed for local execution, it runs exactly the same as the daily script but includes heavy processing like full mailing list ingestion, NLP topic categorization, geographic tracking, and PageRank network topology calculations.
- **`legacy/rebuild.py`**: Deprecated full pipeline runner from before the daily/monthly execution split.

### Data Ingestion
*These scripts pull data from the outside world into local `.parquet` formats.*
- **`core/ingest.py`**: Reads local git history of `raw_data/bitcoin`, parsing commits and files changed.
- **`ingest_bips.py`**: Scrapes the markdown files in `raw_data/bips_repo` to create a structured database of Bitcoin Improvement Proposals (Authors, Status, Category).
- **`ingest_mailing_list.py`**: Downloads and parses historical email archives for the `bitcoin-dev` and `lightning-dev` mailing lists.
- **`ingest_delving.py`**: Hits the Delving Bitcoin Discourse API to retrieve modern research threads, authors, and replies.

### Processing & Metric Calculation
*These scripts calculate statistical features over the raw data.*
- **`core/process.py`**: Main metric generator for GitHub data—calculates churn, retention, cohort survival, and codebase evolution.
- **`core/social.py`**: Focuses on the GitHub pull-request reviewer/author relationships.
- **`extract_reviewers.py`**: Calculates the "Review Score" for developers based on ACKs and comments on GitHub.
- **`process_social.py` / `process_governance.py`**: Analyzes the time-to-consensus or interaction delays for BIPs and governance threads.

### Enrichment & Mapping
*These scripts tie lookups and API metadata to the intermediate data.*
- **`core/enrich.py`**: Pulls GitHub contributor profiles (Locations, Bios) and PR labels, aggressively utilizing `data/cache/enrichment_cache.json`.
- **`enrich_governance.py`**: Ties mailing list and Delving Bitcoin handles to the canonical identities provided in `identity_mappings.json`.
- **`categorize_threads.py`**: Applies keyword/NLP matching to tag topics (Mempool, L2) to raw forum and mailing list text.
- **`map_expertise.py`**: Connects the categorized thread topics back to the specific developers heavily involved in them.

### Network Topology
*Specialized algorithm scripts.*
- **`influence_hubs.py`**: Uses PageRank and graph theory to calculate the node size and edge weight for developers based on who responds to whose code/emails. This is critical for the `orange-dev-network` graph.

### Artifact Generation
*The final step in the pipeline.*
- **`generate_ui_artifacts.py`**: Takes the heavy pandas Dataframes/Parquet files and slices them into dozens of optimized `.json` formats explicitly formatted for the frontend dashboards.
- **`generate_regional_evolution.py`**: Specifically structures JSON payloads used by geographic/map visualizations on the frontend.
- **`generate_emerging_stats.py`**: Focused wrapper for isolating data trends of brand-new ecosystem entrants.

### Debug / Utils
Scripts like `debug_categories.py`, `debug_match.py`, `check_locations.py` are ad-hoc tools for human developers to interrogate the `lookups/` accuracy or test a specific data slice without running the entire pipeline.
