# Script Reference

This document serves as an index of the Python scripts found in the `scripts/` directory, organized by pipeline stage.

### Master Orchestrators
- **`rebuild_daily.py`**: Fast incremental runner for daily updates.
- **`rebuild_monthly.py`**: Comprehensive runner with heavy NLP/Graph math.

---

### 01_ingest/ (Stage 1: Extraction)
*Mirrors data from source repositories into the `data/raw/` staging area.*
- **`core.py`**: Extracts Bitcoin Core Git history and file metadata.
- **`bips.py`**: Parses the BIPs repository for specification tracking.
- **`delving.py`**: Pulls technical research threads from Delving Bitcoin.
- **`mailing_list.py`**: Parses the historical Bitcoin-dev mailing list archives.

---

### 02_process/ (Stage 2: Convergence)
*Refines raw data into the `data/enriched/` intelligence layer.*
- **`core.py`**: Calculates engineering metrics (Churn, Retention, Codebase Evolution).
- **`enrich_identity.py`**: Resolves diverse platform handles to canonical human identities.
- **`governance.py`**: Links BIPs to their social discussion and code impact.
- **`merge_social.py`**: Unifies mailing list and forum data into a single social corpus.
- **`categorize.py`**: Applies technical theme tags to conversations (NLP).
- **`reviews.py`**: Extracts reviewer signals and ACKs from commit messages.
- **`github_social.py`**: Tracks stars, forks, and social engagement on GitHub.

---

### 03_analyze/ (Stage 3: Advanced Intelligence)
*Cross-domain algorithms for deep ecosystem insights.*
- **`influence.py`**: Calculates the social-technical influence graph (PageRank).
- **`expertise.py`**: Bridges the gap between specification authors and code contributors.

---

### 04_deliver/ (Stage 4: Artifact Generation)
*Produces the final `output/` JSONs for the web.*
- **`registry.py`**: Syncs engagement metrics back into the Master Contributor Registry.
- **`ui_artifacts.py`**: Flattens enriched data into lightweight JSON for dashboards.
- **`regional_evolution.py`**: Formats geospatial data for the regional dashboard.

---

### Tools & Ops
- **`lab/`**: Directory for one-off experiments, debugging, and ad-hoc data analysis.
- **`maintenance/`**: Directory for migration scripts and manual data cleanup.
