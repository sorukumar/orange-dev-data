# Data Pipeline Reference

The `orange-dev-data` pipeline is an automated lifecycle for Bitcoin developer intelligence. It transforms raw repository history and social archives into structured "Intelligence Artifacts" using a three-stage manufacturing process.

---

## 1. Master Orchestrators
- **`scripts/rebuild_daily.py` (The Pulse):** Incremental sync of Git logs and social archives. Updates the Master Registry and generates basic KPI artifacts.
- **`scripts/rebuild_monthly.py` (The Deep Rebuild):** Full re-calculation of the network graph (PageRank), deep theme categorization (NLP), and multi-year expertise mapping.

---

## 2. Pipeline Stages

### Stage 1: Extraction (`sources/` -> `raw/`)
Raw data is extracted from mirrors into standardized `.parquet` staging files.
- **Git Logs**: `scripts/core/ingest.py` parses Bitcoin Core Git history into `core_commits.parquet`.
- **Standards**: `scripts/ingest_bips.py` parses `bitcoin/bips` into `bips.parquet`.
- **Social**: `scripts/ingest_mailing_list.py` and `scripts/ingest_delving.py` extract messages from archives into `social_mailing_list.parquet` and `social_delving.parquet`.

### Stage 2: Convergence (`raw/` -> `enriched/`)
The **Master Contributor Registry** (`metadata/identities.json`) is the core filter in this stage.
- **Merge**: `scripts/merge_data.py` combines disparate social sources into `social_combined.parquet`.
- **Identity Sync**: `scripts/core/enrich.py` resolves Git emails/names to canonical human identities, producing `core_contributors.parquet`.
- **Theme Analysis**: `scripts/categorize_threads.py` (Stage 3 in monthly) maps conversations to the technical taxonomy, producing `social_threads.parquet`.
- **Governance Enrichment**: `scripts/enrich_governance.py` calculates BIP maturity based on both Git revisions and social mentions, producing `bips_refined.parquet`.

### Stage 3: Intelligence Mapping
Advanced analytical scripts connect the dots across domains.
- **Structural Influence**: `scripts/influence_hubs.py` calculates the social graph and PageRank to find the primary "signal" voices in the community.
- **Expertise Mapping**: `scripts/map_expertise.py` links BIP authors to their codebase activity and social discussions to verify domain authority.

### Stage 4: Artifact Generation (`enriched/` -> `output/`)
The heavy Parquet data is summarized into lightweight JSON for the web.
- **`scripts/sync_registry.py`**: Updates the Master Registry with current engagement metrics.
- **`scripts/generate_ui_artifacts.py`**: Flattens enriched data into the final `tracker` and `network` artifacts.

---

## 3. Data Integrity Principles
1.  **Symmetry**: Every raw Git extraction has a corresponding enriched Parquet.
2.  **Canonical IDs**: No script uses "raw" names; all analytics are performed against the IDs defined in `metadata/identities.json`.
3.  **Tiered Storage**: `raw/` data is "the past," `enriched/` data is "the present," and `output/` is "the presentation."
