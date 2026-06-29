# Sponsorship Data Maintenance Guide

This document outlines the Hybrid Architecture for updating and expanding sponsorship data in the Orange Dev Tracker.

## Methodology

We use a **Hybrid Architecture** that isolates manual curation from automated data scraping. This guarantees that human curations are never overwritten by automated scripts, while still capturing real-time grant updates.

### 1. Automated Ingestion (Phase 1)
We automatically fetch machine-readable grant data directly from the open-source repositories of major sponsors.

- **Sources**:
  - **OpenSats**: Parsed from `data/sources/opensats/data/projects/*.mdx`.
  - **Brink**: Parsed from `data/sources/brink/_data/team.yml`.
- **Action**: Handled automatically in Phase 1 via `scripts/01_ingest/automated_sponsors.py`.
- **Output**: Generates `data/raw/automated_grants.json`. This extracts developer names, GitHub profiles, grant start dates, and project names.

### 2. Manual Curation (Phase 1)
For sponsors that do not provide machine-readable automated lists (e.g., Chaincode, Spiral, Btrust), or to manually adjust data, we maintain a strict manual registry.

- **File**: `metadata/sponsors.json`.
- **Role**: This is the manual override layer. It allows us to manually log grants or add custom `notes` to automated grants without fear of being overwritten.

### 3. Convergence & Transformation (Phase 2)
We merge the automated ingestion with the manual curation to create a single, unified source of truth.

- **Action**: Handled automatically in Phase 2 via `scripts/02_process/merge_sponsors.py`.
- **Output**: Generates `data/enriched/sponsors_merged.json`.
- **Logic**:
  1. Loads `metadata/sponsors.json` as the base.
  2. Loads `data/raw/automated_grants.json`.
  3. Intelligently merges them by developer `github` or `canonical_name`, avoiding duplicate grants. 
  4. If a manual entry exists for an automated grant, the manual `notes` are preserved, and the rich metadata (`start_date` and `project_name`) from the automation is safely injected.

### 4. Integration
The final unified file (`data/enriched/sponsors_merged.json`) is the *only* file read by downstream pipeline scripts (`unify_contributors.py`, `badges.py`, `maintainers.py`, etc.).

## Technical Rebuild
To run the complete pipeline and ensure all UI elements and Parquet files are updated with new sponsorship data, simply run:
```bash
python3 scripts/rebuild_daily.py
```
