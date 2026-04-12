# Directory Structure

The `orange-dev-data` repository is explicitly structured to strictly separate ingestion logic, raw data sources, intelligence mappings (lookups), and generated frontend artifacts.

Here is a comprehensive breakdown of the root folders and their contents:

---

## 1. `raw_data/`
This folder contains raw source repositories that act as the foundational layer of information. The automated pipeline will run `git pull` on these repositories first to ensure it's dealing with the latest data.
- **`bitcoin/`**: A bare clone of the `bitcoin/bitcoin` core repository. Used for extracting commit history.
- **`bips_repo/`**: A bare clone of the `bitcoin/bips` repository. Used for reading markdown specifications.
- **`delving/`**: *(Optional/Varies)* Staging area or cloned structure for backing up Delving Bitcoin state.

---

## 2. `scripts/`
This folder contains the Python execution layer. No data is stored here. For a detailed breakdown of what each script does, reference the `SCRIPT_REFERENCE.md`.
- **`core/`**: Scripts tightly coupled specifically to the `bitcoin/bitcoin` GitHub repository (e.g., commit ingestion, branch logic).
- **`legacy/`**: Deprecated scripts or one-off migration scripts kept for historical purposes.

---

## 3. `metadata/` (formerly `lookups/`)
The "brain" or "intelligence layer" of the project. These static JSON files are manually curated or partially machine-generated to handle resolving real-world chaos (like developers having 5 different email addresses). 
- *Files include:* `identities/identities.json`, `context/maintainers.json`, `context/sponsors.json`, `context/locations.json`.

---

## 4. `data/`
The persistent storage layer. This directory holds everything from intermediate binary files to the final `.json` artifacts deployed to the dashboards.

### `data/raw/`
The immediate output from the ingestion scripts. Usually stored as `.parquet` files. These files represent exact extracts before any heavy logic or identity merging is performed.

### `data/cache/`
To prevent excessive API calls to GitHub or Delving Bitcoin, temporary responses and state variables are saved here.
- `enrichment_cache.json`

### `data/core/`
This is the **primary deliverable** for the `orange-dev-tracker` frontend.
Contains lightweight JSON statistical summaries.
- `dashboard_vital_signs.json` (High level KPIs)
- `stats_engagement_tiers.json`
- `stats_churn.json`
- `contributors_rich.json` (Profiles for the individual tracking scatter plots)

### `data/governance/`
Artifacts specifically related to BIPs, mailing lists, and specialized thematic discussions.
- `bips.parquet`

### `data/network/` or `data/viz/`
Specialized topological artifacts designed to be loaded by complex networking visualizations (`orange-dev-network`). Contains structure defining nodes (developers) and links (interactions or co-authorships).
