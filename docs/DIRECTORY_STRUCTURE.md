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

---

## 4. `data/`
The persistent storage layer for **internal processing**. This directory holds heavy data like Parquets and caches. Frontends should **never** point directly to this folder.

### `data/raw/`
Immediate output from ingestion scripts (Parquet).

### `data/core/`
Internal consolidated data (Parquets) used for advanced processing and metric calculation.

### `data/cache/`
Temporary responses and state variables (e.g., `enrichment_cache.json`).

---

## 5. `output/`
The **Public Deliverable Layer**. This folder contains lightweight JSON artifacts optimized for frontend applications.

### `output/tracker/`
Primary artifacts for the Core Dashboard (`orange-dev-tracker`).

### `output/network/`
Topological graph data for the influence visualization (`orange-dev-network`).
