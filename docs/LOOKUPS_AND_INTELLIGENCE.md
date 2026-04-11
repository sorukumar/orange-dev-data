# Lookups and Intelligence

The pipeline heavily relies on static JSON files stored in the `lookups/` directory. Because data from GitHub commits, mailing lists, and Delving Bitcoin APIs is notoriously chaotic and disconnected, the `lookups` directory serves as the definitive source of truth to stitch fragmented developer personas and metadata together.

## The Resolvers

### 1. `identity_mappings.json`
**Purpose:** Maps scattered aliases, emails, and handles across different platforms into a single canonical identity.
- **Why it's needed:** A developer might commit code as `Real Name <email@example.com>`, post on the mailing list as `cypherpunk99`, and review code as `real_name_dev`. If not resolved, the dashboard would overcount developers and fail to build accurate expertise graphs.
- **Usage:** Ingestion scripts run alias checks against this mapping dictionary before writing `.parquet` rows.

### 2. `maintainers_lookup.json`
**Purpose:** Defines official repository maintainers, including custom start/end dates for their tenure.
- **Why it's needed:** Maintainers act as gatekeepers rather than just prolific authors. Their activity needs distinct categorization (Review/Merge activity vs Authorship).
- **Usage:** Used by scripts like `scripts/core/process.py` to bucket commit authors and reviewers, surfacing "Maintainer Independence" and "Maintainer Workload" charts.

### 3. `identified_locations.json`
**Purpose:** Resolves arbitrary geographic strings into structured Continent/Country groupings.
- **Why it's needed:** GitHub's location field is free-text. Developers write "London", "UK", "Earth", or "Berlin".
- **Usage:** Used by the enrichment scripts to assign standard geospatial coordinates or macro-regions to contributors, powering mapping functionality and "Regional Evolution" metrics.

### 4. `sponsors_lookup.json` & `sponsors_evidence.json`
**Purpose:** Maps developers to funding entities or corporate sponsors (e.g., Chaincode, Brink, Blockstream, Spiral).
- **Why it's needed:** Helps track the corporate decentralization of the codebase.
- **Usage:** This mapping is manually curated via open-source investigation. The pipeline reads this to categorize the corporate footprint of commits.

## The Caches

While not manually curated, the pipeline generates dynamic intelligence files to preserve internet bandwidth and bypass API rate constraints.

### `enrichment_cache.json` / `enrichment_cache_remote.json`
**Purpose:** Stores responses from the GitHub API. 
- During `scripts/core/enrich.py`, the pipeline asks GitHub about PR sizes, labels, and developer profile information. By saving this here, future `rebuild.py` runs take seconds instead of hours and don't get IP-blocked by GitHub.

### `contributors_missing_location.json`
**Purpose:** An automated ledger. If the script cannot geo-locate a top developer automatically, it writes their name here.
- **Usage:** A human maintainer can periodically check this file, manually investigate the identity, and add the result to `identified_locations.json`.
