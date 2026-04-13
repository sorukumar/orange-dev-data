# Lookups and Intelligence

The pipeline's predictive power and accuracy come from the **Intelligence Layer** stored in `metadata/`. This directory serves as the "Sovereign Context" of the project—it resolves the chaos of raw GitHub and forum handles into a human-auditable registry of the Bitcoin ecosystem.

---

## 🏗️ The Sovereign Context (`metadata/`)

### 1. `identities.json` (The Canonical Resolver)
**Purpose**: Maps scattered aliases, emails, and platform-specific handles to a single, unique **Canonical Name**.
- **The Problem**: A contributor might be `Pieter Wuille` in Git, `sipa` on GitHub, and `pieter.wuille@gmail.com` on the mailing list.
- **The Solution**: Every pipeline script uses this file to resolve raw IDs before any analytics are performed. This prevents data fragmentation and ensures PageRank scores are consolidated.

### 2. `contributors.json` (The Master Registry)
**Purpose**: The central database of all ~2,300 tracked humans.
- **Contents**: Stores badges, roles (Maintainer, Sponsor), expertise areas, and high-level engagement scores.
- **Automation**: This file is updated by `scripts/sync_registry.py` at the end of every pipeline run, merging manually assigned roles with automatically discovered engagement metrics.

### 3. `sponsors.json` (The Funding Graph)
**Purpose**: Tracks developer-to-funder relationships (e.g., Chaincode, Brink, Blockstream).
- **Goal**: Powers the "Funding Diversity" metrics which measure the corporate decentralization of the Bitcoin Core maintenance layer.

### 4. `locations.json` (Geospatial Mapping)
**Purpose**: Resolves arbitrary profile strings (e.g., "London", "SF", "Earth") into standard Continent/Country regions.
- **Usage**: Feeds the "Regional Evolution" dashboard, tracking the geographic shift of Bitcoin R&D over time.

---

## 💾 Behavioral Cache (`data/cache/`)

To preserve API rate limits and ensure fast rebuilds, the pipeline maintains a "memory" of expensive API lookups.
- **`enrichment_cache.json`**: Stores GitHub PR metadata, labels, and profile results.
- **`aliases_lookup.json`**: An optimized, flattened index of the registry used for ultra-fast name resolution during heavy ingestions.

---

## 🛠️ The Intelligence Workflow
1. **Discovery**: Ingestion scripts find a new name/email in a mirror.
2. **Resolution**: The script asks `identities.json`: "Do I know this person?"
3. **Tracking**: 
    - If **Yes**: History is added to the canonical profile.
    - If **No**: The person is recorded in `data/enriched/social_stats.json` for manual review in the next Registry Sync.
4. **Promotion**: High-impact new contributors are eventually graduated to the Master Registry for rich tracking.
