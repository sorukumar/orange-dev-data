# Orange Dev Architecture

This document outlines the architectural relationship between the repositories in the Orange Dev project. It defines the "Separation of Concerns" between the high-performance **Data Engine** and the **Stateless Dashboards**.

---

## 🏗️ System Overview

The system is split into Three Tiers:

1.  **Tier 1: The Engine (`orange-dev-data`)**
    - **Responsibility**: Ingestion, NLP categorization, Identity Resolution, Graph Math.
    - **Storage**: Parquet files (internal) and JSON (public artifacts).
    - **Automation**: Github Actions run Daily/Monthly.

2.  **Tier 2: The Viewers (`orange-dev-tracker` / `orange-dev-network`)**
    - **Responsibility**: Visualization, Interactive Discovery, D3/ECharts rendering.
    - **Statelessness**: No local database. All data is fetched as JSON from the `orange-dev-data` GitHub Pages instance.

3.  **Tier 3: The Assets (bitcoindatalabs.org)**
    - **Responsibility**: Centralized hosting of cross-project assets (CSS, branding, CDN-style JSON data).

---

## 📂 Data Lifecycle & Storage Strategy

The repository uses a **Tiered Storage Pattern** to ensure data integrity.

### 1. `data/sources/` (Raw Mirrors)
Local Git clones and archives of source material. We treat these as read-only foundations for the extraction layer.

### 2. `data/raw/` (Stage 1 Extractions)
Structured Parquets that represent the "First Draft" of the data. 
- **`core_commits.parquet`**: Raw commit logs.
- **`core_messages.parquet`**: Raw ACK/NACK bodies.
- **`github_pr_metadata.parquet`**: PR lifecycle timestamps and review signals.
- **`bips.parquet`**: BIP header extractions.
- **`social_combined.parquet`**: Unified mailing list + Delving discussions.

### 3. `data/enriched/` (Stage 2 Intelligence)
The **Consolidated Intelligence Layer**. This is the "Gold" layer where all diverse data sources are unified into a single technical domain.
- **One Folder**: We moved away from separate `core/`, `governance/`, and `research/` folders to a single `enriched/` directory to simplify cross-domain joining.
- **Prefix-Based Naming**: Files use `core_`, `bips_`, or `social_` prefixes to maintain order.
- **Identity-Synced**: No data arrives here without passing through the `identities.json` resolve filter.
- **`contributors_unified.parquet`**: The **Master Join**. Consolidates code, BIPs, social influence, and efficiency metrics into the final source of truth for individual profiles. Includes **Global Lifecycle Footprint** (unified first/last active across all platforms).
- **Universal Scale**: The pipeline now tracks ~3,445+ unique identities (up from 2,372 legacy contributors) by harvesting forum-only and BIP-only participants.

### 4. `output/` (Stage 3 Public Artifacts)
Lightweight JSON optimized for browser loading. 
- **`output/tracker/`**: Metric cards, contributor footprints, and tables.
- **`output/network/`**: Influence graphs and PageRank rankings.
- **`output/shared/contributors/`**: **Universal Profile Layer**. Sharded JSON profiles for all **301 high-signal contributors** (`authored_commits >= 10` OR `bips_authored > 0`) including commit history by year/category, BIP authorship, first/last social message, and social activity by topic.

---

## ⚙️ Orchestration

We use two primary orchestrators to manage the pipeline complexity:

| Flow | Scope | Cadence |
| :--- | :--- | :--- |
| **`rebuild_daily.py`** | Updates Git mirrors, extractions, and the Master Registry. | Daily |
| **`rebuild_monthly.py`** | Deep NLP thread categorization, global PageRank recalculations, and github_id_map refresh. | Monthly (run locally) |

---

## 🔍 The Intelligence Engine (`metadata/`)

The repository's unique value lies in its **Master Contributor Registry**.
- **`identities.json`**: The **Absolute Bedrock**. Replaced legacy alias strings. Maps 7,659+ identities and generates UUIDs Just-In-Time. This is the primary key for the entire "Grand Join".
- **`contributors.json`**: The "Legacy Encyclopedia". Holds roles, badges, and manual vetting data for the core 2,400 members. Used during Phase 2 to enrich the UUID-based dataset.
- **`sponsors.json`**: Tracks the funding independence of the decentralized developer set.
- **`locations.json`**: Human-audited geographical mapping.
- **`subsystems.json`**: The unified Bitcoin protocol registry. Maps BIPs, source paths, and keywords to technical domains.

## 🛠️ Shared Utilities (`scripts/utils/`)

The pipeline leverages a set of centralized modules to ensure logic is applied consistently across ingestion, processing, and analysis:

- **`subsystem.py`**: The **Standardized Resolver**. Used to identify technical domains (e.g., `wallet-keys`, `lightning`) from file paths, BIP numbers, or unstructured forum text. Every script that needs to categorize data must call this module to ensure the Master Registry remains unified.
- **`identity.py`**: The **Canonical Identity Engine**. Serves as the singleton pipeline gatekeeper — called by every downstream script via `resolver.resolve_*()` methods. Loads `metadata/identities.json` and resolves raw names/emails/logins to canonical UUIDs at runtime.

