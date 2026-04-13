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
- **`bips.parquet`**: BIP header extractions.
- **`social_combined.parquet`**: Unified mailing list + Delving discussions.

### 3. `data/enriched/` (Stage 2 Intelligence)
The **Consolidated Intelligence Layer**. This is the "Gold" layer where all diverse data sources are unified into a single technical domain.
- **One Folder**: We moved away from separate `core/`, `governance/`, and `research/` folders to a single `enriched/` directory to simplify cross-domain joining.
- **Prefix-Based Naming**: Files use `core_`, `bips_`, or `social_` prefixes to maintain order.
- **Identity-Synced**: No data arrives here without passing through the `identities.json` resolve filter.

### 4. `output/` (Stage 3 Public Artifacts)
Lightweight JSON optimized for browser loading. 
- **`output/tracker/`**: Metric cards, contributor galaxys, and tables.
- **`output/network/`**: Influence graphs and PageRank rankings.

---

## ⚙️ Orchestration

We use two primary orchestrators to manage the pipeline complexity:

| Flow | Scope | Cadence |
| :--- | :--- | :--- |
| **`rebuild_daily.py`** | Updates Git mirrors, extractions, and the Master Registry. | Daily (Automated) |
| **`rebuild_monthly.py`** | Deep deep NLP thread categorization and global PageRank recalculations. | Monthly (Manual/Local) |

---

## 🔍 The Intelligence Engine (`metadata/`)

The repository's unique value lies in its **Master Contributor Registry**.
- **`identities.json`**: The canonical resolver. Maps 2,300+ aliases and emails to unique human IDs.
- **`contributors.json`**: The "Encyclopedia" of the Bitcoin Human Layer. Holds every role, badge, and activity score.
- **`sponsors.json`**: Tracks the funding independence of the decentralized developer set.
- **`locations.json`**: Human-audited geographical mapping.
