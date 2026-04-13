# Directory Structure

The `orange-dev-data` repository follows a strict **Three-Tier Data Architecture** designed to separate raw sources from high-value intelligence and public artifacts.

---

## 1. `data/sources/` (The Foundation)
Raw, untouched source repositories (Git/Archives).
- **`bitcoin/`**: Full clone of the `bitcoin/bitcoin` repository for commit & code analysis.
- **`bips/`**: Full clone of the `bitcoin/bips` repository for specification indexing.
- **`delving/`**: Local mirror of the Delving Bitcoin research archive.
- **`mailing_list/`**: Local `public-inbox` git mirror of the Bitcoin-dev mailing list.

---

## 2. `data/raw/` (Stage 1: Extraction)
First-pass structured extractions (Parquet files) representing the "raw truth" of each source.
- **`core_commits.parquet`**: Raw Git history (hashes, dates, authors).
- **`core_messages.parquet`**: Extracted commit message bodies for NLP.
- **`bips.parquet`**: Structured metadata parsed from BIP headers.
- **`social_combined.parquet`**: Unified mailing list and Delving messages before categorization.

---

## 3. `data/enriched/` (Stage 2: Intelligence)
The **Consolidated Intelligence Layer**. This folder holds cleaned, identity-resolved, and categorized datasets. We use **prefix-based naming** to distinguish domains while keeping the pipeline simple.
- **`core_contributors.parquet`**: Identity-synced contributor profiles with LOC and activity metrics.
- **`core_metadata.json`**: Current codebase size, LOC per category, and language stats.
- **`bips_refined.parquet`**: BIPs enriched with social mentions and maturity scores.
- **`social_threads.parquet`**: Conversations categorized into technical themes (NLP).
- **`social_stats.json`**: Discovery data for the Master Registry (PageRank, influence).

---

## 4. `output/` (Stage 3: Showroom)
The **Public Deliverable Layer**. Optimized, lightweight JSON artifacts for production frontend consumption.
- **`output/tracker/`**: KPIs, contributor lists, and BIP tables for the Developer Tracker.
- **`output/network/`**: Graph topology and PageRank ranks for the Network Visualization.

---

## 5. `metadata/`
The "Brain" of the project. Manual overrides and the **Master Contributor Registry**.
- **`identities.json`**: Canonical mapping for 2,300+ developers (Aliases/Emails).
- **`contributors.json`**: The persistent, human-auditable database of Bitcoin's human layer.
- **`maintainers.json` / `sponsors.json`**: Manual whitelists for badges and roles.

---

## 6. `scripts/`
The execution layer.
- **`rebuild_daily.py`**: Fast, incremental sync for daily updates.
- **`rebuild_monthly.py`**: Comprehensive rebuild with NLP and graph re-calculations.
- **`core/`**: Repository-specific logic (Git log parsing, review extraction).
