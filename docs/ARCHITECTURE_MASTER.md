# Orange Dev Tracker - Master LLM Architecture Context
*This document consolidates the entire architectural context of the orange-dev-data repository into a single file.*

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



---
# Pipeline Walkthrough — How Every Number Is Made

This document traces the complete data flow from raw source files to the final
dashboard numbers. It shows exactly which scripts run, what inputs they read,
what they output, and how many unique people exist at each stage.

---

## The Problem This Pipeline Solves

This pipeline has three goals, in order of importance:

### 1. Don't double-count developers
A single Bitcoin developer may appear across all six sources under completely different identifiers:

| # | Source | Raw data location | How the developer appears |
|---|---|---|---|
| 1 | Bitcoin repo git commits | `data/sources/bitcoin/` | `name: Wladimir J. van der Laan`, `email: wlad@foo.com` |
| 2 | BIP repo git commits / BIP headers | `data/sources/bips/` | `name: Wladimir J. van der Laan` in BIP author field |
| 3 | Bitcoin repo GitHub metadata (PRs + reviews) | `data/sources/bitcoin-github-metadata/` | `login: laanwj`, `github_id: 123456` |
| 4 | BIP repo GitHub metadata (PRs + reviews) | `data/sources/bips-github-metadata/` | `login: laanwj`, `github_id: 123456` |
| 5 | Mailing list | `data/sources/mailing_list/` | `email: laanwj@gmail.com`, `name: Wladimir J. van der Laan` |
| 6 | Delving forum | `data/sources/delving/` | `username: laanwj` |

Without resolution, these six rows look like multiple different people. The pipeline collapses them into **one canonical UUID** — one row, one person.

### 2. Cover the full ecosystem
No single source is complete. Someone who only posts on the mailing list won't appear in git. Someone who only reviews PRs on GitHub won't have commits. The pipeline takes a **union** across all six sources so no contributor is invisible just because they work in one area.

### 3. Correctly associate activity across areas
Once every contributor has a UUID, each signal — a commit, a GitHub review, a mailing-list thread, a Delving post, a BIP authorship — is stamped with that UUID. This lets us ask questions like:
- Which developers are active in *both* the codebase and the research forums?
- Who reviews PRs but never commits?
- Which BIP authors are also core committers?

Getting the merge wrong (either splitting one person into two, or merging two people into one) corrupts every downstream number. That is why the identity resolution step (`build_identities.py`) is treated as the most critical step in the entire pipeline.

---

## Stage 0 — Sources (What We Start With)

Raw read-only archives on disk. No processing has happened yet.

| File | What it is |
|---|---|
| `data/sources/bitcoin/` | Local git clone of Bitcoin Core |
| `data/sources/bips/` | Local git clone of the BIPs repo |
| `data/sources/bitcoin-github-metadata/` | Local git clone of `github-metadata-backup-bitcoin-bitcoin` (offline backup of all Bitcoin Core PRs/reviews — **no live API**) |
| `data/sources/bips-github-metadata/` | Local git clone of `github-metadata-backup-bitcoin-bips` (offline backup of all BIPs PRs/reviews) |
| `data/sources/delving/` | Local git clone of the `delving-bitcoin-archive` repo |
| `data/sources/mailing_list/` | Local archive of bitcoin-dev mailing list messages — **no live fetch** |

---

## Phase 1 — Extraction (`scripts/01_ingest/`) → `data/raw/`

Each script reads one source and writes a structured Parquet. No identity
resolution yet — raw names/emails/logins are preserved as-is.

### Scripts and outputs

| Script | Reads | Writes | Key fields |
|---|---|---|---|
| `core.py` | `data/sources/bitcoin/` git log | `data/raw/core_commits.parquet` + `data/raw/core_messages.parquet` | `author_name`, `author_email`, `hash`, `date_utc` |
| `bips.py` | `data/sources/bips/` git log + BIP header files | `data/raw/bips.parquet` | `author_names[]`, `bip_number`, `title`, `layer` |
| `bips_metadata.py` | `data/sources/bips-github-metadata/pulls/*.json` | `data/raw/bips_pr_metadata.parquet` + `data/raw/bips_review_events.parquet` | `author` (login), `github_id`, review events |
| `github_metadata.py` | `data/sources/bitcoin-github-metadata/pulls/*.json` | `data/raw/github_pr_metadata.parquet` + `data/raw/github_review_events.parquet` | `author` (login), `github_id`, `user` (reviewer login) |
| `mailing_list.py` | `data/sources/mailing_list/` local archive | `data/raw/social_mailing_list.parquet` | `author_name`, `author_email` (Drops pre-2011 noise via a strict cutoff) |
| `delving.py` | `data/sources/delving/archive/posts/*.json` | `data/raw/social_delving.parquet` | `author_username`, `author_name` |

> `bips_metadata.py` runs in **both daily and monthly** pipelines, immediately after `bips.py`.

### Raw file inventory (9 files from 6 sources)

The six ingest scripts produce **9 raw Parquet files** in `data/raw/`:

| # | File | Source | Rows | Key identifier cardinality |
|---|---|---|---|---|
| 1 | `core_commits.parquet` | Bitcoin git log | 62,629 commits | 1,307 unique emails · 1,312 unique names |
| 2 | `core_messages.parquet` | Bitcoin git log (commit bodies) | 48,621 messages | 48,621 unique hashes (one per non-merge commit) |
| 3 | `bips.parquet` | BIPs git log + BIP header files | 204 BIPs | 155 unique author names in headers |
| 4 | `bips_pr_metadata.parquet` | BIPs GitHub metadata backup | 1,948 PRs | 732 unique PR author logins · 729 unique GitHub IDs |
| 5 | `bips_review_events.parquet` | BIPs GitHub metadata backup | 30,940 review events | 1,270 unique reviewer logins · 1,265 unique GitHub IDs |
| 6 | `github_pr_metadata.parquet` | Bitcoin GitHub metadata backup | 24,407 PRs | 2,580 unique PR author logins · 2,574 unique GitHub IDs |
| 7 | `github_review_events.parquet` | Bitcoin GitHub metadata backup | 719,117 review events | 4,543 unique reviewer logins · 4,530 unique GitHub IDs |
| 8 | `social_mailing_list.parquet` | Local mailing list archive | 24,294 messages | 1,456 unique emails · 1,493 unique names |
| 9 | `social_delving.parquet` | Delving archive repo | 4,022 posts | 375 unique usernames · 377 unique display names |

Files 4 and 5 (`bips_pr_metadata.parquet`, `bips_review_events.parquet`) were previously missing from pipeline runs. They are now generated by `bips_metadata.py` which runs in both daily and monthly pipelines.

These numbers **overlap massively** — the same person appears in multiple sources.
The next phase resolves this.

---

## Phase 2a — Identity Graph (`scripts/identity/build_identities.py`)

**This is the most important step in the entire pipeline.**

### What it does

1. Takes every `author_name`, `author_email`, `github_login`, `github_id`,
   `delving_username` from all six raw sources.
2. Builds a **graph** where each raw identifier is a node, and two nodes are
   connected by an edge when they co-appear on the same commit, PR, review event, or curated
   entry.
3. Finds all **connected components** — each component is one human.
4. Assigns one canonical UUID per component.
5. Writes `metadata/identities.json`.

### How UUIDs are assigned

```
can_<slug>   ← person appears in metadata/identity_curated.json (129 hand-curated multi-alias people)
auto_<slug>  ← everyone else (single-source or not curated)
```

### Reads

| File | Purpose |
|---|---|
| `metadata/identity_curated.json` | 129 hand-curated aliases (e.g. "laanwj = Wladimir = laanwj@gmail.com") |
| `metadata/github_id_map.json` | Pre-computed github_id → email anchors (870 real-email entries, built by `build_github_id_map.py`) |
| `data/raw/core_commits.parquet` | name+email edges |
| `data/raw/github_pr_metadata.parquet` | login+github_id edges |
| `data/raw/github_review_events.parquet` | reviewer login+github_id edges (people who reviewed but never authored a PR) |
| `data/raw/bips_pr_metadata.parquet` | same as above for BIPs repo |
| `data/raw/bips_review_events.parquet` | BIPs reviewer login+github_id edges |
| `data/raw/social_delving.parquet` | username nodes |
| `data/raw/social_mailing_list.parquet` | name+email edges |
| `data/raw/bips.parquet` | author name nodes |

### Writes

`metadata/identities.json` — the **master lookup table** for the entire pipeline.

### Output counts (current)

```
7,249 total unique identities
  132  can_  (multi-alias people who were hand-curated)
7,117  auto_ (everyone else)

Identities by source origin:
  prgithub    (GitHub PR activity)  ~5,000+
  bipgithub   (BIPs repo PR)        ~1,400+
  mailinglist (mailing list)        ~1,300+
  corecommit  (git commits)         ~1,100+
  delving     (Delving forum)          ~370
  bips        (BIP author headers)     ~150
  curation    (hand-curated)            132
```

> The higher prgithub/bipgithub counts (vs older runs) reflect that `process_review_events()` now ingests login+GH_ID edges from ALL reviewer events, not just PR authors. The email anchor count is also higher: `github_id_map.json` provides 870 corroborated email anchors (vs ~694 with the old zero-trust SHA join).

### The Resolver (`scripts/utils/identity.py`)

Every subsequent script calls one of these three resolver methods instead of
looking up raw names:

```python
resolver.resolve_git(name, email)       # for git commit authors
resolver.resolve_github(login)          # for GitHub logins/reviewers
resolver.resolve_delving(username)      # for Delving usernames
resolver.resolve_mailing_list(handle)   # for mailing list authors
```

Each method looks up the identifier in the index built from `identities.json`.
If found → returns the existing UUID. If not found → mints a new `auto_<slug>`
UUID on the fly (stateless, deterministic from the identifier string).

---

## Phase 2b — Enrichment (`scripts/02_process/`) → `data/enriched/`

These scripts resolve raw identifiers to UUIDs and aggregate activity per UUID.

| Script | Reads | Writes | What it computes |
|---|---|---|---|
| `core.py` | `core_commits.parquet` | `data/enriched/core_contributors.parquet` + multiple `output/tracker/*.json` | commits, additions, deletions per UUID; JSON stats for dashboard |
| `reviews.py` | `data/raw/core_messages.parquet` (commit message bodies) | `data/raw/core_reviews.parquet` + `output/tracker/reviewers_summary.json` | Parses ACK/NACK/utACK trailers from git commit messages to extract reviewer signals |
| `github_social.py` | `data/sources/bitcoin-github-metadata/` JSON files | `data/enriched/github_social_stats.parquet` | Per-login label expertise and PR participation counts (offline, no API) |
| `merge_social.py` | `social_mailing_list.parquet`, `social_delving.parquet` | `data/raw/social_combined.parquet` | Concatenates mailing list + Delving into one flat file; adds mailing-list message links |
| `categorize.py` | `data/raw/social_combined.parquet` | `data/enriched/social_threads.parquet` | Assigns BIP references and rich multi-label categories to every thread |
| `governance.py` | `bips.parquet`, `social_combined.parquet`, `core_commits.parquet`, `core_messages.parquet` | `data/enriched/bips_refined.parquet` + `data/enriched/bips_themes.json` | BIP authorship per UUID; links BIPs to social discussion and commit mentions |

> ⚠️ `data/enriched/social_threads.parquet` (the categorised social data that `influence.py` and `expertise.py` depend on) is produced by `categorize.py`. In `rebuild_daily.py` **`categorize.py` is never called**, so daily runs rely on the last monthly output.

**At this point, all raw names/emails/logins have been replaced by UUIDs.**

---

## Phase 2c — Bootstrap Registry (`scripts/04_deliver/registry.py`) ← runs early

**MUST run before Phase 3.** Both `influence.py` and `unify_contributors.py`
read `metadata/contributors.json`, which this script creates.

### Reads
- `metadata/identities.json`
- `metadata/contributors.json` (if exists — incremental, additive)
- `output/tracker/contributors_rich.json` (written by `02_process/core.py` — must exist for a full sync; handled gracefully on first run)
- `data/enriched/social_stats.json` (for social discovery)

### Writes
- `metadata/contributors.json` — the “encyclopedia” of all known contributors
  with roles, badges, first/last seen timestamps

> ⚠️ `registry.py` does **not** write `output/tracker/contributors_rich.json`. That file is written by `02_process/core.py`. `registry.py` only reads it.

---

## Phase 3 — Intelligence (`scripts/03_analyze/`) → `data/enriched/`

Heavy analytics. Deeper per-UUID aggregations.

### `review_metrics.py`

**Reads:** `data/raw/github_pr_metadata.parquet`, `data/raw/github_review_events.parquet`,
`data/raw/bips_pr_metadata.parquet`, `data/raw/bips_review_events.parquet`, `metadata/contributors.json`  
**Writes:** `data/enriched/contributor_review_metrics.parquet`  
**Computes:** Review latency, ACK speed, PRs authored, review reciprocity  
**Coverage:** Both Bitcoin Core and BIPs GitHub (BIPs parquets concatenated before processing)  
**Key fix:** Uses `resolver.resolve_github(login)` (not `resolve_git`) to
correctly match reviewer logins to their UUIDs.  
**Active-review filter:** Only `commented` and `reviewed` event types are counted as
active reviews. Passive events (`subscribed`, `mentioned`, `referenced`,
`head_ref_force_pushed`, `cross-referenced`, `closed`, `merged`, `labeled`) are
excluded — they inflate the count without reflecting genuine engagement.

```
Current output: 6,077 unique UUIDs
  115  can_ IDs
5,962  auto_ IDs
  0    raw handles
```

### `influence.py`

**Reads:** `data/enriched/social_threads.parquet`, `metadata/identities.json`,
`metadata/contributors.json`, `output/tracker/contributors_rich.json` (code stats, optional)  
**Writes:** `data/enriched/social_stats.json`, `output/network/network_graph.json`  
**Computes:** PageRank centrality across three eras (all-time, post-2016, modern),
contributor archetypes, hybrid influence score combining social + code signals,
**`impact_score`** (0–100 integer) using a fixed-anchor normalization.

**`impact_score` formula:** `min(round(hybrid_score / 3.75 × 100), 100)`  
The anchor `3.75` is the theoretical maximum (1.0 base weights + 1.765 BIP cap + 1.0 maintainer bonus).
BIP bonus is **capped** at `log₂(33) × 0.35 ≈ 1.765` (~32 BIPs) to prevent runaway scores.
`can_satoshi_nakamoto` receives `impact_score = null` — rendered as "Creator" in the frontend.

> ⚠️ `influence.py` reads `social_threads.parquet` which is produced by `categorize.py`.
> On daily runs `categorize.py` is skipped, so this uses the last monthly categorisation.

```
Current output: 7,249 people in social_stats.json
  132  can_ IDs
7,117  auto_ IDs
1,411  with zero hybrid_score (observers / inactive)
```

### `expertise.py`

**Reads:** `data/enriched/bips_refined.parquet`, `data/raw/core_commits.parquet`,
`data/raw/github_pr_metadata.parquet`, `data/raw/bips_pr_metadata.parquet`,
`data/enriched/social_threads.parquet`  
**Writes:** `output/tracker/expertise.json`  
**Computes:** PR label-based domain tags per UUID; identifies “Full-Stack Architects”
(BIP authors who also have code commits)

---

## Phase 3 → 4 — The Grand Join (`scripts/02_process/unify_contributors.py`)

**This is where all sources are merged into one row per developer.**

### Reads (all by UUID)

| File | What it adds |
|---|---|
| `metadata/identities.json` | Base UUID list + display name + platform handles |
| `data/raw/core_commits.parquet` | Commit counts, additions, deletions |
| `metadata/contributors.json` | Roles, badges, first/last seen |
| `data/enriched/contributor_review_metrics.parquet` | Reviewer metrics, PRs authored (Bitcoin Core + BIPs) |
| `data/enriched/social_stats.json` | Social influence score, thread counts, hybrid score, **impact_score** |
| `data/enriched/bips_refined.parquet` | BIPs authored (uses pre-resolved `author_canonical_ids` — NOT re-resolved) |
| `data/raw/social_delving.parquet` | Used for UUID discovery (delving-only contributors) |

### How the union is built

```python
# The union is: everyone seen in ANY source
all_source_uuids = (
    set from identities.json
    | set from core_commits
    | set from social_stats.json      # social-only people
    | set from contributor_review_metrics  # review-only people
)
# = 7,363 total (current run)
```

For each UUID, a LEFT JOIN pulls in whatever columns are available. Someone with
no commits gets 0 for `total_commits`. Someone with no social activity gets 0
for `ml_threads`.

### Writes

`data/enriched/contributors_unified.parquet` — **one row per developer, all signals combined**

```
Current output: 7,363 rows (all unique UUIDs, no duplicates)
  132  can_ IDs
7,231  auto_ IDs
  0    raw handles

Activity breakdown:
  1,142  have commit activity     (total_commits > 0)
  4,947  have review activity     (reviews_count > 0 OR prs_authored > 0)
  1,134  have social activity     (ml_threads > 0 OR delving_threads > 0)
    157  have BIP authorship      (bips_authored > 0)
```

---

## Phase 4 — Delivery (`scripts/04_deliver/`) → `output/`

### `registry.py` (final run)

Refreshes `metadata/contributors.json` with the latest unified data (reads from
`output/tracker/contributors_rich.json` and `data/enriched/social_stats.json`).

> ⚠️ `registry.py` does **not** write `contributors_rich.json`. That file is
> produced by `02_process/core.py` earlier in the pipeline.

### `ui_artifacts.py`

**Reads:** `contributors_unified.parquet`  
**Writes:**
- `output/shared/contributors/registry_index.json` — flat table of all 7,363 rows
  with selected columns for the directory view
- `output/shared/contributors/profiles/{uuid}.json` — **one shard per contributor,
  all 7,363** — no threshold filtering applied. Every developer gets a profile page
  regardless of activity level.
  Each shard is the registry entry plus four embedded enriched datasets loaded
  on demand: per-year commit breakdown by codebase category (`commit_history`),
  BIP authorship list with title/status/theme/link (`bip_list`), first and last
  indexed social message (`first_message` / `last_message`), and per-year social
  activity count by topic (`social_history`).

### `ecosystem_summary.py`

**Reads:** `output/shared/contributors/registry_index.json`  
**Writes:** `output/shared/ecosystem_summary.json`  
**Computes:** The headline numbers shown on the landing page

```
Final headline numbers (current run):
  total_registry   7,363   (all unique developers ever seen)
  total_active     6,231   (has any non-zero activity signal)
  committers       1,142   (total_commits > 0)
  reviewers        4,947   (reviews_count > 0 OR prs_authored > 0)
  research         1,596   (ml or delving activity above threshold)
  standards          157   (authored at least one BIP)
  all_four            70   (active across all four domains)
```

---

## End-to-End Data Flow Diagram

```
data/sources/bitcoin/ ──── core.py ───────────────────→ { core_commits.parquet
                                                           core_messages.parquet }
data/sources/bips/ ──────── bips.py ──────────────────→   bips.parquet
data/sources/bips-github-metadata/ ── bips_metadata.py → { bips_pr_metadata.parquet
                                                             bips_review_events.parquet }
data/sources/bitcoin-github-metadata/ ── github_metadata.py → { github_pr_metadata.parquet
                                                                  github_review_events.parquet }
data/sources/mailing_list/ ── mailing_list.py ────────→   social_mailing_list.parquet  (monthly only)
data/sources/delving/ ───── delving.py ───────────────→   social_delving.parquet

        ↓ all raw parquets feed into ↓

metadata/identity_curated.json (129 hand entries)
                +
        metadata/github_id_map.json  ← pre-computed by build_github_id_map.py (monthly)
                +
        ALL raw parquets
                ↓
        build_identities.py
                ↓
        metadata/identities.json          ← THE MASTER UUID TABLE
        (7,249 unique people)

        ↓ all downstream scripts use resolver.resolve_*() ↓

core_commits.parquet ──────── resolve_git(name, email) ──────→ core_contributors.parquet (via 02_process/core.py)
core_messages.parquet ──────── ACK trailer parse ────────────→ core_reviews.parquet (via reviews.py)
github_pr_metadata.parquet ─┐
bips_pr_metadata.parquet ───┤
                            ├─ resolve_github(login) ────────→ contributor_review_metrics.parquet (via review_metrics.py)
github_review_events.parquet┤
bips_review_events.parquet ─┘
social_mailing_list.parquet ─┐
social_delving.parquet ──────┴─ merge_social.py → social_combined.parquet
                                                         ↓
                                               categorize.py (monthly only)
                                                         ↓
                                           social_threads.parquet  ← used by influence.py & expertise.py
bips.parquet ─── governance.py (+ social_combined, core_commits, core_messages) → bips_refined.parquet

        ↓ registry.py (bootstrap — writes contributors.json) ↓

        ↓ Phase 3 ↓

        review_metrics.py  → contributor_review_metrics.parquet
        influence.py   → social_stats.json + output/network/network_graph.json
        expertise.py   → output/tracker/expertise.json

        ↓ Grand Join ↓

identities.json (base)
+ core_commits.parquet (commits)
+ contributors.json (roles/badges)
+ contributor_review_metrics.parquet (review metrics — Bitcoin Core + BIPs)
+ social_stats.json (influence/hybrid score)
+ bips_refined.parquet (BIP authorship)
+ social_delving.parquet (UUID discovery)
                ↓
        unify_contributors.py
                ↓
        contributors_unified.parquet       ← ONE ROW PER DEVELOPER
        (7,363 rows, all signals)

                ↓
        registry.py (final) → metadata/contributors.json refreshed
                ↓
        ui_artifacts.py → registry_index.json + 7,363 profile shards ({uuid}.json, one per contributor)
                ↓
        ecosystem_summary.py → ecosystem_summary.json
                ↓
        Dashboard landing page numbers
```

---

## Daily vs Monthly Pipeline Differences

| Step | `rebuild_daily.py` | `rebuild_monthly.py` |
|---|---|---|
| `mailing_list.py` | ❌ skipped | ✅ runs |
| `bips_metadata.py` | ✅ runs | ✅ runs |
| `categorize.py` | ❌ skipped | ✅ runs (Phase 3) |
| `generate_regional_evolution.py` | ❌ skipped | ✅ runs (Phase 4) |
| `expertise.py` | ✅ runs | ✅ runs |

Because `categorize.py` only runs monthly, `data/enriched/social_threads.parquet`
is not refreshed on daily runs. `influence.py` runs daily but reads stale social
thread categorisations from the last monthly run. This is likely intentional
(categorisation is slow) but means daily social influence scores don't reflect
new mailing-list/Delving messages.

---

## Known Issues Fixed (May 2026)

| Bug | Effect | Fix |
|---|---|---|
| `review_metrics.py` counted passive GitHub timeline events as "code reviews" | `subscribed`, `mentioned`, `referenced`, `head_ref_force_pushed`, `cross-referenced`, `closed`, `merged`, `labeled` events inflated `reviews_count` by ~18% (e.g. sipa: 4,988 → 4,260 after fix) | Added `ACTIVE_REVIEW_TYPES = {'commented', 'reviewed'}` filter before all aggregations — only genuine interactions count |
| `ui_artifacts.py` generated profile shards only for "high-signal" contributors (`authored_commits >= 10` OR `bips_authored > 0`) | ~7,000 contributors with only review or social activity had no profile page | Removed threshold filter — all 7,363 contributors now get a profile shard |
| `hybrid_score` displayed raw as `3.787` in the Impact Score stat | Unintuitive to users; no reference point; not comparable across builds | Added `impact_score` (0–100 integer) using fixed-anchor normalization: `min(round(hybrid_score / 3.75 × 100), 100)`. Satoshi gets `null` (shown as "Creator"). BIP bonus capped at `log₂(33) × 0.35 ≈ 1.765` (~32 BIPs) to bound the theoretical maximum |
| `impact_score` computed in `influence.py` but not passed through to `contributors_unified.parquet` | Profile shards had `impact_score: null` | Added `impact_score` to `soc_cols` allowlist in `unify_contributors.py` |

## Known Issues Fixed (April 2026)

| Bug | Effect | Fix |
|---|---|---|
| `efficiency.py` used `resolve_git(login, None)` for GitHub logins | Looked up `name:dfletcher` instead of `github:dfletcher` → 4,543 raw handles not resolved | Changed to `resolve_github(login)` (now in `review_metrics.py`) |
| `efficiency.py` built `all_logins` from unresolved `df_events['user']` | Final parquet had raw handles as canonical_ids → inflated registry to 11,913 | Resolve before merge (now in `review_metrics.py`) |
| `efficiency.py` only processed Bitcoin Core PRs/reviews | 171 BIPs-GitHub-only contributors had zero `reviews_count` and `prs_authored` | Renamed to `review_metrics.py`; BIPs parquets concatenated before processing |
| `build_identities.py` stored only one GitHub login per identity | Username-change cases (e.g. `maflcko`→`MarcoFalke`) left old handle unmappable → reviewer minted stale `auto_` UUID | `platforms.github` now stored as list when multiple logins share a GH_ID |
| `mailing_list.py` (Phase 1) baked `canonical_id` before `build_identities.py` ran | Phase 2 identity merges made Phase 1 IDs stale → 97 mailing-list UUIDs stranded with zero activity | Added `restamp_social_ids.py` after `build_identities.py` to re-resolve with fresh Phase 2 resolver |
| `prs_authored` missing from `registry_cols` in `ui_artifacts.py` | Reviewer count was wrong in output | Added to column list |
| `ecosystem_summary.py` not called by either rebuild script | Landing page numbers always stale | Added to end of both `rebuild_daily.py` and `rebuild_monthly.py` |
| `registry.py` ran too late (Phase 4) but `influence.py` and `unify_contributors.py` needed it in Phase 3 | On clean-slate run: `social_stats.json` output 0 people | Moved `registry.py` to run before Phase 3 in both rebuild scripts |
| `unify_contributors.py` hard-crashed when social data was empty | `first_active` column didn't exist if influence produced zero results | Safe column presence check |
| `build_identities.py` zero-trust SHA anchor join hit only 23% of GitHub identities | Complex inline logic cross-checked head_sha against core_commits.parquet → only 694 of 3,019 GitHub identities got a real-email anchor | Extracted email mining to `build_github_id_map.py` (monthly); stores corroborated `github_id → email` pairs with source, corroboration count, and example SHA/PR; `build_identities.py` now reads the map instead of doing inline SHA lookups |

## Open Issues (April 2026)

| Issue | Effect | Status |
|---|---|---|
| `bips_metadata.py` never called by either rebuild script | `bips_pr_metadata.parquet` and `bips_review_events.parquet` were never refreshed; `bipgithub` signal depended on a manually-generated file | **Fixed** — added to Phase 1 of both `rebuild_daily.py` and `rebuild_monthly.py` |
| `build_identities.py` only used PR author logins for GH_ID anchoring | Reviewers who never authored a PR had no GH_ID edge → harder to merge their identities across sources | **Fixed** — added `process_review_events()` in `build_identities.py` that ingests login+GH_ID edges from both `github_review_events.parquet` and `bips_review_events.parquet` |
| `categorize.py` skipped on daily runs | `social_threads.parquet` is stale between monthly runs; daily social influence uses old thread categorisations | **By design?** — confirm intentional or add lightweight re-categorisation to daily pipeline |
| `mailing_list.py` skipped on daily runs | New mailing-list messages not ingested daily | **By design** (archive-based; acceptable for daily cadence) |


---
# Identity Resolution

The most critical step in the entire pipeline. Before any analytics can run, every
raw name, email, GitHub login, and forum username must be collapsed into a single
canonical UUID. This document covers the resolution mechanics, the maintenance
workflow, and the scripts that implement it.

---

## Key Files

| File | Role |
|------|------|
| `scripts/identity/build_identities.py` | Builds the identity graph and writes `identities.json` — run by both rebuild orchestrators |
| `scripts/identity/build_github_id_map.py` | Pre-computes `github_id → email` anchors from offline PR archives — run monthly before `build_identities.py` |
| `metadata/identities.json` | **The generated output.** Master UUID lookup table for the entire pipeline |
| `metadata/identity_curated.json` | **The bedrock input.** 129 hand-curated multi-alias entries that override all automated logic |
| `scripts/utils/identity.py` | Runtime resolver singleton — every downstream script calls `resolver.resolve_*()` instead of looking up raw names |

---

## The 4-Level Resolution Hierarchy

`build_identities.py` applies these rules in strict priority order to prevent both
**identity fragmentation** (one person → two profiles) and the **"Great Collapse"**
(two people → one profile):

### Level 1: Deterministic Anchors (SHA Bridge)
Connects a GitHub login to a Git email using a specific commit SHA as proof of
identity. A PR author's login is linked to the email on the commit at the PR's
`head_sha`.

**Proxy guard**: If the login belongs to a known maintainer-proxy (e.g., `fanquake`,
`laanwj`), the system refuses to anchor it to a random email. It only allows
anchoring to that person's own verified emails listed in `identity_curated.json`.
This prevents a maintainer who merges PRs on behalf of others from absorbing the
contributor's commit history.

### Level 2: Exact String Matching
All names and emails are strictly **lowercased**, stripped, and normalized before comparison. This case-insensitive normalization eliminates duplication bugs (e.g. `MarcoFalke` vs `marcofalke`).
Generic hubs (`unknown`, `bot`, empty strings) are hard-blocked and can never act
as bridges between two different people.

### Level 3: Human Curation (The Bedrock)
**Source**: `metadata/identity_curated.json`
This is prioritized heavily. During resolution, if a raw handle matches an alias in the curated list, it acts as an absolute override, bypassing loose automated graph matching.

If a link exists here, it overrides all automated logic. The policy is: *"In Case of
Doubt, Separate."* Two profiles for one person is less harmful than one profile for
two people.

### Level 4: Fuzzy Matching (The Auditor — Report Only)
A Levenshtein-based similarity check used **only for reporting**, never for
auto-merging. Run via the `--audit` flag (see Maintenance below).

---

## How `build_identities.py` Works

1. Loads all raw parquets (`core_commits`, `github_pr_metadata`,
   `github_review_events`, `bips_pr_metadata`, `bips_review_events`,
   `social_mailing_list`, `social_delving`, `bips`) — every raw identifier in the
   pipeline.
2. Loads `metadata/identity_curated.json` (Level 3 bedrock) and
   `metadata/github_id_map.json` (pre-computed email anchors from
   `build_github_id_map.py`).
3. Builds a **graph** where each raw identifier (name, email, login, github_id) is a
   node, and two nodes are connected when they co-appear on the same commit, PR, or
   review event, or are linked in a curated entry.
4. Finds all **connected components** — each component is one human.
5. Assigns one canonical UUID per component:
   - `can_<slug>` — person appears in `identity_curated.json`
   - `auto_<slug>` — everyone else (single-source or not curated)
6. Writes `metadata/identities.json`.

**Current output**: ~7,659 unique identities (122 `can_`, 7,537 `auto_`).

---

## The Runtime Resolver (`scripts/utils/identity.py`)

Every script downstream of `build_identities.py` calls the resolver singleton
instead of looking up raw strings directly:

```python
resolver.resolve_git(name, email)        # for git commit authors
resolver.resolve_github(login)           # for GitHub PR/review logins
resolver.resolve_delving(username)       # for Delving forum usernames
resolver.resolve_mailing_list(handle)    # for mailing list authors
```

Using the wrong method (e.g., `resolve_git(login, None)` for a GitHub login) is a
known source of regressions — it looks up `name:dfletcher` instead of
`github:dfletcher`, leaving identities unresolved. See the April 2026 fix note in
`PIPELINE_WALKTHROUGH.md`.

---

## Adding a New Identity

If a contributor has two profiles that aren't linking (e.g., a GitHub handle and a
mailing list email appear as separate UUIDs in `identities.json`):

1. Open `metadata/identity_curated.json`.
2. Find the existing entry for this person, or add a new one under `"aliases"`.
3. Add the unlinked handle, email, or name to the appropriate array.
4. Run `python3 scripts/identity/build_identities.py` (or the full pipeline to
   propagate downstream).

### Handling Proxy Merges

If a maintainer starts incorrectly absorbing contributions from others:

1. Add the maintainer's GitHub login to the `PROXIES` set in
   `scripts/identity/build_identities.py`.
2. Verify their canonical identity is correctly defined in `identity_curated.json`.
3. The Level 1 anchor guard will now refuse to bridge their login to unowned emails.

---

## Maintenance — Identity Audit Workflow

### Generating the Audit Report (run on demand)

```bash
python3 scripts/rebuild_monthly.py --audit
```

The `--audit` flag runs `scripts/maintenance/generate_audit_potential_matches.py`
which writes `metadata/audit_potential_matches.json`. This file surfaces:

- Delving and mailing-list users not yet linked to any GitHub identity
- Ranked candidate GitHub profiles to review for potential curation
- Fuzzy name pairs across all identities (potential missed merges)

Review the output, then add high-confidence pairs to `metadata/identity_curated.json`
and re-run `build_identities.py`.

> ⚠️ **Superseded**: `scripts/maintenance/archive/identity_auditor_l4.py` is an
> older, less comprehensive version of this audit that wrote to the same output file.
> Use the `--audit` flag instead.

### Building the github_id_map (monthly)

`metadata/github_id_map.json` pre-computes high-quality `github_id → email` anchors
from the offline PR archives. It is rebuilt automatically every monthly run. To
rebuild it manually:

```bash
python3 scripts/identity/build_github_id_map.py
```

For contributors who appeared in only one PR (below the corroboration threshold for
the main map), there is an optional enrichment step that uses GitHub profile pages as
a second signal:

```bash
# Step 1: generate the list of single-PR contributors
python3 scripts/enrichment/generate_single_pr_input.py

# Step 2: fetch their GitHub profiles (requires GITHUB_TOKEN)
python3 scripts/enrichment/fetch_github_profiles.py \
    --input metadata/single_pr_contributors.json \
    --output metadata/github_profiles_single_pr.json \
    --token $GITHUB_TOKEN

# Step 3: cross-validate and inject confirmed entries into github_id_map.json
python3 scripts/enrichment/ingest_single_pr_profiles.py
```

### Bedrock Reset

If the identity graph becomes corrupted (e.g., unexpected mass merges — the "Great
Collapse"):

```bash
python3 scripts/maintenance/reset_bedrock.py
```

This purges all automated tags, restores `identities.json` to the curated-seeds-only
baseline, and creates a `.bak` backup first.

---

## Dormant Scripts in `02_process/`

Three scripts in `scripts/02_process/` exist but are **not called by either rebuild
orchestrator**. They are kept for potential future re-integration:

| Script | Original purpose | Status |
|--------|-----------------|--------|
| `enrich_identity.py` | GitHub API-based profile enrichment (fetches additional handle/email signals per contributor) | Dormant — the offline `github_id_map.json` approach replaced the live API hits |
| `background_enricher.py` | Rate-limit-aware background API enrichment runner | Dormant — dependent on `enrich_identity.py` |
| `footprint.py` | Per-contributor directory footprint from merge commit analysis | Dormant — never integrated into the grand join |

Do not run these scripts as part of the pipeline. If re-integrating, they would slot
into Phase 2b of the monthly rebuild after `governance.py`.


---
# Script Reference

Index of all scripts in the `scripts/` directory, organized by folder. Scripts marked
**[daily]** and/or **[monthly]** run automatically via the rebuild orchestrators.
Scripts marked **[dormant]** exist in the codebase but are not wired into either
rebuild script.

---

## Master Orchestrators

- **`rebuild_daily.py`**: Incremental pipeline — git sync, extraction, identity build,
  enrichment, light analytics, artifact delivery. Run daily.
- **`rebuild_monthly.py`**: Full pipeline — adds mailing list ingest, `github_id_map`
  rebuild, NLP categorization (`categorize.py`), and regional evolution. Supports
  `--audit` flag to generate `audit_potential_matches.json`.

---

## `identity/` — Identity Graph Construction

Scripts that build `metadata/identities.json` and its supporting inputs. These are
the most critical scripts in the repo — run before anything in `02_process/`.

- **`build_identities.py`** **[daily] [monthly]**: Reads all 9 raw parquets +
  `identity_curated.json` + `github_id_map.json`. Builds a connected-component graph
  and assigns one canonical UUID per human. Writes `metadata/identities.json`.
- **`build_github_id_map.py`** **[monthly]**: Pre-computes `github_id → email` anchors
  from offline PR archives. Writes `metadata/github_id_map.json`. Must run before
  `build_identities.py` in the monthly pipeline.
- **`restamp_social_ids.py`** **[daily] [monthly]**: Re-stamps `canonical_id` in
  `data/raw/social_mailing_list.parquet` against the freshly-rebuilt `identities.json`.
  Runs immediately after `build_identities.py`, before `merge_social.py`, to collapse
  stale Phase 1 auto_ IDs into canonical Phase 2 UUIDs.

---

## `01_ingest/` — Stage 1: Extraction

Reads source mirrors, writes structured Parquets to `data/raw/`. No identity
resolution at this stage — raw names/emails/logins are preserved as-is.

- **`core.py`** **[daily] [monthly]**: Extracts Bitcoin Core git history →
  `core_commits.parquet`, `core_messages.parquet`.
- **`bips.py`** **[daily] [monthly]**: Parses BIPs repo git log and BIP headers →
  `bips.parquet`.
- **`bips_metadata.py`** **[daily] [monthly]**: Extracts BIPs GitHub PR and review
  events → `bips_pr_metadata.parquet`, `bips_review_events.parquet`.
- **`github_metadata.py`** **[daily] [monthly]**: Extracts Bitcoin Core GitHub PR and
  review events → `github_pr_metadata.parquet`, `github_review_events.parquet`.
- **`delving.py`** **[daily] [monthly]**: Ingests Delving Bitcoin archive →
  `social_delving.parquet`.
- **`mailing_list.py`** **[monthly only]**: Parses Bitcoin-dev mailing list archive →
  `social_mailing_list.parquet`. Skipped in daily runs.

---

## `02_process/` — Stage 2: Convergence

Resolves raw identifiers to UUIDs and aggregates activity per UUID.
Reads from `data/raw/`, writes to `data/enriched/` and `output/tracker/`.

- **`reviews.py`** **[daily] [monthly]**: Parses ACK/NACK/utACK trailers from git
  commit message bodies → `data/raw/core_reviews.parquet`,
  `output/tracker/reviewers_summary.json`.
- **`github_social.py`** **[daily] [monthly]**: Parses the local GitHub metadata mirror
  for PR labels and social participation. **100% offline, no API.** →
  `data/enriched/github_social_stats.parquet`.
- **`core.py`** **[daily] [monthly]**: Resolves commit authors to UUIDs, computes
  LOC/churn metrics → `data/enriched/core_contributors.parquet`,
  `output/tracker/contributors_rich.json`,
  `data/enriched/contributor_commit_history.json` (per-UUID yearly commit breakdown
  by codebase category — consumed by `ui_artifacts.py` to populate profile shards).
- **`merge_social.py`** **[daily] [monthly]**: Concatenates mailing list + Delving into
  one flat file → `data/raw/social_combined.parquet`.
- **`governance.py`** **[daily] [monthly]**: Links BIPs to social discussion and commit
  mentions → `data/enriched/bips_refined.parquet`, `data/enriched/bips_themes.json`,
  `data/enriched/contributor_bips.json` (per-UUID BIP authorship list with title,
  status, theme, and link — consumed by `ui_artifacts.py` to populate profile shards).
- **`categorize.py`** **[monthly only]**: NLP categorization of social threads into
  technical themes → `data/enriched/social_threads.parquet`,
  `data/enriched/contributor_message_bookmarks.json` (first and last indexed social
  message per UUID — source, date, subject, link),
  `data/enriched/contributor_social_history.json` (per-UUID yearly social activity
  count by topic — consumed by `ui_artifacts.py` to populate profile shards).
  Skipped in daily runs; `influence.py` and `expertise.py` read the last monthly output.
- **`unify_contributors.py`** **[daily] [monthly]**: The "Grand Join." Merges all
  per-UUID signals (commits, reviews, social, BIPs) into one row per developer →
  `data/enriched/contributors_unified.parquet`.
- **`enrich_identity.py`** **[dormant]**: GitHub API-based profile enrichment. Not
  called by either rebuild script. Replaced by the offline `github_id_map.json`
  approach.
- **`background_enricher.py`** **[dormant]**: Rate-limit-aware runner for
  `enrich_identity.py`. Dormant with its dependency.
- **`footprint.py`** **[dormant]**: Per-contributor directory footprint from merge
  commit analysis. Never integrated into the grand join.

---

## `03_analyze/` — Stage 3: Advanced Intelligence

Heavy analytics — runs after `registry.py` bootstrap in Phase 2.

- **`review_metrics.py`** **[daily] [monthly]**: Computes review latency, ACK velocity,
  and review reciprocity → `data/enriched/contributor_review_metrics.parquet`.
  Covers both Bitcoin Core and BIPs GitHub (concatenates both PR/review parquets).
- **`influence.py`** **[daily] [monthly]**: PageRank across three eras + hybrid
  influence scoring → `data/enriched/social_stats.json`,
  `output/network/network_graph.json`.
- **`expertise.py`** **[daily] [monthly]**: Domain tags from PR labels; identifies
  "Full-Stack Architects" → `output/tracker/expertise.json`.

---

## `04_deliver/` — Stage 4: Artifact Generation

Reads from `data/enriched/`, writes lightweight JSON to `output/`.

- **`registry.py`** **[daily] [monthly]**: Syncs engagement metrics into
  `metadata/contributors.json`. Runs **twice** per pipeline: Phase 2 bootstrap (before
  Phase 3 analytics) and Phase 4 final (after the grand join).
- **`ui_artifacts.py`** **[daily] [monthly]**: Produces `output/shared/contributors/registry_index.json`
  (full registry table) and `output/shared/contributors/profiles/{uuid}.json` (deep
  profiles for all **301 high-signal contributors** — those with
  `authored_commits >= 10` OR `bips_authored > 0`). At startup the script loads four
  enriched files and embeds their data into each shard: `contributor_commit_history.json`,
  `contributor_bips.json`, `contributor_message_bookmarks.json`, and
  `contributor_social_history.json`.
- **`ecosystem_summary.py`** **[daily] [monthly]**: Computes headline numbers from the
  registry index → `output/shared/ecosystem_summary.json`.
- **`generate_regional_evolution.py`** **[monthly only]**: Formats geospatial time-series
  data for the regional dashboard. Skipped in daily runs.
- **`network_home_snapshot.py`** **[daily] [monthly]**: Builds a homepage-specific,
  quality-guarded snapshot for orange-dev-network → `output/shared/network_home_snapshot.json`.
  It enforces stricter active windows (`global_last_active`), computes period-over-period
  deltas (current 30d vs previous 30d), filters bot-like identities in reviewer rankings,
  collapses `auto_*/can_*` reviewer duplicates when slug-linked, and derives top reviewers
  from timestamped review events (30-day window only) rather than lifetime totals.

---

## `utils/` — Shared Libraries

- **`identity.py`**: Runtime resolver singleton. Every downstream script calls
  `resolver.resolve_git()`, `resolver.resolve_github()`, etc. instead of looking up
  raw identifiers. Loads `metadata/identities.json` on init.
- **`subsystem.py`**: Bitcoin protocol taxonomy resolver. Maps file paths, BIP numbers,
  and forum text to canonical technical domains using `metadata/subsystems.json`.

---

## `enrichment/` — On-Demand GitHub API Scripts

Not called by the rebuild orchestrators. Require `GITHUB_TOKEN`.

- **`fetch_github_profiles.py`**: Fetches rich profile data per contributor in
  `github_id_map.json` → `metadata/github_profiles.json`.
- **`github_top_contributors.py`**: Fetches GitHub's top-contributor rankings for
  `bitcoin/bitcoin` → `metadata/github_top_contributors.json`.
- **`generate_single_pr_input.py`**: Generates the input list for the single-PR
  enrichment workflow → `metadata/single_pr_contributors.json`.
- **`ingest_single_pr_profiles.py`**: Cross-validates single-PR profile emails and
  injects confirmed entries into `github_id_map.json`.

---

## `maintenance/` — Pipeline Maintenance Tools

Scripts for one-off data quality operations.

- **`merge_metadata.py`**: Powers the Staging Automation Workflow. Interactively processes `append_grants` and `append_locations` drafts from `metadata/staging/` to update `sponsors.json` and `locations.json` safely.
- **`generate_audit_potential_matches.py`**: Produces `metadata/audit_potential_matches.json`
  for identity curation review. Invoked via `python3 scripts/rebuild_monthly.py --audit`.
- **`reset_bedrock.py`**: Emergency recovery — purges automated identity tags and
  restores `identities.json` to the curated-seeds-only baseline.
- **`validate_commit_counts.py`**: Spot-checks our git-derived commit counts against
  the GitHub API.
- **`quick_rebuild.sh`**: Shortcut that skips ingest and identity phases, re-runs
  enrichment and delivery from `reviews.py` forward. Useful for iterating on analysis
  changes. Note: does not call `ecosystem_summary.py` — landing page numbers will be
  stale until a full rebuild.
- **`archive/`**: Investigation and debug scripts from resolved bug-fix sprints. Kept
  for reference; not intended to be run.

---

## `lab/` — Experiments and Ad-Hoc Analysis

One-off scripts, exploratory analysis, and diagnostic tools. Not called by any
rebuild orchestrator.

Notable additions from the April 2026 pipeline audit:
- **`full_pipeline_audit.py`**: End-to-end count check — traces identifiers from every
  raw source through identity resolution to the final registry.
- **`social_linkage_audit.py`**: Measures how many mailing-list and Delving users are
  linked to a GitHub identity vs isolated social-only profiles.
- **`efficiency_handles_audit.py`**: Diagnostic used to find the 4,543-handle
  unresolved identity bug in `review_metrics.py` (formerly `efficiency.py`).

---

## `legacy/`

- **`rebuild.py`**: Original monolithic rebuild script, predating the numbered-stage
  architecture. Kept for reference only.
- **`migrate_identities.py`**: One-off migration that converted the old alias-string
  format to UUID-keyed identities. Already applied.


---
# Metadata Reference

Every file in `metadata/` is either a **hand-curated input** to the pipeline (edit
carefully), a **generated output** (do not edit directly — re-run the pipeline to
refresh), or an **on-demand enrichment artifact** (run the relevant enrichment script
to update).

---

## Hand-Curated Inputs (edit these to change pipeline behavior)

### `identity_curated.json`
**Role**: Bedrock override file for the identity graph. Level 3 of the 4-level
resolution hierarchy. If a link is defined here it overrides all automated merging.

**Structure**: `{ "special_nodes": { "proxies": [...], "bots": [...] }, "aliases": [...] }`

Each entry in `aliases`:
```json
{
  "canonical_name": "Pieter Wuille",
  "github": "sipa",
  "emails": ["pieter.wuille@gmail.com", "pieter@wuille.net"],
  "aliases": ["sipa", "Pieter Wuille"]
}
```

**Current size**: ~129 entries (multi-alias people who needed hand-curation).

**How to edit**: Add or update an entry, then run `python3 scripts/identity/build_identities.py`.
Full workflow in [IDENTITY_RESOLUTION.md](IDENTITY_RESOLUTION.md).

**Consumed by**: `scripts/identity/build_identities.py`

---

### `maintainers.json`
**Role**: Manual whitelist of Bitcoin Core maintainers. Used to assign the
`maintainer` badge in `contributors.json`.

**Consumed by**: `scripts/04_deliver/registry.py`

---

### `sponsors.json`
**Role**: Tracks developer-to-funder relationships using a **Timeframe-Aware `grants` array**. Supports **Fuzzy Dates** (e.g. `2020` or `2023-05`) instead of strict ISO-8601 constraints. Powers "Funding Diversity" metrics.

```json
{
  "canonical_name": "Ava Chow",
  "grants": [
    {
      "sponsor_id": "blockstream",
      "start_date": "2019",
      "end_date": "2023-05"
    }
  ]
}
```

**Consumed by**: `scripts/04_deliver/registry.py`, `scripts/03_analyze/influence.py`

---

### `locations.json`
**Role**: Maps arbitrary profile location strings (e.g., "SF", "London", "Earth")
to standard Continent/Country regions. Human-audited.

**Consumed by**: `scripts/04_deliver/generate_regional_evolution.py`

---

### `subsystems.json`
**Role**: Centralizes the Bitcoin protocol taxonomy. Maps every domain (e.g.,
`wallet-keys`, `p2p`) to:
- `github_paths`: directories in Bitcoin Core's `src/` tree
- `bips`: BIP numbers owned by each domain
- `keywords` / `patterns`: regex for NLP categorization of social data

**Consumed by**: `scripts/utils/subsystem.py` (singleton used by every script that
needs to categorize activity).  Do not hardcode subsystem logic in individual scripts.

---


---

### `staging/` (LLM-Assisted Metadata Folder)
**Role**: A scratchpad directory (`metadata/staging/`) for LLM agents or community members to draft additions. Contains temporary JSON draft files like `sponsors_draft.json` or `locations_draft.json`.
**Schema per draft**:
```json
{
  "target": "sponsors.json",
  "operation": "append_grants",
  "updates": [...]
}
```
**Consumed by**: `scripts/maintenance/merge_metadata.py` which reads these drafts and interactively merges the `append_grants` or `append_locations` into the canonical `metadata/sponsors.json` and `metadata/locations.json`.

## Generated Outputs (do not edit — re-run pipeline to refresh)

### `identities.json`
**Role**: Master UUID lookup table. The output of `build_identities.py`. Every
downstream script resolves raw names/emails/logins through this file via
`scripts/utils/identity.py`.

**Current size**: ~7,659 unique identities (122 `can_`, 7,537 `auto_`).

**Written by**: `scripts/identity/build_identities.py` (runs in every pipeline build).

**Consumed by**: Every script in `02_process/`, `03_analyze/`, `04_deliver/`.

**Schema per entry**:
```json
{
  "uuid": "can_pieter_wuille",
  "display_name": "Pieter Wuille",
  "sources": ["corecommit", "prgithub", "mailinglist"],
  "git_signatures": { "names": [...], "emails": [...] },
  "github": { "login": "sipa", "github_id": "548488" },
  "delving_username": null
}
```

---

### `contributors.json`
**Role**: The "Encyclopedia" of all known contributors. Holds roles, badges, first/last
seen timestamps, and sponsor data for the full contributor set. This is the registry
that the frontend reads for rich profile data.

**Written by**: `scripts/04_deliver/registry.py` (runs twice per pipeline — Phase 2
bootstrap + Phase 4 final).

**Consumed by**: `scripts/03_analyze/influence.py`, `scripts/03_analyze/review_metrics.py`,
`scripts/02_process/unify_contributors.py`.

---

### `github_id_map.json`
**Role**: Pre-computed mapping of `github_id → email`, built from offline PR archives.
Provides high-quality email anchors to `build_identities.py` without relying on live
API calls. Entries require either curated corroboration or a minimum of 2 independent
PR observations to be included.

**Written by**: `scripts/identity/build_github_id_map.py` (runs every monthly build).

**Consumed by**: `scripts/identity/build_identities.py`.

**Schema per entry**:
```json
{
  "github_id": "548488",
  "login": "sipa",
  "emails": [
    {
      "email": "pieter@wuille.net",
      "email_type": "real",
      "source": "curated",
      "corroboration_count": null,
      "example_sha": null,
      "example_pr": null
    }
  ]
}
```

---

### `audit_potential_matches.json`
**Role**: Human-review report for identity curation. Surfaces unlinked social users and
fuzzy name collision candidates. Used to decide what to add to `identity_curated.json`.

**Written by**: `scripts/maintenance/generate_audit_potential_matches.py`, invoked via
`python3 scripts/rebuild_monthly.py --audit`.

**Consumed by**: Humans only — no pipeline script reads this file.

**Structure**:
```json
{
  "summary": { "delving_mapped_to_identity": ..., "ml_mapped_to_identity": ... },
  "delving_github_candidates": [...],
  "fuzzy_name_matches": [...]
}
```

---

## On-Demand Enrichment Artifacts (require GitHub API / manual enrichment runs)

### `github_profiles.json`
**Role**: Rich GitHub profile data (name, company, blog, bio, public email) fetched via
the GitHub REST API for every contributor in `github_id_map.json`.

**Written by**: `scripts/enrichment/fetch_github_profiles.py --token $GITHUB_TOKEN`
(run manually when you want to refresh profile metadata).

**Consumed by**: `scripts/maintenance/generate_audit_potential_matches.py` (reads public
emails to help surface Delving→GitHub linkage candidates).

---

### `github_top_contributors.json`
**Role**: Top contributors to `bitcoin/bitcoin` ranked by GitHub's contributions count.
Used as a cross-reference sanity check against our git-derived commit counts.

**Written by**: `scripts/enrichment/github_top_contributors.py --token $GITHUB_TOKEN`

**Consumed by**: `scripts/maintenance/validate_commit_counts.py`

---

### `single_pr_contributors.json`
**Role**: Input file listing `github_id`s of contributors whose commit email appeared
in only one PR — below the `CORROBORATION_MIN=2` threshold for automatic inclusion in
`github_id_map.json`. Used as input to the single-PR enrichment workflow.

**Written by**: `scripts/enrichment/generate_single_pr_input.py`

**Used by**: The three-step single-PR enrichment workflow — see
[IDENTITY_RESOLUTION.md](IDENTITY_RESOLUTION.md) for the full sequence.

---

## `data/cache/`

### `enrichment_cache.json`
Stores results of expensive API lookups to avoid redundant calls across runs.
Written and read by `scripts/02_process/enrich_identity.py` (currently dormant —
see IDENTITY_RESOLUTION.md).

### `aliases_lookup.json`
Flattened index of the identity registry for fast name resolution during heavy
ingestion passes. Generated at resolver startup if not present.

---

## `output/shared/contributors/` — Frontend Delivery Artifacts

Written by `scripts/04_deliver/ui_artifacts.py`. These files are the direct data
source for the `orange-dev-network` and `orange-dev-tracker` frontends.

### `registry_contributors.parquet` ⭐ Primary directory data file
**Role**: Flat, Snappy-compressed columnar table of all ~7,400 contributors. This
is the main data file loaded by `directory.js` (replaces the old 10 MB
`registry_index.json` — now only ~850 KB).

**Written by**: `scripts/04_deliver/ui_artifacts.py`

**Consumed by**: `orange-dev-network/js/directory.js` via
[hyparquet](https://www.npmjs.com/package/hyparquet) (`import { parquetRead } from
'https://cdn.jsdelivr.net/npm/hyparquet@1.17.1/+esm'`).

**Nested column encoding**: Parquet is a flat columnar format. Five fields that are
dicts or lists in the source data are **JSON-stringified** when writing parquet and
**JSON.parse()'d** in JavaScript after loading. They are listed in
`registry_metadata.json → metadata.parquet_nested_cols`:

| Column | Original Python type | Parquet type | JS after parse |
|---|---|---|---|
| `expertise_domain_scores` | `dict[str, float]` | `STRING` (JSON) | `Object` |
| `expertise_domains` | `list[str]` | `STRING` (JSON) | `Array` |
| `expertise_by_source` | `dict[str, dict]` | `STRING` (JSON) | `Object` |
| `roles` | `list[str]` | `STRING` (JSON) | `Array` |
| `github` | `dict` | `STRING` (JSON) | `Object` |

All other columns (30+ numeric/float fields, string IDs, dates) are stored natively.
`null` Python values become parquet nulls; `None` in nested fields becomes the
JSON string `"null"` (JS: `null`, safely handled by existing `|| {}` / `|| []` guards).

**Why not change `registry_index.json`?**: `registry_index.json` is kept for backward
compatibility. It's still written by the pipeline but is no longer the primary path
for `directory.js`.

---

### `registry_metadata.json` — Tiny metadata companion
**Role**: Lightweight sidecar to `registry_contributors.parquet`. Loaded first (< 3 KB)
to bootstrap domain maps and supply the column schema for parquet reconstruction.

**Schema**:
```json
{
  "metadata": {
    "count": 7411,
    "generated_at": "2026-05-29T...",
    "sharded_count": 7411,
    "domains": [ { "id": "Consensus", "name": "...", "color": "..." }, ... ],
    "parquet_columns": ["uuid", "display_name", ...],
    "parquet_nested_cols": ["expertise_by_source", "expertise_domain_scores", "expertise_domains", "github", "roles"]
  }
}
```

`parquet_columns` declares the exact column order in the parquet file — the JS uses
this to convert each positional row array from hyparquet back to a named object.
`parquet_nested_cols` declares which of those columns need `JSON.parse()`.

---

### `registry_index.json` — Legacy full registry (backward compat)
**Role**: The original combined file (`{metadata, contributors: [...]}`). Still written
each pipeline run. No longer the primary path for `directory.js` (superseded by parquet
+ metadata split), but retained in case other tooling needs it.

**Size**: ~13 MB (formatted) / ~10 MB (raw). Do not add new frontend consumers — use
the parquet instead.

---

### `profiles/` — Per-contributor profile shards
**Role**: One JSON file per contributor (e.g. `can_pieter_wuille.json`). Fetched
on-demand by `profile.js` when a user opens a profile. Each shard contains the full
contributor record plus enriched data (commit history, BIP list, social bookmarks,
social history). Average shard size ~1–3 KB.

**Written by**: `scripts/04_deliver/ui_artifacts.py`

**Consumed by**: `orange-dev-network/js/profile.js`

