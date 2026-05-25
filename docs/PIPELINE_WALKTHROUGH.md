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
| `mailing_list.py` | `data/sources/mailing_list/` local archive | `data/raw/social_mailing_list.parquet` | `author_name`, `author_email` |
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
