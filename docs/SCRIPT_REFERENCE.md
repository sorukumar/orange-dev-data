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
