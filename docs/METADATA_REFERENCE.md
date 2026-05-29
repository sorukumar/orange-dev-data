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
**Role**: Tracks developer-to-funder relationships (Chaincode, Brink, Blockstream,
etc.). Powers the "Funding Diversity" metrics.

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
