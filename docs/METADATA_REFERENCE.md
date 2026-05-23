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
