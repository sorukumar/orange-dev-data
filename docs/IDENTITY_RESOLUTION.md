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
All names and emails are lowercased, stripped, and normalized before comparison.
Generic hubs (`unknown`, `bot`, empty strings) are hard-blocked and can never act
as bridges between two different people.

### Level 3: Human Curation (The Bedrock)
**Source**: `metadata/identity_curated.json`

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
