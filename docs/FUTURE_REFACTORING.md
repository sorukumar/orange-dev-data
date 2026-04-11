# 🟧 Orange Dev Data: Refactoring Strategy (V3 Proposal)

To ensure that the next refactoring is successful and doesn't break the downstream dashboards, we should move from a "Big Bang" migration to a **Path-Invariant Refactoring** strategy.

---

## 🏗️ Proposed Directory Structure

The goal is to separate **Internal Logic** (Source) from **Intermediate State** (Data) and **Public Interface** (Dist).

### 1. Repository Layout
```bash
orange-dev-data/
├── data/                   # EXCLUSIVELY FOR STATE (No scripts here)
│   ├── raw/                # Unmodified source logs (git, mbox, api)
│   ├── stage/              # Transformed parquet/feather files
│   └── cache/              # LLM results, geocode lookups, etc.
├── metadata/               # HUMAN INTELLIGENCE (The "Source of Truth")
│   ├── identities/         # identities.json, aliases.json
│   ├── sponsors/           # corporate_mappings.json
│   └── context/            # maintainers.json, project_roles.json
├── src/                    # THE ENGINE (Pure Python Packages)
│   ├── orange_dev/         # Root package
│   │   ├── core/           # Path resolution, config, logging
│   │   ├── ingest/         # Harvesters (Git, Social, Mailing List)
│   │   ├── transform/      # enrichment, categorization, merging
│   │   └── analytics/      # Metric calculation & ECharts formatting
├── scripts/                # RUNNERS (Thin wrappers around src/)
│   └── rebuild.py          # The main entrance point
├── dist/                   # THE PUBLIC CONTRACT (UI Artifacts)
│   ├── tracker/            # JSONs for the main dashboard
│   └── network/            # network_graph.json
├── README.md               # Setup and usage
└── requirements.txt        # Dependencies
```

---

## 🤝 The Public Contract (The "Zero-Break" Rule)

The biggest failure point in V2 was deleting the legacy `data/core` and `data/governance` folders. In V3, we maintain **Path Invariance**:

1.  **Dist Folder**: Create a `/dist` directory for the new system artifacts.
2.  **Legacy Mirroring**: During the migration phase, the `export` scripts should write to **BOTH** the new `/dist` paths and the legacy `data/core/` paths.
3.  **Frontend Update**: Only update the Tracker and Network Graph to point to `/dist` **after** verifying that the mirrored data in the legacy folders is visually correct in the existing UI.

---

## 🚀 Recommended Approach: The "Shadow Run" Method

To avoid breakage, I recommend this 4-phase rollout:

### Phase 1: Structural Setup (Code Only)
*   Create the `/src` package structure.
*   Move logic from `scripts/` into `/src/orange_dev` without changing any logic or output paths.
*   **Success Metric**: `rebuild.py` still generates the exact same files in the exact same `data/` folders.

### Phase 2: Metadata Centralization
*   Consolidate `lookups/` into the `/metadata` folder. 
*   Update `src/core/paths.py` to handle the transition (providing a single source of truth for all paths).

### Phase 3: Shadow Data Run
*   Implement the new data processing pipeline in `src/analytics/`.
*   Output the results to a **temporary** directory (e.g., `data/shadow_test/`).
*   Run a comparison script to verify that the numbers in `shadow_test` match the legacy `data/core` metrics.

### Phase 4: Frontend Pivot
*   Update `orange-dev-tracker` to point to a new branch of `orange-dev-data`.
*   Once validated, merge both and point production to the new `/dist` location.

---

## 🛠️ Verification Checklist for Next Time
*   [ ] **Parity Check**: Do the bubble sizes in the Galaxy chart match exactly?
*   [ ] **Path Abstraction**: Is `DATA_PATH_PREFIX` the only place where paths are defined in the JS?
*   [ ] **Incremental Persistence**: Does the new `rebuild.py` correctly respect `state.json` to avoid full reprocessing of mailing lists?

By treating the data structure as an **API Contract**, we ensure that the Tracker and Network Graph stay stable even if the internal Python code is completely rewritten.
