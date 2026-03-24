# Refactoring Tracker: Orange Dev Data v2

This document tracks the migration of scripts and data from the legacy "flat" structure to the new "Pipeline Flow" architecture.

## 🏁 Progress Overview
- [x] **Infrastructure**: `src/core/paths.py` created. [DONE]
- [x] **Lookups**: Migrate `lookups/` to `metadata/`. [DONE]
- [x] **Pilot Channel**: Refactor BIPs (Ingest → Transform → Export). [DONE]
- [x] **Main Pipeline**: Refactor Bitcoin Core & Social (Delving/Mailing List). [DONE]
- [x] **Orchestration**: Update `rebuild.py` to point to `src/`. [DONE]

---

## 🛠️ Script Migration Map

| Original Script | Stage (New src/ path) | Status |
| :--- | :--- | :--- |
| `scripts/ingest_bips.py` | `src/ingest/bips.py` | ✅ Done |
| `scripts/ingest_delving.py` | `src/ingest/social_delving.py` | ✅ Done |
| `scripts/ingest_mailing_list.py` | `src/ingest/social_mailing_list.py` | ✅ Done |
| `scripts/merge_data.py` | `src/transform/merge_social.py` | ✅ Done |
| `scripts/enrich_governance.py` | `src/transform/enrich_governance.py` | ✅ Done |
| `scripts/generate_ui_artifacts.py` | `src/export/tracker_ui.py` | ✅ Done |
| `scripts/core/ingest.py` | `src/ingest/bitcoin_repo.py` | ✅ Done |

---

## 📂 Data Migration Map

| Legacy Data | New Location | Status |
| :--- | :--- | :--- |
| `lookups/` | `metadata/` | ✅ Done |
| `data/raw/` | `data/raw/` | ✅ Preserved |
| `data/core/` | `data/work/core/` | ✅ Done |
| `data/viz/` | `output/network/` | ✅ Done |
| `data/governance/` | `output/tracker/` | ✅ Done |

---

## 🧪 Legacy Support
Until the `orange-dev-tracker` and `orange-dev-network` apps are updated to point to `output/`, the pipeline will continue to mirror JSON files to the old `data/` folder for backward compatibility.
