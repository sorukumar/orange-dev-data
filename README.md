# 🟧 Orange Dev Data Hub

The open-source Data Hub for the Bitcoin development ecosystem. This repository provides unified ingestion and forensic analytics for Bitcoin Core (git-logs), BIPs, Mailing Lists, and Delving Bitcoin research.

---

## 🐍 Mandatory Infrastructure
To ensure data integrity and full support for the analytical pipeline, all scripts **MUST** be run using the Anaconda environment:

```bash
# Core execution path for AI and Humans:
/opt/anaconda3/bin/python3 scripts/rebuild_daily.py
```

## 🏗️ The Numbered Chain Pipeline
The scripts are organized by functional stage to maintain a clean **Sources → Raw → Enriched → Output** lifecycle:

1.  **`01_ingest/`**: Raw extraction from Git mirrors and Discourse APIs.
2.  **`02_process/`**: Identity resolution, social merging, and technical categorization.
3.  **`03_analyze/`**: Global PageRank influence and expertise fingerprinting.
4.  **`04_deliver/`**: Final public artifact generation for the UI dashboards.

## 🚀 Getting Started

### Daily Update (Fast)
```bash
/opt/anaconda3/bin/python3 scripts/rebuild_daily.py
```

### Monthly Rebuild (Deep NLP & Graphs)
```bash
/opt/anaconda3/bin/python3 scripts/rebuild_monthly.py
```

## 📂 Documentation
For detailed architectural maps, reference the `/docs` folder:
- [**Architecture**](docs/ARCHITECTURE.md): Three-tier system overview, data lifecycle, shared utilities.
- [**Pipeline Walkthrough**](docs/PIPELINE_WALKTHROUGH.md): Step-by-step script reference — inputs, outputs, counts, daily vs monthly diff.
- [**Identity Resolution**](docs/IDENTITY_RESOLUTION.md): 4-level resolution hierarchy, `build_identities.py` mechanics, curation workflow, audit procedures.
- [**Metadata Reference**](docs/METADATA_REFERENCE.md): Schema and ownership of every file in `metadata/`.
- [**Script Reference**](docs/SCRIPT_REFERENCE.md): Index of all pipeline scripts with folder, cadence, and one-line purpose.
- [**Environment**](docs/ENVIRONMENT.md): Python & Anaconda configuration.
