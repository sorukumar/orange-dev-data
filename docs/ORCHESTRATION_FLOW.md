# Pipeline Orchestration Flow

This document provides a visual and diagrammatic breakdown of the difference between the **Daily** (`scripts/rebuild_daily.py`) and **Monthly** (`scripts/rebuild_monthly.py`) pipelines.

## 1. The Daily Pipeline (Incremental & Fast)
The daily pipeline is optimized for speed. It is triggered by GitHub Actions every night. It strictly avoids any natural language processing, graphing algorithms, or parsing of massive historical archives (like the mailing list).

```mermaid
graph TD
    classDef sync fill:#2A4B7C,stroke:#0f1a2e,stroke-width:2px,color:#fff
    classDef fast fill:#226D54,stroke:#0d3629,stroke-width:2px,color:#fff
    classDef output fill:#DE8D31,stroke:#8c561b,stroke-width:2px,color:#fff
    classDef disabled fill:#333333,stroke:#111,stroke-width:1px,color:#666,stroke-dasharray: 5 5

    %% PHASE 0
    subgraph Phase0 ["Phase 0: Raw Data Sync (Git Pulls)"]
        repo_btc[bitcoin core]:::sync
        repo_bips[bips_repo]:::sync
        repo_delving[delving forum]:::sync
    end

    %% PHASE 1
    subgraph Phase1 ["Phase 1: Incremental Core Analysis"]
        repo_btc --> ingest_core["scripts/core/ingest.py\n(Extracts new commits -> Parquet)"]:::fast
        ingest_core --> enrich["scripts/core/enrich.py\n(Hits GitHub API for missing IDs via Cache)"]:::fast
        ingest_core --> extract_rev["scripts/extract_reviewers.py\n(Scores fresh ACKs on PRs)"]:::fast
        ingest_core --> process_core["scripts/core/process.py\n(Pandas: Updates Churn & Retention Metrics)"]:::fast
    end

    %% PHASE 2 
    subgraph Phase2 ["Phase 2 & 2.5: Fast Social & Merging"]
        repo_bips --> ingest_bips["scripts/ingest_bips.py\n(Tracks new BIP statuses)"]:::fast
        repo_delving --> ingest_delving["scripts/ingest_delving.py\n(Tracks new Discourse posts)"]:::fast
        
        mailing_list["scripts/ingest_mailing_list.py\n(Massive email archive)"]:::disabled

        ingest_bips --> unify["scripts/merge_data.py\n& enrich_governance.py\n(Maps new handles to Canonical IDs)"]:::fast
        ingest_delving --> unify
    end

    %% PHASE 3
    subgraph Phase3 ["Phase 3: Deep Intelligence"]
        nlp["scripts/categorize_threads.py\n(NLP/Topic Modeling)"]:::disabled
        pagerank["scripts/influence_hubs.py\n(PageRank Graph Math)"]:::disabled
        expertise["scripts/map_expertise.py\n(Expertise matching)"]:::disabled
    end

    %% PHASE 4
    subgraph Phase4 ["Phase 4: Dashboard Generation"]
        process_core --> gen_ui["scripts/generate_ui_artifacts.py\n(Slices fast parquets into JSON)"]:::output
        unify --> gen_ui
        
        gen_ui --> static_json[("data/core/*.json\n(Pushed to GitHub Pages)")]
    end
```

---

## 2. The Monthly Pipeline (Deep Analytics)
The monthly pipeline runs locally on your machine. It executes *everything* the daily pipeline does, plus the computationally expensive "Phase 3" operations. This updates the complex PageRank topologies and NLP classifications.

```mermaid
graph TD
    classDef sync fill:#2A4B7C,stroke:#0f1a2e,stroke-width:2px,color:#fff
    classDef fast fill:#226D54,stroke:#0d3629,stroke-width:2px,color:#fff
    classDef heavy fill:#7A2828,stroke:#4a1717,stroke-width:2px,color:#fff
    classDef output fill:#DE8D31,stroke:#8c561b,stroke-width:2px,color:#fff

    %% PHASE 0 & 1
    subgraph Quick_Ingestion ["Phase 0 & 1: Source & Core"]
        raw[Sync Git Repos]:::sync --> core_processing[core/ingest.py,\ncore/enrich.py,\ncore/process.py]:::fast
    end

    %% PHASE 2
    subgraph Phase2 ["Phase 2/2.5: Social Ingestion"]
        bips[ingest_bips.py]:::fast
        delving[ingest_delving.py]:::fast
        mailing["scripts/ingest_mailing_list.py\n(Parses 15+ years of emails)"]:::heavy
        
        bips --> merging["merge_data.py"]:::fast
        delving --> merging
        mailing --> merging
    end

    %% PHASE 3
    subgraph Phase3 ["Phase 3: Advanced Network Intelligence"]
        merging --> nlp["scripts/categorize_threads.py\n(Applies Regex/NLP to tag conversations)"]:::heavy
        merging --> pagerank["scripts/influence_hubs.py\n(Calculates Network Topologies)"]:::heavy
        nlp --> expertise["scripts/map_expertise.py\n(Finds Domain Experts based on tags)"]:::heavy
        core_processing --> geo["scripts/generate_regional_evolution.py\n(Calculates global continent shifts)"]:::heavy
    end

    %% PHASE 4
    subgraph Phase4 ["Phase 4: UI & Viz Generation"]
        core_processing --> ui_gen["scripts/generate_ui_artifacts.py"]:::output
        pagerank --> ui_gen
        expertise --> ui_gen
        geo --> ui_gen
        
        ui_gen --> data_json[("data/**/*.json\nUpdates UI and Network graphs")]
    end
```

## Summary of Actionable Differences
* **`rebuild_daily.py`** gets the user their immediate PR, Commit, and new Discussion metrics instantly by skipping `mail`, `nlp`, and `topologies`.
* **`rebuild_monthly.py`** is the master generator that resets the global "Expertise", "Influence Graph", and "NLP Themes".
