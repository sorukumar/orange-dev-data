# Pipeline Orchestration Flow

This document details the execution cycles of the Orange Dev Data Engine. The pipeline is split into **Daily** and **Monthly** flows to balance the need for fresh metrics with the high computational cost of NLP and graph theory.

---

## 1. The Daily Pipeline (Incremental & Fast)
The daily pipeline is optimized for speed and is triggered automatically by GitHub Actions. It focuses on repository updates and new contribution tracking while bypassing heavy "thought-space" analytics.

```mermaid
graph TD
    classDef sync fill:#2A4B7C,stroke:#0f1a2e,stroke-width:2px,color:#fff
    classDef fast fill:#226D54,stroke:#0d3629,stroke-width:2px,color:#fff
    classDef output fill:#DE8D31,stroke:#8c561b,stroke-width:2px,color:#fff
    classDef disabled fill:#333333,stroke:#111,stroke-width:1px,color:#666,stroke-dasharray: 5 5

    %% PHASE 0
    subgraph Phase0 [Phase 0 - Sync]
        repo_btc[Bitcoin Core Repo]:::sync
        repo_bips[BIPs Repo]:::sync
        repo_delving[Delving Archive]:::sync
    end

    %% STAGE 1
    subgraph Stage1 [Stage 1 - Extraction]
        repo_btc --> ingest_core["scripts/core/ingest.py\n(core_commits.parquet)"]:::fast
        repo_bips --> ingest_bips["scripts/ingest_bips.py\n(bips.parquet)"]:::fast
        repo_delving --> ingest_delv["ingest_delving.py\n(social_delving.parquet)"]:::fast
    end

    %% STAGE 2
    subgraph Stage2 [Stage 2 - Convergence]
        ingest_core --> enrich_core["scripts/core/enrich.py\n(Resolves Identities)"]:::fast
        ingest_delv --> merge["scripts/merge_data.py"]:::fast
        merge --> enrich_gov["enrich_governance.py"]:::fast
    end

    %% STAGE 3
    subgraph Stage3 [Stage 3 - Intelligence]
        nlp["scripts/categorize_threads.py\n(NLP)"]:::disabled
        pagerank["scripts/influence_hubs.py\n(Graph Math)"]:::disabled
    end

    %% STAGE 4
    subgraph Stage4 [Stage 4 - Artifacts]
        enrich_core --> sync["scripts/sync_registry.py\n(Master Registry Update)"]:::output
        enrich_gov --> gen_ui["scripts/generate_ui_artifacts.py"]:::output
        sync --> output[("output/tracker/*.json")]
        gen_ui --> output
    end
```

---

## 2. The Monthly Pipeline (Deep Analytics)
The monthly pipeline runs locally on an Anaconda environment. It performs a full refresh of the Bitcoin "Intelligence Graph," including PageRank influence and expertise fingerprinting.

```mermaid
graph TD
    classDef sync fill:#2A4B7C,stroke:#0f1a2e,stroke-width:2px,color:#fff
    classDef fast fill:#226D54,stroke:#0d3629,stroke-width:2px,color:#fff
    classDef heavy fill:#7A2828,stroke:#4a1717,stroke-width:2px,color:#fff
    classDef output fill:#DE8D31,stroke:#8c561b,stroke-width:2px,color:#fff

    %% INGESTION
    subgraph Ingestion [Ingestion & Convergence]
        raw[Sync Sources]:::sync --> stage1[All ingest scripts]:::fast
        stage1 --> stage2[Merge & Enrich]:::fast
    end

    %% ANALYTICS
    subgraph Analytics [Advanced Analytics]
        stage2 --> nlp["scripts/categorize_threads.py\n(NLP Topic Modeling)"]:::heavy
        stage2 --> pagerank["scripts/influence_hubs.py\n(Global PageRank Rank)"]:::heavy
        nlp --> map["scripts/map_expertise.py\n(BIPs <-> Commits Linking)"]:::heavy
    end

    %% DELIVERY
    subgraph Delivery [Final Delivery]
        pagerank --> sync_all["scripts/sync_registry.py"]:::output
        map --> sync_all
        sync_all --> artifacts["generate_ui_artifacts.py"]:::output
        artifacts --> last_mile[("output/tracker/*.json\noutput/network/*.json")]
    end
```

## Critical Orchestration Differences
*   **Daily**: Prioritizes "What happened today?" (Commits, New BIPs, Fresh Discussions).
*   **Monthly**: Prioritizes "How has the community changed?" (Who are the influencers? Who are the new experts? How are themes evolving?).
