# Orange Dev Pipeline: Architecture & Master Definitions

This document serves as the master reference for AI assistants working on the `orange-dev-data` pipeline. It outlines critical structural definitions and calculation boundaries that must be strictly adhered to.

## 1. Time Boundary Standardization (T-1)

To prevent partial-day data from artificially deflating rolling window metrics (e.g., sudden artificial drops in 7-day or 30-day commit volumes):

- **Anchor Cutoff**: The internal `now` or `anchor` timestamp used for all calculations must be explicitly set to the end of the *previous* full day (`(datetime.now() - timedelta(days=1)).replace(hour=23, minute=59, second=59)`).
- **Date Export**: When exporting the pipeline generation date (`generated_at`), the string must represent the T-1 date (`(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")`).
- **UI Labeling**: Frontends consuming this data (`orange-dev-tracker`, `orange-dev-network`) should explicitly label freshness as **"Data as of: [Date]"** rather than "Updated: [Date]".

## 2. Core vs Ecosystem Definitions

The distinction between "Core" and "Ecosystem" is standardized across all engineering metrics (Commits, Contributors, PRs Merged).

- **Core Repositories**: The "Core" bucket is explicitly limited to the following three foundational repositories:
  1. `bitcoin/bitcoin` (Bitcoin Core)
  2. `bitcoin-core/gui` (Bitcoin Core GUI)
  3. `bitcoin-core/secp256k1` (libsecp256k1)
  
  *Note: Because forks (like GUI) contain identical commit hashes as the main repo, backend calculations MUST use a set-based union of commit hashes across all three repositories to prevent double-counting.*

- **Ecosystem Repositories**: The "Ecosystem" bucket represents the other tracked repositories in the broader Bitcoin open-source ecosystem (e.g., `guix.sigs`, `HWI`, `qa-assets`, ...).
