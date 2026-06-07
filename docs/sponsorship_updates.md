# Sponsorship Data Maintenance Guide

This document outlines the logical path for updating and expanding the sponsorship data in the Orange Dev Tracker.

## Methodology

We follow a tiered approach to classify developers, moving from zero-cost automation to targeted research.

### 1. Zero-Token: Domain Auto-Mapping
The first step is to identify contributors whose email domains match known corporate or academic sponsors.

- **Action**: Run `python3 analysis/auto_map_domains.py`.
- **Logic**: This script checks the `commits.parquet` for any email domain (e.g., `@brink.dev`, `@chaincode.com`) that matches the `domains` list in `sponsors_lookup.json`.
- **Output**: It generates `proposed_domain_matches.json`.
- **Note**: Always verify these results before committing, as some domains might be shared or legacy.

### 2. Targeted Identification: High-Impact Contributors
We prioritize research on the most active contributors.

- **Action**: Run `python3 analysis/identify_top_devs.py`.
- **Logic**: Filters for developers active in the last 3 years and the top 25 historical contributors.
- **Output**: `analysis/research_targets.csv`.
- **Rule**: Focus on researchers marked as `already_sponsored: False`.

### 3. Verification: Evidence-Based Research
For identified targets, perform targeted web research using the Browser Subagent.

- **Sources to Check**:
    - [Brink Grantees](https://brink.dev/about)
    - [Spiral Grants](https://spiral.xyz/grants)
    - [OpenSats LTS Grants](https://opensats.org/projects/bitcoin-core-lts-grants)
    - [Chaincode Team](https://chaincode.com/team)
    - [Btrust Africa](https://btrust.africa/)
    - Personal Twitter (X) profiles or GitHub bios.
- **Rule of Thumb**: Only classify as "sponsored" if there is a public record of funding. Use "affiliated" for corporate employees where the specific funding for Bitcoin Core work is less clear.

### 4. Data Commitment: The Ledger System
Never update sponsorship without logging the evidence.

- **Update `data/cache/sponsors_lookup.json`**:
    - Add the developer to the `sponsored_developers` array.
    - Fields: `canonical_name`, `github` (if known), `emails` (optional), `sponsor_id`, `status` (active/emeritus), `notes`.
- **Update `data/cache/sponsors_evidence.json`**:
    - Every manual entry MUST have a corresponding entry here.
    - Fields: `canonical_name`, `sponsor_id`, `source_url`, `verification_date`, `notes`.

## Key Rules
1. **No Manual Deduplication**: Rely on `canonical_id` or consistent `canonical_name`. Do not merge identities manually unless the identity mapping system handles it.
2. **Preserve History**: If a developer moves (e.g., Ava Chow from Blockstream to Brink), update their current classification but document the transition in `notes`.
3. **Canonical Status**: Use the name exactly as it appears in `contributors_rich.json` to ensure clean joins for the dashboard.

## Technical Rebuild
After updating JSON files, run the rebuild script to update statistics:
```bash
python3 code/core/rebuild.py
```
*(Note: Ensure paths are correct based on the root directory)*
