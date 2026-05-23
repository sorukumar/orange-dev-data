#!/usr/bin/env python3
"""
generate_single_pr_input.py
===========================
Generates metadata/single_pr_contributors.json — an input file in the same
format as github_id_map.json — containing github_ids for contributors whose
email was seen in only one PR (dropped by CORROBORATION_MIN=2).

Used as input to:
    python3 scripts/enrichment/fetch_github_profiles.py \
        --input metadata/single_pr_contributors.json \
        --output metadata/github_profiles_single_pr.json \
        --token $GITHUB_TOKEN

The fetched profiles expose the public 'email' field from the GitHub profile
page, which (if set) gives us a verified email → github_id link independent
of commit metadata.

Run from repo root:
    python3 scripts/maintenance/generate_single_pr_input.py
"""

import json
import glob
import os
import re
from collections import defaultdict
from datetime import datetime, timezone

# ── Paths ──────────────────────────────────────────────────────────────────────

MAP_PATH    = "metadata/github_id_map.json"
OUTPUT_PATH = "metadata/single_pr_contributors.json"

PR_DIRS = [
    "data/sources/bitcoin-github-metadata/pulls",
    "data/sources/bips-github-metadata/pulls",
]

# ── Helpers ────────────────────────────────────────────────────────────────────

_GITHUB_RE  = re.compile(r"@(?:users\.noreply\.)?github\.com$", re.I)

def _is_real(email: str) -> bool:
    return bool(email and email.strip() and not _GITHUB_RE.search(email.strip()))


def _load_proxy_set(pr_dirs: list, min_merges: int = 5) -> set:
    """Derive proxy logins by counting merged-event actors (same logic as build_github_id_map.py)."""
    merger_counts: dict = {}
    for pr_dir in pr_dirs:
        for fpath in glob.glob(os.path.join(pr_dir, "*.json")):
            try:
                with open(fpath) as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue
            merge_ev = next(
                (ev for ev in data.get("events", []) if ev.get("event") == "merged"),
                None,
            )
            if merge_ev:
                actor = (merge_ev.get("actor") or {}).get("login") or ""
                if actor:
                    merger_counts[actor] = merger_counts.get(actor, 0) + 1
    return {login.lower() for login, cnt in merger_counts.items() if cnt >= min_merges}


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    # Load the set of github_ids that already have a corroborated real email
    with open(MAP_PATH) as f:
        map_data = json.load(f)
    ids_with_real = {
        e["github_id"]
        for e in map_data.get("entries", [])
        if any(m["email_type"] == "real" for m in e.get("emails", []))
    }
    print(f"github_id_map.json: {len(ids_with_real)} IDs already have a real email")

    # Derive proxy set from merged events
    print("Deriving proxies from merged events...")
    proxies = _load_proxy_set(PR_DIRS)
    print(f"  {len(proxies)} proxies: {', '.join(sorted(proxies))}")

    # Mine single-PR real emails
    print("Mining PR JSONs for single-PR contributors...")
    raw_hits: dict  = defaultdict(list)   # (github_id, email) -> [pr_number]
    id_to_login: dict = {}

    for pr_dir in PR_DIRS:
        for fpath in sorted(glob.glob(os.path.join(pr_dir, "*.json"))):
            pr_number = int(os.path.basename(fpath).replace(".json", ""))
            try:
                with open(fpath) as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue
            pull      = data.get("pull", {})
            user      = pull.get("user", {})
            login     = (user.get("login") or "").strip()
            github_id = str(user.get("id") or "")
            head_sha  = (pull.get("head", {}).get("sha") or "").strip()

            if not github_id or not login or not head_sha:
                continue
            if login.lower() in proxies:
                continue
            id_to_login[github_id] = login

            for ev in data.get("events", []):
                if ev.get("event") != "committed" or ev.get("sha") != head_sha:
                    continue
                email = (ev.get("author", {}).get("email") or "").strip().lower()
                if _is_real(email):
                    raw_hits[(github_id, email)].append(pr_number)
                break

    # Select: exactly 1 PR of evidence AND not already in the real-email map
    seen_ids: set = set()
    entries: list = []

    for (github_id, email), pr_numbers in sorted(raw_hits.items()):
        if len(pr_numbers) != 1:
            continue
        if github_id in ids_with_real:
            continue
        if github_id in seen_ids:
            continue
        seen_ids.add(github_id)
        entries.append({
            "github_id": github_id,
            "login":     id_to_login[github_id],
            # Store the candidate email so fetch_github_profiles output can be
            # cross-checked: if profile.email matches this, it's confirmed.
            "candidate_email": email,
            "emails": [],          # empty — no corroborated email yet
        })

    print(f"  {len(entries)} single-PR contributors identified")

    output = {
        "generated":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "description": "Single-PR contributors: candidate email seen in exactly 1 PR. "
                       "Use with fetch_github_profiles.py to retrieve their public GitHub "
                       "profile email for cross-validation.",
        "total_entries": len(entries),
        "entries": entries,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH) or ".", exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Wrote {OUTPUT_PATH}")
    print()
    print("Next step:")
    print(f"  python3 scripts/enrichment/fetch_github_profiles.py \\")
    print(f"      --input {OUTPUT_PATH} \\")
    print(f"      --output metadata/github_profiles_single_pr.json \\")
    print(f"      --token $GITHUB_TOKEN")


if __name__ == "__main__":
    main()
