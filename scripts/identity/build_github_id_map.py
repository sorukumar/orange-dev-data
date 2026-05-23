#!/usr/bin/env python3
"""
build_github_id_map.py
======================
Pre-computes a high-quality github_id → email mapping from all offline PR JSON
archives and the hand-curated identity table.

Output: metadata/github_id_map.json

Run monthly, before build_identities.py:
    python scripts/identity/build_github_id_map.py

Schema per entry:
    {
        "github_id"  : "548488",
        "login"      : "sipa",
        "emails": [
            {
                "email"               : "pieter@wuille.net",
                "email_type"          : "real",        # real | noreply
                "source"              : "curated",     # curated | head_sha_event
                "corroboration_count" : null,          # int for head_sha_event, null for curated/noreply
                "example_sha"         : null,          # commit SHA for head_sha_event hits
                "example_pr"          : null           # PR number for head_sha_event hits
            }
        ]
    }
"""  # noqa: E501

import json
import glob
import os
import re
from collections import defaultdict
from datetime import datetime, timezone

# ── Configuration ──────────────────────────────────────────────────────────────

# Anyone who has performed at least this many merges across all PR archives is
# treated as a proxy and excluded from email mining.  Derived dynamically by
# derive_proxies(); no manual list needed.
PROXY_MERGE_MIN: int = 5

# Minimum number of distinct PRs an (id, email) pair must appear in before the
# email is included in the map.  The blocked_emails safety layer (all curated
# emails are protected) reduces false positives, but single-PR contamination
# (non-proxy maintainer merging someone else's patch as head SHA once) is still
# possible.  Keep at 2 for now; cover single-PR contributors differently later.
CORROBORATION_MIN: int = 2

PR_DIRS: list[str] = [
    "data/sources/bitcoin-github-metadata/pulls",
    "data/sources/bips-github-metadata/pulls",
]

CURATED_PATH: str = "metadata/identity_curated.json"
OUTPUT_PATH:  str = "metadata/github_id_map.json"

_NOREPLY_RE = re.compile(r'^[^@]+@users\.noreply\.github\.com$', re.I)
_GITHUB_RE  = re.compile(r'@(?:users\.noreply\.)?github\.com$', re.I)


# ── Email helpers ──────────────────────────────────────────────────────────────

def _is_real(email: str) -> bool:
    """True for a proper personal address (not a GitHub placeholder)."""
    if not email or not email.strip():
        return False
    return not _GITHUB_RE.search(email.strip())


def _is_noreply(email: str) -> bool:
    return bool(email and _NOREPLY_RE.match(email.strip()))


# ── Step 0: Derive proxy set from merged events ───────────────────────────────

def derive_proxies(pr_dirs: list[str], min_merges: int = PROXY_MERGE_MIN) -> set[str]:
    """
    Scan all PR JSON archives and count how many times each GitHub login
    performed a merge (via the 'merged' event's actor.login field).
    Any login with ≥ min_merges is treated as a proxy and excluded from
    email mining — their PRs often contain other people's commits.

    This replaces a static hardcoded PROXIES list with data-driven detection.
    """
    merger_counts: dict[str, int] = {}

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

    proxies = {login.lower() for login, cnt in merger_counts.items() if cnt >= min_merges}
    print(f"  {len(proxies)} proxy logins derived (≥{min_merges} merges): "
          + ", ".join(sorted(proxies)))
    return proxies


# ── Step 1: Mine PR JSON archives ──────────────────────────────────────────────

def mine_pr_directories(
    pr_dirs: list[str],
    proxies: set[str],
) -> tuple[dict, dict, dict]:
    """
    Walk every PR JSON file in *pr_dirs* and, for each PR, locate the single
    committed event whose sha matches pull.head.sha.  That event's author is
    guaranteed to be the PR author (not a maintainer who merged other commits).

    Returns
    -------
    raw_hits     : dict  (github_id, email) → list of
                         {"sha": str, "pr": int, "login": str}
    email_to_ids : dict  email → set of github_ids that claimed it
                         (used to detect contested/contaminated single-PR emails)
    all_noreply  : dict  github_id → {"login": str, "email": str}
    id_to_login  : dict  github_id → login  (stable; last seen)
    """
    raw_hits     = defaultdict(list)
    email_to_ids: dict[str, set] = defaultdict(set)
    all_noreply  = {}
    id_to_login  = {}

    for pr_dir in pr_dirs:
        for fpath in sorted(glob.glob(os.path.join(pr_dir, "*.json"))):
            pr_number = int(os.path.basename(fpath).replace(".json", ""))
            try:
                with open(fpath) as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue

            pull     = data.get("pull", {})
            user     = pull.get("user", {})
            login    = (user.get("login") or "").strip()
            github_id = str(user.get("id") or "")
            head_sha = (pull.get("head", {}).get("sha") or "").strip()

            if not github_id or not login or not head_sha:
                continue
            if login.lower() in proxies:
                continue

            id_to_login[github_id] = login

            for ev in data.get("events", []):
                if ev.get("event") != "committed" or ev.get("sha") != head_sha:
                    continue

                author = ev.get("author", {})
                email  = (author.get("email") or "").strip().lower()

                if _is_real(email):
                    raw_hits[(github_id, email)].append(
                        {"sha": head_sha, "pr": pr_number, "login": login}
                    )
                    email_to_ids[email].add(github_id)
                elif _is_noreply(email) and github_id not in all_noreply:
                    all_noreply[github_id] = {"login": login, "email": email}

                break  # only one head committed event per PR

    return raw_hits, email_to_ids, all_noreply, id_to_login


# ── Step 2: Load curated anchors ───────────────────────────────────────────────

def load_curated_anchors(path: str, id_to_login: dict) -> tuple[dict, set, set]:
    """
    Returns
    -------
    anchors      : {github_id: [email, ...]}  — curated emails matched to github_ids
    blocked_emails : set of all emails that appear in the curated table, regardless of
                     whether they were matched to a github_id.  These are "owned" emails;
                     any mining result claiming one of these for a *different* owner is
                     rejected in build_map().
    """
    anchors:        dict[str, list[str]] = {}
    blocked_emails: set[str]             = set()

    if not os.path.exists(path):
        print(f"  [warn] {path} not found — skipping curated anchors")
        return anchors, blocked_emails

    with open(path) as f:
        data = json.load(f)

    # Build reverse map: login (lower) → github_id
    login_to_id = {v.lower(): k for k, v in id_to_login.items()}

    for entry in data.get("aliases", []):
        emails = [e.strip().lower() for e in entry.get("emails", []) if e and e.strip()]
        if not emails:
            continue

        # Every curated email is "blocked" — it belongs to a known person
        blocked_emails.update(emails)

        # Try explicit 'github' field first (may be a string or list of logins)
        github_field = entry.get("github") or []
        github_logins = github_field if isinstance(github_field, list) else [github_field]
        matched = False
        for raw_login in github_logins:
            explicit_login = raw_login.strip().lower()
            if explicit_login and explicit_login in login_to_id:
                github_id = login_to_id[explicit_login]
                anchors.setdefault(github_id, []).extend(emails)
                matched = True
                break
        if matched:
            continue

        # Fall back: any alias that is a known GitHub login
        for alias in entry.get("aliases", []):
            alias_lower = alias.strip().lower()
            if alias_lower in login_to_id:
                github_id = login_to_id[alias_lower]
                anchors.setdefault(github_id, []).extend(emails)
                break  # one match is enough for this curated entry

    # Load blocked_anchors: explicit (github_id, email) pairs that must never be linked,
    # even if they pass corroboration. These are false positives where a maintainer/proxy
    # committed to someone else's PR, leaving their email as the head SHA author.
    blocked_anchor_pairs: set[tuple[str, str]] = set()
    for ba in data.get("blocked_anchors", []):
        gid   = str(ba.get("github_id", "")).strip()
        email = str(ba.get("email", "")).strip().lower()
        if gid and email:
            blocked_anchor_pairs.add((gid, email))
            # Also add to blocked_emails so the email is treated as "owned" globally
            blocked_emails.add(email)
    if blocked_anchor_pairs:
        print(f"  {len(blocked_anchor_pairs)} explicit blocked anchors loaded from curated file")

    return anchors, blocked_emails, blocked_anchor_pairs


# ── Step 3: Assemble the map ───────────────────────────────────────────────────

def build_map(
    raw_hits:             dict,
    email_to_ids:         dict,
    all_noreply:          dict,
    id_to_login:          dict,
    curated_anchors:      dict,
    blocked_emails:       set,
    blocked_anchor_pairs: set,
) -> list[dict]:
    """Merge all sources into one list of per-person email entries."""
    id_to_emails: dict[str, list[dict]] = defaultdict(list)

    # Build curated ownership: email → github_id (only for emails we could match to an ID)
    curated_email_to_id: dict[str, str] = {}
    for github_id, emails in curated_anchors.items():
        for email in emails:
            curated_email_to_id[email] = github_id

    # --- Corroborated real emails from head_sha committed events ---
    # Two safety checks before accepting a mined email:
    #   1. Corroboration: must appear across ≥ CORROBORATION_MIN distinct PRs.
    #      Uniqueness (only one github_id claims the email) is NOT sufficient
    #      because the true owner may be proxy-excluded and never appear in
    #      raw_hits at all, making a wrong assignment look unique.
    #   2. Ownership: if the email is in the curated table, it must match the
    #      curated owner — if not, it means this github_id saw a commit from
    #      a different (known) person and is contaminant-rejected.
    #   3. Explicit block: (github_id, email) is in blocked_anchor_pairs —
    #      curated false-positive where a proxy committed to someone else's PR.
    for (github_id, email), occurrences in raw_hits.items():
        pr_count  = len({o["pr"] for o in occurrences})

        if pr_count < CORROBORATION_MIN:
            continue

        # Reject explicitly blocked (github_id, email) pairs
        if (github_id, email) in blocked_anchor_pairs:
            continue

        # Reject if this email is known to belong to a different person
        curated_owner = curated_email_to_id.get(email)
        if email in blocked_emails and curated_owner != github_id:
            continue

        id_to_emails[github_id].append({
            "email":               email,
            "email_type":          "real",
            "source":              "head_sha_event",
            "corroboration_count": pr_count,
            "example_sha":         occurrences[0]["sha"],
            "example_pr":          occurrences[0]["pr"],
        })

    # --- Noreply addresses ---
    # No corroboration needed: the address structurally encodes the github_id
    # (format: <numeric_id>+login@users.noreply.github.com).
    # Stored for completeness but not used for cross-source identity matching.
    for github_id, info in all_noreply.items():
        id_to_emails[github_id].append({
            "email":               info["email"],
            "email_type":          "noreply",
            "source":              "head_sha_event",
            "corroboration_count": None,
            "example_sha":         None,
            "example_pr":          None,
        })

    # --- Curated anchors (supplement / highest confidence) ---
    for github_id, curated_emails in curated_anchors.items():
        existing = {e["email"] for e in id_to_emails[github_id]}
        for email in curated_emails:
            if email not in existing:
                id_to_emails[github_id].append({
                    "email":               email,
                    "email_type":          "real",
                    "source":              "curated",
                    "corroboration_count": None,
                    "example_sha":         None,
                    "example_pr":          None,
                })
                existing.add(email)

    # --- Sort and assemble ---
    def _email_sort_key(e: dict) -> tuple:
        # curated first, then real by corroboration count desc, then noreply
        if e["source"] == "curated":
            return (0, 0)
        if e["email_type"] == "real":
            return (1, -(e["corroboration_count"] or 0))
        return (2, 0)

    entries = []
    for github_id, emails in id_to_emails.items():
        entries.append({
            "github_id": github_id,
            "login":     id_to_login.get(github_id, ""),
            "emails":    sorted(emails, key=_email_sort_key),
        })

    entries.sort(key=lambda e: e["login"].lower())
    return entries


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    print("Deriving proxy set from merged events...")
    proxies = derive_proxies(PR_DIRS)

    print("Mining PR directories...")
    raw_hits, email_to_ids, all_noreply, id_to_login = mine_pr_directories(PR_DIRS, proxies)
    print(f"  {len(raw_hits):,} (github_id, email) pairs found (pre-corroboration)")
    print(f"  {len(all_noreply):,} github_ids with only noreply emails")
    print(f"  {len(id_to_login):,} unique github_ids seen")

    print("Loading curated anchors...")
    curated_anchors, blocked_emails, blocked_anchor_pairs = load_curated_anchors(CURATED_PATH, id_to_login)
    print(f"  {len(curated_anchors)} github_ids matched from curated entries")
    print(f"  {len(blocked_emails)} emails blocked (owned by curated identities)")

    print("Building map...")
    entries = build_map(raw_hits, email_to_ids, all_noreply, id_to_login, curated_anchors, blocked_emails, blocked_anchor_pairs)

    real_count    = sum(1 for e in entries if any(m["email_type"] == "real"    for m in e["emails"]))
    noreply_only  = sum(1 for e in entries if all(m["email_type"] == "noreply" for m in e["emails"]))
    curated_count = sum(
        1 for e in entries if any(m["source"] == "curated" for m in e["emails"])
    )

    output = {
        "generated":           datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_entries":       len(entries),
        "real_email_entries":  real_count,
        "noreply_only_entries": noreply_only,
        "curated_entries":     curated_count,
        "corroboration_min":   CORROBORATION_MIN,
        "entries":             entries,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nWrote {len(entries):,} entries → {OUTPUT_PATH}")
    print(f"  {real_count:,}  have at least one real email")
    print(f"  {noreply_only:,}  have only a noreply address")
    print(f"  {curated_count:,}  entries supplemented from hand-curated table")


if __name__ == "__main__":
    main()
