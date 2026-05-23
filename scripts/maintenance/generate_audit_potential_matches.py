#!/usr/bin/env python3
"""
generate_audit_potential_matches.py
====================================
Produces metadata/audit_potential_matches.json — a structured report used to:
  1. Understand how many developers came from each source and how many overlap
  2. Surface Delving and mailing-list users not yet linked to a GitHub identity,
     with ranked candidate GitHub profiles to review for curation
  3. Flag fuzzy name collisions across all identities (potential missed merges)

Run from repo root:
    python3 scripts/maintenance/generate_audit_potential_matches.py

Reads:
    metadata/identities.json
    metadata/github_profiles.json
    data/raw/social_delving.parquet
    data/raw/social_mailing_list.parquet
    data/raw/core_commits.parquet  (for overlap stats)

Writes:
    metadata/audit_potential_matches.json
"""

import json
import os
import re
import difflib
from collections import defaultdict
from datetime import datetime, timezone
import pandas as pd

# ── Paths ──────────────────────────────────────────────────────────────────────
IDENTITIES_PATH       = "metadata/identities.json"
PROFILES_PATH         = "metadata/github_profiles.json"
DELVING_PARQUET       = "data/raw/social_delving.parquet"
ML_PARQUET            = "data/raw/social_mailing_list.parquet"
COMMITS_PARQUET       = "data/raw/core_commits.parquet"
OUTPUT_PATH           = "metadata/audit_potential_matches.json"

# Minimum similarity for fuzzy name match
FUZZY_THRESHOLD = 0.88

# ── Helpers ────────────────────────────────────────────────────────────────────

_NOREPLY_RE = re.compile(r"@users\.noreply\.github\.com$", re.I)

def _clean(s):
    return str(s or "").strip()

def _is_real_email(email):
    return bool(email and not _NOREPLY_RE.search(email.strip()))

def _name_looks_real(name):
    """Return True if name looks like a first+last human name."""
    n = _clean(name)
    return len(n) >= 5 and " " in n

def _initial_match(n1, n2):
    """'S. Nakamoto' matches 'Satoshi Nakamoto'."""
    p1 = n1.lower().split()
    p2 = n2.lower().split()
    if len(p1) < 2 or len(p2) < 2:
        return False
    if p1[-1] == p2[-1] and p1[0][0] == p2[0][0]:
        return True
    if set(x.strip(",") for x in p1) == set(x.strip(",") for x in p2):
        return True
    return False

def _fuzzy(a, b):
    return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Audit: Potential Identity Matches")
    print("=" * 60)

    # ── Load identities ────────────────────────────────────────────────────────
    print("\nLoading identities.json...")
    with open(IDENTITIES_PATH) as f:
        id_data = json.load(f)
    identities = id_data.get("identities", [])
    print(f"  {len(identities):,} identities loaded")

    # Build lookup indexes
    # uuid -> identity record
    by_uuid = {r["uuid"]: r for r in identities}
    # github_login (lower) -> uuid
    gh_login_to_uuid = {}
    # delving_username (lower) -> uuid
    dlv_to_uuid = {}
    # email (lower) -> uuid
    email_to_uuid = {}
    # name (lower) -> uuid
    name_to_uuid = {}

    for r in identities:
        uuid = r["uuid"]
        platforms = r.get("platforms", {})
        gh = _clean(platforms.get("github", "")).lower()
        dlv = _clean(platforms.get("delving", "")).lower()
        if gh:
            gh_login_to_uuid[gh] = uuid
        if dlv:
            dlv_to_uuid[dlv] = uuid
        for e in r.get("git_signatures", {}).get("emails", []):
            if _clean(e):
                email_to_uuid[_clean(e).lower()] = uuid
        for n in r.get("git_signatures", {}).get("names", []):
            if _clean(n):
                name_to_uuid[_clean(n).lower()] = uuid

    # ── Source overlap / coverage stats ───────────────────────────────────────
    print("\nComputing source coverage stats...")

    source_sets = defaultdict(set)  # source_name -> {uuid, ...}
    for r in identities:
        for src in r.get("sources", []):
            source_sets[src].add(r["uuid"])

    # Additional: count Delving and mailing-list raw entries
    dlv_raw_users = {}   # username.lower() -> {display_name, uuid_or_none}
    ml_raw_emails = {}   # email.lower()    -> {name, uuid_or_none}

    if os.path.exists(DELVING_PARQUET):
        df_dlv = pd.read_parquet(DELVING_PARQUET)
        for _, row in df_dlv.drop_duplicates(subset=["author_username"]).iterrows():
            uname = _clean(row.get("author_username", "")).lower()
            dname = _clean(row.get("author_name", ""))
            if uname:
                dlv_raw_users[uname] = {
                    "username": uname,
                    "display_name": dname,
                    "uuid": dlv_to_uuid.get(uname),
                }
        print(f"  {len(dlv_raw_users):,} unique Delving users from parquet")

    if os.path.exists(ML_PARQUET):
        df_ml = pd.read_parquet(ML_PARQUET)
        for _, row in df_ml.drop_duplicates(subset=["author_email"]).iterrows():
            email = _clean(row.get("author_email", "")).lower()
            name  = _clean(row.get("author_name", ""))
            if email and _is_real_email(email):
                ml_raw_emails[email] = {
                    "email": email,
                    "name": name,
                    "uuid": email_to_uuid.get(email) or name_to_uuid.get(name.lower()),
                }
        print(f"  {len(ml_raw_emails):,} unique mailing list emails from parquet")

    # Commit authors
    commit_uuids = set()
    if os.path.exists(COMMITS_PARQUET):
        df_co = pd.read_parquet(COMMITS_PARQUET)
        for _, row in df_co.iterrows():
            e = _clean(row.get("author_email", "")).lower()
            n = _clean(row.get("author_name", "")).lower()
            u = email_to_uuid.get(e) or name_to_uuid.get(n)
            if u:
                commit_uuids.add(u)

    # Resolved counts
    dlv_mapped   = sum(1 for v in dlv_raw_users.values() if v["uuid"])
    dlv_unmapped = len(dlv_raw_users) - dlv_mapped
    ml_mapped    = sum(1 for v in ml_raw_emails.values() if v["uuid"])
    ml_unmapped  = len(ml_raw_emails) - ml_mapped

    # Multi-source identities
    multi_source = [r for r in identities if len(r.get("sources", [])) >= 2]

    # Source breakdown
    all_sources = sorted(source_sets.keys())
    source_counts = {s: len(source_sets[s]) for s in all_sources}

    # Cross-source overlap matrix (pairwise)
    overlap_matrix = {}
    for i, s1 in enumerate(all_sources):
        for s2 in all_sources[i+1:]:
            key = f"{s1} ∩ {s2}"
            overlap_matrix[key] = len(source_sets[s1] & source_sets[s2])

    # How many appear in only one source
    single_source = [r for r in identities if len(r.get("sources", [])) == 1]
    single_by_source = defaultdict(int)
    for r in single_source:
        srcs = r.get("sources", [])
        if srcs:
            single_by_source[srcs[0]] += 1

    summary = {
        "total_identities": len(identities),
        "can_ids": sum(1 for r in identities if r["uuid"].startswith("can_")),
        "auto_ids": sum(1 for r in identities if r["uuid"].startswith("auto_")),
        "multi_source_identities": len(multi_source),
        "single_source_identities": len(single_source),
        "source_counts": source_counts,
        "single_source_breakdown": dict(single_by_source),
        "source_overlap_matrix": overlap_matrix,
        "delving_raw_users": len(dlv_raw_users),
        "delving_mapped_to_identity": dlv_mapped,
        "delving_unmapped": dlv_unmapped,
        "ml_raw_emails": len(ml_raw_emails),
        "ml_mapped_to_identity": ml_mapped,
        "ml_unmapped": ml_unmapped,
        "commit_uuids": len(commit_uuids),
    }

    print(f"\n  Summary:")
    print(f"    Total identities:      {summary['total_identities']:,}")
    print(f"    Multi-source merges:   {summary['multi_source_identities']:,}")
    print(f"    Delving mapped:        {dlv_mapped}/{len(dlv_raw_users)}")
    print(f"    Mailing list mapped:   {ml_mapped}/{len(ml_raw_emails)}")

    # ── Load GitHub profiles ───────────────────────────────────────────────────
    profiles_by_login = {}
    if os.path.exists(PROFILES_PATH):
        print("\nLoading GitHub profiles...")
        with open(PROFILES_PATH) as f:
            gh_profiles = json.load(f).get("profiles", {})
        for gid, prof in gh_profiles.items():
            login = _clean(prof.get("login", "")).lower()
            if login:
                profiles_by_login[login] = {
                    "github_id": gid,
                    "login": prof.get("login", ""),
                    "display_name": _clean(prof.get("name", "")),
                    "email": _clean(prof.get("email", "")).lower(),
                    "twitter": _clean(prof.get("twitter_username", "")).lower(),
                    "bio": _clean(prof.get("bio", "")),
                    "company": _clean(prof.get("company", "")),
                }
        print(f"  {len(profiles_by_login):,} GitHub profiles loaded")

    # ── Section 1: Delving ↔ GitHub candidate matches ─────────────────────────
    # For each Delving user NOT yet linked to a GitHub identity, find potential
    # GitHub profile matches using multiple signals.
    print("\nFinding Delving ↔ GitHub candidate matches...")

    delving_github_candidates = []
    delving_already_mapped = []

    for uname, dlv in sorted(dlv_raw_users.items()):
        existing_uuid = dlv["uuid"]
        dname = dlv["display_name"]

        # Already linked — confirm the link details
        if existing_uuid:
            rec = by_uuid.get(existing_uuid, {})
            gh_login = rec.get("platforms", {}).get("github")
            delving_already_mapped.append({
                "delving_username": uname,
                "delving_display_name": dname,
                "uuid": existing_uuid,
                "linked_github": gh_login,
                "sources": rec.get("sources", []),
            })
            continue

        # Not yet linked — find candidates
        candidates = []

        # Signal 1: username == github login
        if uname in profiles_by_login:
            prof = profiles_by_login[uname]
            candidates.append({
                "github_login": prof["login"],
                "github_display_name": prof["display_name"],
                "github_email": prof["email"],
                "match_signals": ["username_equals_login"],
                "score": 1.0,
            })

        # Signal 2: Delving username matches GitHub twitter_username
        for login, prof in profiles_by_login.items():
            if prof["twitter"] and prof["twitter"] == uname:
                # avoid duplicate from signal 1
                if not any(c["github_login"].lower() == login for c in candidates):
                    candidates.append({
                        "github_login": prof["login"],
                        "github_display_name": prof["display_name"],
                        "github_email": prof["email"],
                        "match_signals": ["twitter_equals_delving_username"],
                        "score": 0.95,
                    })

        # Signal 3: Delving display name matches GitHub profile display name
        if _name_looks_real(dname):
            for login, prof in profiles_by_login.items():
                gh_name = prof["display_name"]
                if not _name_looks_real(gh_name):
                    continue
                if dname.lower() == gh_name.lower():
                    if not any(c["github_login"].lower() == login for c in candidates):
                        candidates.append({
                            "github_login": prof["login"],
                            "github_display_name": gh_name,
                            "github_email": prof["email"],
                            "match_signals": ["display_name_exact"],
                            "score": 0.92,
                        })
                elif _initial_match(dname, gh_name):
                    if not any(c["github_login"].lower() == login for c in candidates):
                        candidates.append({
                            "github_login": prof["login"],
                            "github_display_name": gh_name,
                            "github_email": prof["email"],
                            "match_signals": ["display_name_initial"],
                            "score": 0.88,
                        })
                elif (abs(len(dname) - len(gh_name)) <= 5
                      and _fuzzy(dname, gh_name) >= FUZZY_THRESHOLD):
                    if not any(c["github_login"].lower() == login for c in candidates):
                        sim = _fuzzy(dname, gh_name)
                        candidates.append({
                            "github_login": prof["login"],
                            "github_display_name": gh_name,
                            "github_email": prof["email"],
                            "match_signals": ["display_name_fuzzy"],
                            "score": round(sim, 3),
                        })

        if candidates:
            candidates.sort(key=lambda x: x["score"], reverse=True)
            delving_github_candidates.append({
                "delving_username": uname,
                "delving_display_name": dname,
                "top_candidates": candidates[:5],
            })

    delving_github_candidates.sort(
        key=lambda x: x["top_candidates"][0]["score"] if x["top_candidates"] else 0,
        reverse=True
    )
    print(f"  {len(delving_github_candidates):,} unmapped Delving users with ≥1 candidate")
    print(f"  {len(delving_already_mapped):,} Delving users already linked to an identity")

    # ── Section 2: Mailing list ↔ GitHub candidate matches ────────────────────
    # For each mailing list email not yet linked to a GitHub identity, find
    # candidate GitHub profiles with a matching public profile email.
    print("\nFinding mailing-list ↔ GitHub candidate matches...")

    ml_github_candidates = []
    for email, ml in sorted(ml_raw_emails.items()):
        if ml["uuid"]:
            continue  # already resolved
        name = ml["name"]
        candidates = []

        # Signal 1: email matches GitHub profile public email
        for login, prof in profiles_by_login.items():
            if prof["email"] == email and prof["email"]:
                candidates.append({
                    "github_login": prof["login"],
                    "github_display_name": prof["display_name"],
                    "match_signals": ["profile_email_exact"],
                    "score": 1.0,
                })

        # Signal 2: name similarity with GitHub profile display name
        if _name_looks_real(name) and not candidates:
            for login, prof in profiles_by_login.items():
                gh_name = prof["display_name"]
                if not _name_looks_real(gh_name):
                    continue
                if name.lower() == gh_name.lower():
                    candidates.append({
                        "github_login": prof["login"],
                        "github_display_name": gh_name,
                        "match_signals": ["display_name_exact"],
                        "score": 0.92,
                    })

        if candidates:
            candidates.sort(key=lambda x: x["score"], reverse=True)
            ml_github_candidates.append({
                "ml_email": email,
                "ml_name": name,
                "top_candidates": candidates[:3],
            })

    ml_github_candidates.sort(
        key=lambda x: x["top_candidates"][0]["score"] if x["top_candidates"] else 0,
        reverse=True
    )
    print(f"  {len(ml_github_candidates):,} unmapped mailing-list addresses with ≥1 candidate")

    # ── Section 3: Fuzzy name matches across all identities ───────────────────
    print("\nRunning fuzzy name audit across all identities...")
    records = []
    for r in identities:
        names = set([r["display_name"]])
        names.update(r.get("git_signatures", {}).get("names", []))
        gh = r.get("platforms", {}).get("github")
        if gh:
            if isinstance(gh, list):
                names.update(gh)
            else:
                names.add(gh)
        clean = [n for n in names if n and len(n) > 2]
        if clean:
            records.append({
                "uuid": r["uuid"],
                "display": r["display_name"],
                "names": clean,
                "sources": r.get("sources", []),
            })

    fuzzy_matches = []
    for i in range(len(records)):
        r1 = records[i]
        for j in range(i + 1, len(records)):
            r2 = records[j]
            found = False
            for n1 in r1["names"]:
                for n2 in r2["names"]:
                    n1l, n2l = n1.lower(), n2.lower()
                    if n1l == n2l:
                        score = 0.99
                    elif _initial_match(n1l, n2l):
                        score = 0.95
                    else:
                        if abs(len(n1l) - len(n2l)) > 6:
                            continue
                        score = _fuzzy(n1l, n2l)
                    if score >= FUZZY_THRESHOLD:
                        fuzzy_matches.append({
                            "score": round(score, 3),
                            "id1": {"uuid": r1["uuid"], "display": r1["display"], "match_string": n1, "sources": r1["sources"]},
                            "id2": {"uuid": r2["uuid"], "display": r2["display"], "match_string": n2, "sources": r2["sources"]},
                        })
                        found = True
                        break
                if found:
                    break

    fuzzy_matches.sort(key=lambda x: x["score"], reverse=True)
    print(f"  {len(fuzzy_matches):,} fuzzy name collisions (score ≥ {FUZZY_THRESHOLD})")

    # ── Write output ───────────────────────────────────────────────────────────
    output = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "summary": summary,
        "delving_already_mapped": delving_already_mapped,
        "delving_github_candidates": delving_github_candidates,
        "ml_github_candidates": ml_github_candidates,
        "fuzzy_name_matches": fuzzy_matches,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅  Wrote {OUTPUT_PATH}")
    print(f"    {summary['total_identities']:,} total identities")
    print(f"    {summary['multi_source_identities']:,} merged across 2+ sources")
    print(f"    {len(delving_github_candidates):,} Delving users needing GitHub curation")
    print(f"    {len(ml_github_candidates):,} mailing-list addresses needing GitHub curation")
    print(f"    {len(fuzzy_matches):,} fuzzy name pairs to review")

if __name__ == "__main__":
    main()
