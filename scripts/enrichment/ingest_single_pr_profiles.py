#!/usr/bin/env python3
"""
ingest_single_pr_profiles.py
============================
After running fetch_github_profiles.py against single_pr_contributors.json,
this script cross-validates each profile's public email against the candidate
email found in the PR commit, then injects confirmed entries into
github_id_map.json.

A match is confirmed when:
  - profile.email is set (user has made their email public on GitHub), AND
  - profile.email == candidate_email   (exact match, case-insensitive)

This gives us an independent second signal — GitHub profile email page — that
confirms the commit email belongs to this github_id, making it safe to include
in the map without corroboration from a second PR.

Usage (run from repo root):
    python3 scripts/maintenance/ingest_single_pr_profiles.py

Reads:
    metadata/single_pr_contributors.json
    metadata/github_profiles_single_pr.json
    metadata/github_id_map.json

Writes:
    metadata/github_id_map.json  (updated in place — originals backed up in .bak)
"""

import json
import os
import shutil
from datetime import datetime, timezone

# ── Paths ──────────────────────────────────────────────────────────────────────

SINGLE_PR_PATH  = "metadata/single_pr_contributors.json"
PROFILES_PATH   = "metadata/github_profiles_single_pr.json"
MAP_PATH        = "metadata/github_id_map.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> None:
    # ── Load inputs ────────────────────────────────────────────────────────────
    with open(SINGLE_PR_PATH) as f:
        single_pr = json.load(f)
    with open(PROFILES_PATH) as f:
        profiles_data = json.load(f)
    with open(MAP_PATH) as f:
        map_data = json.load(f)

    candidates  = {e["github_id"]: e for e in single_pr.get("entries", [])}
    profiles    = profiles_data.get("profiles", {})
    map_entries = map_data.get("entries", [])

    # Index existing map entries by github_id for fast lookup
    existing_ids = {e["github_id"] for e in map_entries}

    # ── Cross-validate ─────────────────────────────────────────────────────────
    confirmed       = []
    profile_no_email = 0
    email_mismatch  = 0
    not_fetched     = 0

    for github_id, cand in sorted(candidates.items()):
        profile = profiles.get(github_id)
        if not profile or profile.get("status") != "ok":
            not_fetched += 1
            continue

        profile_email = (profile.get("email") or "").strip().lower()
        if not profile_email:
            profile_no_email += 1
            continue

        candidate_email = cand.get("candidate_email", "").strip().lower()
        if profile_email != candidate_email:
            email_mismatch += 1
            continue

        # Confirmed: profile email == commit email → safe to add to map
        confirmed.append({
            "github_id": github_id,
            "login":     cand["login"],
            "email":     candidate_email,
        })

    print(f"Single-PR candidates  : {len(candidates)}")
    print(f"Not yet fetched       : {not_fetched}")
    print(f"Profile has no email  : {profile_no_email}")
    print(f"Email mismatch        : {email_mismatch}")
    print(f"Confirmed matches     : {len(confirmed)}")

    if not confirmed:
        print("Nothing to inject — exiting.")
        return

    # ── Inject into github_id_map.json ────────────────────────────────────────
    # Backup first
    shutil.copy2(MAP_PATH, MAP_PATH + ".bak")
    print(f"\nBacked up {MAP_PATH} → {MAP_PATH}.bak")

    new_entries = 0
    updated_entries = 0

    for item in confirmed:
        github_id = item["github_id"]
        email     = item["email"]
        new_email_obj = {
            "email":               email,
            "email_type":          "real",
            "source":              "github_profile",   # distinct source tag
            "corroboration_count": None,
            "example_sha":         None,
            "example_pr":          None,
        }

        if github_id not in existing_ids:
            # Entirely new entry
            map_entries.append({
                "github_id": github_id,
                "login":     item["login"],
                "emails":    [new_email_obj],
            })
            existing_ids.add(github_id)
            new_entries += 1
        else:
            # Add email to existing entry if not already there
            for entry in map_entries:
                if entry["github_id"] == github_id:
                    existing_emails = {e["email"] for e in entry.get("emails", [])}
                    if email not in existing_emails:
                        entry["emails"].append(new_email_obj)
                        updated_entries += 1
                    break

    print(f"New map entries added : {new_entries}")
    print(f"Existing entries updated: {updated_entries}")

    # Recompute summary counts
    real_count   = sum(1 for e in map_entries if any(m["email_type"] == "real"    for m in e.get("emails", [])))
    noreply_only = sum(1 for e in map_entries if all(m["email_type"] == "noreply" for m in e.get("emails", [])))

    map_data["generated"]          = _now_iso()
    map_data["total_entries"]      = len(map_entries)
    map_data["real_email_entries"] = real_count
    map_data["noreply_only_entries"] = noreply_only
    map_data["entries"]            = sorted(map_entries, key=lambda e: e.get("login", "").lower())

    with open(MAP_PATH, "w") as f:
        json.dump(map_data, f, indent=2)
    print(f"\nUpdated {MAP_PATH} ({len(map_entries)} total entries)")
    print("Re-run build_identities.py to rebuild identities with new anchors.")


if __name__ == "__main__":
    main()
