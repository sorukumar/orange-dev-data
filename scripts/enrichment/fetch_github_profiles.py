#!/usr/bin/env python3
"""
fetch_github_profiles.py
========================
Fetches rich GitHub profile data for every contributor in github_id_map.json
by calling the GitHub REST API (GET /users/{login}).

Input:  metadata/github_id_map.json
Output: metadata/github_profiles.json

Run from the repository root:
    python3 scripts/enrichment/fetch_github_profiles.py --token ghp_xxx
    python3 scripts/enrichment/fetch_github_profiles.py  # reads GITHUB_TOKEN env var
    python3 scripts/enrichment/fetch_github_profiles.py --force-refresh

Schema of each profile entry (keyed by github_id string):
    {
        "github_id":        "548488",
        "login":            "sipa",
        "name":             "Pieter Wuille",
        "email":            null,
        "location":         "US",
        "company":          "Chaincode Labs",
        "blog":             "https://...",
        "bio":              null,
        "public_repos":     42,
        "public_gists":     0,
        "followers":        5000,
        "following":        10,
        "created_at":       "2010-01-01T00:00:00Z",
        "updated_at":       "2026-01-01T00:00:00Z",
        "twitter_username": null,
        "hireable":         null,
        "fetched_at":       "2026-04-18T12:00:00Z",
        "status":           "ok"           # or "not_found"
    }
"""

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

# Use certifi's CA bundle when available (required on macOS python.org builds)
try:
    import certifi
    _SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    _SSL_CTX = ssl.create_default_context()

# ── Paths (relative to repo root) ─────────────────────────────────────────────

INPUT_PATH  = "metadata/github_id_map.json"
OUTPUT_PATH = "metadata/github_profiles.json"

# ── API settings ───────────────────────────────────────────────────────────────

API_BASE          = "https://api.github.com/users/{login}"
RATE_LIMIT_FLOOR  = 10          # sleep when remaining requests fall to this value
PROGRESS_INTERVAL = 50          # print a progress line every N profiles
MAX_RETRIES       = 3           # max retries on 403 / 429
BASE_BACKOFF_S    = 2.0         # initial backoff in seconds (doubles each retry)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_request(url: str, token: str) -> urllib.request.Request:
    return urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept":        "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent":    "orange-dev-data/fetch_github_profiles",
        },
    )


def _check_rate_limit(headers: dict) -> None:
    """
    Inspect X-RateLimit-Remaining; if we're at/below the floor, sleep until
    X-RateLimit-Reset (plus a small buffer) so we never exhaust the quota.
    """
    try:
        remaining = int(headers.get("X-RateLimit-Remaining", 999))
        reset_ts  = int(headers.get("X-RateLimit-Reset", 0))
    except (TypeError, ValueError):
        return

    if remaining <= RATE_LIMIT_FLOOR:
        now   = time.time()
        sleep = max(reset_ts - now + 5, 1)
        print(
            f"  [rate-limit] {remaining} requests remaining — "
            f"sleeping {sleep:.0f}s until reset..."
        )
        time.sleep(sleep)


def _fetch_profile(login: str, token: str) -> dict:
    """
    Call GET /users/{login} with exponential backoff on 403/429.
    Returns the parsed profile dict, or {"status": "not_found"} on 404.
    Raises RuntimeError after MAX_RETRIES exhausted.
    """
    url = API_BASE.format(login=login)

    for attempt in range(1, MAX_RETRIES + 1):
        req = _build_request(url, token)
        try:
            with urllib.request.urlopen(req, context=_SSL_CTX) as resp:
                _check_rate_limit(resp.headers)
                body = json.loads(resp.read().decode("utf-8"))
            return body

        except urllib.error.HTTPError as exc:
            _check_rate_limit(exc.headers)

            if exc.code == 404:
                return {"status": "not_found"}

            if exc.code in (403, 429):
                if attempt < MAX_RETRIES:
                    backoff = BASE_BACKOFF_S * (2 ** (attempt - 1))
                    retry_after = exc.headers.get("Retry-After")
                    if retry_after:
                        try:
                            backoff = max(backoff, float(retry_after))
                        except ValueError:
                            pass
                    print(
                        f"  [backoff] HTTP {exc.code} for {login!r} — "
                        f"attempt {attempt}/{MAX_RETRIES}, sleeping {backoff:.1f}s..."
                    )
                    time.sleep(backoff)
                    continue
                raise RuntimeError(
                    f"HTTP {exc.code} for {login!r} after {MAX_RETRIES} retries"
                ) from exc

            # Any other HTTP error: propagate immediately
            raise RuntimeError(f"HTTP {exc.code} for {login!r}: {exc.reason}") from exc

    # Should never be reached, but satisfies type checkers
    raise RuntimeError(f"Exhausted retries for {login!r}")


def _extract_fields(raw: dict, github_id: str, login: str, fetched_at: str) -> dict:
    """Normalise the GitHub API response into our target schema."""
    return {
        "github_id":        github_id,
        "login":            raw.get("login", login),
        "name":             raw.get("name"),
        "email":            raw.get("email") or None,
        "location":         raw.get("location") or None,
        "company":          raw.get("company") or None,
        "blog":             raw.get("blog") or None,
        "bio":              raw.get("bio") or None,
        "public_repos":     raw.get("public_repos"),
        "public_gists":     raw.get("public_gists"),
        "followers":        raw.get("followers"),
        "following":        raw.get("following"),
        "created_at":       raw.get("created_at"),
        "updated_at":       raw.get("updated_at"),
        "twitter_username": raw.get("twitter_username"),
        "hireable":         raw.get("hireable"),
        "fetched_at":       fetched_at,
        "status":           "ok",
    }


# ── I/O ────────────────────────────────────────────────────────────────────────

def _load_id_map(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    entries = data.get("entries", [])
    if not entries:
        sys.exit(f"ERROR: no 'entries' found in {path}")
    return entries


def _load_existing_profiles(path: str) -> dict:
    """Return profiles dict from existing output file, or {} if none."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data.get("profiles", {})
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  [warn] could not read existing {path}: {exc} — starting fresh")
        return {}


def _save_profiles(path: str, profiles: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    output = {
        "generated":    _now_iso(),
        "total_fetched": len(profiles),
        "profiles":     profiles,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)


# ── Summary ────────────────────────────────────────────────────────────────────

def _print_summary(profiles: dict) -> None:
    ok_profiles    = [p for p in profiles.values() if p.get("status") == "ok"]
    not_found      = sum(1 for p in profiles.values() if p.get("status") == "not_found")
    has_email      = sum(1 for p in ok_profiles if p.get("email"))
    has_location   = sum(1 for p in ok_profiles if p.get("location"))
    has_company    = sum(1 for p in ok_profiles if p.get("company"))

    print()
    print("─" * 52)
    print(f"  Total entries in output : {len(profiles)}")
    print(f"  Status ok               : {len(ok_profiles)}")
    print(f"  Not found (404)         : {not_found}")
    print(f"  Have public email       : {has_email}  ({has_email/max(len(ok_profiles),1)*100:.1f}%)")
    print(f"  Have location           : {has_location}  ({has_location/max(len(ok_profiles),1)*100:.1f}%)")
    print(f"  Have company            : {has_company}  ({has_company/max(len(ok_profiles),1)*100:.1f}%)")
    print("─" * 52)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch GitHub user profiles for all contributors in github_id_map.json"
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("GITHUB_TOKEN", ""),
        help="GitHub Personal Access Token (falls back to GITHUB_TOKEN env var)",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Re-fetch all profiles, ignoring any previously cached data",
    )
    parser.add_argument(
        "--input",
        default=INPUT_PATH,
        metavar="PATH",
        help=f"Path to github_id_map.json (default: {INPUT_PATH})",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_PATH,
        metavar="PATH",
        help=f"Path for output github_profiles.json (default: {OUTPUT_PATH})",
    )
    args = parser.parse_args()

    if not args.token:
        sys.exit(
            "ERROR: GitHub token is required.\n"
            "  Pass --token ghp_xxx  or  set the GITHUB_TOKEN environment variable."
        )

    # ── Load inputs ────────────────────────────────────────────────────────────
    print(f"Loading contributor list from {args.input}...")
    entries = _load_id_map(args.input)
    total   = len(entries)
    print(f"  {total} contributors found.")

    if args.force_refresh:
        profiles = {}
        print("  --force-refresh: ignoring any existing cached profiles.")
    else:
        profiles = _load_existing_profiles(args.output)
        already  = sum(1 for gid in (e.get("github_id") for e in entries)
                       if profiles.get(str(gid), {}).get("fetched_at"))
        if already:
            print(f"  Resuming: {already} profiles already fetched, {total - already} remaining.")

    # ── Fetch loop ─────────────────────────────────────────────────────────────
    fetched_this_run = 0

    for i, entry in enumerate(entries):
        github_id = str(entry.get("github_id", "")).strip()
        login     = str(entry.get("login", "")).strip()

        if not github_id or not login:
            print(f"  [skip] entry {i} missing github_id or login — skipping")
            continue

        # Incremental: skip if already fetched (unless --force-refresh)
        existing = profiles.get(github_id, {})
        if existing.get("fetched_at") and not args.force_refresh:
            continue

        try:
            raw = _fetch_profile(login, args.token)
        except RuntimeError as exc:
            print(f"  [error] {exc} — skipping")
            continue

        fetched_at = _now_iso()

        if raw.get("status") == "not_found":
            profiles[github_id] = {
                "github_id": github_id,
                "login":     login,
                "status":    "not_found",
                "fetched_at": fetched_at,
            }
        else:
            profiles[github_id] = _extract_fields(raw, github_id, login, fetched_at)

        fetched_this_run += 1

        # Progress print every PROGRESS_INTERVAL fetches
        if fetched_this_run % PROGRESS_INTERVAL == 0:
            done_total = sum(1 for e in entries
                             if profiles.get(str(e.get("github_id", "")), {}).get("fetched_at"))
            print(f"  Fetched {done_total}/{total} ({login})...")

        # Checkpoint: save every PROGRESS_INTERVAL to protect against interruption
        if fetched_this_run % PROGRESS_INTERVAL == 0:
            _save_profiles(args.output, profiles)

    # ── Final save ─────────────────────────────────────────────────────────────
    _save_profiles(args.output, profiles)
    print(f"\nDone. {fetched_this_run} profiles fetched this run.")
    print(f"Output written to {args.output}")

    _print_summary(profiles)


if __name__ == "__main__":
    main()
