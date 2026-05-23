"""
Validate commit counts for top contributors against GitHub API.

Compares our git-derived commit stats (from registry_index.json) against
GitHub's /repos/bitcoin/bitcoin/contributors contributions count.

Key definitions
---------------
  Our total_commits       = authored + merge commits in local git log
  Our authored_commits    = commits where is_merge=False
  GitHub contributions    = commits GitHub attributes to the account
                            (includes merge commits the person pushed)
  Source                  = data/sources/bitcoin (bitcoin/bitcoin only; BIPs excluded)

Expected discrepancies
----------------------
  1. Old commits (2009-2013) — authored before GitHub accounts; no account attribution.
  2. Account renames — GitHub may not carry old commits to the new login.
  3. Suspended / deleted accounts — GitHub API omits them entirely.
  4. Bot accounts — some merge activity recorded under bot logins.
  5. Our merge_commits — maintainers have high merge counts that GitHub also credits.

Usage
-----
  python3 scripts/maintenance/validate_commit_counts.py          # use cached GH data
  python3 scripts/maintenance/validate_commit_counts.py --fetch  # re-fetch from API

Outputs
-------
  data/validation/commit_count_check.json   machine-readable comparison table
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]

REGISTRY_INDEX  = ROOT / "output/shared/contributors/registry_index.json"
GH_TOP_PATH     = ROOT / "metadata/github_top_contributors.json"
OUTPUT_PATH     = ROOT / "data/validation/commit_count_check.json"

# GitHub API
OWNER = "bitcoin"
REPO  = "bitcoin"
TOP_N = 100  # fetch/check this many from GitHub


# ---------------------------------------------------------------------------
# Token loading (same pattern as other scripts)
# ---------------------------------------------------------------------------
def get_token() -> str | None:
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        env_path = ROOT / ".env"
        if env_path.exists():
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("GITHUB_TOKEN="):
                        token = line.split("=", 1)[1].strip()
                        break
    return token or None


# ---------------------------------------------------------------------------
# GitHub fetch
# ---------------------------------------------------------------------------
def fetch_github_contributors(token: str, top_n: int) -> list[dict]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/contributors"
    results = []
    page = 1

    while len(results) < top_n:
        params = {"per_page": 100, "page": page, "anon": "false"}
        resp = requests.get(url, headers=headers, params=params, timeout=30)

        if resp.status_code == 403:
            reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait = max(reset - int(time.time()), 1)
            print(f"  Rate limited — sleeping {wait}s…")
            time.sleep(wait)
            continue

        resp.raise_for_status()
        page_data = resp.json()
        if not page_data:
            break

        results.extend(page_data)
        print(f"  Page {page}: {len(page_data)} contributors (total {len(results)})")

        if len(page_data) < 100:
            break
        page += 1

    return results[:top_n]


def load_or_fetch_github(force_fetch: bool) -> tuple[list[dict], str]:
    """Return (contributors_list, source_label)."""
    if not force_fetch and GH_TOP_PATH.exists():
        with open(GH_TOP_PATH) as f:
            cached = json.load(f)
        generated = cached.get("generated") or cached.get("fetched_at")
        print(f"  Using cached GitHub data ({GH_TOP_PATH.name}")
        if generated:
            print(f"  Fetched at: {generated}")
        return cached["contributors"][:TOP_N], f"cached ({generated or 'unknown date'})"

    token = get_token()
    if not token:
        sys.exit(
            "ERROR: GITHUB_TOKEN not set. Set it in .env or as an environment variable.\n"
            "  Alternatively, run without --fetch to use the cached metadata file."
        )

    print(f"Fetching top {TOP_N} contributors from GitHub API…")
    raw = fetch_github_contributors(token, TOP_N)

    contributors = [
        {
            "rank": i + 1,
            "login": entry["login"],
            "github_id": entry["id"],
            "contributions": entry["contributions"],
            "type": entry.get("type", "User"),
        }
        for i, entry in enumerate(raw)
    ]

    # Also save back to the cached location so future runs can use it
    payload = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "repo": f"{OWNER}/{REPO}",
        "total_fetched": len(contributors),
        "note": (
            "contributions = total commits GitHub attributes to this account "
            "(includes merge commits). Sorted descending."
        ),
        "contributors": contributors,
    }
    GH_TOP_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(GH_TOP_PATH, "w") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")
    print(f"  Saved fresh data to {GH_TOP_PATH.name}")

    return contributors, "freshly fetched"


# ---------------------------------------------------------------------------
# Registry index loader
# ---------------------------------------------------------------------------
def load_registry() -> dict[str, dict]:
    """Return login.lower() -> registry entry for all entries that have a login.

    Also maps secondary GitHub logins (from identities.json platforms.github lists)
    so that old/renamed accounts like murchandamus resolve to the canonical entry.
    """
    with open(REGISTRY_INDEX) as f:
        data = json.load(f)

    mapping = {}
    for entry in data["contributors"]:
        gh = entry.get("github")
        login = None
        if isinstance(gh, dict):
            login = gh.get("login")
        elif isinstance(gh, str):
            login = gh
        if login:
            mapping[login.lower()] = entry

    # Supplement with secondary logins from identities.json
    identities_path = Path("metadata/identities.json")
    if identities_path.exists():
        with open(identities_path) as f:
            ident_data = json.load(f)
        identities = ident_data.get("identities", []) if isinstance(ident_data, dict) else ident_data
        uuid_to_entry = {e.get("uuid"): e for e in data["contributors"] if e.get("uuid")}
        for ident in identities:
            uuid = ident.get("uuid")
            gh_field = ident.get("platforms", {}).get("github")
            if not gh_field or not isinstance(gh_field, list):
                continue
            entry = uuid_to_entry.get(uuid)
            if not entry:
                continue
            for secondary_login in gh_field:
                key = secondary_login.lower()
                if key not in mapping:  # don't overwrite primary
                    mapping[key] = entry

    return mapping


# ---------------------------------------------------------------------------
# Main comparison
# ---------------------------------------------------------------------------
def run(force_fetch: bool):
    print("=" * 70)
    print("  Commit Count Validation — bitcoin/bitcoin")
    print("=" * 70)
    print()
    print("  Source: data/sources/bitcoin (git log, bitcoin repo only)")
    print("  Merges ARE included in both total_commits and GitHub contributions.")
    print()

    if not REGISTRY_INDEX.exists():
        sys.exit(f"ERROR: {REGISTRY_INDEX} not found. Run the full pipeline first.")

    gh_contributors, gh_source = load_or_fetch_github(force_fetch)
    registry = load_registry()

    print(f"\n  Registry entries with a GitHub login : {len(registry):,}")
    print(f"  GitHub top-{TOP_N} contributors       : {len(gh_contributors):,}")
    print(f"  GitHub data source                   : {gh_source}")
    print()

    rows = []
    unmatched_gh = []

    for gh in gh_contributors:
        login = gh["login"].lower()
        gh_contributions = gh["contributions"]

        our = registry.get(login)
        if our is None:
            unmatched_gh.append(gh)
            continue

        total     = int(our.get("total_commits") or 0)
        authored  = int(our.get("authored_commits") or 0)
        merges    = int(our.get("merge_commits") or 0)

        # Delta vs total (most apples-to-apples since GitHub also counts merges)
        delta_total   = total - gh_contributions
        delta_pct     = ((total - gh_contributions) / gh_contributions * 100) if gh_contributions else 0

        # Flag big discrepancies (>30% off in either direction)
        flag = ""
        if abs(delta_pct) > 50:
            flag = "⚠️ LARGE GAP"
        elif abs(delta_pct) > 30:
            flag = "⚠️  big gap"

        rows.append({
            "gh_rank":        gh["rank"],
            "login":          gh["login"],
            "display_name":   our.get("display_name", ""),
            "gh_contributions": gh_contributions,
            "our_total":      total,
            "our_authored":   authored,
            "our_merges":     merges,
            "delta_total":    delta_total,
            "delta_pct":      round(delta_pct, 1),
            "flag":           flag,
        })

    # Print table
    header = (
        f"  {'#':>3}  {'login':<22}  {'GH contrib':>10}  "
        f"{'our total':>9}  {'authored':>8}  {'merges':>7}  {'Δ%':>6}  note"
    )
    sep = "  " + "-" * (len(header) - 2)
    print(header)
    print(sep)

    for r in rows:
        print(
            f"  {r['gh_rank']:>3}  {r['login']:<22}  {r['gh_contributions']:>10,}  "
            f"{r['our_total']:>9,}  {r['our_authored']:>8,}  {r['our_merges']:>7,}  "
            f"{r['delta_pct']:>+6.1f}%  {r['flag']}"
        )

    # Unmatched GitHub accounts (we have no registry entry for them)
    if unmatched_gh:
        print(f"\n  GitHub accounts with no match in registry ({len(unmatched_gh)}):")
        for gh in unmatched_gh[:20]:
            print(f"    #{gh['rank']:>3}  {gh['login']:<28}  {gh['contributions']:>6,}  (no registry entry)")
        if len(unmatched_gh) > 20:
            print(f"    … and {len(unmatched_gh) - 20} more")

    # Summary stats
    matched   = len(rows)
    big_gaps  = sum(1 for r in rows if r["flag"])
    avg_delta = sum(r["delta_pct"] for r in rows) / matched if matched else 0

    print()
    print(f"  Matched     : {matched} / {len(gh_contributors)}")
    print(f"  Big gaps    : {big_gaps} (>30% delta)")
    print(f"  Avg Δ%      : {avg_delta:+.1f}%  (positive = we count more than GitHub)")
    print()
    print("  Key notes:")
    print("    • Positive Δ = our count is higher than GitHub's.")
    print("      Common reasons: pre-GitHub era commits, account renames,")
    print("      or commits authored by an older email not tied to the account.")
    print("    • Negative Δ = GitHub counts more (rare; can happen if commits")
    print("      are attributed to an account we haven't resolved correctly).")
    print("    • Commits are from bitcoin/bitcoin only — BIPs repo excluded.")

    # Save JSON output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    output = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "github_data_source": gh_source,
        "registry_source": str(REGISTRY_INDEX),
        "commit_source": "data/sources/bitcoin (bitcoin/bitcoin git log only; BIPs excluded)",
        "note": (
            "total_commits = authored_commits + merge_commits from local git log. "
            "gh_contributions = GitHub API /contributors endpoint (also includes merges). "
            "delta_pct = (our_total - gh_contributions) / gh_contributions * 100."
        ),
        "summary": {
            "gh_top_n": len(gh_contributors),
            "matched": matched,
            "unmatched_gh_count": len(unmatched_gh),
            "big_gap_count": big_gaps,
            "avg_delta_pct": round(avg_delta, 2),
        },
        "rows": rows,
        "unmatched_github_accounts": unmatched_gh,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
        f.write("\n")

    print(f"\n  Saved to {OUTPUT_PATH.relative_to(ROOT)}")
    print()


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate our commit counts against GitHub API for bitcoin/bitcoin"
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Re-fetch contributor data from GitHub API (requires GITHUB_TOKEN)",
    )
    args = parser.parse_args()
    run(force_fetch=args.fetch)
