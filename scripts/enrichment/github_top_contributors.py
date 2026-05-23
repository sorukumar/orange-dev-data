"""
Fetch top contributors to bitcoin/bitcoin from the GitHub API.

GitHub's /repos/{owner}/{repo}/contributors endpoint returns contributors sorted
by commit count (descending), max 100 per page. We paginate until we have
at least TOP_N or run out of results.

Output: metadata/github_top_contributors.json
Schema per entry:
  {
    "rank": 1,
    "login": "laanwj",
    "github_id": 126646,
    "contributions": 5491,   # GitHub's count (all commits attributed to that account)
    "type": "User"           # "User" | "Bot"
  }
"""

import json
import os
import time
from datetime import datetime, timezone

import requests

# --- Configuration ---
OWNER = "bitcoin"
REPO = "bitcoin"
TOP_N = 300          # fetch up to this many; covers all meaningful contributors
OUTPUT_PATH = "metadata/github_top_contributors.json"


def get_token():
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("GITHUB_TOKEN="):
                        token = line.split("=", 1)[1].strip()
                        break
    return token


def fetch_contributors(token: str, top_n: int) -> list[dict]:
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
            print(f"  Rate limited — sleeping {wait}s...")
            time.sleep(wait)
            continue

        resp.raise_for_status()
        page_data = resp.json()
        if not page_data:
            break

        results.extend(page_data)
        print(f"  Page {page}: fetched {len(page_data)} contributors (total so far: {len(results)})")

        if len(page_data) < 100:
            break  # last page
        page += 1

    return results[:top_n]


def run():
    token = get_token()
    if not token:
        raise RuntimeError("GITHUB_TOKEN not set — set it in .env or as an env variable")

    print(f"Fetching top {TOP_N} contributors for {OWNER}/{REPO}...")
    raw = fetch_contributors(token, TOP_N)

    contributors = []
    for rank, entry in enumerate(raw, start=1):
        contributors.append({
            "rank": rank,
            "login": entry["login"],
            "github_id": entry["id"],
            "contributions": entry["contributions"],
            "type": entry.get("type", "User"),
        })

    output = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "repo": f"{OWNER}/{REPO}",
        "total_fetched": len(contributors),
        "note": (
            "contributions = total commits GitHub attributes to this account "
            "(includes merge commits where they are author). "
            "Sorted by contributions descending."
        ),
        "contributors": contributors,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
        f.write("\n")

    print(f"Saved {len(contributors)} contributors to {OUTPUT_PATH}")
    print(f"\nTop 10:")
    for c in contributors[:10]:
        print(f"  #{c['rank']:>3}  {c['login']:<25s} {c['contributions']:>6,} commits")


if __name__ == "__main__":
    run()
