import os
import json
import urllib.request
import re
import argparse
from datetime import datetime
import time

# --- Configuration ---
OUTPUT_FILE = "data/raw/irc_meetings.json"
CURRENT_YEAR = datetime.now().year
WEEKLY_YEARS = [CURRENT_YEAR - 1, CURRENT_YEAR]  # Default: last year + this year
BACKFILL_YEARS = list(range(2015, CURRENT_YEAR + 1))  # Full archive
BASE_URL = "https://achow101.com/ircmeetings"

def fetch_url(url):
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            return response.read().decode('utf-8')
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def extract_links(html):
    # Looking for href="bitcoin-core-dev.YYYY-MM-DD_HH_MM.log.json"
    pattern = r'href="(bitcoin-core-dev\.(\d{4}-\d{2}-\d{2})_\d{2}_\d{2}\.log\.json)"'
    return re.findall(pattern, html)

def load_existing():
    """Load existing meetings and return as a dict keyed by date."""
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                existing = json.load(f)
            return {m['date']: m for m in existing}
        except Exception as e:
            print(f"Warning: Could not load existing data: {e}")
    return {}

def main():
    parser = argparse.ArgumentParser(description="Fetch IRC meeting logs from achow101.com")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="One-time backfill: fetch all years (2015–present) instead of just recent years",
    )
    args = parser.parse_args()

    years = BACKFILL_YEARS if args.backfill else WEEKLY_YEARS
    mode = "BACKFILL (2015–present)" if args.backfill else f"WEEKLY ({WEEKLY_YEARS})"
    print(f"IRC Meeting Ingest — mode: {mode}")

    # Load existing data to skip already-fetched meetings
    existing = load_existing()
    print(f"Existing meetings in cache: {len(existing)}")

    new_count = 0

    for year in years:
        print(f"Fetching index for {year}...")
        index_url = f"{BASE_URL}/{year}/"
        html = fetch_url(index_url)
        if not html:
            continue

        links = extract_links(html)
        # Filter out dates we already have
        new_links = [(f, d) for f, d in links if d not in existing]
        print(f"Found {len(links)} meetings in {year}, {len(new_links)} new.")

        for filename, date_str in new_links:
            json_url = f"{BASE_URL}/{year}/{filename}"
            print(f"  Downloading {date_str}...")

            content = fetch_url(json_url)
            if not content:
                continue

            try:
                data = json.loads(content)
                existing[date_str] = {
                    "date": date_str,
                    "url": json_url,
                    "messages": data.get("messages", [])
                }
                new_count += 1
            except Exception as e:
                print(f"Error parsing {filename}: {e}")

            time.sleep(0.2)  # Be polite to the server

    if new_count == 0:
        print("All meetings already up to date.")
    else:
        print(f"Fetched {new_count} new meetings.")

    # Write merged result
    all_meetings = list(existing.values())
    all_meetings.sort(key=lambda x: x['date'], reverse=True)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_meetings, f, indent=2)

    print(f"Total: {len(all_meetings)} meetings saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
