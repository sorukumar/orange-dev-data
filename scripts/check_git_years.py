import subprocess
from datetime import datetime
import collections

repo_path = "raw_data/mailing_list"
cmd = ["git", "-C", repo_path, "log", "--format=%ad", "--date=short"]
result = subprocess.run(cmd, capture_output=True, text=True)

years = collections.Counter()
for line in result.stdout.splitlines():
    try:
        years[line.split('-')[0]] += 1
    except:
        pass

for year in sorted(years.keys()):
    print(f"{year}: {years[year]}")
