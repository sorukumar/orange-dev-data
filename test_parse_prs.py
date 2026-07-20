import re
import os
import glob
import pandas as pd

files = glob.glob("data/sources/bitcoin/doc/release-notes/*.md")
pr_to_milestone = {}
for f in files:
    filename = os.path.basename(f)
    # Extract version from filename, e.g., release-notes-31.1.md -> 31.1
    m = re.search(r'release-notes-(.+)\.md', filename)
    if not m: continue
    version = m.group(1)
    # the milestone format used is 'v31.1' if it's parsed, or just '31.1'
    # Wait, releases.py uses '31.1', wait let's check df['milestone'].
    with open(f, 'r') as file:
        content = file.read()
        # Find all #12345 (ignoring repo prefixes like bitcoin-core/gui#123)
        # Actually, let's just find anything like "- #12345"
        prs = re.findall(r'^-\s+#(\d+)', content, re.MULTILINE)
        for pr_str in prs:
            pr_to_milestone[int(pr_str)] = version

print(f"Extracted {len(pr_to_milestone)} PR mappings from release notes.")
print("Sample:", list(pr_to_milestone.items())[:5])
