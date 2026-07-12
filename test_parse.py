import re
import os
import glob

files = glob.glob("data/sources/bitcoin/doc/release-notes/*.md")
for f in files:
    with open(f, 'r') as file:
        content = file.read()
        # Find all PRs like "- #12345" or "- #123("
        prs = re.findall(r'^-\s+#\d+', content, re.MULTILINE)
        print(f"{os.path.basename(f)}: {len(prs)} PRs found")
