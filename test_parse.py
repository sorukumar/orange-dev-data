import os
import yaml
import glob
import re

opensats_files = glob.glob("data/sources/opensats/data/projects/*.mdx")
for f in opensats_files:
    with open(f, "r") as file:
        content = file.read()
        match = re.match(r"^---\n(.*?)\n---", content, re.DOTALL)
        if match:
            frontmatter = yaml.safe_load(match.group(1))
            git_url = frontmatter.get("git", "")
            if "github.com" in git_url:
                parts = git_url.strip("/").split("/")
                if len(parts) >= 4:
                    handle = parts[3]
                    print(f"OpenSats Project: {frontmatter.get('title')}, Handle: {handle}, Git: {git_url}")

