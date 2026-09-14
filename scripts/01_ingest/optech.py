import os
import json
import re
import subprocess
from datetime import datetime
import glob

# --- Configuration ---
OPTECH_REPO_URL = "https://github.com/bitcoinops/bitcoinops.github.io.git"
OPTECH_DIR = "data/sources/optech"
NEWSLETTER_DIR = os.path.join(OPTECH_DIR, "_posts", "en", "newsletters")
OUTPUT_FILE = "data/raw/optech.json"

def clone_or_pull_repo():
    if not os.path.exists(OPTECH_DIR):
        print(f"Cloning {OPTECH_REPO_URL} into {OPTECH_DIR}...")
        subprocess.run(["git", "clone", OPTECH_REPO_URL, OPTECH_DIR], check=True)
    else:
        print(f"Pulling latest changes in {OPTECH_DIR}...")
        subprocess.run(["git", "-C", OPTECH_DIR, "pull", "origin", "master"], check=True)

def parse_frontmatter(content):
    """Extracts Jekyll YAML frontmatter from the markdown content."""
    frontmatter = {}
    if content.startswith("---"):
        end_idx = content.find("---", 3)
        if end_idx != -1:
            yaml_content = content[3:end_idx]
            for line in yaml_content.splitlines():
                if ":" in line:
                    key, value = line.split(":", 1)
                    frontmatter[key.strip()] = value.strip().strip("'\"")
    return frontmatter

def extract_notable_changes(content):
    """
    Extracts the 'Notable code and documentation changes' section.
    Usually it is demarcated by a header like '## Notable code and documentation changes'
    and ends at the next '## ' header or end of file.
    """
    # Regex to find the notable changes section
    # Note: Optech sometimes varies the exact heading name slightly, e.g., "Notable code changes", "Notable commits"
    pattern = re.compile(r'##\s*Notable (?:code|commit|documentation)[\s\S]*?(?=## |\Z)', re.IGNORECASE)
    match = pattern.search(content)
    if match:
        section = match.group(0).strip()
        return section
    return None

def extract_notable_prs(content):
    """
    Parses notable changes section for specific PR mentions.
    Returns a list of dicts: [{'repo_alias': 'Bitcoin Core', 'pr_number': 32471, 'description': '...'}]
    """
    pattern = re.compile(r'(?:^|\n)[-*]\s+\[([^\]]+?)\s+#(\d+)\](?:\[\]|\([^\)]*\))\s*(.*?)(?=\n[-*]\s+\[|\Z)', re.IGNORECASE | re.DOTALL)
    prs = []
    for match in pattern.finditer(content):
        repo_alias = match.group(1).strip()
        pr_number = int(match.group(2))
        desc = match.group(3).strip()
        prs.append({
            "repo_alias": repo_alias,
            "pr_number": pr_number,
            "description": desc
        })
    return prs

def main():
    clone_or_pull_repo()
    
    if not os.path.exists(NEWSLETTER_DIR):
        print(f"Error: Newsletter directory not found at {NEWSLETTER_DIR}")
        return
        
    md_files = glob.glob(os.path.join(NEWSLETTER_DIR, "*.md"))
    print(f"Found {len(md_files)} newsletter files.")
    
    newsletters = []
    
    for file_path in md_files:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        filename = os.path.basename(file_path)
        frontmatter = parse_frontmatter(content)
        notable_changes = extract_notable_changes(content)
        
        if notable_changes:
            date_str = filename[:10]
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d").isoformat()
            except ValueError:
                date_obj = None
                
            parsed_prs = extract_notable_prs(notable_changes)
                
            newsletters.append({
                "filename": filename,
                "title": frontmatter.get("title", ""),
                "date": date_obj,
                "notable_changes_raw": notable_changes,
                "prs": parsed_prs
            })
            
    # Sort by date descending
    newsletters.sort(key=lambda x: x["date"] or "", reverse=True)
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(newsletters, f, indent=2)
        
    print(f"Extracted notable changes from {len(newsletters)} newsletters.")
    print(f"Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
