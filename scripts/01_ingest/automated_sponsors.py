import os
import yaml
import json
import glob
import re

def extract_github_handle(url):
    if not url or "github.com" not in url:
        return None
    url = url.split("?")[0].strip("/")
    parts = url.split("/")
    if len(parts) >= 4:
        # e.g., https://github.com/fanquake
        return parts[3]
    return None

def process_brink(data_dir):
    team_path = os.path.join(data_dir, "data/sources/brink/_data/team.yml")
    results = []
    if not os.path.exists(team_path):
        print(f"Brink team.yml not found at {team_path}")
        return results
        
    with open(team_path, "r") as f:
        team = yaml.safe_load(f)
        
    for category in ["grantees", "alumni"]:
        for person in team.get(category, []) or []:
            name = person.get("name")
            handle = None
            link = person.get("link", "")
            if "github.com" in link:
                handle = extract_github_handle(link)
            
            # Sometimes Brink puts the handle in the name, e.g., "Michael Ford (fanquake)"
            m = re.search(r"\(([^)]+)\)", name)
            if m and not handle:
                handle = m.group(1)
                
            clean_name = re.sub(r" \([^)]+\)", "", name)
                
            results.append({
                "name": clean_name,
                "github": handle,
                "sponsor": "brink",
                "start_date": None,
                "project_name": "Bitcoin Core"
            })
    return results

def process_opensats(data_dir):
    results = []
    projects_dir = os.path.join(data_dir, "data/sources/opensats/data/projects")
    if not os.path.exists(projects_dir):
        print(f"OpenSats projects dir not found at {projects_dir}")
        return results
        
    for file_path in glob.glob(os.path.join(projects_dir, "*.mdx")):
        with open(file_path, "r") as f:
            content = f.read()
            match = re.match(r"^---\n(.*?)\n---", content, re.DOTALL)
            if match:
                try:
                    frontmatter = yaml.safe_load(match.group(1))
                    git_url = frontmatter.get("git", "")
                    handle = extract_github_handle(git_url)
                    nym = frontmatter.get("nym")
                    if handle and nym:
                        results.append({
                            "name": nym,
                            "github": handle,
                            "sponsor": "opensats",
                            "start_date": frontmatter.get("dateAdded")[:7] if frontmatter.get("dateAdded") else None,
                            "project_name": frontmatter.get("title")
                        })
                except Exception as e:
                    print(f"Error parsing {file_path}: {e}")
    return results

def main():
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    brink_grants = process_brink(root_dir)
    opensats_grants = process_opensats(root_dir)
    
    all_grants = brink_grants + opensats_grants
    
    out_dir = os.path.join(root_dir, "data/raw")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "automated_grants.json")
    
    with open(out_path, "w") as f:
        json.dump(all_grants, f, indent=2)
        
    print(f"✅ Extracted {len(all_grants)} automated grants to {out_path}")

if __name__ == "__main__":
    main()
