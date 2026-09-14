import os
import json
import re
from datetime import datetime

# The known tracking issues for major projects in Bitcoin Core
import os
import json
import re
from datetime import datetime

METADATA_FILE = "metadata/projects.json"
SOURCE_DIR = "data/sources/bitcoin-github-metadata/issues"
OUTPUT_FILE = "data/raw/tracking_issues.json"

def load_projects_metadata():
    """Loads curated project metadata from metadata/projects.json."""
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    print(f"Warning: {METADATA_FILE} not found.")
    return {}

def parse_checklists(body):
    """
    Parses markdown checklists from the issue body.
    Matches lines like: 
    - [x] Task 1
    - [ ] Task 2
    * [x] Task 3
    """
    tasks = []
    if not body:
        return tasks
        
    pattern = re.compile(r'^\s*[-*]\s+\[([ xX])\]\s+(.+)$', re.MULTILINE)
    
    matches = pattern.findall(body)
    for check_str, text in matches:
        is_checked = check_str.strip().lower() == 'x'
        
        pr_pattern = re.compile(r'#(\d{5})|pull/(\d{5})')
        pr_matches = pr_pattern.findall(text)
        linked_prs = []
        for p1, p2 in pr_matches:
            if p1: linked_prs.append(int(p1))
            if p2: linked_prs.append(int(p2))
            
        tasks.append({
            "text": text.strip(),
            "is_completed": is_checked,
            "linked_prs": list(set(linked_prs))
        })
        
    return tasks

def main():
    projects_meta = load_projects_metadata()
    if not projects_meta:
        print("No project metadata available. Exiting.")
        return
        
    if not os.path.exists(SOURCE_DIR):
        print(f"Error: Source directory {SOURCE_DIR} not found. Have you cloned the metadata repo?")
        return
        
    projects = []
    
    for issue_id_str, meta in projects_meta.items():
        issue_id = int(issue_id_str)
        project_name = meta.get("project_name", f"Issue #{issue_id}")
        
        file_path = os.path.join(SOURCE_DIR, f"{issue_id}.json")
        if not os.path.exists(file_path):
            print(f"Warning: Issue #{issue_id} ({project_name}) JSON not found.")
            continue
            
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        issue_data = data.get('issue', {})
        if not issue_data:
            print(f"Warning: Issue #{issue_id} has unexpected JSON structure.")
            continue
            
        title = issue_data.get('title', project_name)
        body = issue_data.get('body', '')
        created_at = issue_data.get('created_at')
        updated_at = issue_data.get('updated_at')
        html_url = issue_data.get('html_url', f"https://github.com/bitcoin/bitcoin/issues/{issue_id}")
        
        tasks = parse_checklists(body)
        total_tasks = len(tasks)
        completed_tasks = sum(1 for t in tasks if t['is_completed'])
        
        completion_percentage = 0
        if total_tasks > 0:
            completion_percentage = round((completed_tasks / total_tasks) * 100)
            
        print(f"Processed #{issue_id} ({project_name}): {completed_tasks}/{total_tasks} tasks ({completion_percentage}%)")
        
        projects.append({
            "issue_id": issue_id,
            "project_name": project_name,
            "description": meta.get("description", ""),
            "category": meta.get("category", "General"),
            "champion": meta.get("champion"),
            "bip": meta.get("bip"),
            "title": title,
            "url": html_url,
            "created_at": created_at,
            "updated_at": updated_at,
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "completion_percentage": completion_percentage,
            "tasks": tasks
        })
        
    projects.sort(key=lambda x: x.get('updated_at') or "", reverse=True)
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(projects, f, indent=2)
        
    print(f"\nSaved {len(projects)} tracking issues to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

