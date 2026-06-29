import os
import json
import pandas as pd
import re

INPUT_PR_PARQUET = "data/raw/github_pr_metadata.parquet"
OUTPUT_JSON = "output/tracker/releases.json"
CACHE_FILE = "data/raw/pr_summaries_cache.json"

def parse_version(v_str):
    matches = re.findall(r'\d+', str(v_str))
    if not matches:
        return (0, 0, 0)
    ints = [int(m) for m in matches]
    while len(ints) < 3:
        ints.append(0)
    return tuple(ints[:3])

def load_cache(path):
    if os.path.exists(path):
        with open(path, 'r') as f:
            try:
                return json.load(f)
            except:
                return {}
    return {}

def process_releases():
    if not os.path.exists(INPUT_PR_PARQUET):
        print(f"{INPUT_PR_PARQUET} does not exist. Run github_metadata.py first.")
        return

    df = pd.read_parquet(INPUT_PR_PARQUET)
    df = df[(df['repository_name'] == 'bitcoin/bitcoin') & (df['milestone'].notna())]
    
    pr_cache = load_cache(CACHE_FILE)
    
    # Load identities to map author to UUID
    github_to_uuid = {}
    if os.path.exists("metadata/identities.json"):
        with open("metadata/identities.json", "r") as f:
            identities_data = json.load(f).get('identities', [])
            for identity in identities_data:
                gh_logins = identity.get('platforms', {}).get('github')
                if gh_logins:
                    if isinstance(gh_logins, list):
                        for login in gh_logins:
                            github_to_uuid[login.lower()] = identity['uuid']
                    else:
                        github_to_uuid[gh_logins.lower()] = identity['uuid']
                        
    releases_data = []
    grouped = df.groupby('milestone')
    
    for milestone, group in grouped:
        merged_dates = group['merged_at'].dropna()
        if not merged_dates.empty:
            max_date = pd.to_datetime(merged_dates, utc=True).max()
            last_active_date = max_date.strftime("%b %d, %Y")
            open_prs = group[group['closed_at'].isna()]
            status = "released" if open_prs.empty else "upcoming"
        else:
            last_active_date = "TBD"
            status = "upcoming"
             
        release_obj = {
            "version": milestone,
            "status": status,
            "last_active_date": last_active_date,
            "summary": f"Release notes and changes for Bitcoin Core {milestone}.",
            "prs": []
        }
        
        for _, pr in group.iterrows():
            pr_num = str(pr['pr_number'])
            title = pr['title']
            labels_str = pr['labels']
            author = str(pr.get('author', ''))
            author_uuid = github_to_uuid.get(author.lower(), None) if author else None
            
            # Calculate merge time
            merge_time_days = None
            if pd.notna(pr.get('created_at')) and pd.notna(pr.get('merged_at')):
                created_at = pd.to_datetime(pr['created_at'], utc=True)
                merged_at = pd.to_datetime(pr['merged_at'], utc=True)
                merge_time_days = (merged_at - created_at).days
            
            categories = []
            if pd.notna(labels_str):
                labels = labels_str.split('|')
                for label in labels:
                    categories.append(label)
            if not categories:
                categories = ["Uncategorized"]
                
            pub_summary = title
            tech_summary = "Technical details pending."
            
            if pr_num in pr_cache:
                pub_summary = pr_cache[pr_num].get("public_summary", title)
                tech_summary = pr_cache[pr_num].get("technical_summary", "Technical details pending.")
                
            pr_data = {
                "pr": f"#{pr_num}",
                "description": title,
                "author": author,
                "author_uuid": author_uuid,
                "merge_time_days": merge_time_days,
                "categories": categories,
                "public_summary": pub_summary,
                "technical_summary": tech_summary
            }
            release_obj["prs"].append(pr_data)
            
        releases_data.append(release_obj)

    releases_data.sort(key=lambda x: parse_version(x["version"]), reverse=True)

    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(releases_data, f, indent=2)
        
    print(f"Exported {len(releases_data)} releases to {OUTPUT_JSON}")

if __name__ == "__main__":
    process_releases()
