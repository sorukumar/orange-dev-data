import os
import json
import pandas as pd
import re

INPUT_PR_PARQUET = "data/raw/github_pr_metadata.parquet"
OUTPUT_JSON = "output/tracker/releases.json"
CACHE_FILE = "data/raw/pr_summaries_cache.json"
HIGHLIGHTS_CACHE_FILE = "data/raw/release_highlights_cache.json"

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

def is_high_signal(labels_str, is_recent):
    if pd.isna(labels_str):
        return False
    labels = str(labels_str).lower()
    
    # Drop low signal unconditionally
    if any(drop in labels for drop in ['test', 'doc', 'refactor', 'build', 'ci']):
        return False
        
    if is_recent:
        # Tier 1 (recent): keep broadly
        return any(keep in labels for keep in ['consensus', 'p2p', 'rpc', 'rest', 'zmq', 'wallet', 'mempool', 'gui', 'policy'])
    else:
        # Tier 2 (old): strict "super super high signal"
        return any(keep in labels for keep in ['consensus', 'cryptography', 'p2p'])

def process_releases():
    if not os.path.exists(INPUT_PR_PARQUET):
        print(f"{INPUT_PR_PARQUET} does not exist. Run github_metadata.py first.")
        return

    df = pd.read_parquet(INPUT_PR_PARQUET)
    df = df[(df['repository_name'] == 'bitcoin/bitcoin') & (df['merged_at'].notna())].copy()
    
    # 1. Compute Release Cutoff Dates from explicitly tagged PRs
    tagged_df = df[df['milestone'].notna()]
    cutoff_dates = {}
    for ms, group in tagged_df.groupby('milestone'):
        max_date = pd.to_datetime(group['merged_at'], utc=True).max()
        cutoff_dates[ms] = max_date
        
    # Sort cutoffs by version
    sorted_cutoffs = sorted([(ms, date) for ms, date in cutoff_dates.items() if pd.notna(date)], key=lambda x: parse_version(x[0]))
    
    def infer_milestone_only(row):
        if pd.notna(row['milestone']):
            return row['milestone']
            
        # For untagged, find the earliest milestone whose cutoff is AFTER the merge date
        merged = pd.to_datetime(row['merged_at'], utc=True)
        if pd.isna(merged):
            return None
            
        inferred_ms = None
        for ms, cutoff in sorted_cutoffs:
            if merged <= cutoff:
                inferred_ms = ms
                break
        
        # If merged after the latest known cutoff, assign to the "next" / latest milestone
        if not inferred_ms and sorted_cutoffs:
            inferred_ms = sorted_cutoffs[-1][0]
                
        return inferred_ms

    df['inferred_milestone'] = df.apply(infer_milestone_only, axis=1)
    df = df[df['inferred_milestone'].notna()]
    
    # Calculate total PRs for the milestone BEFORE applying high-signal filter
    total_prs_per_milestone = df.groupby('inferred_milestone').size().to_dict()

    def filter_high_signal(row):
        inferred_ms = row['inferred_milestone']
        ms_version = parse_version(inferred_ms)
        is_recent = ms_version >= (24, 0, 0)
        if not is_high_signal(row['labels'], is_recent):
            return None
        return inferred_ms

    df['milestone'] = df.apply(filter_high_signal, axis=1)
    df = df[df['milestone'].notna()]
    
    pr_cache = load_cache(CACHE_FILE)
    highlights_cache = load_cache(HIGHLIGHTS_CACHE_FILE)
    
    # Load identities to map author to UUID and real name
    github_to_uuid = {}
    github_to_name = {}
    if os.path.exists("metadata/identities.json"):
        with open("metadata/identities.json", "r") as f:
            identities_data = json.load(f).get('identities', [])
            for identity in identities_data:
                gh_logins = identity.get('platforms', {}).get('github')
                display_name = identity.get('display_name')
                if gh_logins:
                    if isinstance(gh_logins, list):
                        for login in gh_logins:
                            github_to_uuid[login.lower()] = identity['uuid']
                            if display_name: github_to_name[login.lower()] = display_name
                    else:
                        github_to_uuid[gh_logins.lower()] = identity['uuid']
                        if display_name: github_to_name[gh_logins.lower()] = display_name
                        
    # Load expertise domains for canonical mapping
    expertise_domains = {}
    if os.path.exists("metadata/expertise_domains.json"):
        with open("metadata/expertise_domains.json", "r") as f:
            domains_data = json.load(f).get('domains', [])
            for d in domains_data:
                expertise_domains[d['id']] = d['name']
                
    def map_label_to_domain(label):
        label_lower = label.lower()
        if 'consensus' in label_lower or 'validation' in label_lower:
            return expertise_domains.get('Consensus', 'Consensus')
        if 'p2p' in label_lower or 'network' in label_lower:
            return expertise_domains.get('Network', 'P2P Network')
        if 'wallet' in label_lower:
            return expertise_domains.get('Wallet', 'Wallet & Keys')
        if 'tx fees and policy' in label_lower or 'mempool' in label_lower or 'policy' in label_lower:
            return expertise_domains.get('Mempool', 'Mempool & Fees')
        if any(x in label_lower for x in ['rpc', 'rest', 'zmq', 'refactoring', 'utils', 'log', 'libs', 'build system', 'tests', 'ci failed', 'docs', 'windows']):
            return expertise_domains.get('Infrastructure', 'Core Infrastructure')
        if 'script' in label_lower or 'covenant' in label_lower:
            return expertise_domains.get('Script', 'Script & Covenants')
        if 'crypto' in label_lower:
            return expertise_domains.get('Cryptography', 'Cryptography')
        if 'gui' in label_lower:
            return expertise_domains.get('Infrastructure', 'Core Infrastructure')
            
        return expertise_domains.get('Ecosystem', 'Ecosystem')

    releases_data = []
    grouped = df.groupby('milestone')
    
    for milestone, group in grouped:
        merged_dates = group['merged_at'].dropna()
        
        # Check if official release notes exist for this milestone in the source repo
        clean_ms = str(milestone).lstrip('v')
        notes_path_1 = f"data/sources/bitcoin/doc/release-notes/release-notes-{clean_ms}.md"
        notes_path_2 = f"data/sources/bitcoin/doc/release-notes/release-notes-{clean_ms}.0.md"
        is_released = os.path.exists(notes_path_1) or os.path.exists(notes_path_2)
        
        if not merged_dates.empty:
            max_date = pd.to_datetime(merged_dates, utc=True).max()
            last_active_date = max_date.strftime("%b %d, %Y")
            status = "released" if is_released else "upcoming"
        else:
            last_active_date = "TBD"
            status = "upcoming"
             
        release_obj = {
            "version": milestone,
            "status": status,
            "last_active_date": last_active_date,
            "summary": f"Release notes and changes for Bitcoin Core {milestone}.",
            "release_summary": None,
            "total_prs_in_release": total_prs_per_milestone.get(milestone, 0),
            "highlights": [],
            "prs": []
        }
        
        # Inject AI highlights if available
        if str(milestone) in highlights_cache:
            release_obj["release_summary"] = highlights_cache[str(milestone)].get("release_summary")
            release_obj["highlights"] = highlights_cache[str(milestone)].get("highlights", [])
        
        for _, pr in group.iterrows():
            pr_num = str(pr['pr_number'])
            title = pr['title']
            labels_str = pr['labels']
            author = str(pr.get('author', ''))
            author_uuid = github_to_uuid.get(author.lower(), None) if author else None
            author_name = github_to_name.get(author.lower(), None) if author else None
            
            # Calculate merge time
            merge_time_days = None
            if pd.notna(pr.get('created_at')) and pd.notna(pr.get('merged_at')):
                created_at = pd.to_datetime(pr['created_at'], utc=True)
                merged_at = pd.to_datetime(pr['merged_at'], utc=True)
                merge_time_days = (merged_at - created_at).days
            
            categories = []
            if pd.notna(labels_str):
                labels = labels_str.split('|')
                mapped_categories = set()
                for label in labels:
                    mapped_cat = map_label_to_domain(label)
                    mapped_categories.add(mapped_cat)
                categories = list(mapped_categories)
            if not categories:
                categories = [expertise_domains.get('Ecosystem', 'Ecosystem')]
                
            pub_summary = title
            tech_summary = "Technical details pending."
            impact_category = "Maintenance & Tech Debt"
            
            if pr_num in pr_cache:
                pub_summary = pr_cache[pr_num].get("public_summary", title)
                tech_summary = pr_cache[pr_num].get("technical_summary", "Technical details pending.")
                impact_category = pr_cache[pr_num].get("impact_category", "Maintenance & Tech Debt")
                
            pr_data = {
                "pr": f"#{pr_num}",
                "description": title,
                "author": author,
                "author_uuid": author_uuid,
                "author_name": author_name,
                "merge_time_days": merge_time_days,
                "categories": categories,
                "impact_category": impact_category,
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
