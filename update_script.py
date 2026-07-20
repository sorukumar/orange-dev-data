import re

path = '/Users/saurabhkumar/Desktop/Work/github/orange-dev-data/scripts/lab/compare_knots.py'
with open(path, 'r') as f:
    code = f.read()

code = code.replace("""def load_display_names():
    mapping = {}
    identities_file = PROJECT_ROOT / "metadata" / "identities.json"
    if identities_file.exists():
        with open(identities_file, 'r') as f:
            data = json.load(f)
            for record in data.get("identities", []):
                mapping[record["uuid"]] = record.get("display_name", record["uuid"])
    return mapping""", """def load_identities():
    mapping = {}
    identities_file = PROJECT_ROOT / "metadata" / "identities.json"
    if identities_file.exists():
        with open(identities_file, 'r') as f:
            data = json.load(f)
            for record in data.get("identities", []):
                mapping[record["uuid"]] = {
                    "name": record.get("display_name", record["uuid"]),
                    "github": record.get("platforms", {}).get("github")
                }
    return mapping""")

code = code.replace("""def get_graveyard_data(repo_dir, incremental_query):
    fmt = "%aI|||%cI|||%an|||%ae|||%s"
    log = run_cmd(
        f'git log {incremental_query} --no-merges --format="{fmt}"',
        cwd=repo_dir
    )
    
    commits = []
    for line in log.split('\\n'):
        if not line.strip():
            continue
        parts = line.split('|||', 4)
        if len(parts) < 5:
            continue
        
        author_date_str = parts[0].strip()
        committer_date_str = parts[1].strip()
        author_name = parts[2].strip()
        author_email = parts[3].strip()
        subject = parts[4].strip()""", """def get_graveyard_data(repo_dir, incremental_query):
    fmt = "%H|||%aI|||%cI|||%an|||%ae|||%s"
    log = run_cmd(
        f'git log {incremental_query} --no-merges --format="{fmt}"',
        cwd=repo_dir
    )
    
    commits = []
    for line in log.split('\\n'):
        if not line.strip():
            continue
        parts = line.split('|||', 5)
        if len(parts) < 6:
            continue
        
        commit_hash = parts[0].strip()
        author_date_str = parts[1].strip()
        committer_date_str = parts[2].strip()
        author_name = parts[3].strip()
        author_email = parts[4].strip()
        subject = parts[5].strip()""")

code = code.replace("""        commits.append({
            "author_date": a_date,
            "committer_date": c_date,
            "author_name": author_name,
            "author_uuid": author_uuid,
            "subject": subject,
            "delta_days": delta_days
        })""", """        commits.append({
            "hash": commit_hash,
            "author_date": a_date,
            "committer_date": c_date,
            "author_name": author_name,
            "author_uuid": author_uuid,
            "subject": subject,
            "delta_days": delta_days
        })""")

code = code.replace("""    print("Extracting metrics...")
    display_names = load_display_names()""", """    print("Extracting metrics...")
    identities = load_identities()""")

code = code.replace("""    def format_dev(uuid, counts_dict):
        name = display_names.get(uuid, uuid)
        has_profile = uuid in display_names
        if not has_profile and uuid.startswith('auto_'):
            name = uuid.replace('auto_', '').replace('_', ' ').title()
        return {'uuid': uuid, 'name': name, 'commits': counts_dict.get(uuid, 0), 'has_profile': has_profile}""", """    def format_dev(uuid, counts_dict):
        ident = identities.get(uuid, {})
        name = ident.get("name", uuid)
        github = ident.get("github")
        if isinstance(github, list) and len(github) > 0:
            github = github[0]
        has_profile = uuid in identities
        if not has_profile and uuid.startswith('auto_'):
            name = uuid.replace('auto_', '').replace('_', ' ').title()
        return {
            'uuid': uuid, 
            'name': name, 
            'commits': counts_dict.get(uuid, 0), 
            'has_profile': has_profile,
            'github': github
        }""")

code = code.replace("""    both_sorted = sorted([{'uuid': dev, 'name': display_names.get(dev, dev), 'commits_knots': knots_counts[dev], 'commits_core_all_time': core_counts.get(dev, 0), 'has_profile': dev in display_names} for dev in both], 
                         key=lambda x: x['commits_knots'], reverse=True)""", """    both_sorted = []
    for dev in both:
        ident = identities.get(dev, {})
        name = ident.get("name", dev)
        github = ident.get("github")
        if isinstance(github, list) and len(github) > 0:
            github = github[0]
        both_sorted.append({
            'uuid': dev,
            'name': name,
            'github': github,
            'commits_knots': knots_counts[dev],
            'commits_core_all_time': core_counts.get(dev, 0),
            'has_profile': dev in identities
        })
    both_sorted.sort(key=lambda x: x['commits_knots'], reverse=True)""")

code = code.replace("""    for dev in both_sorted:
        dev_commits = commits_by_author.get(dev['uuid'], [])
        dev['provenance'] = classify_provenance(dev, dev_commits)""", """    for dev in both_sorted:
        dev_commits = commits_by_author.get(dev['uuid'], [])
        dev['provenance'] = classify_provenance(dev, dev_commits)
        if dev['provenance'] in ['ancient_ghost', 'ghost', 'fast_tracked']:
            # Earliest salvaged commits
            salvaged = [c for c in dev_commits if c['delta_days'] > 60]
            salvaged.sort(key=lambda c: c['author_date'])
            dev['sample_commits'] = salvaged[:3]""")

with open(path, 'w') as f:
    f.write(code)

print("Updated script successfully!")
