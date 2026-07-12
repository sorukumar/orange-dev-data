import os
import sys
import json
import subprocess
import difflib
from pathlib import Path
from datetime import datetime
import random

# Setup paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from scripts.utils.subsystem import score_with_details

# Import identity resolver
try:
    from scripts.utils.identity import resolver
except ImportError:
    print("Warning: Could not import identity resolver.")
    resolver = None

# Load Core PR Subject Index
core_pr_subjects = set()
try:
    pr_index_path = PROJECT_ROOT / "data" / "enriched" / "core_pr_subjects.json"
    if pr_index_path.exists():
        with open(pr_index_path, 'r', encoding='utf-8') as f:
            core_pr_subjects = set(json.load(f))
        print(f"Loaded {len(core_pr_subjects)} Core PR subjects.")
    else:
        print("Warning: core_pr_subjects.json not found. Run build_core_pr_index.py first.")
except Exception as e:
    print(f"Error loading Core PR index: {e}")


SOURCES_DIR = PROJECT_ROOT / "data" / "sources"
OUTPUT_DIR = PROJECT_ROOT / "output" / "lab"

CORE_DIR = SOURCES_DIR / "bitcoin"
KNOTS_DIR = SOURCES_DIR / "bitcoinknots"

CORE_REPO_URL = "https://github.com/bitcoin/bitcoin.git"
KNOTS_REPO_URL = "https://github.com/bitcoinknots/bitcoin.git"

def run_cmd(cmd, cwd=None, check=True):
    result = subprocess.run(cmd, cwd=cwd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if check and result.returncode != 0:
        print(f"Command failed: {cmd}")
        print(f"Error: {result.stderr}")
        sys.exit(1)
    return result.stdout.strip()

def get_default_branch(repo_dir, remote="origin"):
    run_cmd(f"git remote set-head {remote} -a", cwd=repo_dir, check=False)
    ref = run_cmd(f"git rev-parse --abbrev-ref {remote}/HEAD", cwd=repo_dir)
    return ref.replace(f"{remote}/", "")

def update_repo(repo_url, repo_dir):
    if not repo_dir.exists():
        print(f"Cloning {repo_url}...")
        run_cmd(f"git clone {repo_url} {repo_dir}")
    else:
        print(f"Updating {repo_dir.name}...")
        run_cmd("git fetch origin", cwd=repo_dir)
    branch = get_default_branch(repo_dir, "origin")
    run_cmd(f"git checkout {branch} && git pull origin {branch}", cwd=repo_dir)
    return branch

def setup_repos():
    print("Setting up repositories...")
    SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    core_branch = update_repo(CORE_REPO_URL, CORE_DIR)
    knots_branch = update_repo(KNOTS_REPO_URL, KNOTS_DIR)
    remotes = run_cmd("git remote", cwd=CORE_DIR)
    if "knots" not in remotes.split():
        run_cmd(f"git remote add knots {KNOTS_DIR}", cwd=CORE_DIR)
    run_cmd("git fetch knots", cwd=CORE_DIR)
    return core_branch, knots_branch

def get_git_hygiene(repo_dir, branch, since=None):
    since_arg = f'--since="{since}" ' if since else ''
    merges = int(run_cmd(f'git log {branch} {since_arg}--merges --format="%H" | wc -l', cwd=repo_dir))
    no_merges = int(run_cmd(f'git log {branch} {since_arg}--no-merges --format="%H" | wc -l', cwd=repo_dir))
    total = merges + no_merges
    return {
        "merges": merges,
        "clean_commits": no_merges,
        "total": total,
        "merge_ratio": (merges / total) if total > 0 else 0
    }

def get_divergence(core_branch, knots_branch):
    compare_str = f"{core_branch}...knots/{knots_branch}"
    left_right = run_cmd(f"git rev-list --left-right --count {compare_str}", cwd=CORE_DIR)
    core_ahead, knots_ahead = map(int, left_right.split())
    diff_stat = run_cmd(f"git diff --shortstat {compare_str}", cwd=CORE_DIR)
    
    # Get dirstat for narrative insight
    dirstat = run_cmd(f"git diff --dirstat {compare_str}", cwd=CORE_DIR)
    dir_breakdown = {}
    for line in dirstat.split('\n'):
        if '%' in line:
            parts = line.strip().split('%', 1)
            if len(parts) == 2:
                pct = float(parts[0].strip())
                folder = parts[1].strip()
                dir_breakdown[folder] = pct
                
    return {
        "commits_core_ahead": core_ahead,
        "commits_knots_ahead": knots_ahead,
        "diff_shortstat": diff_stat,
        "dirstat": dir_breakdown
    }

def load_identities():
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
    return mapping

def get_authors_detailed(repo_dir, branch_query, since=None):
    since_arg = f'--since="{since}" ' if since else ''
    log = run_cmd(f'git log {branch_query} {since_arg}--no-merges --format="%an|%ae"', cwd=repo_dir)
    authors_counts = {}
    for line in log.split('\n'):
        if not line.strip(): continue
        name, email = line.split('|', 1)
        name, email = name.strip(), email.strip()
        if resolver:
            uuid = resolver.resolve_git(name, email)
        else:
            uuid = name.lower().replace(' ', '_')
        authors_counts[uuid] = authors_counts.get(uuid, 0) + 1
    return authors_counts

def get_commits_over_time(repo_dir, branch_query, since=None):
    since_arg = f'--since="{since}" ' if since else ''
    log = run_cmd(f'git log {branch_query} {since_arg}--no-merges --format="%ad" --date=short', cwd=repo_dir)
    trend = {}
    for line in log.split('\n'):
        if not line.strip(): continue
        month = line[:7] # YYYY-MM
        trend[month] = trend.get(month, 0) + 1
    return trend

# Manual overrides for known edge cases where deterministic metadata matching fails
# (e.g. heavily rewritten commit subjects by Luke Dashjr, or broken git author mapping)
MANUAL_OVERRIDES = {
    "auto_codeabysss": "ancient_ghost",
    "can_alicexbt": "ancient_ghost"
}

def classify_provenance(dev_dict, dev_commits, luke_uuid="can_luke_dashjr"):
    if dev_dict['uuid'] == luke_uuid:
        return "lead"
        
    if dev_dict['uuid'] in MANUAL_OVERRIDES:
        return MANUAL_OVERRIDES[dev_dict['uuid']]
    
    if not dev_commits:
        return "active_dual" if dev_dict['commits_core_1yr'] > 0 else "ancient_ghost"
        
    core_matches = 0
    for c in dev_commits:
        subj = c['subject'].strip().lower()
        body = c.get('body', '').lower()
        if subj in core_pr_subjects or 'github-pull:' in body or 'rebased-from:' in body:
            core_matches += 1
        else:
            # Try fuzzy matching against the PR index (find any subject with > 80% similarity)
            # To avoid checking 70k strings, we only check if at least 4 words match
            words = set(subj.split())
            if len(words) > 3:
                matched = False
                for pr_subj in core_pr_subjects:
                    if len(pr_subj) > 10 and len(words.intersection(pr_subj.split())) >= 4:
                        similarity = difflib.SequenceMatcher(None, subj, pr_subj).ratio()
                        if similarity > 0.75:
                            core_matches += 1
                            matched = True
                            break
                if matched:
                    continue

    # If the majority of their commits match Core PRs, they are a ghost
    if core_matches >= len(dev_commits) * 0.5:
        avg_delta = sum(c['delta_days'] for c in dev_commits) / len(dev_commits)
        if avg_delta > 60:
            return "ancient_ghost"
        else:
            return "fast_tracked"
            
    # If majority do not match Core PRs, they likely intentionally contributed to Knots
    return "active_dual"

def get_graveyard_data(repo_dir, incremental_query):
    fmt = "---COMMIT---%n%H|||%aI|||%cI|||%an|||%ae|||%s|||%b"
    log = run_cmd(
        f'git log {incremental_query} --no-merges --format="{fmt}"',
        cwd=repo_dir
    )
    
    commits = []
    for block in log.split('---COMMIT---\n'):
        if not block.strip():
            continue
        parts = block.split('|||', 6)
        if len(parts) < 6:
            continue
        
        commit_hash = parts[0].strip()
        author_date_str = parts[1].strip()
        committer_date_str = parts[2].strip()
        author_name = parts[3].strip()
        author_email = parts[4].strip()
        subject = parts[5].strip()
        body = parts[6].strip() if len(parts) > 6 else ""
        
        if resolver:
            author_uuid = resolver.resolve_git(author_name, author_email)
        else:
            author_uuid = author_name.lower().replace(' ', '_')
        
        try:
            a_date = author_date_str[:10]
            c_date = committer_date_str[:10]
            a_dt = datetime.strptime(a_date, "%Y-%m-%d")
            c_dt = datetime.strptime(c_date, "%Y-%m-%d")
            delta_days = (c_dt - a_dt).days
        except (ValueError, IndexError):
            a_date = author_date_str[:10]
            c_date = committer_date_str[:10]
            delta_days = 0
        
        commits.append({
            "hash": commit_hash,
            "author_date": a_date,
            "committer_date": c_date,
            "author_name": author_name,
            "author_uuid": author_uuid,
            "subject": subject,
            "body": body,
            "delta_days": delta_days
        })
    return commits

def analyze(core_branch, knots_branch):
    print("Extracting metrics...")
    identities = load_identities()
    
    core_hygiene = get_git_hygiene(CORE_DIR, core_branch)
    knots_hygiene = get_git_hygiene(KNOTS_DIR, knots_branch)
    
    divergence = get_divergence(core_branch, knots_branch)
    
    # Core developers (All Time)
    core_counts = get_authors_detailed(CORE_DIR, core_branch)
    core_all_time_authors = set(core_counts.keys())
    
    # Knots developers (ONLY incremental commits on top of core, All Time)
    knots_incremental_query = f"{core_branch}..knots/{knots_branch}"
    knots_counts = get_authors_detailed(CORE_DIR, knots_incremental_query)
    
    knots_authors = set(knots_counts.keys())
    
    both = list(knots_authors.intersection(core_all_time_authors))
    core_only = list(core_all_time_authors - knots_authors)
    knots_only = list(knots_authors - core_all_time_authors)
    
    def format_dev(uuid, counts_dict):
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
        }

    # Sort Knots developers by commit count (Bus Factor analysis)
    knots_devs_sorted = sorted([format_dev(dev, knots_counts) for dev in knots_authors], 
                               key=lambda x: x['commits'], reverse=True)
                               
    knots_only_sorted = sorted([format_dev(dev, knots_counts) for dev in knots_only], 
                               key=lambda x: x['commits'], reverse=True)
                               
    both_sorted = []
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
    both_sorted.sort(key=lambda x: x['commits_knots'], reverse=True)
    
    print("Extracting graveyard data (author vs committer dates)...")
    graveyard_raw = get_graveyard_data(CORE_DIR, knots_incremental_query)
    
    commits_by_author = {}
    for c in graveyard_raw:
        commits_by_author.setdefault(c['author_uuid'], []).append(c)

    for dev in both_sorted:
        dev_commits = commits_by_author.get(dev['uuid'], [])
        dev['provenance'] = classify_provenance(dev, dev_commits)
        if dev['provenance'] in ['ancient_ghost', 'ghost', 'fast_tracked', 'lead']:
            salvaged = [c for c in dev_commits if c['delta_days'] > 60]
            salvaged.sort(key=lambda c: c['author_date'])
            samples = []
            if len(salvaged) > 1:
                # Append oldest and newest
                samples = [salvaged[0], salvaged[-1]]
            elif len(salvaged) == 1:
                samples = salvaged
            
            for c in samples:
                # Apply categorization
                cat, _, _ = score_with_details(c['subject'])
                c['category'] = cat
            dev['sample_commits'] = samples
        
    total_knots_commits = sum(knots_counts.values())
    luke_commits = knots_counts.get("can_luke_dashjr", 0)
    community_commits = sum(knots_counts.get(dev, 0) for dev in knots_only)
    
    luke_salvaged = 0
    luke_fast_tracked = 0
    luke_native = 0
    
    ancient_ghost_commits = 0
    fast_tracked_commits = 0
    knots_community_commits = 0
    
    for c in graveyard_raw:
        is_luke = (c['author_uuid'] == "can_luke_dashjr")
        subj = c['subject'].strip().lower()
        body = c.get('body', '').lower()
        
        # Check against deterministic metadata
        if subj in core_pr_subjects or 'github-pull:' in body or 'rebased-from:' in body:
            if c['delta_days'] > 60:
                if is_luke: luke_salvaged += 1
                else: ancient_ghost_commits += 1
            else:
                if is_luke: luke_fast_tracked += 1
                else: fast_tracked_commits += 1
        else:
            if is_luke: luke_native += 1
            else: knots_community_commits += 1
            
    # Note: community_commits is updated dynamically here, so it overrides the old logic.
    community_commits = knots_community_commits
    
    provenance_summary = {
        "total_incremental_commits": total_knots_commits,
        "luke": {
            "commits": luke_commits,
            "pct": round((luke_commits / total_knots_commits) * 100, 1) if total_knots_commits > 0 else 0,
            "breakdown": {
                "salvaged": luke_salvaged,
                "fast_tracked": luke_fast_tracked,
                "native": luke_native
            }
        },
        "salvaged_from_core": {
            "commits": ancient_ghost_commits,
            "pct": round((ancient_ghost_commits / total_knots_commits) * 100, 1) if total_knots_commits > 0 else 0
        },
        "fast_tracked": {
            "commits": fast_tracked_commits,
            "pct": round((fast_tracked_commits / total_knots_commits) * 100, 1) if total_knots_commits > 0 else 0
        },
        "knots_community": {
            "commits": community_commits,
            "pct": round((community_commits / total_knots_commits) * 100, 1) if total_knots_commits > 0 else 0
        }
    }
    
    graveyard_salvaged = [c for c in graveyard_raw if c['delta_days'] > 60]
    
    for c in graveyard_salvaged:
        cat, _, _ = score_with_details(c['subject'])
        c['category'] = cat
    graveyard_recent = [c for c in graveyard_raw if c['delta_days'] <= 60]
    
    graveyard_summary = {
        "total_commits": len(graveyard_raw),
        "salvaged_count": len(graveyard_salvaged),
        "recent_count": len(graveyard_recent),
        "max_delta_days": max((c['delta_days'] for c in graveyard_raw), default=0),
        "avg_delta_days": round(sum(c['delta_days'] for c in graveyard_raw) / len(graveyard_raw), 1) if graveyard_raw else 0
    }
    
    graveyard_plot_data = graveyard_salvaged + (random.sample(graveyard_recent, min(50, len(graveyard_recent))) if graveyard_recent else [])
    graveyard_plot_data.sort(key=lambda c: c['author_date'])

    # Trends
    core_trend = get_commits_over_time(CORE_DIR, core_branch)
    knots_trend = get_commits_over_time(CORE_DIR, knots_incremental_query)
    
    print("Extracting 1-year hygiene and mindshare...")
    one_year_ago = "1 year ago"
    core_hygiene_1yr = get_git_hygiene(CORE_DIR, core_branch, since=one_year_ago)
    knots_hygiene_1yr = get_git_hygiene(KNOTS_DIR, knots_branch, since=one_year_ago)
    
    core_counts_1yr = get_authors_detailed(CORE_DIR, core_branch, since=one_year_ago)
    
    from datetime import timedelta
    one_year_ago_dt = datetime.now() - timedelta(days=365)
    one_year_ago_str = one_year_ago_dt.strftime("%Y-%m-%d")
    
    knots_active_uuids = set()
    for c in graveyard_raw:
        if c['author_date'] >= one_year_ago_str:
            knots_active_uuids.add(c['author_uuid'])
            
    ghost_uuids = set(dev['uuid'] for dev in both_sorted if dev.get('provenance') in ['ancient_ghost', 'fast_tracked'])
    knots_active_filtered = knots_active_uuids - ghost_uuids
    
    overlap_metrics = {
        "core_total": len(core_all_time_authors),
        "knots_total": len(knots_authors),
        "both_count": len(both),
        "core_only_count": len(core_only),
        "knots_only_count": len(knots_only),
        "knots_only_devs": knots_only_sorted,
        "both_devs": both_sorted,
        "knots_top_devs": knots_devs_sorted[:5], # Top 5 for bus factor
        "mindshare_1yr": {
            "core_active": len(core_counts_1yr),
            "knots_active": len(knots_active_filtered)
        }
    }
    
    results = {
        "metadata": {
            "generated_at": datetime.now().isoformat() + "Z",
            "timeframe": "All-time",
            "core_branch": core_branch,
            "knots_branch": knots_branch
        },
        "hygiene": {
            "all_time": {
                "core": core_hygiene,
                "knots": knots_hygiene
            },
            "one_year": {
                "core": core_hygiene_1yr,
                "knots": knots_hygiene_1yr
            }
        },
        "divergence": divergence,
        "trend": {
            "core": core_trend,
            "knots": knots_trend
        },
        "overlap": overlap_metrics,
        "provenance": provenance_summary,
        "graveyard": {
            "summary": graveyard_summary,
            "commits": graveyard_plot_data
        }
    }
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUTPUT_DIR / "knots_comparison.json"
    with open(out_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Successfully wrote results to {out_file}")

if __name__ == "__main__":
    c_branch, k_branch = setup_repos()
    analyze(c_branch, k_branch)
