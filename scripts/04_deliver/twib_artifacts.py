import os
import json
import pandas as pd
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from scripts.utils.twib_data import get_weekly_activity

def format_user(username):
    return f"[@{username}](https://github.com/{username})"

def build_markdown_from_json(data):
    lines = []
    
    start_date = data.get("start_date", "N/A")
    end_date = data.get("end_date", "N/A")
    lines.append(f"# 📰 This Week in Bitcoin ({start_date} to {end_date})")
    lines.append("")
    
    # 1. TLDR
    lines.append("## 📌 The TL;DR")
    tldr = data.get("tldr", [])
    if tldr:
        for point in tldr:
            lines.append(f"- {point}")
    else:
        lines.append("*No major summaries available this week.*")
    lines.append("")
    
    # 2. Merged PRs
    lines.append("## 🚢 Core Code (Merged This Week)")
    lines.append("The most critical pull requests merged into Bitcoin Core, ordered by community review activity.")
    lines.append("")
    merged_cats = data.get("categories", {}).get("merged", [])
    
    all_merged_prs = []
    for cat in merged_cats:
        for pr in cat['prs']:
            pr['category_name'] = cat['name']
            all_merged_prs.append(pr)
            
    all_merged_prs.sort(key=lambda x: x.get('importance', 0), reverse=True)
    
    if not all_merged_prs:
        lines.append("*No major merges this week.*")
        lines.append("")
    else:
        for pr in all_merged_prs:
            lines.append(f"#### [#{pr['number']}: {pr['title']}](https://github.com/bitcoin/bitcoin/pull/{pr['number']})")
            
            activity_str = f"*(Activity: {pr.get('review_count', 0)} review events)*" if pr.get('review_count', 0) > 0 else ""
            lines.append(f"**Author:** {format_user(pr['author']['username']) if isinstance(pr['author'], dict) else format_user(pr['author'])} | **[{pr['category_name']}]** {activity_str}")
            
            if pr.get('summary'):
                lines.append(f"> {pr['summary']}")
            if pr.get('technical_summary'):
                lines.append("")
                lines.append(f"**Technical Details:** {pr['technical_summary']}")
            lines.append("")
    
    # 3. Hot PRs
    lines.append("## 🔍 Under Review (Hot PRs)")
    lines.append("The most actively discussed and reviewed open pull requests right now.")
    lines.append("")
    hot_cats = data.get("categories", {}).get("hot", [])
    
    all_hot_prs = []
    for cat in hot_cats:
        for pr in cat['prs']:
            pr['category_name'] = cat['name']
            all_hot_prs.append(pr)
            
    all_hot_prs.sort(key=lambda x: x.get('event_count', 0), reverse=True)
    
    if not all_hot_prs:
        lines.append("*No hot PRs this week.*")
        lines.append("")
    else:
        for pr in all_hot_prs:
            lines.append(f"#### [#{pr['number']}: {pr['title']}](https://github.com/bitcoin/bitcoin/pull/{pr['number']})")
            
            activity_str = f"*(Activity: {pr.get('event_count', 0)} review events this week)*"
            lines.append(f"**Author:** {format_user(pr['author']['username']) if isinstance(pr['author'], dict) else format_user(pr['author'])} | **[{pr['category_name']}]** {activity_str}")
            
            if pr.get('summary'):
                lines.append(f"> {pr['summary']}")
            lines.append("")
            
    # 4. Discussions
    lines.append("## 🗣️ Research & Governance")
    lines.append("Top active threads across mailing lists and research forums.")
    lines.append("")
    for thread in data.get("discussions", []):
        lines.append(f"### [{thread['subject']}]({thread['link']})")
        lines.append(f"**Source:** {thread['source']} | **Started By:** {thread['author']} | **Messages:** {thread['message_count']}")
        if thread.get('summary'):
            lines.append(f"> {thread['summary']}")
        if thread.get('technical_summary'):
            lines.append("")
            lines.append(f"**Technical Details:** {thread['technical_summary']}")
        lines.append("")

    # 5. Shoutouts
    lines.append("## 🏆 Contributor Shoutouts")
    shoutouts = data.get("shoutouts", {})
    
    new_contribs = shoutouts.get("new_contributors", [])
    if new_contribs:
        lines.append("### 🎉 First-Time Merges")
        contrib_list = ", ".join([format_user(c['username']) if isinstance(c, dict) else format_user(c) for c in new_contribs])
        lines.append(f"Welcome to the codebase: {contrib_list}")
        lines.append("")
        
    top_authors = shoutouts.get("top_authors", [])
    if top_authors:
        lines.append("### ✍️ Top Authors")
        authors_list = ", ".join([format_user(a['user']['username']) if isinstance(a['user'], dict) else format_user(a['user']) for a in top_authors])
        lines.append(f"The most active PR authors this week: {authors_list}")
        lines.append("")

    top_reviewers = shoutouts.get("top_reviewers", [])
    if top_reviewers:
        lines.append("### 🕵️ Top Reviewers")
        reviewers_list = ", ".join([format_user(r['user']['username']) if isinstance(r['user'], dict) else format_user(r['user']) for r in top_reviewers])
        lines.append(f"Providing critical review and testing: {reviewers_list}")
        lines.append("")

    return "\n".join(lines)

def main():
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    
    print("Loading weekly activity data...")
    weekly_data = get_weekly_activity(root_dir, days_back=7)
    
    # Load caches
    cache_path = os.path.join(root_dir, "data", "cache", "twib_summaries.json")
    thread_cache_path = os.path.join(root_dir, "data", "cache", "thread_summaries_cache.json")
    pr_cache_path = os.path.join(root_dir, "data", "raw", "pr_summaries_cache.json")
    
    twib_cache = {}
    thread_cache = {}
    pr_cache = {}
    
    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            try: twib_cache = json.load(f)
            except: pass
            
    if os.path.exists(thread_cache_path):
        with open(thread_cache_path, "r") as f:
            try: thread_cache = json.load(f)
            except: pass
            
    if os.path.exists(pr_cache_path):
        with open(pr_cache_path, "r") as f:
            try: pr_cache = json.load(f)
            except: pass

    # Flatten all merged PRs from weekly_data
    all_merged_prs = []
    categorized_merged = weekly_data.get('categorized_merged_prs', {})
    for cat, prs in categorized_merged.items():
        for pr in prs:
            pr['fallback_category'] = cat
            all_merged_prs.append(pr)
            
    # Apply misc categorization from cache (legacy)
    misc_cats = twib_cache.get("misc_categories", {})
    
    # Category normalization map
    LEGACY_CATEGORY_MAP = {
        "🛠️ Build, CI & Testing": "Maintenance & Tech Debt",
        "⚙️ Build, CI & Testing": "Maintenance & Tech Debt",
        "📝 Documentation": "Maintenance & Tech Debt",
        "🛡️ Consensus & Cryptography": "Security & Consensus",
        "⚡ P2P & Network": "Network & Privacy",
        "👛 Wallet & Keys": "Wallet & User Tools",
        "📡 RPC, APIs & ZMQ": "Wallet & User Tools",
        "🖥️ GUI": "Wallet & User Tools",
        "🔄 Misc / Other": "Maintenance & Tech Debt",
        "Build, CI & Testing": "Maintenance & Tech Debt",
        "Documentation": "Maintenance & Tech Debt",
        "Consensus & Cryptography": "Security & Consensus",
        "P2P & Network": "Network & Privacy",
        "Wallet & Keys": "Wallet & User Tools",
        "RPC, APIs & ZMQ": "Wallet & User Tools",
        "GUI": "Wallet & User Tools",
        "Misc / Other": "Maintenance & Tech Debt"
    }
    
    CATEGORY_ORDER = [
        "Security & Consensus",
        "Strategic Initiatives",
        "Performance & Optimization",
        "Network & Privacy",
        "Wallet & User Tools",
        "Maintenance & Tech Debt"
    ]
    
    # Re-group PRs using impact_category
    new_categorized_merged = {}
    
    for pr in all_merged_prs:
        pr_str = str(pr['pr_number'])
        summary_obj = pr_cache.get(pr_str, {})
        
        # Determine the final category
        if isinstance(summary_obj, dict) and summary_obj.get("impact_category"):
            cat = summary_obj["impact_category"]
        else:
            cat = pr['fallback_category']
            if cat == '🔄 Misc / Other':
                cat = misc_cats.get(pr_str, '🔄 Misc / Other')
            cat = LEGACY_CATEGORY_MAP.get(cat, cat) # Normalize
            
        if cat not in new_categorized_merged:
            new_categorized_merged[cat] = []
        new_categorized_merged[cat].append(pr)
        
    # Re-sort PRs within each category by review_count (importance) descending
    for cat, prs in new_categorized_merged.items():
        prs.sort(key=lambda x: (x.get('importance', 0), x.get('merged_at', '')), reverse=True)
        
    # Sort categories based on predefined order
    sorted_merged_categories = []
    for sorted_cat in CATEGORY_ORDER:
        if sorted_cat in new_categorized_merged:
            sorted_merged_categories.append((sorted_cat, new_categorized_merged.pop(sorted_cat)))
            
    # Add any remaining categories (e.g. from fallback)
    for cat in sorted(new_categorized_merged.keys()):
        sorted_merged_categories.append((cat, new_categorized_merged[cat]))

    # --- Do the same for HOT PRs ---
    all_hot_prs = []
    categorized_hot = weekly_data.get('categorized_hot_prs', {})
    for cat, prs in categorized_hot.items():
        for pr in prs:
            pr['fallback_category'] = cat
            all_hot_prs.append(pr)
            
    new_categorized_hot = {}
    
    for pr in all_hot_prs:
        pr_str = str(pr['pr_number'])
        summary_obj = pr_cache.get(pr_str, {})
        
        if isinstance(summary_obj, dict) and summary_obj.get("impact_category"):
            cat = summary_obj["impact_category"]
        else:
            cat = pr['fallback_category']
            cat = LEGACY_CATEGORY_MAP.get(cat, cat) # Normalize
            
        if cat not in new_categorized_hot:
            new_categorized_hot[cat] = []
        new_categorized_hot[cat].append(pr)
        
    # Re-sort Hot PRs within each category by event_count descending
    for cat, prs in new_categorized_hot.items():
        prs.sort(key=lambda x: (x.get('event_count', 0), x.get('created_at', '')), reverse=True)
        
    sorted_hot_categories = []
    for sorted_cat in CATEGORY_ORDER:
        if sorted_cat in new_categorized_hot:
            sorted_hot_categories.append((sorted_cat, new_categorized_hot.pop(sorted_cat)))
            
    for cat in sorted(new_categorized_hot.keys()):
        sorted_hot_categories.append((cat, new_categorized_hot[cat]))


    tldr_summary = twib_cache.get("tldr_summary", "")
    tldr_cleaned = [line.lstrip('*- \t') for line in tldr_summary.split('\n') if line.lstrip('*- \t')]

    # Build the JSON data structure
    newsletter_data = {
        "start_date": weekly_data['start_date'],
        "end_date": weekly_data['end_date'],
        "stats": {
            "total_merged": weekly_data['total_merged'],
            "total_reviews": weekly_data['total_reviews'],
            "total_threads": weekly_data['total_threads'],
            "new_contributors": len(weekly_data['new_contributors'])
        },
        "tldr": tldr_cleaned,
        "categories": { "merged": [], "hot": [] },
        "discussions": [],
        "shoutouts": {
            "new_contributors": weekly_data['new_contributors'],
            "top_authors": weekly_data['top_authors'],
            "top_reviewers": weekly_data['top_reviewers'],
            "all_active": weekly_data['contributors']
        }
    }

    # Code Categories (Merged)
    for category, prs in sorted_merged_categories:
        cat_data = {"name": category, "prs": []}
        for pr in prs:
            pr_str = str(pr['pr_number'])
            summary_obj = pr_cache.get(pr_str, {})
            
            public_summary = summary_obj.get("public_summary", "") if isinstance(summary_obj, dict) else summary_obj
            technical_summary = summary_obj.get("technical_summary", "") if isinstance(summary_obj, dict) else ""
            cat_data["prs"].append({
                "number": pr['pr_number'],
                "title": pr['title'],
                "author": pr.get('author_obj') or pr.get('author'),
                "summary": public_summary,
                "technical_summary": technical_summary,
                "importance": pr.get("importance", 0),
                "review_count": pr.get("importance", 0)  # Add review count clearly for frontend
            })
        newsletter_data["categories"]["merged"].append(cat_data)

    # Code Categories (Hot PRs)
    for category, prs in sorted_hot_categories:
        cat_data = {"name": category, "prs": []}
        for pr in prs:
            pr_str = str(pr['pr_number'])
            summary_obj = pr_cache.get(pr_str, {})
            
            public_summary = summary_obj.get("public_summary", "") if isinstance(summary_obj, dict) else summary_obj
            technical_summary = summary_obj.get("technical_summary", "") if isinstance(summary_obj, dict) else ""
            cat_data["prs"].append({
                "number": pr['pr_number'],
                "title": pr['title'],
                "author": pr.get('author_obj') or pr.get('author'),
                "event_count": pr['event_count'],
                "summary": public_summary,
                "technical_summary": technical_summary
            })
        newsletter_data["categories"]["hot"].append(cat_data)

    # Research & Governance
    all_threads = weekly_data.get('active_threads', [])
    for thread in all_threads:
        tid_str = str(thread.get('thread_id', ''))
        subj = str(thread.get('subject', ''))
        
        # Priority: tid_ lookup -> thread_<subject[:20]> in new cache -> legacy twib_cache
        summary_obj = thread_cache.get(f"tid_{tid_str}") or thread_cache.get(f"thread_{subj[:20]}") or twib_cache.get(f"thread_{subj[:20]}", {})
        
        public_summary = summary_obj.get("public_summary", "") if isinstance(summary_obj, dict) else summary_obj
        technical_summary = summary_obj.get("technical_summary", "") if isinstance(summary_obj, dict) else ""
        newsletter_data["discussions"].append({
            "source": thread['source'].title().replace('_', ' '),
            "subject": thread['subject'],
            "author": thread.get('author'),
            "link": thread['link'],
            "message_count": thread['message_count'],
            "summary": public_summary,
            "technical_summary": technical_summary
        })

    # Save Artifacts
    output_dir = os.path.join(root_dir, "output", "twib", "data")
    archive_dir = os.path.join(root_dir, "output", "twib", "archive")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)
    
    today_str = weekly_data['end_date']
    json_filename = os.path.join(output_dir, f"newsletter_{today_str}.json")
    latest_json = os.path.join(output_dir, "latest.json")
    archive_index_file = os.path.join(output_dir, "archive_index.json")
    
    with open(json_filename, "w") as f: json.dump(newsletter_data, f, indent=2)
    with open(latest_json, "w") as f: json.dump(newsletter_data, f, indent=2)
        
    archive_index = []
    if os.path.exists(archive_index_file):
        with open(archive_index_file, "r") as f:
            try: archive_index = json.load(f)
            except: pass
            
    entry = {"date": today_str, "file": f"newsletter_{today_str}.json"}
    if entry not in archive_index:
        archive_index.insert(0, entry)
        
    archive_index = sorted(archive_index, key=lambda x: x["date"], reverse=True)
    with open(archive_index_file, "w") as f: json.dump(archive_index, f, indent=2)
        
    md_filename = os.path.join(archive_dir, f"newsletter_{today_str}.md")
    markdown_report = build_markdown_from_json(newsletter_data)
    with open(md_filename, "w") as f: f.write(markdown_report)

    print(f"✅ Generated TWIB Artifacts to output/twib/ for {today_str}")

if __name__ == "__main__":
    main()
