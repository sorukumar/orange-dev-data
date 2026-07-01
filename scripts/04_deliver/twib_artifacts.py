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
    lines.append("The most critical pull requests merged into Bitcoin Core.")
    lines.append("")
    merged_cats = data.get("categories", {}).get("merged", [])
    for cat in merged_cats:
        lines.append(f"### {cat['name']}")
        if not cat['prs']:
            lines.append("*No major merges in this category.*")
            lines.append("")
            continue
        for pr in cat['prs']:
            lines.append(f"#### [#{pr['number']}: {pr['title']}](https://github.com/bitcoin/bitcoin/pull/{pr['number']})")
            lines.append(f"**Author:** {format_user(pr['author']['username']) if isinstance(pr['author'], dict) else format_user(pr['author'])}")
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
    for cat in hot_cats:
        if not cat['prs']:
            continue
        lines.append(f"### {cat['name']}")
        for pr in cat['prs']:
            lines.append(f"#### [#{pr['number']}: {pr['title']}](https://github.com/bitcoin/bitcoin/pull/{pr['number']})")
            lines.append(f"**Author:** {format_user(pr['author']['username']) if isinstance(pr['author'], dict) else format_user(pr['author'])}")
            lines.append(f"*(Activity: {pr.get('event_count', 0)} review events this week)*")
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
    
    cache_path = os.path.join(root_dir, "data", "cache", "twib_summaries.json")
    cache = {}
    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            try: cache = json.load(f)
            except: pass

    # Apply misc categorization from cache
    misc_cats = cache.get("misc_categories", {})
    categorized_merged = weekly_data.get('categorized_merged_prs', {})
    misc_prs = categorized_merged.pop('🔄 Misc / Other', [])
    for pr in misc_prs:
        cat = misc_cats.get(str(pr['pr_number']), '🔄 Misc / Other')
        if cat not in categorized_merged: categorized_merged[cat] = []
        categorized_merged[cat].append(pr)

    tldr_summary = cache.get("tldr_summary", "")
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
    for category, prs in categorized_merged.items():
        cat_data = {"name": category, "prs": []}
        for pr in prs:
            summary_obj = cache.get(f"pr_{pr['pr_number']}", {})
            public_summary = summary_obj.get("public_summary", "") if isinstance(summary_obj, dict) else summary_obj
            technical_summary = summary_obj.get("technical_summary", "") if isinstance(summary_obj, dict) else ""
            cat_data["prs"].append({
                "number": pr['pr_number'],
                "title": pr['title'],
                "author": pr['author_obj'],
                "summary": public_summary,
                "technical_summary": technical_summary,
                "importance": pr.get("importance", 0)
            })
        newsletter_data["categories"]["merged"].append(cat_data)

    # Code Categories (Hot PRs)
    categorized_hot = weekly_data.get('categorized_hot_prs', {})
    for category, prs in categorized_hot.items():
        cat_data = {"name": category, "prs": []}
        for pr in prs:
            summary_obj = cache.get(f"pr_{pr['pr_number']}", {})
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
    top_2_threads = weekly_data.get('active_threads', [])[:2]
    for thread in top_2_threads:
        summary_obj = cache.get(f"thread_{thread['subject'][:20]}", {})
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
