import json
import os
import re

def main():
    optech_path = "data/raw/optech.json"
    output_path = "output/shared/optech_crossref.json"

    if not os.path.exists(optech_path):
        print(f"File not found: {optech_path}")
        return

    with open(optech_path, 'r', encoding='utf-8') as f:
        newsletters = json.load(f)

    # Dictionary mapping PR number to Optech coverage
    # We'll only map Bitcoin Core PRs since that's what we show in releases/tracker
    crossref = {}

    for newsletter in newsletters:
        date = newsletter.get("date", "")
        # Assuming newsletter URL format is https://bitcoinops.org/en/newsletters/YYYY/MM/DD/
        url = ""
        if date and len(date) >= 10:
            ymd = date[:10].split("-")
            if len(ymd) == 3:
                url = f"https://bitcoinops.org/en/newsletters/{ymd[0]}/{ymd[1]}/{ymd[2]}/"
                
        for pr in newsletter.get("prs", []):
            repo = pr.get("repo_alias", "").lower()
            if "bitcoin core" in repo or "bitcoin" == repo:
                pr_num = str(pr.get("pr_number"))
                
                # Only keep the first (most recent) mention if a PR is mentioned multiple times
                if pr_num not in crossref:
                    # Clean up the description to act as a brief snippet
                    desc = pr.get("description", "")
                    
                    crossref[pr_num] = {
                        "optech_url": url,
                        "date": date[:10] if date else "",
                        "snippet": desc
                    }
                    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(crossref, f, indent=2)
        
    print(f"Generated Optech cross-reference for {len(crossref)} Bitcoin Core PRs.")
    print(f"Saved to {output_path}")

if __name__ == "__main__":
    main()
