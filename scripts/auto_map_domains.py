import pandas as pd
import json
import os

# Paths
commits_path = "/Users/saurabhkumar/Desktop/Work/github/orange-dev-tracker/data/core/commits.parquet"
sponsors_path = "/Users/saurabhkumar/Desktop/Work/github/orange-dev-tracker/data/cache/sponsors_lookup.json"
rich_path = "/Users/saurabhkumar/Desktop/Work/github/orange-dev-tracker/data/core/contributors_rich.json"

def auto_map_sponsorship():
    # 1. Load Sponsor Data to get domains
    with open(sponsors_path, "r") as f:
        sponsors_data = json.load(f)
    
    # Create domain to sponsor_id map
    domain_to_id = {}
    for s in sponsors_data.get("sponsors", []):
        for domain in s.get("domains", []):
            domain_to_id[domain.lower()] = s["id"]
    
    # Also use corporate_domains from classification_rules as fallback or additional info
    # But usually domains in sponsors[] are more specific.
    
    # 2. Load Commits to find candidates
    print("Loading commits...")
    df = pd.read_parquet(commits_path)
    
    # Extract unique (name, email) pairs
    authors = df[['author_name', 'author_email']].drop_duplicates()
    
    # 3. Identify matches
    matches = []
    seen_names = {s['canonical_name'] for s in sponsors_data.get("sponsored_developers", [])}
    
    for idx, row in authors.iterrows():
        name = row['author_name']
        email = row['author_email'] if row['author_email'] else ""
        domain = email.split('@')[-1].lower() if '@' in email else ""
        
        if domain in domain_to_id:
            sponsor_id = domain_to_id[domain]
            
            # If we haven't already classified this person manually
            if name not in seen_names:
                matches.append({
                    "canonical_name": name,
                    "emails": [email],
                    "sponsor_id": sponsor_id,
                    "status": "active",
                    "notes": f"Auto-detected via domain @{domain}"
                })
                # Prevent duplicate names in this batch
                seen_names.add(name)

    print(f"Detected {len(matches)} new potential sponsored developers via email domain.")
    
    # Sort matches by impact (if possible) or just print them
    # For now, let's just show them.
    if matches:
        # Convert to DataFrame for pretty printing
        match_df = pd.DataFrame(matches)
        print("\nProposed New Classifications:")
        print(match_df[['canonical_name', 'sponsor_id', 'emails']].to_string())
        
        # Save to a temp file for user review
        with open("/Users/saurabhkumar/Desktop/Work/github/orange-dev-tracker/analysis/proposed_domain_matches.json", "w") as f:
            json.dump(matches, f, indent=2)
        print(f"\nSaved proposed matches to analysis/proposed_domain_matches.json")
    else:
        print("No new matches found via domain mapping.")

if __name__ == "__main__":
    auto_map_sponsorship()
