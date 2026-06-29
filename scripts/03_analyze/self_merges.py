import pandas as pd
import os
import re

# --- Configuration ---
COMMITS_RESOLVED_PARQUET = "data/enriched/commits_resolved.parquet"
PR_METADATA_PARQUET = "data/raw/github_pr_metadata.parquet"
REVIEW_EVENTS_PARQUET = "data/raw/github_review_events.parquet"
OUTPUT_PARQUET = "data/enriched/self_merges_detailed.parquet"

ACK_PATTERNS = [
    r"(?:^|\s)(ACK)\b",
    r"(?:^|\s)(utACK)\b",
    r"(?:^|\s)(Tested[\s-]?ACK)\b",
    r"(?:^|\s)(tACK)\b",
    r"(?:^|\s)(Concept[\s-]?ACK)\b",
    r"(?:^|\s)(Approach[\s-]?ACK)\b",
    r"(?:^|\s)(crACK)\b",
]

def generate_self_merges():
    print("Generating self-merges detailed analytics...")
    
    if not os.path.exists(COMMITS_RESOLVED_PARQUET):
        print(f"Error: {COMMITS_RESOLVED_PARQUET} not found.")
        return
        
    df_commits = pd.read_parquet(COMMITS_RESOLVED_PARQUET)
    
    # Filter for self merges only
    if 'is_self_merge' not in df_commits.columns:
        print("Warning: is_self_merge column not found in commits. Skipping.")
        return
        
    df_self = df_commits[df_commits['is_self_merge'] == True].copy()
    
    if df_self.empty:
        print("No self-merges found.")
        pd.DataFrame().to_parquet(OUTPUT_PARQUET, index=False)
        return
        
    print(f"Found {len(df_self)} self-merge commits. Analyzing ACKs...")
    
    # Ensure pr_number is clean
    df_self['pr_number'] = pd.to_numeric(df_self['pr_number'], errors='coerce')
    df_self = df_self.dropna(subset=['pr_number'])
    
    # Load PR Metadata
    if os.path.exists(PR_METADATA_PARQUET):
        df_prs = pd.read_parquet(PR_METADATA_PARQUET)
        df_prs['pr_number'] = pd.to_numeric(df_prs['pr_number'], errors='coerce')
        # Merge title, merged_at
        df_self = df_self.merge(df_prs[['repository_name', 'pr_number', 'title', 'merged_at']], 
                                on=['repository_name', 'pr_number'], how='left')
        df_self['html_url'] = "https://github.com/" + df_self['repository_name'] + "/pull/" + df_self['pr_number'].astype(str)
    else:
        df_self['html_url'] = None
        df_self['title'] = None
        df_self['merged_at'] = df_self['integration_date']
        
    # Load Review Events to count prior ACKs
    ack_counts = {}
    if os.path.exists(REVIEW_EVENTS_PARQUET):
        df_reviews = pd.read_parquet(REVIEW_EVENTS_PARQUET)
        df_reviews['pr_number'] = pd.to_numeric(df_reviews['pr_number'], errors='coerce')
        
        # Filter for comments and reviews
        df_comments = df_reviews[df_reviews['event_type'].isin(['commented', 'reviewed'])].copy()
        df_comments['timestamp'] = pd.to_datetime(df_comments['timestamp'], utc=True)
        
        for idx, row in df_self.iterrows():
            pr = row['pr_number']
            repo = row['repository_name']
            merged_at = pd.to_datetime(row['merged_at'], utc=True) if pd.notna(row['merged_at']) else pd.Timestamp.max.tz_localize('UTC')
            maintainer = row['canonical_id']
            
            # Find reviews for this PR
            pr_reviews = df_comments[(df_comments['pr_number'] == pr) & (df_comments['repository_name'] == repo)]
            # Filter reviews BEFORE the merge timestamp
            pr_reviews = pr_reviews[pr_reviews['timestamp'] < merged_at]
            
            unique_ackers = set()
            for _, r in pr_reviews.iterrows():
                body = str(r['body']).upper()
                state = str(r['state']).upper()
                user = r['user']
                
                # Exclude the maintainer themselves
                if user == maintainer:
                    continue
                    
                is_approval = (state == 'APPROVED')
                if not is_approval:
                    for p in ACK_PATTERNS:
                        if re.search(p, body, re.IGNORECASE):
                            is_approval = True
                            break
                            
                if is_approval:
                    unique_ackers.add(user)
                    
            ack_counts[(repo, pr)] = len(unique_ackers)
            
    df_self['ack_count'] = df_self.apply(lambda row: ack_counts.get((row['repository_name'], row['pr_number']), 0), axis=1).astype(int)
    
    # Categorize
    def categorize(acks):
        if acks == 0:
            return "Ninja Merge"
        elif acks <= 2:
            return "Light Review"
        else:
            return "Administrative Merge"
            
    df_self['category'] = df_self['ack_count'].apply(categorize)
    
    # Format and save
    out_cols = ['canonical_id', 'repository_name', 'pr_number', 'html_url', 'title', 'merged_at', 'ack_count', 'category']
    # If html_url or title missed, make sure they are strings
    if 'html_url' not in df_self.columns: df_self['html_url'] = ""
    if 'title' not in df_self.columns: df_self['title'] = ""
    
    df_out = df_self[out_cols]
    
    os.makedirs(os.path.dirname(OUTPUT_PARQUET), exist_ok=True)
    df_out.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"Saved {len(df_out)} detailed self-merges to {OUTPUT_PARQUET}")

if __name__ == "__main__":
    generate_self_merges()
