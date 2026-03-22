"""
extract_reviewers.py

Parses ACK/NACK trailers from Bitcoin Core commit messages to extract reviewer data.

Bitcoin Core uses a convention where reviewers add trailers to commits:
- ACK <hash>
- Tested ACK <hash>
- utACK <hash>  (untested ACK)
- Concept ACK
- NACK
- Reviewed-by: Name <email>
- Tested-by: Name <email>

NOTE: Future Enhancement - Consider hybrid approach with GitHub PR API
for more complete reviewer data (includes non-merged PRs, review comments).
See: https://docs.github.com/en/rest/pulls/reviews

This module implements Option 1: Git-based ACK trailer parsing.
"""

import pandas as pd
import re
import os
from collections import defaultdict

# --- Configuration ---
MESSAGES_INPUT = "data/core/commit_messages.parquet"
REVIEWS_OUTPUT = "data/core/reviews.parquet"
REVIEWERS_SUMMARY_OUTPUT = "data/core/reviewers_summary.json"

# --- ACK Pattern Matching ---
# Bitcoin Core ACK patterns (case insensitive)
ACK_PATTERNS = [
    # Standard ACKs with optional hash
    r"(?:^|\s)(ACK)\s+([a-f0-9]{6,40})?",
    r"(?:^|\s)(utACK)\s+([a-f0-9]{6,40})?",
    r"(?:^|\s)(Tested[\s-]?ACK)\s+([a-f0-9]{6,40})?",
    r"(?:^|\s)(tACK)\s+([a-f0-9]{6,40})?",
    r"(?:^|\s)(Concept[\s-]?ACK)",
    r"(?:^|\s)(crACK)",  # Code Review ACK
    
    # NACKs
    r"(?:^|\s)(NACK)",
    r"(?:^|\s)(Concept[\s-]?NACK)",
]

# Git trailer patterns (Reviewed-by, Tested-by, etc.)
TRAILER_PATTERNS = [
    r"Reviewed-by:\s*(.+?)(?:\s*<([^>]+)>)?$",
    r"Tested-by:\s*(.+?)(?:\s*<([^>]+)>)?$",
    r"Acked-by:\s*(.+?)(?:\s*<([^>]+)>)?$",
    r"Co-authored-by:\s*(.+?)(?:\s*<([^>]+)>)?$",
]

# Pattern to extract reviewer name from ACK line context
# Often ACKs are formatted as "ACK abc123 - reviewer comment" or signed
SIGNED_ACK_PATTERN = r"(?:ACK|utACK|tACK).*?(?:^|\n)\s*[-—]\s*(.+?)(?:\s*<([^>]+)>)?$"


def extract_reviews_from_body(commit_hash, body):
    """
    Extract all review signals from a commit message body.
    Handles both single-line trailers and multi-line ACK blocks.
    """
    reviews = []
    
    if not body:
        return reviews
    
    lines = body.split('\n')
    
    # Track context for block-style ACKs
    # ACKs for top commit:
    #   fanquake:
    #     ACK abc123
    current_context_name = None
    in_ack_block = False
    
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        if not line_stripped:
            continue
        
        # Detect start of ACK block
        if re.search(r'^(ACKs|Reviewers|Reviewed-by) (from|for|:)', line_stripped, re.IGNORECASE):
            in_ack_block = True
            continue
            
        # Common pattern: "Name:" on its own line followed by ACK on next line
        name_colon_match = re.search(r'^[\s]*([a-zA-Z0-9\s._-]+):$', line_stripped)
        if name_colon_match:
            current_context_name = name_colon_match.group(1).strip()
            continue
        
        # Check ACK patterns
        for pattern in ACK_PATTERNS:
            match = re.search(pattern, line_stripped, re.IGNORECASE)
            if match:
                review_type = match.group(1).upper().replace("-", " ").replace("  ", " ")
                
                reviewer_name = current_context_name
                reviewer_email = None
                
                # If no context name, try same-line extraction
                if not reviewer_name:
                    name_match = re.search(r"(?:ACK|NACK)\s+(?:[a-f0-9]{6,40})?\s*[-—:]?\s*(.+)", line_stripped, re.IGNORECASE)
                    if name_match:
                        potential_name = name_match.group(1).strip()
                        if potential_name and len(potential_name) > 2 and not potential_name.startswith(('http', '//', '#')):
                            email_match = re.search(r'<([^>]+@[^>]+)>', potential_name)
                            if email_match:
                                reviewer_email = email_match.group(1).lower()
                                reviewer_name = re.sub(r'\s*<[^>]+>\s*', '', potential_name).strip()
                            else:
                                words = potential_name.split()
                                if len(words) <= 4:
                                    reviewer_name = potential_name
                
                # If STILL no name, look up one line (common in some formats)
                if not reviewer_name and i > 0:
                    prev_line = lines[i-1].strip()
                    if prev_line and len(prev_line) < 40 and not any(x in prev_line.upper() for x in ["ACK", "COMMIT", "MERGE"]):
                         reviewer_name = prev_line
                
                reviews.append({
                    "commit_hash": commit_hash,
                    "reviewer_name": reviewer_name,
                    "reviewer_email": reviewer_email,
                    "review_type": review_type,
                    "raw_line": line_stripped[:200]
                })
                break  # One match per line

        # Reset context if we hit a non-indented line that doesn't look like a name
        if current_context_name and not line.startswith(' ') and len(line_stripped) > 40:
             current_context_name = None
        
        # Check trailer patterns (Reviewed-by, etc.)
        for pattern in TRAILER_PATTERNS:
            match = re.search(pattern, line_stripped, re.IGNORECASE)
            if match:
                reviewer_name = match.group(1).strip() if match.group(1) else None
                reviewer_email = match.group(2).lower().strip() if len(match.groups()) > 1 and match.group(2) else None
                
                if "reviewed-by" in line_stripped.lower():
                    review_type = "Reviewed-by"
                elif "tested-by" in line_stripped.lower():
                    review_type = "Tested-by"
                elif "acked-by" in line_stripped.lower():
                    review_type = "Acked-by"
                elif "co-authored-by" in line_stripped.lower():
                    review_type = "Co-authored-by"
                else:
                    review_type = "Trailer"
                
                reviews.append({
                    "commit_hash": commit_hash,
                    "reviewer_name": reviewer_name,
                    "reviewer_email": reviewer_email,
                    "review_type": review_type,
                    "raw_line": line_stripped[:200]
                })
                break
    
    return reviews


def process_messages():
    """
    Main processing function.
    Reads commit messages, extracts reviews, saves to parquet.
    """
    if not os.path.exists(MESSAGES_INPUT):
        print(f"Error: {MESSAGES_INPUT} not found. Run ingest.py first with message extraction.")
        return
    
    print(f"Loading commit messages from {MESSAGES_INPUT}...")
    messages_df = pd.read_parquet(MESSAGES_INPUT)
    
    print(f"Processing {len(messages_df)} commit messages...")
    
    all_reviews = []
    commits_with_reviews = 0
    
    for _, row in messages_df.iterrows():
        commit_hash = row['hash']
        body = row.get('body', '')
        
        reviews = extract_reviews_from_body(commit_hash, body)
        
        if reviews:
            commits_with_reviews += 1
            all_reviews.extend(reviews)
    
    print(f"Found {len(all_reviews)} review signals in {commits_with_reviews} commits.")
    
    if not all_reviews:
        print("No reviews found. Check if commit messages contain ACK trailers.")
        return
    
    # Create DataFrame
    reviews_df = pd.DataFrame(all_reviews)
    
    # Save
    os.makedirs(os.path.dirname(REVIEWS_OUTPUT), exist_ok=True)
    reviews_df.to_parquet(REVIEWS_OUTPUT, index=False)
    print(f"Saved reviews to {REVIEWS_OUTPUT}")
    
    # Generate summary stats
    generate_reviewer_summary(reviews_df)


def generate_reviewer_summary(reviews_df):
    """
    Generate summary statistics for reviewers.
    """
    import json
    
    # Count by review type
    type_counts = reviews_df['review_type'].value_counts().to_dict()
    
    # Top reviewers (by name, excluding None)
    named_reviews = reviews_df[reviews_df['reviewer_name'].notna()]
    
    # Normalize names (lowercase, strip)
    named_reviews = named_reviews.copy()
    named_reviews['reviewer_normalized'] = named_reviews['reviewer_name'].str.lower().str.strip()
    
    top_reviewers = named_reviews['reviewer_normalized'].value_counts().head(50).to_dict()
    
    # Reviews per year (need to join with commits for date)
    # For now, just output totals
    
    summary = {
        "total_reviews": len(reviews_df),
        "unique_commits_reviewed": reviews_df['commit_hash'].nunique(),
        "review_types": type_counts,
        "top_reviewers": top_reviewers,
        "reviews_with_name": len(named_reviews),
        "reviews_anonymous": len(reviews_df) - len(named_reviews)
    }
    
    with open(REVIEWERS_SUMMARY_OUTPUT, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"Saved reviewer summary to {REVIEWERS_SUMMARY_OUTPUT}")
    print(f"  Total reviews: {summary['total_reviews']}")
    print(f"  Unique commits with reviews: {summary['unique_commits_reviewed']}")
    print(f"  Top 5 reviewers: {list(top_reviewers.keys())[:5]}")


if __name__ == "__main__":
    process_messages()
