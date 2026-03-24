import pandas as pd
import re
import os
import sys
import json
from collections import defaultdict

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import WORK_DIR, TRACKER_DIR

# --- Configuration (Centralized via paths.py) ---
MESSAGES_INPUT = os.path.join(WORK_DIR, "core", "commit_messages.parquet")
REVIEWS_OUTPUT = os.path.join(WORK_DIR, "core", "reviews.parquet")
REVIEWERS_SUMMARY_OUTPUT = os.path.join(TRACKER_DIR, "reviewers_summary.json")

# --- patterns ---
ACK_PATTERNS = [
    r"(?:^|\s)(ACK)\s+([a-f0-9]{6,40})?", r"(?:^|\s)(utACK)\s+([a-f0-9]{6,40})?",
    r"(?:^|\s)(Tested[\s-]?ACK)\s+([a-f0-9]{6,40})?", r"(?:^|\s)(tACK)\s+([a-f0-9]{6,40})?",
    r"(?:^|\s)(Concept[\s-]?ACK)", r"(?:^|\s)(crACK)", r"(?:^|\s)(NACK)", r"(?:^|\s)(Concept[\s-]?NACK)"
]
TRAILER_PATTERNS = [
    r"Reviewed-by:\s*(.+?)(?:\s*<([^>]+)>)?$", r"Tested-by:\s*(.+?)(?:\s*<([^>]+)>)?$",
    r"Acked-by:\s*(.+?)(?:\s*<([^>]+)>)?$", r"Co-authored-by:\s*(.+?)(?:\s*<([^>]+)>)?$"
]

def extract_reviews_from_body(commit_hash, body):
    reviews = []
    if not body: return reviews
    
    lines = body.split('\n')
    current_context_name = None
    
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        if not line_stripped: continue
        
        # Start of ACK blocks
        if re.search(r'^(ACKs|Reviewers|Reviewed-by) (from|for|:)', line_stripped, re.IGNORECASE): continue
            
        # "Name:" header
        name_colon_match = re.search(r'^[\s]*([a-zA-Z0-9\s._-]+):$', line_stripped)
        if name_colon_match:
            current_context_name = name_colon_match.group(1).strip()
            continue
        
        # Check standard ACK patterns
        for pattern in ACK_PATTERNS:
            match = re.search(pattern, line_stripped, re.IGNORECASE)
            if match:
                review_type = match.group(1).upper()
                reviewer_name = current_context_name
                reviewer_email = None
                
                # Same-line extraction fallback
                if not reviewer_name:
                    name_match = re.search(r"(?:ACK|NACK)\s+(?:[a-f0-9]{6,40})?\s*[-—:]?\s*(.+)", line_stripped, re.IGNORECASE)
                    if name_match:
                        potential_name = name_match.group(1).strip()
                        if 2 < len(potential_name) < 40:
                            reviewer_name = potential_name
                
                reviews.append({
                    "commit_hash": commit_hash,
                    "reviewer_name": reviewer_name,
                    "reviewer_email": reviewer_email,
                    "review_type": review_type
                })
                break 

        # Check trailer patterns
        for pattern in TRAILER_PATTERNS:
            match = re.search(pattern, line_stripped, re.IGNORECASE)
            if match:
                reviews.append({
                    "commit_hash": commit_hash,
                    "reviewer_name": match.group(1).strip() if match.group(1) else None,
                    "reviewer_email": match.group(2).lower().strip() if len(match.groups()) > 1 and match.group(2) else None,
                    "review_type": "Trailer"
                })
                break
    return reviews

def main():
    print("--- Reviewer Extraction (New Architecture) ---")
    if not os.path.exists(MESSAGES_INPUT):
        print(f"Error: {MESSAGES_INPUT} not found.")
        return
    
    df = pd.read_parquet(MESSAGES_INPUT)
    print(f"Processing {len(df)} commit bodies...")
    
    all_reviews = []
    for _, row in df.iterrows():
        reviews = extract_reviews_from_body(row['hash'], row.get('body', ''))
        if reviews: all_reviews.extend(reviews)
    
    if not all_reviews:
        print("No reviews discovered.")
        return
    
    reviews_df = pd.DataFrame(all_reviews)
    os.makedirs(os.path.dirname(REVIEWS_OUTPUT), exist_ok=True)
    reviews_df.to_parquet(REVIEWS_OUTPUT, index=False)
    print(f"Saved {len(all_reviews)} review signals to {REVIEWS_OUTPUT}")
    
    # Generate summary stats
    summary = {
        "total_reviews": len(reviews_df),
        "unique_commits": reviews_df['commit_hash'].nunique(),
        "top_reviewers": reviews_df['reviewer_name'].value_counts().head(50).to_dict()
    }
    
    os.makedirs(os.path.dirname(REVIEWERS_SUMMARY_OUTPUT), exist_ok=True)
    with open(REVIEWERS_SUMMARY_OUTPUT, 'w') as f:
        json.dump(summary, f, indent=2)

if __name__ == "__main__":
    main()
