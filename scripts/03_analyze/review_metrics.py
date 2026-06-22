import pandas as pd
import numpy as np
import re
import os
import json
from datetime import datetime

# --- Configuration ---
PR_METADATA_INPUT = "data/raw/github_pr_metadata.parquet"
REVIEW_EVENTS_INPUT = "data/raw/github_review_events.parquet"
BIPS_PR_METADATA_INPUT = "data/raw/bips_pr_metadata.parquet"
BIPS_REVIEW_EVENTS_INPUT = "data/raw/bips_review_events.parquet"
CONTRIBUTORS_REGISTRY = "metadata/contributors.json"
REVIEW_METRICS_OUTPUT = "data/enriched/contributor_review_metrics.parquet"

# Patterns for approvals
ACK_PATTERNS = [
    r"(?:^|\s)(ACK)\b",
    r"(?:^|\s)(utACK)\b",
    r"(?:^|\s)(Tested[\s-]?ACK)\b",
    r"(?:^|\s)(tACK)\b",
    r"(?:^|\s)(Concept[\s-]?ACK)\b",
    r"(?:^|\s)(Approach[\s-]?ACK)\b",
    r"(?:^|\s)(crACK)\b",
]

CONCEPT_ACK_PATTERN = r"(?:^|\s)(Concept[\s-]?ACK)\b"

import sys
sys.path.append(os.getcwd())
from scripts.utils.identity import resolver

def calculate_review_metrics():
    print("Loading datasets...")
    df_prs = pd.read_parquet(PR_METADATA_INPUT)
    df_events = pd.read_parquet(REVIEW_EVENTS_INPUT)

    df_prs['repo_tier'] = 'tier1'
    df_events['repo_tier'] = 'tier1'

    # Include BIPs GitHub data (same schema as Bitcoin Core parquets)
    if os.path.exists(BIPS_PR_METADATA_INPUT):
        bips_prs = pd.read_parquet(BIPS_PR_METADATA_INPUT)
        bips_prs['repo_tier'] = 'tier2'
        df_prs = pd.concat([df_prs, bips_prs], ignore_index=True)
    if os.path.exists(BIPS_REVIEW_EVENTS_INPUT):
        bips_events = pd.read_parquet(BIPS_REVIEW_EVENTS_INPUT)
        bips_events['repo_tier'] = 'tier2'
        df_events = pd.concat([df_events, bips_events], ignore_index=True)
    
    # Using Master Identity Resolver
    print("Using identity resolver for canonical mapping...")
            
    # Convert timestamps
    df_prs['created_at'] = pd.to_datetime(df_prs['created_at'])
    df_events['timestamp'] = pd.to_datetime(df_events['timestamp'])
    
    # Identify signal types
    print("Filtering signals...")
    def get_signals(row):
        body = str(row['body']).upper()
        state = str(row['state']).upper()
        is_approval = (state == 'APPROVED')
        is_concept = False
        
        if not is_approval:
            for p in ACK_PATTERNS:
                if re.search(p, body, re.IGNORECASE):
                    is_approval = True
                    break
        
        if re.search(CONCEPT_ACK_PATTERN, body, re.IGNORECASE):
            is_concept = True
            
        return pd.Series([is_approval, is_concept])

    df_events[['is_approval', 'is_concept']] = df_events.apply(get_signals, axis=1)

    # Resolve canonical identities BEFORE merge so all downstream uses (incl. all_logins) get UUIDs
    df_events['user'] = df_events['user'].apply(lambda x: resolver.resolve_github(x))
    df_prs['author'] = df_prs['author'].apply(lambda x: resolver.resolve_github(x))

    # Join PR metadata
    df_merged = df_events.merge(df_prs[['pr_number', 'created_at', 'author']], on='pr_number', suffixes=('', '_pr'))
    
    # Calculate Latency
    df_merged['latency_days'] = (df_merged['timestamp'] - df_merged['created_at']).dt.total_seconds() / (24 * 3600)
    
    # Sanitize: ignore self-reviews and negative latency
    df_merged = df_merged[(df_merged['user'] != df_merged['author']) & (df_merged['latency_days'] >= 0)]

    # Only count active review participation (comments and formal reviews).
    # The raw parquet also contains passive timeline events (subscribed, mentioned,
    # cross-referenced, head_ref_force_pushed, etc.) that must not inflate reviews_count.
    ACTIVE_REVIEW_TYPES = {'commented', 'reviewed'}
    df_active = df_merged[df_merged['event_type'].isin(ACTIVE_REVIEW_TYPES)]

    # Era-specific review counts
    post_2016_start = pd.Timestamp('2016-01-01', tz='UTC')
    modern_cutoff = df_active['timestamp'].max() - pd.DateOffset(years=3)
    p2016_reviewer_counts = df_active[df_active['timestamp'] >= post_2016_start].groupby('user').agg(
        p2016_reviews_count=('pr_number', 'nunique')
    ).reset_index()
    modern_reviewer_counts = df_active[df_active['timestamp'] >= modern_cutoff].groupby('user').agg(
        modern_reviews_count=('pr_number', 'nunique')
    ).reset_index()

    # Capture review activity range for frontend timelines
    review_date_range = df_active.groupby('user').agg(
        first_review_date=('timestamp', 'min'),
        last_review_date=('timestamp', 'max')
    ).reset_index()

    print("Aggregating reviewer metrics...")
    # 1. Reviewer Performance (Interaction Speed)
    reviewer_perf = df_active.groupby('user').agg(
        avg_review_latency_days=('latency_days', 'mean'),
        reviews_count=('pr_number', 'nunique')
    ).reset_index()
    
    # 2. Approval Performance (ACK Speed) — ACKs only come from comments/reviews
    approval_perf = df_active[df_active['is_approval']].groupby('user').agg(
        avg_approval_latency_days=('latency_days', 'mean'),
        approvals_count=('pr_number', 'nunique')
    ).reset_index()

    tier1_reviews = df_active[df_active['repo_tier'] == 'tier1'].groupby('user').agg(
        tier1_reviews_count=('pr_number', 'nunique')
    ).reset_index()

    tier2_reviews = df_active[df_active['repo_tier'] == 'tier2'].groupby('user').agg(
        tier2_reviews_count=('pr_number', 'nunique')
    ).reset_index()
    
    reviewer_metrics = reviewer_perf.merge(approval_perf, on='user', how='left')
    reviewer_metrics = reviewer_metrics.merge(tier1_reviews, on='user', how='left')
    reviewer_metrics = reviewer_metrics.merge(tier2_reviews, on='user', how='left')
    reviewer_metrics = reviewer_metrics.merge(p2016_reviewer_counts, on='user', how='left')
    reviewer_metrics = reviewer_metrics.merge(modern_reviewer_counts, on='user', how='left')
    reviewer_metrics = reviewer_metrics.merge(review_date_range, on='user', how='left')
    
    # 3. Author Clout (Time-to-ACK for their PRs)
    print("Aggregating author metrics...")
    first_acks = df_active[df_active['is_approval']].groupby('pr_number').agg(
        first_ack_latency=('latency_days', 'min'),
        author=('author', 'first')
    ).reset_index()
    
    author_perf = first_acks.groupby('author').agg(
        avg_pr_ack_velocity_days=('first_ack_latency', 'mean')
    ).reset_index()
    
    # 4. Author Concept Lead Time
    first_concepts = df_active[df_active['is_concept']].groupby('pr_number').agg(
        first_concept_latency=('latency_days', 'min'),
        author=('author', 'first')
    ).reset_index()
    
    author_concepts = first_concepts.groupby('author').agg(
        avg_concept_lead_time_days=('first_concept_latency', 'mean')
    ).reset_index()
    
    author_metrics = df_prs.groupby('author').agg(prs_authored=('pr_number', 'nunique')).reset_index()

    tier1_authored = df_prs[df_prs['repo_tier'] == 'tier1'].groupby('author').agg(
        tier1_prs_authored=('pr_number', 'nunique')
    ).reset_index()

    tier2_authored = df_prs[df_prs['repo_tier'] == 'tier2'].groupby('author').agg(
        tier2_prs_authored=('pr_number', 'nunique')
    ).reset_index()

    author_metrics = author_metrics.merge(tier1_authored, on='author', how='left')
    author_metrics = author_metrics.merge(tier2_authored, on='author', how='left')
    author_metrics = author_metrics.merge(author_perf, on='author', how='left')
    author_metrics = author_metrics.merge(author_concepts, on='author', how='left')
    
    # 5. Final Join to Registry IDs
    print("Joining all metrics...")
    # Initialize unified contributor stats
    all_logins = set(df_prs['author'].unique()) | set(df_events['user'].unique())  # both already resolved above
    metrics_df = pd.DataFrame({'canonical_id': list(all_logins)})
    
    metrics_df = metrics_df.merge(reviewer_metrics.rename(columns={'user': 'canonical_id'}), on='canonical_id', how='left')
    metrics_df = metrics_df.merge(author_metrics.rename(columns={'author': 'canonical_id'}), on='canonical_id', how='left')
    
    metrics_df.rename(columns={'canonical_id': 'canonical_id'}, inplace=True)
    
    # Calculate Reciprocity
    metrics_df['review_reciprocity'] = metrics_df['reviews_count'].fillna(0) / metrics_df['prs_authored'].replace(0, np.nan)
    
    # Fill NAs
    metrics_df = metrics_df.fillna({
        'reviews_count': 0,
        'tier1_reviews_count': 0,
        'tier2_reviews_count': 0,
        'approvals_count': 0,
        'prs_authored': 0,
        'tier1_prs_authored': 0,
        'tier2_prs_authored': 0,
        'p2016_reviews_count': 0,
        'modern_reviews_count': 0
    })
    
    # Save output
    os.makedirs(os.path.dirname(REVIEW_METRICS_OUTPUT), exist_ok=True)
    metrics_df.to_parquet(REVIEW_METRICS_OUTPUT, index=False)

    print(f"Review metrics calculated for {len(metrics_df)} contributors.")
    print(f"Top contributors by Reciprocity:\n{metrics_df.sort_values('review_reciprocity', ascending=False).head(5)[['canonical_id', 'review_reciprocity']]}")
    print(f"Fastest Approvers (Avg Latency):\n{metrics_df.sort_values('avg_approval_latency_days').head(5)[['canonical_id', 'avg_approval_latency_days']]}")

if __name__ == "__main__":
    calculate_review_metrics()
