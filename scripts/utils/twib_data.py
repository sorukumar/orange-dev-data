import pandas as pd
from datetime import datetime, timedelta
import os

def get_category_from_labels(labels):
    if not isinstance(labels, (list, str)):
        return '🔄 Misc / Other'
    labels_str = str(labels).lower()
    if 'wallet' in labels_str:
        return '👛 Wallet & Keys'
    elif 'p2p' in labels_str or 'network' in labels_str:
        return '⚡ P2P & Network'
    elif 'consensus' in labels_str or 'crypto' in labels_str:
        return '🛡️ Consensus & Cryptography'
    elif 'rpc' in labels_str or 'api' in labels_str or 'zmq' in labels_str or 'rest' in labels_str:
        return '📡 RPC, APIs & ZMQ'
    elif 'gui' in labels_str:
        return '🖥️ GUI'
    elif 'build' in labels_str or 'ci' in labels_str or 'test' in labels_str or 'qa' in labels_str:
        return '🛠️ Build, CI & Testing'
    elif 'doc' in labels_str:
        return '📝 Documentation'
    else:
        return '🔄 Misc / Other'

def get_weekly_activity(root_dir, days_back=7):
    # Dynamically anchor the end_date to the most recent Sunday at 23:59:59 UTC
    now = pd.Timestamp.now(tz='UTC')
    # dayofweek: Monday=0, Sunday=6. 
    # If today is Sunday (6), subtract 0 days. If Monday (0), subtract 1 day.
    days_since_sunday = (now.dayofweek + 1) % 7
    most_recent_sunday = now - timedelta(days=days_since_sunday)
    end_date = pd.Timestamp(year=most_recent_sunday.year, month=most_recent_sunday.month, day=most_recent_sunday.day, hour=23, minute=59, second=59, tz='UTC')
    
    start_date = end_date - timedelta(days=days_back) + timedelta(seconds=1)
    
    # Pre-load PR data
    pr_path = os.path.join(root_dir, "data/enriched/enriched_prs.parquet")
    try:
        pr_df = pd.read_parquet(pr_path)
        pr_df['merged_at'] = pd.to_datetime(pr_df['merged_at'], utc=True, errors='coerce')
    except Exception as e:
        print(f"Error reading PRs: {e}")
        pr_df = pd.DataFrame()
    
    unique_authors = {}
    total_merged = 0
    total_reviews = 0
    total_threads = 0
    top_reviewers = []
    top_authors = []
    new_contributors = []
    
    # Read reviews early to calculate importance
    reviews_path = os.path.join(root_dir, "data/enriched/enriched_reviews.parquet")
    try:
        reviews_df = pd.read_parquet(reviews_path)
        reviews_df['timestamp'] = pd.to_datetime(reviews_df['timestamp'], utc=True, errors='coerce')
        # Filter out passive GitHub events (like 'subscribed', 'labeled') to only count genuine interactions
        if 'event_type' in reviews_df.columns:
            reviews_df = reviews_df[reviews_df['event_type'].isin(['commented', 'reviewed'])]
    except Exception as e:
        print(f"Error reading reviews: {e}")
        reviews_df = pd.DataFrame()

    IDENTITY_MAP = {}
    try:
        identity_path = os.path.join(root_dir, "data/enriched/contributors_unified.parquet")
        if os.path.exists(identity_path):
            idf = pd.read_parquet(identity_path)
            for _, row in idf.iterrows():
                if pd.notna(row.get('uuid')) and pd.notna(row.get('display_name')):
                    IDENTITY_MAP[str(row['uuid'])] = str(row['display_name'])
    except Exception as e:
        print(f"Warning: Could not load identity map: {e}")

    # 1. Fetch enriched PRs
    try:
        merged_prs = pr_df[(pr_df['merged_at'].notna()) & (pr_df['merged_at'] >= start_date) & (pr_df['merged_at'] <= end_date)].copy()
        total_merged = len(merged_prs)
        
        # Calculate top authors
        merged_prs_clean = merged_prs[~merged_prs['author'].astype(str).str.lower().str.contains('bot', na=False)]
        author_counts = merged_prs_clean.groupby(['author', 'uuid'], dropna=False).size().reset_index(name='count')
        author_counts = author_counts.sort_values('count', ascending=False)
        
        def safe_uuid(u):
            return u if pd.notna(u) else None

        top_authors = [{"user": {"username": row['author'], "uuid": safe_uuid(row['uuid'])}, "count": row['count']} for row in author_counts.head(5).to_dict('records')]

        for _, row in merged_prs_clean.iterrows():
            if 'bot' not in str(row['author']).lower():
                unique_authors[row['author']] = safe_uuid(row['uuid'])
        
        # Find new contributors (first merged_at is within this week)
        first_merges = pr_df.dropna(subset=['merged_at']).groupby(['author', 'uuid'], dropna=False)['merged_at'].min().reset_index()
        new_contribs_df = first_merges[(first_merges['merged_at'] >= start_date) & (first_merges['merged_at'] <= end_date)]
        
        new_contributors = [{"username": row['author'], "uuid": safe_uuid(row['uuid'])} for _, row in new_contribs_df.iterrows() if 'bot' not in str(row['author']).lower()]

        # Calculate importance (all-time review events)
        if not reviews_df.empty:
            all_time_pr_counts = reviews_df.groupby('pr_number').size().reset_index(name='importance')
            merged_prs = pd.merge(merged_prs, all_time_pr_counts, on='pr_number', how='left')
            merged_prs['importance'] = merged_prs['importance'].fillna(0).astype(int)
        else:
            merged_prs['importance'] = 0

        merged_prs = merged_prs.sort_values(by=['importance', 'merged_at'], ascending=[False, False])
        merged_prs['category'] = merged_prs['labels'].apply(get_category_from_labels)
        
        pr_data = merged_prs[['pr_number', 'title', 'author', 'uuid', 'merged_at', 'category', 'importance']].copy()
        pr_data['merged_at'] = pr_data['merged_at'].dt.strftime('%Y-%m-%d')
        
        categorized_prs = {}
        for _, row in pr_data.iterrows():
            cat = row['category']
            if cat not in categorized_prs:
                categorized_prs[cat] = []
            
            row_dict = row.to_dict()
            row_dict['author_obj'] = {"username": row['author'], "uuid": safe_uuid(row['uuid'])}
            del row_dict['uuid']
            categorized_prs[cat].append(row_dict)
            
    except Exception as e:
        print(f"Error reading PRs: {e}")
        categorized_prs = {}

    # 2. Fetch Hot PRs
    try:
        if reviews_df.empty:
            raise ValueError("reviews_df is empty")
        
        recent_reviews = reviews_df[(reviews_df['timestamp'] >= start_date) & (reviews_df['timestamp'] <= end_date)].copy()
        total_reviews = len(recent_reviews)
        
        recent_reviews_clean = recent_reviews[~recent_reviews['user'].astype(str).str.lower().str.contains('bot', na=False)]
        reviewer_counts = recent_reviews_clean.groupby(['user', 'uuid'], dropna=False).size().reset_index(name='count')
        reviewer_counts = reviewer_counts.sort_values('count', ascending=False)
        top_reviewers = [{"user": {"username": row['user'], "uuid": safe_uuid(row['uuid'])}, "count": row['count']} for row in reviewer_counts.head(5).to_dict('records')]

        for _, row in recent_reviews_clean.iterrows():
            unique_authors[row['user']] = safe_uuid(row['uuid'])
        
        hot_pr_counts = recent_reviews.groupby('pr_number').size().reset_index(name='event_count')
        hot_pr_counts = hot_pr_counts.sort_values(by='event_count', ascending=False).head(5)
        
        hot_prs = pd.merge(hot_pr_counts, pr_df, on='pr_number', how='left')
        hot_prs['category'] = hot_prs['labels'].apply(get_category_from_labels)
        
        hot_pr_data = {}
        for _, row in hot_prs.iterrows():
            cat = row['category']
            if pd.isna(row['title']):
                continue
            if cat not in hot_pr_data:
                hot_pr_data[cat] = []
            hot_pr_data[cat].append({
                'pr_number': row['pr_number'],
                'title': row['title'],
                'author': row['author'],
                'author_obj': {"username": row['author'], "uuid": safe_uuid(row['uuid'])},
                'event_count': row['event_count']
            })
            
    except Exception as e:
        print(f"Error reading hot PRs: {e}")
        hot_pr_data = {}

    # 3. Fetch active social threads
    try:
        social_path = os.path.join(root_dir, "data/raw/social_combined.parquet")
        social_df = pd.read_parquet(social_path)
        social_df['date'] = pd.to_datetime(social_df['date'], utc=True)
        # Pre-compute thread originators from the full social_df
        thread_openers = social_df.sort_values(by=['is_reply', 'date']).groupby('thread_id').first().reset_index()
        opener_map = thread_openers.set_index('thread_id')[['author_name', 'canonical_id']].to_dict('index')

        recent_social = social_df[(social_df['date'] >= start_date) & (social_df['date'] <= end_date)]
        
        total_threads = len(recent_social.groupby(['source', 'subject']))
        
        thread_agg = recent_social.groupby(['source', 'subject']).agg(
            thread_id=('thread_id', 'first'),
            message_count=('message_id', 'count'),
            link=('link', 'first')
        ).reset_index()
        
        thread_agg = thread_agg.sort_values(by='message_count', ascending=False).head(5)
        social_data = thread_agg.to_dict(orient='records')
        
        for thread in social_data:
            tid = thread.get('thread_id')
            opener = opener_map.get(tid, {})
            uuid = str(opener.get('canonical_id', ''))
            author_name = opener.get('author_name', 'Unknown')

            if uuid and uuid != 'nan' and uuid != 'None':
                display_name = IDENTITY_MAP.get(uuid, author_name)
                thread['author_obj'] = {"username": display_name, "uuid": uuid}
                thread['author'] = display_name
            else:
                thread['author'] = author_name
    except Exception as e:
        print(f"Error reading social data: {e}")
        social_data = []

    # Clean up contributors (remove bots like DrahtBot)
    clean_authors = [{"username": u, "uuid": unique_authors[u]} for u in unique_authors if 'bot' not in str(u).lower()]
    clean_authors = sorted(clean_authors, key=lambda x: str(x['username']).lower())

    return {
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "categorized_merged_prs": categorized_prs,
        "categorized_hot_prs": hot_pr_data,
        "active_threads": social_data,
        "contributors": clean_authors,
        "total_merged": total_merged,
        "total_reviews": total_reviews,
        "total_threads": total_threads,
        "top_reviewers": top_reviewers,
        "top_authors": top_authors,
        "new_contributors": new_contributors
    }
