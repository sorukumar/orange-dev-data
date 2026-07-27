import pandas as pd
import hashlib

def build_thread_context(df, thread_id, window_start=None, window_end=None, max_replies=3, max_length=2000):
    """
    Builds a rich context string for an LLM summarizer containing the original post 
    and the most recent replies within the given time window.
    
    Args:
        df (pd.DataFrame): The social_threads dataframe.
        thread_id (str): The ID of the thread.
        window_start (pd.Timestamp, optional): Start of the window.
        window_end (pd.Timestamp, optional): End of the window.
        max_replies (int): Maximum number of replies to include.
        max_length (int): Maximum character length of the combined snippet.
        
    Returns:
        tuple: (context_string, md5_hash)
    """
    # Filter for the specific thread
    t_df = df[df['thread_id'] == thread_id].copy()
    if t_df.empty:
        return "", ""
        
    t_df = t_df.sort_values('date')
    
    # 1. Original Post (OP)
    opener_mask = ~t_df['is_reply']
    op_snippet = ""
    if opener_mask.any():
        op_snippet = str(t_df[opener_mask].iloc[0].get('body_snippet', '')).strip()
    
    # 2. Replies in window
    replies_df = t_df[t_df['is_reply']]
    if window_start is not None:
        replies_df = replies_df[replies_df['date'] >= window_start]
    if window_end is not None:
        replies_df = replies_df[replies_df['date'] <= window_end]
        
    # Get the most recent unique replies (by author/canonical_id)
    # Sort descending by date to get latest, then take max_replies, then re-sort ascending for chronological order
    recent_replies = replies_df.sort_values('date', ascending=False).drop_duplicates('canonical_id').head(max_replies)
    recent_replies = recent_replies.sort_values('date', ascending=True)
    
    rep_snippets = []
    for _, row in recent_replies.iterrows():
        snippet = str(row.get('body_snippet', '')).strip()
        if snippet:
            date_str = row['date'].strftime('%Y-%m-%d')
            rep_snippets.append(f"[{date_str} - {row.get('author_name', 'Unknown')}] {snippet}")
            
    # 3. Construct Context String
    context = "ORIGINAL CONTEXT:\n"
    context += op_snippet + "\n\n"
    
    if rep_snippets:
        context += "NEW DEVELOPMENTS:\n"
        for rep in rep_snippets:
            context += f"- {rep}\n"
    else:
        context += "NEW DEVELOPMENTS:\n(No new replies in the current window.)\n"
        
    # Truncate to max_length to avoid token explosion
    if len(context) > max_length:
        context = context[:max_length-3] + "..."
        
    # 4. Generate Hash
    context_hash = hashlib.md5(context.encode('utf-8')).hexdigest()
    
    return context, context_hash
