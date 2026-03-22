import pandas as pd
import json
from collections import Counter

def extract_network():
    print("Loading social data...")
    df = pd.read_parquet('data/governance/social.parquet')
    
    # 1. Create a lookup: Message ID -> Author
    # We clean the message IDs to ensure matches
    df['msg_id_clean'] = df['message_id'].str.strip('<>')
    msg_to_author = df.dropna(subset=['msg_id_clean']).set_index('msg_id_clean')['canonical_id'].to_dict()
    
    edges = []
    
    print("Tracing reply chains...")
    for _, row in df.iterrows():
        author = row['canonical_id']
        reply_to = row['reply_to']
        
        if pd.isna(reply_to) or not author:
            continue
            
        # Clean the reply_to ID
        target_mid = reply_to.strip('<>')
        
        # Find who they were replying to
        recipient = msg_to_author.get(target_mid)
        
        if recipient and recipient != author:
            # Sort to handle A->B and B->A as the same "interaction pair" for top hubs
            pair = tuple(sorted([author, recipient]))
            edges.append({
                'source': author,
                'target': recipient,
                'pair': pair,
                'thread_id': row['thread_id']
            })

    # 2. Identify Top Hubs (Interaction counts)
    edge_counts = Counter([e['pair'] for e in edges])
    
    print("\n--- TOP TECHNICAL VOLLEYS (Interactions) ---")
    for (p1, p2), count in edge_counts.most_common(15):
        print(f"{count:4d} interactions | {p1} <--> {p2}")

    # 3. Identify Centrality (Who is replied to the most?)
    recipients = Counter([e['target'] for e in edges])
    print("\n--- TOP INFLUENCE HUBS (Most Replied To) ---")
    for name, count in recipients.most_common(15):
        print(f"{count:4d} replies received | {name}")

if __name__ == "__main__":
    extract_network()
