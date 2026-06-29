import pandas as pd
import json
import os
import sys

# --- Configuration ---
SELF_MERGES_PARQUET = "data/enriched/self_merges_detailed.parquet"
IDENTITIES_JSON = "metadata/identities.json"
OUTPUT_JSON = "output/network/stats_self_merges.json"

def deliver_self_merges():
    print("Formatting self-merge receipts for delivery...")
    
    if not os.path.exists(SELF_MERGES_PARQUET):
        print(f"Error: {SELF_MERGES_PARQUET} not found.")
        return
        
    df_merges = pd.read_parquet(SELF_MERGES_PARQUET)
    
    if df_merges.empty:
        print("No self-merges found to deliver.")
        with open(OUTPUT_JSON, 'w') as f:
            json.dump({"self_merges": []}, f)
        return
        
    # Load Master Identities
    with open(IDENTITIES_JSON, 'r') as f:
        identities_data = json.load(f)['identities']
        
    uuid_to_name = {ident['uuid']: ident['display_name'] for ident in identities_data}
    
    # We want to use the maintainer's display name, but also we can group them
    # For a flat JSON array, it's easy for the frontend to filter by maintainer_id.
    
    receipts = []
    
    # Sort by merged_at descending so newest are first
    df_merges['merged_at'] = pd.to_datetime(df_merges['merged_at'], utc=True)
    df_merges = df_merges.sort_values(by='merged_at', ascending=False)
    
    for _, row in df_merges.iterrows():
        cid = row['canonical_id']
        display_name = uuid_to_name.get(cid, cid)
        
        receipt = {
            "maintainer_id": cid,
            "maintainer_name": display_name,
            "repository": row['repository_name'],
            "pr_number": int(row['pr_number']),
            "url": row.get('html_url', ""),
            "title": row.get('title', ""),
            "merged_at": row['merged_at'].isoformat() if pd.notna(row['merged_at']) else None,
            "ack_count": int(row['ack_count']),
            "category": row['category']
        }
        receipts.append(receipt)
        
    payload = {
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "self_merges": receipts
    }
    
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(payload, f, indent=2)
        
    print(f"Saved {len(receipts)} self-merge receipts to {OUTPUT_JSON}")

if __name__ == "__main__":
    deliver_self_merges()
