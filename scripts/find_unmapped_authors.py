import pandas as pd
import json
import os

def find_unmapped():
    mapping_path = 'lookups/identity_mappings.json'
    social_path = 'data/raw/social_combined.parquet'
    output_path = 'research/unmapped_authors.txt'
    
    if not os.path.exists(mapping_path):
        print(f"Error: {mapping_path} not found")
        return
    
    if not os.path.exists(social_path):
        print(f"Error: {social_path} not found")
        return

    with open(mapping_path) as f:
        mapping = json.load(f)
    
    mapped_ids = set()
    for entry in mapping['aliases']:
        mapped_ids.add(entry['canonical_name'].lower())
        for alias in entry.get('aliases', []):
            mapped_ids.add(alias.lower())
        for email in entry.get('emails', []):
            mapped_ids.add(email.lower())
            
    df = pd.read_parquet(social_path)
    
    # Check canonical_id
    unmapped_mask = ~df['canonical_id'].str.lower().isin(mapped_ids) & df['canonical_id'].notna()
    unmapped_df = df[unmapped_mask]
    
    counts = unmapped_df['canonical_id'].value_counts()
    
    total_unmapped = len(counts)
    significant_unmapped = len(counts[counts >= 10])
    
    print(f"Total Unique Unmapped Authors: {total_unmapped}")
    print(f"Unmapped Authors with 10+ posts: {significant_unmapped}")
    
    top_50 = counts.head(50)
    print("\nTop 50 Unmapped Authors:")
    print(top_50)
    
    # Save to file
    os.makedirs('research', exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(f"Report Generated: {pd.Timestamp.now()}\n")
        f.write(f"Total Unique Unmapped Authors: {total_unmapped}\n")
        f.write(f"Unmapped Authors with 10+ posts: {significant_unmapped}\n")
        f.write("-" * 40 + "\n")
        f.write("Posts | Author ID\n")
        f.write("-" * 40 + "\n")
        for name, count in counts.items():
            f.write(f"{count:5} | {name}\n")
            
    print(f"\nFull list saved to {output_path}")

if __name__ == "__main__":
    find_unmapped()
