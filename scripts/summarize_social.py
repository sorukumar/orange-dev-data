import pandas as pd
import json

def summarize_social():
    path = "data/raw/social_combined.parquet"
    if not pd.io.common.file_exists(path):
        print(f"File {path} not found.")
        return

    df = pd.read_parquet(path)
    print(f"Total social records: {len(df)}")
    print(df.columns)
    
    unique_authors = df[['author_name', 'author_email', 'canonical_id']].drop_duplicates()
    print(f"Total unique social authors: {len(unique_authors)}")
    
    # Check how many are not mapped to a 'known' canonical name
    # We can load the mappings to check
    mapping_path = "lookups/identity_mappings.json"
    with open(mapping_path, 'r') as f:
        mapping_data = json.load(f)
    
    known_canonical_names = {entry['canonical_name'] for entry in mapping_data['aliases']}
    
    unmapped = unique_authors[~unique_authors['canonical_id'].isin(known_canonical_names)]
    print(f"Unmapped unique authors: {len(unmapped)}")
    print("\nTop unmapped contributors by post count:")
    
    post_counts = df['canonical_id'].value_counts()
    unmapped_counts = post_counts[~post_counts.index.isin(known_canonical_names)]
    print(unmapped_counts.head(20))

if __name__ == "__main__":
    summarize_social()
