import pandas as pd

def analyze_trends():
    path = "data/raw/social_combined_categorized.parquet"
    df = pd.read_parquet(path)
    df['year'] = df['date'].dt.year
    
    # Filter for target categories
    targets = [
        'covenants', 'mempool-fees', 'lightning', 
        'privacy', 'soft-fork-activation', 'mining', 'wallet-keys'
    ]
    
    # Expand multi-label categories if necessary, 
    # but based on data_dictionary.md, 'category' is the primary one.
    
    pivot = df[df['category'].isin(targets)].pivot_table(
        index='year', 
        columns='category', 
        values='message_id', 
        aggfunc='count',
        fill_value=0
    )
    
    print("Yearly Message Count for Key Categories:")
    print(pivot.tail(10))

if __name__ == "__main__":
    analyze_trends()
