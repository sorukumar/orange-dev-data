import pandas as pd
import os

def main():
    path = "data/raw/social_combined.parquet"
    if not os.path.exists(path):
        print("No combined data found.")
        return

    df = pd.read_parquet(path)
    df['year'] = df['date'].dt.year

    print(f"Total posts: {len(df)}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")

    print("\nPosts per year:")
    yearly = df.groupby('year').size()
    for year, count in yearly.items():
        print(f"{year}: {count}")

    print("\nPosts per source:")
    source = df.groupby('source').size()
    for src, count in source.items():
        print(f"{src}: {count}")

    print("\nPosts per year and source:")
    pivot = df.groupby(['year', 'source']).size().unstack(fill_value=0)
    print(pivot)

if __name__ == "__main__":
    main()