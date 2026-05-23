import pandas as pd
import os

def check_link():
    # 1. Load Git Signatures
    df_commits = pd.read_parquet('data/raw/core_commits.parquet')
    
    # Check if fanquake email matches sipa
    sipa_email = "pieter.wuille@gmail.com"
    fanquake_logins = ["fanquake", "Michael Ford"]
    
    print("Checking core_commits for direct link...")
    matches = df_commits[(df_commits['author_name'].isin(fanquake_logins)) & (df_commits['author_email'].str.lower() == sipa_email)]
    if not matches.empty:
        print("Found direct link in core_commits:")
        print(matches[['hash', 'author_name', 'author_email']])
    else:
        print("No direct link in core_commits.")

    # 2. Check PR Metadata
    print("\nChecking PR metadata for direct link...")
    for p in ['data/raw/github_pr_metadata.parquet', 'data/raw/bips_pr_metadata.parquet']:
        if os.path.exists(p):
            df = pd.read_parquet(p)
            for _, row in df.iterrows():
                login = row.get('author')
                head_sha = row.get('head_sha')
                
                if login in fanquake_logins and head_sha:
                    # Check if head_sha maps to sipa in core_commits
                    sipa_commits = df_commits[(df_commits['hash'] == head_sha) & (df_commits['author_email'].str.lower() == sipa_email)]
                    if not sipa_commits.empty:
                        print(f"FOUND IT! PR authored by {login} with head_sha {head_sha} which is a commit by {sipa_email}")
                        print(sipa_commits[['hash', 'author_name', 'author_email']])

if __name__ == "__main__":
    check_link()
