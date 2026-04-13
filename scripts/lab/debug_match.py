
import pandas as pd
import json

legacy_df = pd.read_parquet("data/bitcoin_contributors_data.parquet")
email_map = {}
name_map = {}
for idx, row in legacy_df.iterrows():
    if pd.notna(row.get('Email')):
        emails = [e.strip().lower() for e in str(row['Email']).split(',')]
        for e in emails:
            if e: email_map[e] = row
    if pd.notna(row.get('Name')):
        names = [n.strip().lower() for n in str(row['Name']).split(',')]
        for n in names:
            if n: name_map[n] = row

print(f"Index size: {len(email_map)} emails, {len(name_map)} names")

target_name = "practicalswift"
if target_name.lower() in name_map:
    print(f"Match found for {target_name} in name_map!")
    row = name_map[target_name.lower()]
    print(f"Login from row: {row.get('Login')}")
else:
    print(f"Match NOT found for {target_name} in name_map.")
    # Show some names
    print("Sample names in map:", list(name_map.keys())[:20])

# Check if practicalswift is in names
print("Is 'practicalswift' in names?", any("practicalswift" in str(n) for n in legacy_df['Name'].dropna()))
