import sys

with open("scripts/identity/build_identities.py", "r") as f:
    text = f.read()

search = """        # Determine canonical name & UUID
        canonical_name = "Unknown"
        curated_names = [e["canonical_name"] for e in curated_data if e.get("canonical_name")]
        matches = [n for n in decoded_names if n in curated_names]"""

replace = """        # Determine canonical name & UUID
        canonical_name = "Unknown"
        curated_names_lower = {e["canonical_name"].lower(): e["canonical_name"] for e in curated_data if e.get("canonical_name")}
        matches = [curated_names_lower[n.lower()] for n in decoded_names if n.lower() in curated_names_lower]"""

if search in text:
    text = text.replace(search, replace)
    with open("scripts/identity/build_identities.py", "w") as f:
        f.write(text)
    print("Fixed UUID matching!")
else:
    print("Search string not found")
