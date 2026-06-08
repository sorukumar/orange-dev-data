import sys

with open("scripts/identity/build_identities.py", "r") as f:
    text = f.read()

# Add back the missing loop block
search = """        for _, row in df.iterrows():
            authors = row.get('author_names', [])"""

replace = """        for _, row in df.iterrows():
            authors = row.get('author_names', [])
            for n in authors:
                n = str(n).strip()
                if clean_val(n):
                    n_node = f"NAME:{n}"
                    if n != n.title(): add_node_and_edge(G, n_node, f"NAME:{n.title()}", "bips")
                    if n != n.lower(): add_node_and_edge(G, n_node, f"NAME:{n.lower()}", "bips")
                    G.add_node(n_node)
                    node_sources[n_node].add("bips")"""

if search in text:
    text = text.replace(search, replace)
    with open("scripts/identity/build_identities.py", "w") as f:
        f.write(text)
    print("Fixed!")
else:
    print("Search string not found")
