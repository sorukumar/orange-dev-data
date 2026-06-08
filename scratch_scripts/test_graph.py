import json
import networkx as nx

with open("metadata/identity_curated.json") as f:
    curated_data = json.load(f)["aliases"]
    
G = nx.Graph()

for entry in curated_data:
    canon = entry.get("canonical_name", "").lower()
    for alias in entry.get("aliases", []):
        G.add_edge(canon, alias.lower())
    for email in entry.get("emails", []):
        G.add_edge(canon, email.lower())

with open("metadata/github_id_map.json") as f:
    id_map = json.load(f)
    for m in id_map.values():
        login = str(m.get("login")).lower()
        gh_id = str(m.get("github_id"))
        G.add_edge(login, gh_id)
        for em in m.get("emails", []):
            G.add_edge(gh_id, str(em.get("email")).lower())

with open("metadata/github_profiles.json") as f:
    prof = json.load(f)
    for p in prof.values():
        login = str(p.get("login")).lower()
        gh_id = str(p.get("github_id"))
        name = str(p.get("name")).lower()
        G.add_edge(login, gh_id)
        # We know build_identities links login -> name if name is valid
        if name and name not in ['none', 'null', 'unknown']:
            G.add_edge(login, name)

try:
    path = nx.shortest_path(G, source="satoshinakamotobitcoin", target="satoshi nakamoto")
    print("Path for SatoshiNakamotoBitcoin:", path)
except Exception as e:
    print("No path found for SatoshiNakamotoBitcoin:", e)

try:
    path = nx.shortest_path(G, source="crypomen9", target="satoshi nakamoto")
    print("Path for crypomen9:", path)
except Exception as e:
    print("No path found for crypomen9:", e)

