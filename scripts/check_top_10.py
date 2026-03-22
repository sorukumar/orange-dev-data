import json

with open('data/viz/network_graph.json', 'r') as f:
    data = json.load(f)

nodes = data['nodes']

print("--- Foundation (All-Time) ---")
all_time = sorted(nodes, key=lambda x: x['ranks']['all'])
for i in range(15):
    node = all_time[i]
    print(f"{node['ranks']['all']}. {node['id']} (Score: {node['scores']['all']:.6f})")

print("\n--- Post-Segwit (2016+) ---")
post_2016 = sorted(nodes, key=lambda x: x['ranks']['p2016'])
for i in range(15):
    node = post_2016[i]
    print(f"{node['ranks']['p2016']}. {node['id']} (Score: {node['scores']['p2016']:.6f})")

print("\n--- Modern R&D (2023+) ---")
modern = sorted(nodes, key=lambda x: x['ranks']['modern'])
for i in range(15):
    node = modern[i]
    print(f"{node['ranks']['modern']}. {node['id']} (Score: {node['scores']['modern']:.6f})")
