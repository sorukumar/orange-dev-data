
import json
import os

CACHE_FILE = "data/enrichment_cache.json"
with open(CACHE_FILE, "r") as f:
    cache = json.load(f)

print(f"Cache size: {len(cache)}")
target_names = ["Harris", "ENikS", "DrahtBot"]
for name in target_names:
    found = False
    for k, v in cache.items():
        if name.lower() in k.lower():
            print(f"Match for {name}: {k} -> {v}")
            found = True
    if not found:
        print(f"No match for {name}")
