
import json
from collections import Counter

with open('data/contributors_rich.json', 'r') as f:
    contributors = json.load(f)

locations = [c.get('location') for c in contributors if c.get('location')]
print(Counter(locations).most_common(50))
