
import json

with open('data/contributors_rich.json', 'r') as f:
    contributors = json.load(f)

region_mapping = {
    "brazil": "Latin America",
    "argentina": "Latin America",
    "mexico": "Latin America",
    "chile": "Latin America",
    "colombia": "Latin America",
    "peru": "Latin America",
    "venezuela": "Latin America",
    "kenya": "Africa",
    "nigeria": "Africa",
    "south africa": "Africa",
    "ghana": "Africa",
    "ethiopia": "Africa",
}

emerging_contributors = []
for c in contributors:
    loc = (c.get('location') or "").lower()
    for country, region in region_mapping.items():
        if country in loc:
            emerging_contributors.append({
                "name": c.get('name'),
                "region": region,
                "cohort": c.get('cohort_year'),
                "impact": c.get('impact')
            })

print(json.dumps(emerging_contributors, indent=2))
