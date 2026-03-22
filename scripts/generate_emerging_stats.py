
import json
from collections import defaultdict

with open('data/contributors_rich.json', 'r') as f:
    contributors = json.load(f)

region_mapping = {
    # Latin America
    "brazil": "Latin America",
    "argentina": "Latin America",
    "mexico": "Latin America",
    "chile": "Latin America",
    "colombia": "Latin America",
    "peru": "Latin America",
    "venezuela": "Latin America",
    "uruguay": "Latin America",
    "ecuador": "Latin America",
    "paraguay": "Latin America",
    "bolivia": "Latin America",
    "guatemala": "Latin America",
    "el salvador": "Latin America",
    "costa rica": "Latin America",
    "panama": "Latin America",
    # Africa
    "kenya": "Africa",
    "nigeria": "Africa",
    "south africa": "Africa",
    "ghana": "Africa",
    "egypt": "Africa",
    "ethiopia": "Africa",
    "senegal": "Africa",
    "tanzania": "Africa",
    "uganda": "Africa",
    "morocco": "Africa",
    "algeria": "Africa",
    "tunisia": "Africa",
}

history = defaultdict(lambda: {"Africa": 0, "Latin America": 0})
years = set()

for c in contributors:
    loc = (c.get('location') or "").lower()
    cohort = c.get('cohort_year')
    if not cohort: continue
    
    found_region = None
    for country, region in region_mapping.items():
        if country in loc:
            found_region = region
            break
    
    if found_region:
        years.add(cohort)
        history[cohort][found_region] += 1

sorted_years = sorted(list(years))
# Filter to last 10 years for better visualization if needed, but lets keep all for now
data_africa = [history[y]["Africa"] for y in sorted_years]
data_latam = [history[y]["Latin America"] for y in sorted_years]

output = {
    "xAxis": [str(y) for y in sorted_years],
    "series": [
        {
            "name": "Africa",
            "type": "bar",
            "data": data_africa
        },
        {
            "name": "Latin America",
            "type": "bar",
            "data": data_latam
        }
    ]
}

with open('data/stats_emerging_regions.json', 'w') as f:
    json.dump(output, f, indent=2)

print("Generated data/stats_emerging_regions.json")
