
import json

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

results = []
for c in contributors:
    loc = (c.get('location') or "").lower()
    found_region = None
    for country, region in region_mapping.items():
        if country in loc:
            found_region = region
            break
    
    if found_region:
        results.append({
            "name": c.get('name'),
            "region": found_region,
            "cohort": c.get('cohort_year'),
            "impact": c.get('impact'),
            "last_active": c.get('last_active_year')
        })

# Group by cohort and region
cohort_stats = {}
for r in results:
    cohort = r['cohort']
    region = r['region']
    if cohort not in cohort_stats:
        cohort_stats[cohort] = {"Africa": 0, "Latin America": 0}
    cohort_stats[cohort][region] += 1

print("Detailed Contributors:")
print(json.dumps(results, indent=2))
print("\nCohort-wise count:")
print(json.dumps(cohort_stats, indent=2))
