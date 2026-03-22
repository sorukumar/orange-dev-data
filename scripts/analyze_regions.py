
import json
from collections import defaultdict

with open('data/contributors_rich.json', 'r') as f:
    contributors = json.load(f)

# Hardcoded mapping for demonstration - in a real scenario this might be more robust
region_mapping = {
    "USA": "North America",
    "Canada": "North America",
    "UK": "Europe",
    "Germany": "Europe",
    "France": "Europe",
    "Netherlands": "Europe",
    "Switzerland": "Europe",
    "Austria": "Europe",
    "Portugal": "Europe",
    "Ukraine": "Europe",
    "Russia": "Europe",
    "Spain": "Europe",
    "Italy": "Europe",
    "Sweden": "Europe",
    "Norway": "Europe",
    "Finland": "Europe",
    "Denmark": "Europe",
    "Belgium": "Europe",
    "Poland": "Europe",
    "Czech Republic": "Europe",
    "Brazil": "Latin America",
    "Argentina": "Latin America",
    "Mexico": "Latin America",
    "Chile": "Latin America",
    "Colombia": "Latin America",
    "El Salvador": "Latin America",
    "Nigeria": "Africa",
    "Kenya": "Africa",
    "South Africa": "Africa",
    "Ghana": "Africa",
    "Ethiopia": "Africa",
    "China": "Asia",
    "Japan": "Asia",
    "India": "Asia",
    "Singapore": "Asia",
    "South Korea": "Asia",
    "Vietnam": "Asia",
    "Australia": "Oceania",
    "New Zealand": "Oceania",
}

# Helper to guess region from location string
def get_region(location):
    if not location:
        return "Unknown"
    location = location.lower()
    for country, region in region_mapping.items():
        if country.lower() in location:
            return region
    return "Other"

history = defaultdict(lambda: defaultdict(int))
years = set()

for c in contributors:
    cohort = c.get('cohort_year')
    if not cohort: continue
    years.add(cohort)
    region = get_region(c.get('location'))
    history[cohort][region] += 1

sorted_years = sorted(list(years))
regions = ["North America", "Europe", "Latin America", "Africa", "Asia", "Oceania", "Other", "Unknown"]

result = {
    "xAxis": [str(y) for y in sorted_years],
    "series": []
}

for r in regions:
    data = []
    for y in sorted_years:
        data.append(history[y][r])
    result["series"].append({
        "name": r,
        "type": "bar",
        "stack": "total",
        "data": data
    })

print(json.dumps(result))
