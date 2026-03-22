
import json
from collections import defaultdict

with open('data/core/contributors_rich.json', 'r') as f:
    contributors = json.load(f)

# Consolidated categories
# 1. North America
# 2. Europe
# 3. Asia Pacific (Asia + Oceania)
# 4. Latin America
# 5. Africa
# 6. Undisclosed (Other + Unknown)

region_mapping = {
    # North America
    "usa": "North America", "united states": "North America", "canada": "North America", 
    "san francisco": "North America", "new york": "North America", "austin": "North America",
    "seattle": "North America", "california": "North America", "toronto": "North America",
    "vancouver": "North America", "los angeles": "North America", "chicago": "North America",
    "boston": "North America", "washington": "North America", "texas": "North America",
    "colorado": "North America", "quebec": "North America", "montreal": "North America",
    "atlant": "North America", "brooklyn": "North America", "arizona": "North America",
    "florida": "North America", "michigan": "North America", "virginia": "North America",
    "massachusetts": "North America", "oregon": "North America", "atx": "North America",
    ", ny": "North America", ", ca": "North America", ", tx": "North America", ", ma": "North America",
    ", wa": "North America", ", il": "North America", ", ga": "North America", ", nc": "North America",
    ", pa": "North America", ", fl": "North America", ", co": "North America", ", nj": "North America",
    ", va": "North America", ", md": "North America", ", oh": "North America", ", mi": "North America",
    ", or": "North America", ", az": "North America", ", ut": "North America", ", dc": "North America",
    # Europe
    "uk": "Europe", "united kingdom": "Europe", "london": "Europe", "germany": "Europe",
    "berlin": "Europe", "france": "Europe", "paris": "Europe", "netherlands": "Europe",
    "amsterdam": "Europe", "switzerland": "Europe", "zurich": "Europe", "austria": "Europe",
    "vienna": "Europe", "portugal": "Europe", "lisbon": "Europe", "spain": "Europe",
    "madrid": "Europe", "italy": "Europe", "sweden": "Europe", "stockholm": "Europe",
    "ukraine": "Europe", "kyiv": "Europe", "russia": "Europe", "moscow": "Europe",
    "poland": "Europe", "finland": "Europe", "denmark": "Europe", "norway": "Europe",
    "ireland": "Europe", "dublin": "Europe", "belgium": "Europe", "brussels": "Europe",
    "czech": "Europe", "prague": "Europe", "estonia": "Europe", "tallinn": "Europe",
    "lithuania": "Europe", "latvia": "Europe", "hungary": "Europe", "budapest": "Europe",
    "romania": "Europe", "bulgaria": "Europe", "greece": "Europe", "athens": "Europe",
    "geneva": "Europe", "munich": "Europe", "hamburg": "Europe", "barcelona": "Europe",
    "croatia": "Europe", "serbia": "Europe", "slovakia": "Europe", "slovenia": "Europe",
    "malta": "Europe", "cyprus": "Europe", "luxembourg": "Europe", "turkey": "Europe",
    "istanbul": "Europe", "ankara": "Europe",
    # Asia Pacific
    "china": "Asia Pacific", "beijing": "Asia Pacific", "shanghai": "Asia Pacific", 
    "japan": "Asia Pacific", "tokyo": "Asia Pacific", "india": "Asia Pacific", 
    "bangalore": "Asia Pacific", "singapore": "Asia Pacific", "south korea": "Asia Pacific",
    "seoul": "Asia Pacific", "vietnam": "Asia Pacific", "israel": "Asia Pacific", 
    "tel aviv": "Asia Pacific", "thailand": "Asia Pacific", "australia": "Asia Pacific", 
    "sydney": "Asia Pacific", "melbourne": "Asia Pacific", "brisbane": "Asia Pacific",
    "new zealand": "Asia Pacific", "auckland": "Asia Pacific", "pune": "Asia Pacific",
    "hyderabad": "Asia Pacific", "delhi": "Asia Pacific", "mumbai": "Asia Pacific",
    "taiwan": "Asia Pacific", "hong kong": "Asia Pacific", "malaysia": "Asia Pacific",
    "indonesia": "Asia Pacific", "philippines": "Asia Pacific", "bangkok": "Asia Pacific",
    "uae": "Asia Pacific", "dubai": "Asia Pacific", "abu dhabi": "Asia Pacific",
    "bcn": "Europe", "odisha": "Asia Pacific", "bhubaneswar": "Asia Pacific",
    "perth": "Asia Pacific", "seattle": "North America", "casablanca": "Africa",
    "morocco": "Africa", "switzerland": "Europe", "lausanne": "Europe",
    "mempool": "North America", # Mapping virtual Bitcoin infra to a core hub
    # Latin America
    "brazil": "Latin America", "sao paulo": "Latin America", "argentina": "Latin America",
    "buenos aires": "Latin America", "mexico": "Latin America", "chile": "Latin America",
    "colombia": "Latin America", "el salvador": "Latin America", "venezuela": "Latin America",
    "peru": "Latin America", "uruguay": "Latin America", "ecuador": "Latin America",
    "guatemala": "Latin America", "panama": "Latin America", "costa rica": "Latin America",
    "rio de janeiro": "Latin America", "santiago": "Latin America", "bogota": "Latin America",
    "bolivia": "Latin America", "paraguay": "Latin America", "panama": "Latin America",
    # Africa
    "kenya": "Africa", "nairobi": "Africa", "nigeria": "Africa", "lagos": "Africa",
    "south africa": "Africa", "cape town": "Africa", "johannesburg": "Africa",
    "ghana": "Africa", "accra": "Africa", "ethiopia": "Africa", "egypt": "Africa",
    "cairo": "Africa", "morocco": "Africa", "casablanca": "Africa", "tunisia": "Africa",
    "senegal": "Africa", "uganda": "Africa", "kampala": "Africa", "tanzania": "Africa",
    "rwanda": "Africa", "algeria": "Africa",
}

import unicodedata

def normalize_text(text):
    if not text: return ""
    # Normalize unicode characters (remove accents)
    return "".join(c for c in unicodedata.normalize('NFD', text)
                  if unicodedata.category(c) != 'Mn').lower()

def get_region(location):
    if not location: return "Undisclosed"
    location = normalize_text(location)
    for key, region in region_mapping.items():
        if key in location:
            return region
    return "Undisclosed"

history = defaultdict(lambda: defaultdict(int))
years = set()

for c in contributors:
    cohort = c.get('cohort_year')
    if not cohort: continue
    years.add(cohort)
    region = get_region(c.get('location'))
    history[cohort][region] += 1

sorted_years = sorted(list(years))
regions = ["North America", "Europe", "Asia Pacific", "Latin America", "Africa", "Undisclosed"]

output = {
    "xAxis": [str(y) for y in sorted_years],
    "series": []
}

for r in regions:
    data = []
    for y in sorted_years:
        data.append(history[y][r])
    output["series"].append({
        "name": r,
        "type": "line",
        "stack": "total",
        "areaStyle": {},
        "data": data
    })

with open('data/core/stats_regional_evolution.json', 'w') as f:
    json.dump(output, f, indent=2)

print("Generated data/core/stats_regional_evolution.json with consolidated categories")
