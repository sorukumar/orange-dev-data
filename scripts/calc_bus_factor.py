import json
import os
import math

def calculate_bus_factor():
    input_file = os.path.join(os.path.dirname(__file__), '../output/tracker/contributors_rich.json')
    output_file = os.path.join(os.path.dirname(__file__), '../output/tracker/stats_bus_factor.json')
    
    with open(input_file, 'r') as f:
        data = json.load(f)
        
    year_stats = {}
    
    for contributor in data:
        login = contributor.get('login') or contributor.get('name')
        history = contributor.get('history', {})
        for year, categories in history.items():
            total_commits_year = sum(categories.values())
            if total_commits_year > 0:
                if year not in year_stats:
                    year_stats[year] = []
                year_stats[year].append({
                    'login': login,
                    'commits': total_commits_year
                })
                
    years = sorted(year_stats.keys())
    historical_xAxis = []
    historical_bus_factor = []
    historical_total_contributors = []
    
    headline_bus_factor = 0
    headline_top_contributors = []
    
    for year in years:
        contributors_this_year = year_stats[year]
        # Sort descending by commits
        contributors_this_year.sort(key=lambda x: x['commits'], reverse=True)
        
        total_commits = sum(c['commits'] for c in contributors_this_year)
        target_commits = total_commits / 2.0
        
        cumulative = 0
        bus_factor = 0
        top_logins = []
        
        for c in contributors_this_year:
            cumulative += c['commits']
            bus_factor += 1
            top_logins.append(c['login'])
            if cumulative >= target_commits:
                break
                
        historical_xAxis.append(year)
        historical_bus_factor.append(bus_factor)
        historical_total_contributors.append(len(contributors_this_year))
        
        # We will use the latest year for the headline
        headline_bus_factor = bus_factor
        headline_top_contributors = top_logins
        
    latest_year = years[-1] if years else "N/A"
        
    output_data = {
        "headline": {
            "bus_factor": headline_bus_factor,
            "window": "1_year",
            "period": str(latest_year),
            "top_contributors": headline_top_contributors
        },
        "historical": {
            "xAxis": historical_xAxis,
            "bus_factor": historical_bus_factor,
            "total_contributors": historical_total_contributors
        }
    }
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
        
    print(f"Generated {output_file}")
    print(f"Latest Bus Factor ({latest_year}): {headline_bus_factor}")
    print(f"Top Contributors: {headline_top_contributors}")

if __name__ == "__main__":
    calculate_bus_factor()
