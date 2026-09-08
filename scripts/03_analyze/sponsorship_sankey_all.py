import pandas as pd
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.append(os.getcwd())
from scripts.utils.identity import resolver

OUTPUT_PATH = 'output/network/sankey_all_sponsors.json'
SPONSORS_FILE = '../orange-funding-data/data/enriched/sponsors_merged.json'

def get_active_sponsor(dev_obj):
    """Return the currently active sponsor from a sponsored_developers object."""
    grants = dev_obj.get('grants', [])
    for grant in grants:
        if not grant.get('end_date'):
            return grant.get('sponsor_id', 'Unknown')
        
        # Check if end date is in the future
        try:
            end_dt = datetime.strptime(grant['end_date'], "%Y-%m-%d")
            if end_dt > datetime.now():
                return grant.get('sponsor_id', 'Unknown')
        except:
            pass
    return "Independent"

def main():
    print("--- Generating Full Sponsorship & Influence Sankey Data ---")
    
    # 1. Load Sponsors from orange-funding-data
    try:
        with open(SPONSORS_FILE) as f:
            sponsors_data = json.load(f).get('sponsored_developers', [])
    except Exception as e:
        print(f"Error loading {SPONSORS_FILE}: {e}")
        return
        
    developers = {}
    for dev in sponsors_data:
        gh = dev.get('github', '').lower()
        if not gh:
            continue
            
        cid = resolver.resolve_github(gh)
        if cid:
            sponsor = get_active_sponsor(dev)
            if sponsor != "Unknown":
                if sponsor.lower() == 'chaincode': sponsor = 'Chaincode Labs'
                elif sponsor.lower() == 'okcoin': sponsor = 'OKCoin'
                elif sponsor.lower() == 'mit_dci': sponsor = 'MIT DCI'
                else: sponsor = sponsor.title()
                
                developers[cid] = {
                    'name': dev.get('canonical_name', gh),
                    'github': gh,
                    'sponsor': sponsor
                }

    # 2.5 Load PR Metadata to resolve Merge Commit Categories
    print("Loading PR metadata to resolve merge categories...")
    pr_to_category = {}
    try:
        df_prs = pd.read_parquet('data/raw/github_pr_metadata.parquet')
        
        # Simple mapping from GitHub label to our commit categories
        LABEL_MAP = {
            'Consensus': 'Consensus', 'Wallet': 'Wallet', 'P2P': 'P2P Network',
            'RPC/REST/ZMQ': 'Node & RPC', 'GUI': 'GUI', 'Mempool': 'Mempool',
            'Tests': 'Tests', 'Build system': 'Build & CI', 'Docs': 'Documentation',
            'Cryptography': 'Cryptography', 'Scripts and tools': 'Script',
            'Utils/log/libs': 'Utilities', 'Data corruption/recovery': 'Database',
            'Refactoring': 'Utilities'
        }
        
        for _, row in df_prs.iterrows():
            pr_num = str(row.get('pr_number'))
            labels = row.get('labels')
            if not labels or pd.isna(labels): continue
            
            # Use the first mapped label we find
            assigned = None
            for label in labels.split(','):
                label = label.strip()
                if label in LABEL_MAP:
                    assigned = LABEL_MAP[label]
                    break
            
            if assigned:
                pr_to_category[pr_num] = assigned
                
    except Exception as e:
        print(f"Warning: Could not load PR metadata: {e}")

    # 3. Load Commits
    print("Loading commits_resolved.parquet...")
    try:
        df = pd.read_parquet('data/enriched/commits_resolved.parquet')
    except Exception as e:
        print(f"Error loading commits: {e}")
        return
        
    df['date_utc'] = pd.to_datetime(df['date_utc'], utc=True)
    
    # 4. Filter for only developers we have sponsor data for, and Core repository
    df = df[df['canonical_id'].isin(developers.keys())]
    df = df[df['repository_name'] == 'bitcoin/bitcoin']
    
    # Re-assign Merge Commit Categories based on PR number
    def assign_real_category(row):
        if row['is_merge'] == True and pd.notna(row.get('pr_number')):
            pr_num = str(row['pr_number']).replace('.0', '')
            return pr_to_category.get(pr_num, 'Merge')
        return row['category']
        
    df['category'] = df.apply(assign_real_category, axis=1)
    
    df = df[df['category'].notna()]
    df = df[df['category'] != 'other']
    df = df[df['category'] != 'Unknown']
    df = df[df['category'] != 'Merge'] # Drop any merges we couldn't resolve

    now = datetime.now(timezone.utc)
    periods = {
        '1yr': now - timedelta(days=365),
        '3yr': now - timedelta(days=365*3),
        '5yr': now - timedelta(days=365*5),
        'all': datetime(2000, 1, 1, tzinfo=timezone.utc)
    }
    
    output_data = {'periods': {}}
    
    for period_name, start_date in periods.items():
        print(f"Processing period: {period_name} (Since {start_date.strftime('%Y-%m-%d')})")
        df_period = df[df['date_utc'] >= start_date]
        
        nodes = {}
        links = []
        
        # Calculate total influence for each dev in this period to select top N
        dev_influence = {}
        for cid, dev_data in developers.items():
            df_m = df_period[df_period['canonical_id'] == cid]
            if df_m.empty: continue
            
            authored = len(df_m[df_m['is_merge'] == False])
            merged = len(df_m[df_m['is_merge'] == True])
            influence = authored + (merged * 5)
            if influence > 0:
                dev_influence[cid] = influence
                
        # Keep top 60 devs to prevent chart from being an unreadable mess
        top_cids = sorted(dev_influence, key=dev_influence.get, reverse=True)[:60]
        
        for cid in top_cids:
            dev_data = developers[cid]
            df_m = df_period[df_period['canonical_id'] == cid]
            
            sponsor_name = dev_data['sponsor']
            dev_name = dev_data['name']
            
            cat_groups = df_m.groupby('category')
            for category, group in cat_groups:
                authored = len(group[group['is_merge'] == False])
                merged = len(group[group['is_merge'] == True])
                influence = authored + (merged * 5)
                
                if influence > 0:
                    links.append({"source": sponsor_name, "target": dev_name, "value": influence})
                    links.append({"source": dev_name, "target": category, "value": influence})
                    
                    nodes[sponsor_name] = {"name": sponsor_name, "category": 0}
                    nodes[dev_name] = {"name": dev_name, "category": 1}
                    nodes[category] = {"name": category, "category": 2}

        link_sums = {}
        for l in links:
            key = (l['source'], l['target'])
            link_sums[key] = link_sums.get(key, 0) + l['value']
            
        final_links = [{"source": k[0], "target": k[1], "value": v} for k, v in link_sums.items()]
        
        output_data['periods'][period_name] = {
            "nodes": list(nodes.values()),
            "links": final_links
        }
        
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(output_data, f, indent=2)
        
    print(f"Successfully wrote {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
