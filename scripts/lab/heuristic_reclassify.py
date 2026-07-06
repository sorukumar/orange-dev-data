import json

def get_heuristic_category(title, pub_summary, tech_summary):
    text = (title + " " + pub_summary + " " + tech_summary).lower()
    
    if any(k in text for k in ["dos", "denial of service", "consensus", "fork", "bip", "vulnerability", "cve"]):
        return "Security & Consensus"
    if any(k in text for k in ["speedup", "optimize", "faster", "reduce memory", "performance", "benchmark"]):
        return "Performance & Optimization"
    if any(k in text for k in ["p2p", "tor", "i2p", "cjdns", "relay", "addrman", "txorphanage"]):
        return "Network & Privacy"
    if any(k in text for k in ["rpc", "gui", "qt", "psbt", "hardware wallet", "coin selection", "coin control"]):
        return "Wallet & User Tools"
    if any(k in text for k in ["cluster mempool", "v3 transaction", "kernel", "erlay", "stratum v2"]):
        return "Strategic Initiatives"
    return "Maintenance & Tech Debt"

def main():
    with open('data/raw/pr_summaries_cache.json', 'r') as f:
        cache = json.load(f)
        
    updated = 0
    for pr_num, data in cache.items():
        if data.get("impact_category") == "Maintenance & Tech Debt":
            title = data.get("title", "") # We don't have title in cache directly but it's fine
            pub = data.get("public_summary", "")
            tech = data.get("technical_summary", "")
            new_cat = get_heuristic_category(title, pub, tech)
            if new_cat != "Maintenance & Tech Debt":
                cache[pr_num]["impact_category"] = new_cat
                updated += 1
                
    print(f"Heuristically updated {updated} PRs.")
    
    with open('data/raw/pr_summaries_cache.json', 'w') as f:
        json.dump(cache, f, indent=2)

if __name__ == "__main__":
    main()
