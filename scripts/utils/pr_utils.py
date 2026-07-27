import pandas as pd

def is_high_signal(labels_str, is_recent, review_count=0, title=""):
    """
    Evaluates whether a PR is 'high signal' (worth deep LLM summarization)
    based on its labels, title, age, and community review engagement.
    """
    labels = str(labels_str).lower() if pd.notna(labels_str) else ""
    title_str = str(title).lower() if pd.notna(title) else ""
    
    tier_1 = ['consensus', 'validation', 'cryptography', 'p2p', 'wallet', 'mempool', 'policy', 'secp256k1', 'bip']
    tier_3 = ['test', 'doc', 'refactor', 'build', 'ci', 'fuzz']
    
    # For older historical PRs, we only care about the absolute core features
    if not is_recent:
        return any(keep in labels or keep in title_str for keep in ['consensus', 'cryptography', 'p2p', 'secp256k1', 'bip'])
        
    # Check Tier 1 (Threshold 0) - Always high signal
    if any(keep in labels or keep in title_str for keep in tier_1):
        return True
        
    # Check Tier 3 (Typically dropped noise, requires massive engagement to be saved)
    if any(drop in labels or drop in title_str for drop in tier_3):
        return review_count >= 50
        
    # Check Tier 2 (Everything else, requires solid engagement)
    return review_count >= 25
