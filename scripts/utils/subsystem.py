import json
import re
import os

class SubsystemResolver:
    def __init__(self, subsystems_path=None):
        if subsystems_path is None:
            # Default to the standard location relative to the project root
            # Assuming this script is in scripts/utils/subsystem.py
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            subsystems_path = os.path.join(base_dir, 'metadata/subsystems.json')
            
        if not os.path.exists(subsystems_path):
            # Fallback for different execution contexts
            subsystems_path = 'metadata/subsystems.json'
            
        with open(subsystems_path, 'r') as f:
            self.subsystems = json.load(f)
        
        # Pre-compile regex patterns
        self.compiled_patterns = {}
        for sub_id, data in self.subsystems.items():
            self.compiled_patterns[sub_id] = [
                re.compile(p, re.IGNORECASE) for p in data.get('patterns', [])
            ]
            
        # BIP to ID mapping
        self.bip_to_id = {}
        for sub_id, data in self.subsystems.items():
            for bip in data.get('bips', []):
                self.bip_to_id[str(bip)] = sub_id

    def get_subsystem_by_path(self, file_path):
        """
        Returns the subsystem ID for a given file path.
        Matches against the github_paths registry.
        """
        if not file_path:
            return 'other'
            
        # Normalize path
        path = file_path.replace('\\', '/')
        
        # Check all subsystems
        best_match = 'other'
        longest_prefix = -1
        
        for sub_id, data in self.subsystems.items():
            for p in data.get('github_paths', []):
                # Ensure p is normalized
                p_norm = p.rstrip('/')
                
                # Exact match or directory prefix match
                if path == p_norm or path.startswith(p_norm + '/'):
                    # Use longest prefix match to be more specific
                    if len(p_norm) > longest_prefix:
                        longest_prefix = len(p_norm)
                        best_match = sub_id
                        
        return best_match

    def get_subsystem_by_bip(self, bip_number):
        """
        Returns the subsystem ID for a given BIP number.
        """
        if bip_number is None:
            return 'other'
            
        # Handle string or int, and clean up (e.g. 'BIP 141' -> '141')
        bip_str = str(bip_number).upper().replace('BIP', '').replace('-', '').replace('#', '').strip()
        # Remove leading zeros
        bip_str = bip_str.lstrip('0') if bip_str != '0' else '0'
        
        return self.bip_to_id.get(bip_str, 'other')

    def score_with_details(self, text, bip_refs=None):
        """
        Returns (primary_id, list_of_all_ids, confidence_score)
        """
        if not text:
            return 'other', ['other'], 0.0
            
        bip_refs = bip_refs or []
        text_lower = text.lower()
        scores = {}
        
        for sub_id, data in self.subsystems.items():
            raw = 0.0
            # Keywords
            for kw in data.get('keywords', []):
                if kw.lower() in text_lower:
                    raw += 1.0
            
            # Patterns
            for pat in self.compiled_patterns.get(sub_id, []):
                if pat.search(text):
                    raw += 2.0

            # BIP hits
            for bip in bip_refs:
                if str(bip) in data.get('bips', []):
                    raw += 3.0
            
            if raw > 0:
                weight = data.get('weight', 50)
                scores[sub_id] = raw * (weight / 100.0)
        
        if not scores:
            return 'other', ['other'], 0.0
            
        sorted_cats = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        primary = sorted_cats[0][0]
        primary_score = sorted_cats[0][1]

        # All categories that scored at least 20% of the primary score
        threshold = primary_score * 0.20
        all_cats = [c for c, s in sorted_cats if s >= threshold]

        # Confidence: how dominant is the primary?
        total = sum(s for _, s in sorted_cats)
        confidence = round(primary_score / total, 3) if total > 0 else 0.0

        return primary, all_cats, confidence

    def get_subsystem_by_text(self, text):
        primary, _, _ = self.score_with_details(text)
        return primary

# Singleton instance for easy access
_RESOLVER = None

def _get_resolver():
    global _RESOLVER
    if _RESOLVER is None:
        _RESOLVER = SubsystemResolver()
    return _RESOLVER

def get_subsystem_by_path(file_path):
    return _get_resolver().get_subsystem_by_path(file_path)

def get_subsystem_by_bip(bip_number):
    return _get_resolver().get_subsystem_by_bip(bip_number)

def get_subsystems():
    return _get_resolver().subsystems

def get_subsystem_by_text(text):
    return _get_resolver().get_subsystem_by_text(text)

def score_with_details(text, bip_refs=None):
    return _get_resolver().score_with_details(text, bip_refs)

if __name__ == "__main__":
    # Quick test
    print(f"Testing path 'src/wallet/wallet.cpp': {get_subsystem_by_path('src/wallet/wallet.cpp')}")
    print(f"Testing BIP 141: {get_subsystem_by_bip(141)}")
    print(f"Testing text 'lightning network channels': {get_subsystem_by_text('lightning network channels')}")
