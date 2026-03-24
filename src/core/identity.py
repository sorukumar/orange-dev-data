import pandas as pd
import networkx as nx
import json
import os
import sys

# Ensure root directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.core.paths import ID_PATH

class Consolidator:
    @staticmethod
    def load_aliases_lookup():
        """Loads the centralized identity mappings from metadata/."""
        if not os.path.exists(ID_PATH):
            print(f"Warning: {ID_PATH} not found. Using empty alias list.")
            return []
        
        with open(ID_PATH, 'r') as f:
            data = json.load(f)
        
        return data.get('aliases', [])
    
    @staticmethod
    def normalize(commits_df):
        """
        Consolidates contributors based on shared Name or Email.
        Returns the dataframe with 'canonical_id' and 'canonical_name' columns.
        """
        print("Consolidating contributor identities (Graph-based)...")
        
        G = nx.Graph()
        
        # IGNORE SHARED EMAILS/NAMES (Bots, Scripts)
        IGNORE_EMAILS = {
            "90386131+bitcoin-core-merge-script@users.noreply.github.com",
            "bitcoin-core-merge-script@users.noreply.github.com",
            "noreply@github.com"
        }
        
        IGNORE_NAMES = {
            "merge-script",
            "Bitcoin Core Merge Script",
            "GitHub"
        }
        
        for _, row in commits_df.iterrows():
            name = str(row['author_name'])
            email = str(row['author_email']).lower()
            
            name_node = f"NAME:{name}"
            email_node = f"EMAIL:{email}"

            if email in IGNORE_EMAILS or name in IGNORE_NAMES:
                if name not in IGNORE_NAMES:
                     G.add_node(name_node, type='name', value=name)
                if email not in IGNORE_EMAILS:
                     G.add_node(email_node, type='email', value=email)
                continue

            # Add nodes and link them
            G.add_node(name_node, type='name', value=name)
            G.add_node(email_node, type='email', value=email)
            G.add_edge(name_node, email_node)

        # Inject Known Aliases from Metadata
        aliases_data = Consolidator.load_aliases_lookup()
        print(f"Injecting {len(aliases_data)} curated alias groups...")
        
        lookup_canonical_names = set()
        for entry in aliases_data:
            canonical_name = entry.get('canonical_name', '')
            if not canonical_name: continue
            
            lookup_canonical_names.add(canonical_name)
            aliases = entry.get('aliases', [])
            emails = entry.get('emails', [])
            
            canonical_node = f"NAME:{canonical_name}"
            G.add_node(canonical_node, type='name', value=canonical_name)
            
            # Link all aliases to the canonical name
            for alias in aliases:
                alias_node = f"NAME:{alias}"
                G.add_node(alias_node, type='name', value=alias)
                G.add_edge(canonical_node, alias_node)
            
            # Link all emails to the canonical name
            for email in emails:
                email_node = f"EMAIL:{email.lower()}"
                G.add_node(email_node, type='email', value=email.lower())
                G.add_edge(canonical_node, email_node)
            
        # Find Connected Components (distinct identities)
        mapping = {} 
        canonical_names = {} 
        
        for idx, component in enumerate(nx.connected_components(G)):
            group_id = idx
            names_in_group = []
            
            for node in component:
                mapping[node] = group_id
                if node.startswith("NAME:"):
                    names_in_group.append(node.split(":", 1)[1])
            
            # Selection Priority: 1. Curated Canonical Name, 2. Longest Name
            if names_in_group:
                lookup_matches = [n for n in names_in_group if n in lookup_canonical_names]
                if lookup_matches:
                    canonical_names[group_id] = lookup_matches[0]
                else:
                    canonical_names[group_id] = max(names_in_group, key=len)
            else:
                canonical_names[group_id] = "Unknown"

        # Apply mapping back to DataFrame
        email_to_id = {k.split(":", 1)[1]: v for k, v in mapping.items() if k.startswith("EMAIL:")}
        name_to_id = {k.split(":", 1)[1]: v for k, v in mapping.items() if k.startswith("NAME:")}
        
        commits_df['canonical_id'] = commits_df['author_email'].str.lower().map(email_to_id)
        mask = commits_df['canonical_id'].isna()
        commits_df.loc[mask, 'canonical_id'] = commits_df.loc[mask, 'author_name'].map(name_to_id)
        
        commits_df['canonical_name'] = commits_df['canonical_id'].map(canonical_names)
        
        print(f"Identity consolidation complete: {commits_df['canonical_id'].nunique()} unique humans identified.")
        return commits_df
