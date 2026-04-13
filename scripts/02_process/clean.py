import pandas as pd
import networkx as nx
import json
import os

# --- Configuration ---
ALIASES_LOOKUP_PATH = "data/cache/aliases_lookup.json"

class Consolidator:
    @staticmethod
    def load_aliases_lookup():
        """
        Load external alias lookup file.
        
        NOTE: Future Enhancement - Consider GitHub API to auto-resolve 
        username->name mapping. See: https://api.github.com/users/{username}
        """
        if not os.path.exists(ALIASES_LOOKUP_PATH):
            print(f"Warning: {ALIASES_LOOKUP_PATH} not found. Using empty alias list.")
            return []
        
        with open(ALIASES_LOOKUP_PATH, 'r') as f:
            data = json.load(f)
        
        return data.get('aliases', [])
    
    @staticmethod
    def normalize(commits_df):
        """
        Consolidates contributors based on shared Name or Email.
        Returns the dataframe with 'canonical_id' and 'canonical_name' columns.
        """
        print("Consolidating contributor identities...")
        
        # 1. Build a Graph where nodes are (Type, Value)
        # Type is 'name' or 'email'.
        # Edges represent that a single commit linked them.
        G = nx.Graph()
        
        # IGNORE SHARED EMAILS/NAMES
        # These are used by multiple people (e.g. bots) and cause incorrect merging of identities
        IGNORE_EMAILS = {
            "90386131+bitcoin-core-merge-script@users.noreply.github.com",
            "bitcoin-core-merge-script@users.noreply.github.com"
        }
        
        IGNORE_NAMES = {
            "merge-script",
            "Bitcoin Core Merge Script"
        }
        
        for _, row in commits_df.iterrows():
            name = row['author_name']
            email = row['author_email']
            
            name_node = f"NAME:{name}"
            email_node = f"EMAIL:{email}"

            if email in IGNORE_EMAILS or name in IGNORE_NAMES:
                # Do not link this email to the name
                if name not in IGNORE_NAMES:
                     G.add_node(name_node, type='name', value=name)
                
                if email not in IGNORE_EMAILS:
                     G.add_node(email_node, type='email', value=email)
                     
                continue

            # Add nodes
            G.add_node(name_node, type='name', value=name)
            G.add_node(email_node, type='email', value=email)
            
            # Link them
            G.add_edge(name_node, email_node)

        # 2. Load External Alias Lookup and inject edges
        aliases_data = Consolidator.load_aliases_lookup()
        print(f"Loaded {len(aliases_data)} alias groups from {ALIASES_LOOKUP_PATH}")
        
        alias_edges_added = 0
        for entry in aliases_data:
            canonical_name = entry.get('canonical_name', '')
            aliases = entry.get('aliases', [])
            emails = entry.get('emails', [])
            
            # Collect all name nodes for this identity
            all_names = [canonical_name] + aliases
            name_nodes = [f"NAME:{n}" for n in all_names if n]
            
            # Collect all email nodes for this identity
            email_nodes = [f"EMAIL:{e.lower()}" for e in emails if e]
            
            # Ensure all nodes exist in graph (some may not have commits)
            for node in name_nodes:
                if not G.has_node(node):
                    G.add_node(node, type='name', value=node.split(":", 1)[1])
            
            for node in email_nodes:
                if not G.has_node(node):
                    G.add_node(node, type='email', value=node.split(":", 1)[1])
            
            # Link all names to each other
            for i, n1 in enumerate(name_nodes):
                for n2 in name_nodes[i+1:]:
                    if G.has_node(n1) and G.has_node(n2):
                        G.add_edge(n1, n2)
                        alias_edges_added += 1
            
            # Link all emails to the canonical name
            canonical_node = f"NAME:{canonical_name}"
            if G.has_node(canonical_node):
                for email_node in email_nodes:
                    if G.has_node(email_node):
                        G.add_edge(canonical_node, email_node)
                        alias_edges_added += 1
        
        print(f"Injected {alias_edges_added} alias edges from lookup file.")
            
        # 3. Find Connected Components (distinct identities)
        # Each component is a unique person
        mapping = {} # node -> canonical_id
        canonical_names = {} # canonical_id -> display_name (most common name or first found)
        
        # Build a set of canonical names from lookup for priority selection
        lookup_canonical_names = {entry['canonical_name'] for entry in aliases_data if entry.get('canonical_name')}
        
        for idx, component in enumerate(nx.connected_components(G)):
            group_id = idx
            names_in_group = []
            
            for node in component:
                mapping[node] = group_id
                if node.startswith("NAME:"):
                    names_in_group.append(node.split(":", 1)[1])
            
            # Pick a canonical name for the group
            # Priority: 1) Name from lookup file, 2) Longest name
            if names_in_group:
                # Check if any name is in the lookup canonical names
                lookup_matches = [n for n in names_in_group if n in lookup_canonical_names]
                if lookup_matches:
                    canonical_names[group_id] = lookup_matches[0]
                else:
                    canonical_names[group_id] = max(names_in_group, key=len)
            else:
                canonical_names[group_id] = "Unknown"

        # 4. Map back to DataFrame
        email_to_id = {k.split(":", 1)[1]: v for k, v in mapping.items() if k.startswith("EMAIL:")}
        name_to_id = {k.split(":", 1)[1]: v for k, v in mapping.items() if k.startswith("NAME:")}
        
        commits_df['canonical_id'] = commits_df['author_email'].map(email_to_id)
        
        # Fill missing with name map
        mask = commits_df['canonical_id'].isna()
        commits_df.loc[mask, 'canonical_id'] = commits_df.loc[mask, 'author_name'].map(name_to_id)
        
        # Map ID to Name
        commits_df['canonical_name'] = commits_df['canonical_id'].map(canonical_names)
        
        print(f"Consolidated into {commits_df['canonical_id'].nunique()} unique identities.")
        return commits_df
