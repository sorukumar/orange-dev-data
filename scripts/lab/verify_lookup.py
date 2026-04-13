import sys
import os

# Add the project root to sys.path to import our modules
sys.path.append(os.getcwd() + '/code/core')

import process

process.MaintainerLookup.load()
maintainers = process.MaintainerLookup.get_all_maintainers()

print(f"Total maintainers: {len(maintainers)}")
for m in maintainers:
    if m['id'] == 'theuni':
        print(f"Cory Fields (theuni) merge_authority: {m.get('merge_authority')}")
        print(f"Cory Fields (theuni) evidence: {m.get('evidence')}")
        break
