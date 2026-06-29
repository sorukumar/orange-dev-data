import sys
import importlib
sys.path.append('.')
maintainers_mod = importlib.import_module('scripts.04_deliver.maintainers')
SponsorLookup = maintainers_mod.SponsorLookup
MaintainerLookup = maintainers_mod.MaintainerLookup
MaintainerLookup.load()
SponsorLookup.load()

print("Sponsors Map:", SponsorLookup._email_to_sponsor.keys())

for m in MaintainerLookup.get_all_maintainers():
    if m.get('status') == 'active' and m.get('type') != 'ecosystem':
        sponsor = None
        for email in m.get('emails', []):
            sponsor = SponsorLookup.get_sponsor_name(email)
            if sponsor:
                break
        print(f"{m['name']}: {sponsor}")
