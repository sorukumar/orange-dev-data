import json
import re

CURATED_FILE = "metadata/identity_curated.json"

def get_slug(text):
    if not text: return "unknown"
    if str(text).startswith("auto_"): return str(text)[5:]
    text = str(text).lower()
    slug = re.sub(r'[^a-z0-9]', '_', text)
    slug = re.sub(r'_+', '_', slug).strip('_')
    return slug

with open(CURATED_FILE, "r") as f:
    curated_data = json.load(f).get("aliases", [])

curated_owner = {}
for entry in curated_data:
    canon = entry.get("canonical_name", "").lower()
    slug = get_slug(canon)
    names = [canon.lower()] + [a.lower() for a in entry.get("aliases", [])]
    emails = [e.lower() for e in entry.get("emails", [])]
    gh = entry.get("github")
    if gh: names.append(gh.lower())
    
    for n in names: curated_owner[n] = slug
    for e in emails: curated_owner[e] = slug

print("Email mzumsande@gmail.com slug:", curated_owner.get("mzumsande@gmail.com"))
print("Login marcofalke slug:", curated_owner.get("marcofalke"))

email_slug = curated_owner.get("mzumsande@gmail.com")
login_slug = curated_owner.get("marcofalke")

if email_slug and login_slug and email_slug != login_slug:
    print("Conflict check fires!")
else:
    print("Conflict check DOES NOT fire!")
