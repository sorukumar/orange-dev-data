import json
import re

def slugify(text):
    if not text:
        return "unknown"
    text = text.lower()
    text = re.sub(r'[^a-z0-9]', '_', text)
    return re.sub(r'_+', '_', text).strip('_')

def main():
    # Load existing data
    with open("metadata/identities.json", "r") as f:
        id_data = json.load(f).get("aliases", [])
        
    with open("metadata/contributors.json", "r") as f:
        contrib_data = json.load(f).get("contributors", [])

    new_identities = []
    uuid_map = {}

    for record in id_data:
        display_name = record.get("canonical_name", "Unknown")
        uuid = slugify(display_name)
        if not uuid:
            uuid = "unknown"
        
        git_names = [display_name] + record.get("aliases", [])
        emails = record.get("emails", [])
        
        platforms = {}
        if record.get("github"): platforms["github"] = record.get("github")
        if record.get("delving"): platforms["delving"] = record.get("delving")
        platforms["mailing_list"] = []
        
        new_record = {
            "uuid": uuid,
            "display_name": display_name,
            "git_signatures": {
                "names": list(set(git_names)),
                "emails": list(set(emails))
            },
            "platforms": platforms
        }
        
        new_identities.append(new_record)
        
        for gn in git_names: uuid_map[gn.lower()] = new_record
        for em in emails: uuid_map[em.lower()] = new_record
        if platforms.get("github"): uuid_map[platforms["github"].lower()] = new_record
        if platforms.get("delving"): uuid_map[platforms["delving"].lower()] = new_record

    print(f"Ingested {len(new_identities)} from identities.json")

    new_contrib_count = 0
    merged_contrib_count = 0

    for c in contrib_data:
        keys_to_check = [
            c.get("id"),
            c.get("display_name"),
            c.get("github", {}).get("login") if c.get("github") else None
        ] + c.get("identities", {}).get("aliases", []) + c.get("identities", {}).get("emails", [])
        
        keys_to_check = [k.lower() for k in keys_to_check if k]
        
        matched_record = None
        for k in keys_to_check:
            if k in uuid_map:
                matched_record = uuid_map[k]
                break
                
        if matched_record:
            merged_contrib_count += 1
            names = matched_record["git_signatures"]["names"]
            for a in c.get("identities", {}).get("aliases", []):
                if a not in names: names.append(a)
            
            if "via Bitcoin Development Mailing List" in str(c.get("id", "")):
                if c.get("id") not in matched_record["platforms"]["mailing_list"]:
                    matched_record["platforms"]["mailing_list"].append(c.get("id"))
                    
            if c.get("github", {}).get("login") and "github" not in matched_record["platforms"]:
                matched_record["platforms"]["github"] = c.get("github").get("login")
                
        else:
            new_contrib_count += 1
            display_name = c.get("display_name") or c.get("id")
            uuid = slugify(display_name)
            if not uuid: uuid = "unknown_contrib"
            
            original_uuid = uuid
            counter = 1
            while any(existing["uuid"] == uuid for existing in new_identities):
                uuid = f"{original_uuid}_{counter}"
                counter += 1
            
            aliases = c.get("identities", {}).get("aliases", [])
            c_emails = c.get("identities", {}).get("emails", [])
            
            git_names = list(set([display_name] + aliases)) if isinstance(aliases, list) else [display_name]
            if c.get("id") and c.get("id") not in git_names: git_names.append(c.get("id"))
            
            platforms = {"mailing_list": []}
            if c.get("github", {}).get("login"):
                platforms["github"] = c.get("github").get("login")
                
            if "via Bitcoin Development Mailing List" in str(c.get("id", "")):
                platforms["mailing_list"].append(c.get("id"))
                
            new_record = {
                "uuid": uuid,
                "display_name": display_name,
                "git_signatures": {
                    "names": git_names,
                    "emails": c_emails
                },
                "platforms": platforms
            }
            new_identities.append(new_record)
            
            for gn in git_names: uuid_map[gn.lower()] = new_record
            for em in c_emails: uuid_map[em.lower()] = new_record
            if platforms.get("github"): uuid_map[platforms["github"].lower()] = new_record

    print(f"Merged {merged_contrib_count} existing identities with new data.")
    print(f"Created {new_contrib_count} new identities from contributors.json.")
    print(f"Total entries in new identities: {len(new_identities)}")
    
    with open("metadata/identities_v2.json", "w") as f:
        json.dump({"_meta": {"description": "UUID-based Master Identity Resolver"}, "identities": new_identities}, f, indent=2)
    print("Wrote output to metadata/identities_v2.json")

if __name__ == "__main__":
    main()
