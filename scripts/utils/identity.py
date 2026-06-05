import json
import os
import re
from pathlib import Path

try:
    from src.core.paths import METADATA_DIR
except ImportError:
    METADATA_DIR = Path("metadata")

_NOREPLY_EMAIL_RE = re.compile(r'^(?:\d+)\+([^@]+)@users\.noreply\.github\.com$', re.I)

IDENTITIES_FILE = METADATA_DIR / "identities.json"

class IdentityResolver:
    def __init__(self):
        self._identities = []
        self._uuid_map = {}
        self._load()

    def _load(self):
        """Loads the explicitly compiled identities database into a lookup map."""
        if not IDENTITIES_FILE.exists():
            print("Warning: identities.json not found! Identity tracking will be fully degraded.")
            return
            
        with open(IDENTITIES_FILE, "r") as f:
            data = json.load(f)
            self._identities = data.get("identities", [])
            
        self._rebuild_index()

    def _rebuild_index(self):
        """Rebuilds the quick-lookup hash map strictly from the master file."""
        self._uuid_map = {}
        for record in self._identities:
            uuid = record["uuid"]
            
            # Map platforms
            platforms = record.get("platforms", {})
            if "github" in platforms and platforms["github"]:
                gh = platforms["github"]
                gh_logins = gh if isinstance(gh, list) else [gh]
                for login in gh_logins:
                    self._uuid_map[f"github:{login.lower()}"] = uuid
            if "delving" in platforms and platforms["delving"]:
                self._uuid_map[f"delving:{platforms['delving'].lower()}"] = uuid
            for ml in platforms.get("mailing_list", []):
                self._uuid_map[f"ml:{ml.lower()}"] = uuid
                
            # Map git signatures
            sigs = record.get("git_signatures", {})
            for name in sigs.get("names", []):
                self._uuid_map[f"name:{name.lower()}"] = uuid
            for email in sigs.get("emails", []):
                self._uuid_map[f"email:{email.lower()}"] = uuid

    def _slugify(self, text):
        if not text:
            return "unknown"
        if str(text).startswith("auto_"):
            return str(text)[5:]
        
        # 1. Basic Latin simplification
        text = text.lower()
        slug = re.sub(r'[^a-z0-9]', '_', text)
        slug = re.sub(r'_+', '_', slug).strip('_')
        
        # 2. Entropy Fallback (for non-Latin names)
        if not slug or len(slug) < 3:
            import hashlib
            h = hashlib.md5(text.encode('utf-8')).hexdigest()[:6]
            slug = f"id_{h}"
            
        return slug

    def _extract_noreply_login(self, email):
        if not email:
            return None
        email = email.strip().lower()
        m = _NOREPLY_EMAIL_RE.match(email)
        if m:
            return m.group(1)
        parts = email.split('@', 1)
        if len(parts) == 2 and parts[1] == 'users.noreply.github.com':
            return parts[0]
        return None

    def _mint_stateless_uuid(self, identifier):
        """Returns a generic UUID for unrecognized developers without mutating the DB."""
        slug = self._slugify(identifier)
        return f"auto_{slug}"

    def resolve_github(self, login):
        if not login: return None
        lookup = f"github:{login.lower()}"
        if lookup in self._uuid_map:
            return self._uuid_map[lookup]
        return self._mint_stateless_uuid(login)

    def resolve_delving(self, username):
        if not username: return None
        lookup = f"delving:{username.lower()}"
        if lookup in self._uuid_map:
            return self._uuid_map[lookup]
        return self._mint_stateless_uuid(username)

    def resolve_mailing_list(self, handle):
        if not handle: return None
        lookup = f"ml:{handle.lower()}"
        if lookup in self._uuid_map:
            return self._uuid_map[lookup]
        
        email_lookup = f"email:{handle.lower()}"
        if email_lookup in self._uuid_map:
            return self._uuid_map[email_lookup]
            
        if "via Bitcoin" in handle:
            name_part = handle.split("via")[0].strip().strip("'\"")
            name_lookup = f"name:{name_part.lower()}"
            if name_lookup in self._uuid_map:
                return self._uuid_map[name_lookup]
            return self._mint_stateless_uuid(name_part)
            
        return self._mint_stateless_uuid(handle)
        
    def resolve_git(self, name, email=None):
        if email:
            lookup = f"email:{email.lower()}"
            if lookup in self._uuid_map:
                return self._uuid_map[lookup]
            noreply_login = self._extract_noreply_login(email)
            if noreply_login:
                github_lookup = f"github:{noreply_login.lower()}"
                if github_lookup in self._uuid_map:
                    return self._uuid_map[github_lookup]
                name_lookup = f"name:{noreply_login.lower()}"
                if name_lookup in self._uuid_map:
                    return self._uuid_map[name_lookup]

        if name:
            clean_name = name
            if " via " in clean_name:
                clean_name = clean_name.split(" via ")[0]
            clean_name = re.sub(r'\[.*?\]', '', clean_name)
            clean_name = clean_name.strip("'\" \t")

            lookup = f"name:{clean_name.lower()}"
            if lookup in self._uuid_map:
                return self._uuid_map[lookup]

            if "_" in clean_name and any(domain in clean_name.replace("_", ".") for domain in ["gmail.com", "pm.me", "protonmail.com", "github.com"]):
                potential_email = clean_name.replace("_gmail_com", "@gmail.com").replace("_pm_me", "@pm.me").replace("_at_", "@").replace("_dot_", ".")
                if "@" in potential_email:
                    email_lookup = f"email:{potential_email.lower()}"
                    if email_lookup in self._uuid_map:
                        return self._uuid_map[email_lookup]

        return self._mint_stateless_uuid(email if email else name)

resolver = IdentityResolver()
