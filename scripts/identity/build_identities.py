import os
import re
import json
import pandas as pd
import networkx as nx
from collections import defaultdict
from datetime import datetime
from email.header import decode_header as _mime_decode_header

CURATED_FILE = "metadata/identity_curated.json"
OUTPUT_FILE = "metadata/identities.json"

_MIME_RE = re.compile(r'=\?[^?]+\?[BbQq]\?[^?]+\?=', re.ASCII)

def decode_mime_name(text: str) -> str:
    """Decode a MIME encoded-word string (RFC 2047) to plain unicode.

    Handles both fully-encoded names like '=?UTF-8?B?...?=' and mixed strings
    like 'Jan =?UTF-8?B?...?='. Returns the original string unchanged if it
    contains no encoded-word tokens.
    """
    if not text or not _MIME_RE.search(text):
        return text
    try:
        parts = _mime_decode_header(text)
        decoded_parts = []
        for raw, charset in parts:
            if isinstance(raw, bytes):
                decoded_parts.append(raw.decode(charset or 'utf-8', errors='replace'))
            else:
                decoded_parts.append(raw)
        result = ' '.join(p.strip() for p in decoded_parts if p.strip())
        return result or text
    except Exception:
        return text

def get_slug(text):
    import re
    if not text: return "unknown"
    if str(text).startswith("auto_"): return str(text)[5:]
    text = str(text).lower()
    slug = re.sub(r'[^a-z0-9]', '_', text)
    slug = re.sub(r'_+', '_', slug).strip('_')
    if not slug or len(slug) < 3:
        import hashlib
        slug = "id_" + hashlib.md5(text.encode('utf-8')).hexdigest()[:6]
    return slug

def build_identities():
    print(f"Building exhaustive universal identity database...")
    
    with open(CURATED_FILE, "r") as f:
        curated_full = json.load(f)
        curated_data = curated_full.get("aliases", [])
        special_nodes = curated_full.get("special_nodes", {})

    # Load explicit (github_id, email) pairs that must never be linked in the graph,
    # even if github_id_map.json was generated before the block was added or before
    # the email was added to the curated table.  Mirrors the same protection in
    # build_github_id_map.py so that a stale map file cannot reintroduce a bridge.
    _gh_id_map_blocked: set[tuple[str, str]] = set()
    for _ba in curated_full.get("blocked_anchors", []):
        _gid   = str(_ba.get("github_id", "")).strip()
        _email = str(_ba.get("email", "")).strip().lower()
        if _gid and _email:
            _gh_id_map_blocked.add((_gid, _email))
    
    G = nx.Graph()
    node_sources = defaultdict(set)
    # 0. Build Curated Map for Safety Check
    # This prevents maintainers who open PRs on behalf of others from merging identities.
    curated_owner = {} # node_string -> canonical_name_slug
    for entry in curated_data:
        canon = entry.get("canonical_name", "").lower()
        if not canon: continue
        canon_slug = get_slug(canon)
        names = [canon] + [a.lower() for a in entry.get("aliases", [])]
        emails = [e.lower() for e in entry.get("emails", [])]
        gh = entry.get("github")
        if gh:
            gh_list = gh if isinstance(gh, list) else [gh]
            for g in gh_list: names.append(g.lower())
        
        for n in names: curated_owner[n] = canon_slug
        for e in emails: curated_owner[e] = canon_slug

    # Load dynamic maintainers
    try:
        with open("metadata/maintainers.json", "r") as f:
            maintainers_data = json.load(f).get("maintainers", [])
    except:
        maintainers_data = []

    human_proxies = set(special_nodes.get("proxies", []))
    for m in maintainers_data:
        if m.get("github"): human_proxies.add(str(m.get("github")).lower())
        for alias in m.get("aliases", []): human_proxies.add(str(alias).lower())

    PROXIES = set([p.lower() for p in human_proxies])
    BOTS = set([b.lower() for b in special_nodes.get("bots", [])])
    
    IGNORE_EMAILS = {"", "none", "(none)", "ariel@ficticio.com"}
    IGNORE_NAMES = {"unknown", "none", "anonymous", "=", "ariel", "blitzboom", "m0ray", "mewantsbitcoins", "carlos pizarro"}
    UNSAFE_ALIASES = {"devrandom", "dev random", "dan helfman", "miron", "danube"}

    # Map of canonical slug -> set of all its names/emails
    canonical_assets = defaultdict(set)
    # Map of all strings -> canonical slug
    curated_owner = {} 
    
    for entry in curated_data:
        canon = entry.get("canonical_name", "").lower()
        if not canon: continue
        slug = get_slug(canon)
        names = [canon.lower()] + [a.lower() for a in entry.get("aliases", [])]
        emails = [e.lower() for e in entry.get("emails", [])]
        gh = entry.get("github")
        if gh:
            gh_list = gh if isinstance(gh, list) else [gh]
            for g in gh_list: names.append(g.lower())
        
        for n in names: 
            curated_owner[n] = slug
            canonical_assets[slug].add(n)
        for e in emails: 
            curated_owner[e] = slug
            canonical_assets[slug].add(e)

    def clean_val(val):
        if not val: return ""
        v = str(val).strip().lower()
        if v in IGNORE_NAMES or v in IGNORE_EMAILS: return ""
        if len(v) < 2: return ""
        return v

    def add_node_and_edge(G, u, v, src):
        u_val = u.split(":", 1)[1].lower()
        v_val = v.split(":", 1)[1].lower()
        if u_val in IGNORE_NAMES or v_val in IGNORE_NAMES: return
        if u_val in IGNORE_EMAILS or v_val in IGNORE_EMAILS: return
        
        # Categorically drop edges involving bots
        if u_val in BOTS or v_val in BOTS: return
        
        # Identity Protection: Don't link a Proxy (Maintainer) to an Unsafe Alias (colliding handle)
        if (u_val in PROXIES and v_val in UNSAFE_ALIASES) or (v_val in PROXIES and u_val in UNSAFE_ALIASES):
            # We still add the nodes so they exist, but we skip the EDGE that bridges them
            G.add_node(u); G.add_node(v)
            return

        G.add_node(u); G.add_node(v)
        node_sources[u].add(src); node_sources[v].add(src)
        G.add_edge(u, v)

    # --- Ingest Git Commits ---
    if os.path.exists("data/raw/core_commits.parquet"):
        print("Indexing Git Commits...")
        df_commits = pd.read_parquet("data/raw/core_commits.parquet")
        for _, row in df_commits.iterrows():
            n, e = str(row['author_name']).strip(), str(row['author_email']).strip().lower()
            if not clean_val(n) or not clean_val(e): continue
            add_node_and_edge(G, f"NAME:{n}", f"EMAIL:{e}", "corecommit")

    # --- Load pre-computed github_id → email map (built by build_github_id_map.py) ---
    gh_id_map: dict[str, list[str]] = {}  # github_id → [real_email, ...]
    _map_path = "metadata/github_id_map.json"
    if os.path.exists(_map_path):
        print("Loading github_id_map...")
        with open(_map_path) as _f:
            _map_data = json.load(_f)
        _blocked_skipped = 0
        for _entry in _map_data.get("entries", []):
            _gid = _entry["github_id"]
            _emails = [
                em["email"] for em in _entry.get("emails", [])
                if em.get("email_type") == "real"
                # Honours blocked_anchors from identity_curated.json: skip any
                # (github_id, email) pair that is a known false positive even if
                # it passed the corroboration threshold in a previous map rebuild.
                and (_gid, em["email"].strip().lower()) not in _gh_id_map_blocked
            ]
            _blocked_skipped += len(_entry.get("emails", [])) - len(_emails)
            if _emails:
                gh_id_map[_gid] = _emails
        if _blocked_skipped:
            print(f"  {_blocked_skipped} gh_id_map email(s) suppressed by blocked_anchors")
        print(f"  {len(gh_id_map):,} github_ids with real email anchors")
    else:
        print("  [warn] github_id_map.json not found — GitHub email anchoring skipped")

    # helper to process PR metadata
    def process_pr_metadata(file_path, src_tag):
        if os.path.exists(file_path):
            print(f"Parsing PR Metadata ({src_tag})...")
            df = pd.read_parquet(file_path)
            for _, row in df.iterrows():
                login = str(row.get('author', '')).strip()
                gh_id = str(row.get('github_id', '')).strip()

                if not clean_val(login): continue
                l_node = f"NAME:{login}"; G.add_node(l_node); node_sources[l_node].add(src_tag)
                if gh_id:
                    id_node = f"GH_ID:{gh_id}"; G.add_node(id_node); node_sources[id_node].add(src_tag); G.add_edge(id_node, l_node)
                    for email in gh_id_map.get(gh_id, []):
                        add_node_and_edge(G, id_node, f"EMAIL:{email}", src_tag)

    # 2. GitHub PR Data (Core & BIPs)
    process_pr_metadata("data/raw/github_pr_metadata.parquet", "prgithub")
    process_pr_metadata("data/raw/bips_pr_metadata.parquet", "bipgithub")

    # 2b. GitHub Review Events (Core & BIPs) — adds login↔GH_ID edges for reviewers
    # who may never have authored a PR and would otherwise have no GH_ID anchor.
    def process_review_events(file_path, src_tag):
        if not os.path.exists(file_path):
            return
        print(f"Parsing Review Events ({src_tag})...")
        df = pd.read_parquet(file_path)
        for _, row in df.iterrows():
            login = str(row.get('user', '')).strip()
            gh_id = str(row.get('github_id', '')).strip()
            if not clean_val(login):
                continue
            l_node = f"NAME:{login}"
            G.add_node(l_node)
            node_sources[l_node].add(src_tag)
            if gh_id and gh_id not in ('', 'None', 'nan'):
                id_node = f"GH_ID:{gh_id}"
                G.add_node(id_node)
                node_sources[id_node].add(src_tag)
                G.add_edge(id_node, l_node)

    process_review_events("data/raw/github_review_events.parquet", "prgithub")
    process_review_events("data/raw/bips_review_events.parquet", "bipgithub")

    # 3. Delving Data
    if os.path.exists("data/raw/social_delving.parquet"):
        print("Parsing Delving...")
        df = pd.read_parquet("data/raw/social_delving.parquet")
        for _, row in df.iterrows():
            n = str(row['author_name']).strip()
            # Use author_username (actual Delving handle) as the DLV_ID, not the pre-resolved
            # canonical_id. canonical_id stores "auto_username" which would make the resolver
            # index the key "delving:auto_username" instead of "delving:username".
            username = str(row.get('author_username', '') or '').strip()
            if not username:
                # Fallback for old parquet without author_username column
                can_id = str(row.get('canonical_id', '')).strip()
                username = can_id[5:] if can_id.startswith('auto_') else can_id
            if not clean_val(n) and not clean_val(username):
                continue
            if clean_val(n):
                n_node = f"NAME:{n}"
                G.add_node(n_node)
                node_sources[n_node].add("delving")
            if clean_val(username):
                dlv_node = f"DLV_ID:{username.lower()}"
                G.add_node(dlv_node)
                node_sources[dlv_node].add("delving")
                if clean_val(n):
                    G.add_edge(n_node, dlv_node)

    # 4. Mailing List
    ml_file = "data/raw/social_mailing_list.parquet"
    if os.path.exists(ml_file):
        print("Parsing Mailing List...")
        try:
            df = pd.read_parquet(ml_file)
            for _, row in df.iterrows():
                n = str(row.get('author_name', '')).strip()
                e = str(row.get('author_email', '')).strip().lower()
                clean_n = clean_val(n)
                clean_e = clean_val(e)
                if not clean_n and not clean_e: continue
                n_node = f"NAME:{n}"
                e_node = f"EMAIL:{e}"
                if clean_n and clean_e:
                    add_node_and_edge(G, n_node, e_node, "mailinglist")
                elif clean_n:
                    G.add_node(n_node)
                    node_sources[n_node].add("mailinglist")
                elif clean_e:
                    G.add_node(e_node)
                    node_sources[e_node].add("mailinglist")
        except: pass

    # 5. BIP File Authors
    if os.path.exists("data/raw/bips.parquet"):
        print("Parsing BIP Author headers...")
        df = pd.read_parquet("data/raw/bips.parquet")
        for _, row in df.iterrows():
            authors = row.get('author_names', [])
            for n in authors:
                n = str(n).strip()
                if clean_val(n):
                    n_node = f"NAME:{n}"
                    G.add_node(n_node)
                    node_sources[n_node].add("bips")

    # 5b. GitHub Profiles — public email + display-name bridge
    # Two signals that are orthogonal to github_id_map (SHA-based):
    #   A) public profile email  → GH_ID ↔ EMAIL edge (links GitHub account to mailing-list email)
    #   B) display name ≠ login  → NAME:{login} ↔ NAME:{Real Name} edge
    #      This is the KEY bridge for Delving/mailing-list: many users post with real names,
    #      not their GitHub login. Example: DLV_ID:darosior posted as "Antoine Poinsot";
    #      profile name="Antoine Poinsot" for login "darosior" collapses them into one identity.
    _NOREPLY_PAT = re.compile(r"@users\.noreply\.github\.com$", re.I)
    _profiles_path = "metadata/github_profiles.json"
    _profiles_all: dict = {}
    if os.path.exists(_profiles_path):
        print("Loading GitHub profiles (public email + display-name bridge)...")
        with open(_profiles_path) as _pf:
            _profiles_all = json.load(_pf).get("profiles", {})
        _email_edges = 0
        _name_edges = 0
        for _gid, _prof in _profiles_all.items():
            _login = str(_prof.get("login") or "").strip()
            _disp_name = str(_prof.get("name") or "").strip()
            _pub_email = str(_prof.get("email") or "").strip().lower()
            if not _login:
                continue
            _l_node = f"NAME:{_login}"
            # Edge A: GH_ID ↔ public profile email
            if _pub_email and clean_val(_pub_email) and not _NOREPLY_PAT.search(_pub_email):
                _id_node = f"GH_ID:{_gid}"
                G.add_node(_id_node); node_sources[_id_node].add("ghprofile")
                G.add_node(_l_node);  node_sources[_l_node].add("ghprofile")
                G.add_edge(_id_node, _l_node)
                add_node_and_edge(G, _id_node, f"EMAIL:{_pub_email}", "ghprofile")
                _email_edges += 1
            # Edge B: login NAME: ↔ display-name NAME: (only real names — must contain a space)
            if (_disp_name and clean_val(_disp_name)
                    and _disp_name.lower() != _login.lower()
                    and " " in _disp_name and len(_disp_name) >= 5
                    and _disp_name.lower() not in UNSAFE_ALIASES
                    and _disp_name.lower() not in BOTS):
                add_node_and_edge(G, _l_node, f"NAME:{_disp_name}", "ghprofile")
                _name_edges += 1
        print(f"  {_email_edges:,} public profile email edges")
        print(f"  {_name_edges:,} display-name bridge edges (login → real name)")

    # 5c. Single-PR candidate emails
    # Contributors with exactly one PR were dropped by github_id_map's CORROBORATION_MIN=2.
    # For those with no profile email (Step 5b didn't add an edge), the candidate_email
    # from their single commit is still our best signal.  We only add it when:
    #   • The github_id is NOT already in gh_id_map (avoid duplicate/conflicting edges)
    #   • The profile has no public email (if it did, Step 5b already covered it)
    #   • The candidate_email looks like a real address (not a noreply)
    _spc_path = "metadata/single_pr_contributors.json"
    if os.path.exists(_spc_path):
        print("Loading single-PR candidate emails...")
        with open(_spc_path) as _sf:
            _spc_entries = json.load(_sf).get("entries", [])
        _confirmed = 0
        _candidate_only = 0
        for _entry in _spc_entries:
            _gid = str(_entry.get("github_id") or "").strip()
            _cand_email = str(_entry.get("candidate_email") or "").strip().lower()
            if not _gid or not _cand_email or not clean_val(_cand_email):
                continue
            if _NOREPLY_PAT.search(_cand_email):
                continue
            # Skip if gh_id_map already provides real email anchors for this ID
            if _gid in gh_id_map:
                continue
            _id_node = f"GH_ID:{_gid}"
            G.add_node(_id_node); node_sources[_id_node].add("singlepr")
            # Cross-validate: does the GitHub profile publicly list the same email?
            _prof_email = str(_profiles_all.get(_gid, {}).get("email") or "").strip().lower()
            if _prof_email and _prof_email == _cand_email:
                # Both commit and public profile agree → high confidence
                add_node_and_edge(G, _id_node, f"EMAIL:{_cand_email}", "singlepr")
                _confirmed += 1
            elif _prof_email and _prof_email != _cand_email:
                # Profile email already added in Step 5b; skip conflicting candidate
                pass
            else:
                # No profile email to cross-validate → add candidate as-is (best available signal)
                add_node_and_edge(G, _id_node, f"EMAIL:{_cand_email}", "singlepr")
                _candidate_only += 1
        print(f"  {_confirmed:,} confirmed (candidate == profile email)")
        print(f"  {_candidate_only:,} candidate-only (no profile email to cross-validate)")

    # 6. Inject Curated Edges
    print("Injecting explicit curated edges...")
    for entry in curated_data:
        canon = entry.get("canonical_name")
        if not canon: continue
        c_node = f"NAME:{canon}"
        G.add_node(c_node)
        node_sources[c_node].add("curation")
        
        for alias in entry.get("aliases", []):
            if not clean_val(alias): continue
            a_node = f"NAME:{alias}"
            G.add_node(a_node)
            node_sources[a_node].add("curation")
            G.add_edge(c_node, a_node)
            
        for email in entry.get("emails", []):
            if not clean_val(email): continue
            e_node = f"EMAIL:{email.lower()}"
            G.add_node(e_node)
            node_sources[e_node].add("curation")
            G.add_edge(c_node, e_node)
            
        if entry.get("github"):
            gh_node = f"NAME:{entry['github']}"
            G.add_node(gh_node)
            node_sources[gh_node].add("curation")
            G.add_edge(c_node, gh_node)

    # 7. Connected Components = Unique Identities
    print("Computing Universal Map...")
    identities = []
    for idx, component in enumerate(nx.connected_components(G)):
        names = []
        emails = []
        gh_ids = []
        dlv_ids = []
        sources = set()
        
        for node in component:
            sources.update(node_sources[node])
            val = node.split(":", 1)[1]
            if node.startswith("NAME:"): names.append(val)
            elif node.startswith("EMAIL:"): emails.append(val)
            elif node.startswith("GH_ID:"): gh_ids.append(val)
            elif node.startswith("DLV_ID:"): dlv_ids.append(val)

        # Decode any MIME encoded-word names (RFC 2047) — e.g. =?UTF-8?B?...?=
        # stored raw from git log. Decoded names are used for display and UUID;
        # original raw values are preserved in git_signatures for graph membership.
        decoded_names = [decode_mime_name(n) for n in names]

        # Determine canonical name & UUID
        canonical_name = "Unknown"
        curated_names = [e["canonical_name"] for e in curated_data if e.get("canonical_name")]
        matches = [n for n in decoded_names if n in curated_names]

        if matches:
            canonical_name = matches[0]
            base_uuid = f"can_{get_slug(canonical_name)}"
        elif decoded_names:
            # Prefer names that don't look like logins (i.e. contain a space or
            # have non-ASCII chars typical of real names), then fall back to longest.
            real_names = [n for n in decoded_names if ' ' in n or not n.isascii()]
            pool = real_names if real_names else decoded_names
            canonical_name = max(pool, key=len)
            base_uuid = f"auto_{get_slug(canonical_name)}"
        elif emails:
            canonical_name = emails[0].split('@')[0]
            base_uuid = f"auto_{get_slug(canonical_name)}"
        else:
            canonical_name = f"Anonymous_{idx}"
            base_uuid = f"auto_{idx}"

            
        uuid = base_uuid
        counter = 1
        while any(i["uuid"] == uuid for i in identities):
            uuid = f"{base_uuid}_{counter}"
            counter += 1
            
        # Prioritize platform handles
        github_handle = None
        # If we have a curated entry, use it
        cur_entry = next((e for e in curated_data if e.get("canonical_name") == canonical_name), None)
        if cur_entry: github_handle = cur_entry.get("github")
        
        # If not curated, collect ALL logins tagged prgithub/bipgithub in this component.
        # Storing multiple logins handles GitHub username changes (e.g. maflcko → MarcoFalke):
        # the resolver indexes every known alias so review events under the old handle still
        # resolve to the same UUID instead of minting a ghost auto_ record.
        if not github_handle:
            gh_name_nodes = [n for n in component if n.startswith("NAME:") and ("prgithub" in node_sources[n] or "bipgithub" in node_sources[n])]
            if gh_name_nodes:
                all_logins = [n.split(":", 1)[1] for n in gh_name_nodes]
                github_handle = all_logins if len(all_logins) > 1 else all_logins[0]
            
        identities.append({
            "uuid": uuid,
            "display_name": canonical_name,
            "git_signatures": {"names": list(set(decoded_names)), "emails": list(set(emails))},
            "platforms": {
                "github": github_handle,
                "github_id": gh_ids[0] if gh_ids else None,
                "delving": dlv_ids[0] if dlv_ids else None
            },
            "sources": list(sources)
        })
        
    print(f"Generated {len(identities)} high-fidelity identities.")
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump({"_meta": {"total": len(identities), "generated_at": datetime.now().isoformat()}, "identities": identities}, f, indent=2)

if __name__ == "__main__":
    build_identities()
