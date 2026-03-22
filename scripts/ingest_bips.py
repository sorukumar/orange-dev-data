import os
import subprocess
import pandas as pd
import re
import json
from datetime import datetime
import sys

# --- Configuration ---
BIPS_REPO_URL = "https://github.com/bitcoin/bips"
BIPS_REPO_PATH = "data/raw_archives/bips"
OUTPUT_PARQUET = "data/raw/bips.parquet"
ALIASES_PATH = "lookups/identity_mappings.json"
STATE_PATH = "data/state.json"

def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, 'w') as f:
        json.dump(state, f, indent=2)

def run_command(cmd, cwd=None):
    """Runs a shell command and returns stdout."""
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    if result.returncode != 0:
        return ""
    return result.stdout

def setup_repo():
    """Clones the BIPs repository if it doesn't exist."""
    if not os.path.exists(BIPS_REPO_PATH):
        print(f"Cloning BIPs repo to {BIPS_REPO_PATH}...")
        # Do NOT use --depth 1, we need full history for git log
        subprocess.run(["git", "clone", BIPS_REPO_URL, BIPS_REPO_PATH], check=True)
    else:
        # Check if it's a shallow clone
        shallow = run_command(["git", "-C", BIPS_REPO_PATH, "rev-parse", "--is-shallow-repository"]).strip()
        if shallow == "true":
            print("Shallow repo detected. Converting to full clone...")
            subprocess.run(["git", "-C", BIPS_REPO_PATH, "fetch", "--unshallow"], check=True)
        else:
            print("BIPs repo exists and is full.")

def load_aliases():
    """Loads and flattens the aliases lookup for rapid searching."""
    if not os.path.exists(ALIASES_PATH):
        print(f"Warning: Aliases file not found at {ALIASES_PATH}")
        return {}
    
    with open(ALIASES_PATH, 'r') as f:
        data = json.load(f)
    
    lookup = {}
    for entry in data.get("aliases", []):
        canonical = entry["canonical_name"]
        # Map canonical name (lowercase) to itself
        lookup[canonical.lower()] = canonical
        # Map aliases
        for alias in entry.get("aliases", []):
            lookup[alias.lower()] = canonical
        # Map emails
        for email in entry.get("emails", []):
            lookup[email.lower()] = canonical
            
    return lookup

def map_author(name_or_email, lookup):
    """Maps a name or email to a canonical ID (name) using the lookup table."""
    if not name_or_email:
        return None
    clean = name_or_email.strip().lower()
    # Simple direct lookup
    if clean in lookup:
        return lookup[clean]
    
    return name_or_email.strip()

def parse_authors(author_str, lookup):
    """
    Parses BIP Author strings like:
    Eric Lombrozo <elombrozo@gmail.com>
    Johnson Lau <jl2012@xbt.hk>
    Pieter Wuille <pieter.wuille@gmail.com>
    """
    if not author_str or author_str == "Unknown":
        return []
        
    # Split by common separators: comma, newline
    parts = re.split(r",|\n", author_str)
    authors = []
    
    for p in parts:
        p = p.strip()
        if not p: continue
        
        # Match "Name <email>"
        match = re.search(r"([^<>\n]+)(?:<([^<>]+)>)?", p)
        if match:
            name = match.group(1).strip()
            # Clean up optional "Author:" prefix
            name = re.sub(r"^Authors?\s*:\s*", "", name, flags=re.IGNORECASE).strip()
            
            email = match.group(2).strip() if match.group(2) else None
            
            canonical_id = name
            if email:
                canonical_id = map_author(email, lookup)
            
            # If email didn't map, try mapping the name
            if canonical_id == name:
                canonical_id = map_author(name, lookup)
                
            authors.append({
                "name": name,
                "email": email,
                "canonical_id": canonical_id
            })
        else:
            # Fallback for just name
            clean_p = re.sub(r"^Authors?\s*:\s*", "", p, flags=re.IGNORECASE).strip()
            canonical_id = map_author(clean_p, lookup)
            authors.append({
                "name": clean_p,
                "email": None,
                "canonical_id": canonical_id
            })
            
    return authors

def extract_header_text(content):
    """Robustly extracts the header section from BIP content."""
    # Try <pre>...</pre> (MediaWiki style)
    match = re.search(r"<pre>(.*?)</pre>", content, re.DOTALL)
    if match:
        return match.group(1)
    
    # Try ```...``` (Open-block style in .md)
    match = re.search(r"```(.*?)\n```", content, re.DOTALL)
    if match:
        return match.group(1)
    
    # Try just the start of the file if it looks like a header
    if "BIP:" in content[:500]:
        lines = []
        for line in content.split("\n")[:50]:
            if re.match(r"^\s*[A-Z][a-zA-Z-]+:", line):
                lines.append(line)
            elif lines and (line.strip().startswith("==") or line.strip().startswith("##")):
                break
        return "\n".join(lines)
        
    return content[:2000]

def parse_bip_header(content, lookup):
    """Extracts metadata from the header of a BIP file."""
    header_text = extract_header_text(content)
        
    metadata = {}
    fields_to_check = [
        "BIP", "Title", "Author", "Authors", "Status", "Type", 
        "Layer", "Created", "Assigned", "License", "Last-Modified"
    ]
    
    for field in fields_to_check:
        pattern = rf"^\s*{field}:\s*(.*?)(?=\n\s*[A-Z][a-zA-Z0-9-]+:\s*|\Z)"
        match = re.search(pattern, header_text, re.MULTILINE | re.DOTALL | re.IGNORECASE)
            
        if match:
            val = match.group(1).strip()
            metadata[field.lower()] = val
            
    bip_id = metadata.get("bip", "Unknown")
    author_raw = metadata.get("authors") or metadata.get("author") or "Unknown"
    authors_parsed = parse_authors(author_raw, lookup)
    created_date = metadata.get("created") or metadata.get("assigned") or "Unknown"
    
    return {
        "bip_id": bip_id,
        "title": metadata.get("title", "Unknown"),
        "status": metadata.get("status", "Unknown"),
        "type": metadata.get("type", "Unknown"),
        "layer": metadata.get("layer", "Unknown"),
        "created_date_header": created_date,
        "authors_parsed": authors_parsed,
        "authors_raw": author_raw
    }

def get_git_history(filename):
    """Extracts first/last seen dates and commit count for a file via git."""
    cmd = ["git", "-C", BIPS_REPO_PATH, "log", "--follow", "--format=%at|%an|%ae", "--", filename]
    output = run_command(cmd)
    
    if not output:
        return None
        
    lines = output.strip().split("\n")
    timestamps = []
    git_contributors = set()
    
    for line in lines:
        parts = line.split("|")
        if len(parts) >= 1 and parts[0].isdigit():
            ts = int(parts[0])
            timestamps.append(ts)
            if len(parts) >= 3:
                git_contributors.add(parts[2].lower())
            
    if not timestamps:
        return None
        
    return {
        "first_commit": datetime.fromtimestamp(min(timestamps)),
        "last_commit": datetime.fromtimestamp(max(timestamps)),
        "revision_count": len(timestamps),
        "unique_git_contributors_count": len(git_contributors)
    }

def main():
    print("--- Stage 1: BIP Ingestion Starting ---")
    setup_repo()
    
    # Get latest commit
    latest_commit = run_command(["git", "-C", BIPS_REPO_PATH, "rev-parse", "HEAD"]).strip()
    state = load_state()
    
    last_processed_commit = state.get("bips", {}).get("latest_commit", "")
    if last_processed_commit == latest_commit and os.path.exists(OUTPUT_PARQUET):
        print(f"BIPs repo is up to date at commit {latest_commit}. Skipping re-parsing.")
        return

    lookup = load_aliases()
    
    # Find all mediawiki and markdown files
    all_files = os.listdir(BIPS_REPO_PATH)
    bip_files = [f for f in all_files if f.startswith("bip-") and (f.endswith(".mediawiki") or f.endswith(".md"))]
    bip_files.sort()
    
    records = []
    print(f"Parsing {len(bip_files)} BIP files for metadata and git history...")
    
    for i, filename in enumerate(bip_files):
        if i % 50 == 0:
            print(f"  Processed {i}/{len(bip_files)} files...")
            
        path = os.path.join(BIPS_REPO_PATH, filename)
        try:
            with open(path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
                
            meta = parse_bip_header(content, lookup)
            history = get_git_history(filename)
            
            canonical_ids = list(set(a["canonical_id"] for a in meta["authors_parsed"] if a["canonical_id"]))
            
            record = {
                "bip_id": meta["bip_id"],
                "file_name": filename,
                "title": meta["title"],
                "status": meta["status"],
                "type": meta["type"],
                "layer": meta["layer"],
                "created_date_header": meta["created_date_header"],
                "authors_json": json.dumps(meta["authors_parsed"]),
                "author_canonical_ids": canonical_ids,
                "author_names": [a["name"] for a in meta["authors_parsed"]],
            }
            
            if history:
                record["git_created_at"] = history["first_commit"]
                record["git_updated_at"] = history["last_commit"]
                record["revision_count"] = history["revision_count"]
                record["unique_git_contributors_count"] = history["unique_git_contributors_count"]
            else:
                record["git_created_at"] = None
                record["git_updated_at"] = None
                record["revision_count"] = 0
                record["unique_git_contributors_count"] = 0
                
            records.append(record)
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            
    df = pd.DataFrame(records)
    
    os.makedirs("data/governance", exist_ok=True)
    df.to_parquet(OUTPUT_PARQUET, index=False)
    
    print("\n--- Ingestion Complete ---")
    print(f"Total BIPs Parsed: {len(df)}")
    print(f"Artifact Saved: {OUTPUT_PARQUET}")
    
    # Update state
    state.setdefault("bips", {})["latest_commit"] = latest_commit
    state["bips"]["total_bips"] = len(df)
    if "git_updated_at" in df and not df["git_updated_at"].isna().all():
        state["bips"]["latest_update"] = df["git_updated_at"].max().isoformat()
    
    save_state(state)
    
    if "status" in df:
        print("\nStatus Distribution:")
        print(df["status"].value_counts().head(10))

if __name__ == "__main__":
    main()
