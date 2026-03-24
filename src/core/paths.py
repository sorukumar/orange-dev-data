import os

# Root Directory (Assumes this file is in src/core/paths.py)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# --- SOURCE DATA (The Warehouse) ---
DATA_DIR = os.path.join(ROOT, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
WORK_DIR = os.path.join(DATA_DIR, "work")   # Intermediate Parquet files
CACHE_DIR = os.path.join(DATA_DIR, "cache") # GitHub/Geo API Caches

# --- INTELLIGENCE LAYER (The Warehouse-Curated) ---
METADATA_DIR = os.path.join(ROOT, "metadata")
ID_PATH = os.path.join(METADATA_DIR, "identities", "identities.json")
GEO_PATH = os.path.join(METADATA_DIR, "context", "locations.json")
MAINTAINERS_PATH = os.path.join(METADATA_DIR, "context", "maintainers.json")
SPONSORS_PATH = os.path.join(METADATA_DIR, "context", "sponsors.json")

# --- STANDALONE PRODUCT (The Showroom) ---
OUTPUT_DIR = os.path.join(ROOT, "output")
SHARED_DIR = os.path.join(OUTPUT_DIR, "shared")    # Reusable Parquets
TRACKER_DIR = os.path.join(OUTPUT_DIR, "tracker")  # JSONs for Tracker UI
NETWORK_DIR = os.path.join(OUTPUT_DIR, "network")  # JSONs for Network UI

# Helper function to ensure directories exist
def ensure_folders():
    for folder in [RAW_DIR, WORK_DIR, CACHE_DIR, SHARED_DIR, TRACKER_DIR, NETWORK_DIR]:
        os.makedirs(folder, exist_ok=True)
