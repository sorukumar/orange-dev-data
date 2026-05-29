import argparse
import os
import subprocess
import sys
import json
import pandas as pd

# Force unbuffered stdout so tee/pipe sees output in real time
sys.stdout.reconfigure(line_buffering=True)

def load_env():
    """Load environment variables from .env file"""
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value
        print("✅ Loaded .env file")

def run(command, cwd=None):
    """Execute a system command and check for errors"""
    if command.startswith("python3 "):
        command = command.replace("python3 ", f'"{sys.executable}" ', 1)
        
    print(f"\n--- Running: {command} ---")
    result = subprocess.run(command, shell=True, cwd=cwd)
    if result.returncode != 0:
        if "git pull" in command:
             print("⚠️  Git pull failed. Continuing with current local data.")
             return True
        print(f"⚠️  Command failed with exit code {result.returncode}")
        return False
    return True

def count_step(label, root_dir):
    """Print a dev-count snapshot after a pipeline step. Reads from relevant output files."""
    sep = "─" * 60
    print(f"\n{sep}")
    print(f"  📊  DEV COUNT SNAPSHOT — {label}")
    print(sep)

    # ── Raw parquets (Phase 1) ─────────────────────────────────────────────────
    raw_files = {
        "core_commits.parquet":        ("author_email",   "Commit authors (unique emails)"),
        "github_pr_metadata.parquet":  ("github_id",      "GitHub PR authors (unique IDs)"),
        "github_review_events.parquet":("github_id",      "GitHub reviewers (unique IDs)"),
        "bips_pr_metadata.parquet":    ("github_id",      "BIPs PR authors (unique IDs)"),
        "bips_review_events.parquet":  ("github_id",      "BIPs reviewers (unique IDs)"),
        "social_mailing_list.parquet": ("author_email",   "Mailing list (unique emails)"),
        "social_delving.parquet":      ("author_username","Delving (unique usernames)"),
    }
    any_raw = False
    for fname, (col, desc) in raw_files.items():
        fpath = os.path.join(root_dir, "data", "raw", fname)
        if os.path.exists(fpath):
            try:
                df = pd.read_parquet(fpath, columns=[col])
                n = df[col].dropna().nunique()
                print(f"    {desc:<45} {n:>7,}")
                any_raw = True
            except Exception:
                pass
    if any_raw:
        print()

    # ── identities.json ────────────────────────────────────────────────────────
    id_path = os.path.join(root_dir, "metadata", "identities.json")
    if os.path.exists(id_path):
        with open(id_path) as f:
            id_data = json.load(f)
        ids = id_data.get("identities", [])
        can_count  = sum(1 for r in ids if r["uuid"].startswith("can_"))
        auto_count = sum(1 for r in ids if r["uuid"].startswith("auto_"))
        multi_src  = sum(1 for r in ids if len(r.get("sources", [])) >= 2)

        # per-source
        from collections import Counter
        src_counts = Counter()
        for r in ids:
            for s in r.get("sources", []):
                src_counts[s] += 1

        print(f"    {'identities.json — total':<45} {len(ids):>7,}")
        print(f"    {'  can_ (hand-curated multi-alias)':<45} {can_count:>7,}")
        print(f"    {'  auto_ (everyone else)':<45} {auto_count:>7,}")
        print(f"    {'  merged across 2+ sources':<45} {multi_src:>7,}")
        for src, cnt in sorted(src_counts.items(), key=lambda x: -x[1]):
            label_src = f"  origin: {src}"
            print(f"    {label_src:<45} {cnt:>7,}")
        print()

    # ── audit_potential_matches.json summary ───────────────────────────────────
    audit_path = os.path.join(root_dir, "metadata", "audit_potential_matches.json")
    if os.path.exists(audit_path):
        with open(audit_path) as f:
            audit = json.load(f)
        s = audit.get("summary", {})
        print(f"    {'audit — Delving users mapped':<45} {s.get('delving_mapped_to_identity',0):>7,} / {s.get('delving_raw_users',0):,}")
        print(f"    {'audit — Delving candidates to curate':<45} {len(audit.get('delving_github_candidates',[])):>7,}")
        print(f"    {'audit — ML emails mapped':<45} {s.get('ml_mapped_to_identity',0):>7,} / {s.get('ml_raw_emails',0):,}")
        print(f"    {'audit — fuzzy name pairs to review':<45} {len(audit.get('fuzzy_name_matches',[])):>7,}")
        print()

    # ── contributor_review_metrics.parquet ───────────────────────────────────
    eff_path = os.path.join(root_dir, "data", "enriched", "contributor_review_metrics.parquet")
    if os.path.exists(eff_path):
        df_eff = pd.read_parquet(eff_path)
        if "canonical_id" in df_eff.columns:
            n = df_eff["canonical_id"].dropna().nunique()
            print(f"    {'contributor_review_metrics — unique UUIDs':<45} {n:>7,}")
            print()

    # ── social_stats.json ─────────────────────────────────────────────────────
    soc_path = os.path.join(root_dir, "data", "enriched", "social_stats.json")
    if os.path.exists(soc_path):
        with open(soc_path) as f:
            soc = json.load(f)
        contribs = soc.get("contributors", [])
        n_ml  = sum(1 for c in contribs if (c.get("ml_threads") or 0) > 0)
        n_dlv = sum(1 for c in contribs if (c.get("delving_threads") or 0) > 0)
        print(f"    {'social_stats — unique UUIDs':<45} {len(contribs):>7,}")
        print(f"    {'  with mailing list activity':<45} {n_ml:>7,}")
        print(f"    {'  with Delving activity':<45} {n_dlv:>7,}")
        print()

    # ── contributors_unified.parquet ──────────────────────────────────────────
    uni_path = os.path.join(root_dir, "data", "enriched", "contributors_unified.parquet")
    if os.path.exists(uni_path):
        df_uni = pd.read_parquet(uni_path)
        n_total   = len(df_uni)
        n_commits = int((df_uni.get("total_commits", 0) > 0).sum()) if "total_commits" in df_uni.columns else 0
        n_review  = int(((df_uni.get("reviews_count", 0) > 0) | (df_uni.get("prs_authored", 0) > 0)).sum()) if "reviews_count" in df_uni.columns else 0
        n_social  = int(((df_uni.get("ml_threads", 0) > 0) | (df_uni.get("delving_threads", 0) > 0)).sum()) if "ml_threads" in df_uni.columns else 0
        n_bips    = int((df_uni.get("bips_authored", 0) > 0).sum()) if "bips_authored" in df_uni.columns else 0
        # multi-source: has at least 2 of the 4 signal types
        signals = (
            ((df_uni.get("total_commits", pd.Series(0)) > 0).astype(int)
             if "total_commits" in df_uni.columns else pd.Series(0, index=df_uni.index))
            + ((df_uni.get("reviews_count", pd.Series(0)) > 0).astype(int)
               if "reviews_count" in df_uni.columns else pd.Series(0, index=df_uni.index))
            + ((df_uni.get("ml_threads", pd.Series(0)) > 0).astype(int)
               if "ml_threads" in df_uni.columns else pd.Series(0, index=df_uni.index))
            + ((df_uni.get("delving_threads", pd.Series(0)) > 0).astype(int)
               if "delving_threads" in df_uni.columns else pd.Series(0, index=df_uni.index))
        )
        n_cross = int((signals >= 2).sum())
        print(f"    {'contributors_unified — ONE ROW PER DEV':<45} {n_total:>7,}")
        print(f"    {'  commit activity (total_commits > 0)':<45} {n_commits:>7,}")
        print(f"    {'  review activity (reviews > 0)':<45} {n_review:>7,}")
        print(f"    {'  social activity (ML or Delving > 0)':<45} {n_social:>7,}")
        print(f"    {'  BIP authorship (bips_authored > 0)':<45} {n_bips:>7,}")
        print(f"    {'  cross-source (2+ signal types)':<45} {n_cross:>7,}")
        print()

    # ── ecosystem_summary.json ────────────────────────────────────────────────
    eco_path = os.path.join(root_dir, "output", "shared", "ecosystem_summary.json")
    if os.path.exists(eco_path):
        with open(eco_path) as f:
            eco = json.load(f)
        grp = eco.get("groups", eco)  # support both flat and nested formats
        print(f"    {'ecosystem_summary — total_registry':<45} {grp.get('total_registry',0):>7,}")
        print(f"    {'ecosystem_summary — total_active':<45} {grp.get('total_active',0):>7,}")
        print(f"    {'ecosystem_summary — committers':<45} {grp.get('committers',0):>7,}")
        print(f"    {'ecosystem_summary — reviewers':<45} {grp.get('reviewers',0):>7,}")
        print(f"    {'ecosystem_summary — research':<45} {grp.get('research',0):>7,}")
        print(f"    {'ecosystem_summary — standards':<45} {grp.get('standards',0):>7,}")
        print()

    print(sep)

def main():
    parser = argparse.ArgumentParser(description="Monthly rebuild pipeline")
    parser.add_argument(
        "--audit",
        action="store_true",
        help="Also regenerate audit_potential_matches.json for identity curation review",
    )
    parser.add_argument(
        "--background",
        action="store_true",
        help="Run the pipeline in the background (detached process). Output is written to logs/rebuild_monthly_<date>.log",
    )
    args = parser.parse_args()

    if args.background:
        from datetime import datetime
        import shlex
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        log_path  = os.path.join(root_dir, "logs", f"rebuild_monthly_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        # Build the same command without --background to avoid recursion
        cmd = [sys.executable, __file__]
        if args.audit:
            cmd.append("--audit")
        with open(log_path, "w") as log_f:
            proc = subprocess.Popen(cmd, stdout=log_f, stderr=subprocess.STDOUT,
                                    cwd=root_dir, start_new_session=True)
        print(f"🚀 Pipeline started in background (PID {proc.pid})")
        print(f"   Log: {log_path}")
        print(f"   Monitor: tail -f {log_path}")
        return

    run_audit = args.audit

    print("🚀 Starting MONTHLY Automated Pipeline (Deep Analytics)...")
    if run_audit:
        print("   (--audit mode: will regenerate audit_potential_matches.json)")

    load_env()
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    
    for folder in ["enriched", "cache", "network", "raw", "sources"]:
        os.makedirs(os.path.join(root_dir, "data", folder), exist_ok=True)

    # PHASE 0: Fresh Sync
    print("\n--- PHASE 0: Raw Data Sync ---")

    # Health check: report which source repos are present vs missing
    sources = {
        "bitcoin (Core git)":            ("data/sources/bitcoin",                 False),
        "bips (BIPs git)":               ("data/sources/bips",                    False),
        "delving (archive)":             ("data/sources/delving",                 False),
        "bitcoin-github-metadata (PRs)": ("data/sources/bitcoin-github-metadata", False),
        "bips-github-metadata (PRs)":    ("data/sources/bips-github-metadata",    False),
        "mailing_list/shard_0":          ("data/sources/mailing_list/shard_0",    True),   # bare repo
    }
    print("  Source repo status:")
    for label, (rel_path, is_bare) in sources.items():
        full = os.path.join(root_dir, rel_path)
        sentinel = os.path.join(full, "HEAD") if is_bare else os.path.join(full, ".git")
        is_valid = os.path.exists(sentinel)
        items    = len(os.listdir(full)) if os.path.isdir(full) else 0
        status   = ("✅ bare repo" if is_bare else "✅ git repo") if is_valid else \
                   ("⚠️  empty dir (will clone)" if items == 0 else "⚠️  no git sentinel")
        print(f"    {label:<40} {status}")
    print()

    run("git -C data/sources/bitcoin pull origin master", cwd=root_dir)
    run("git -C data/sources/bips pull origin master", cwd=root_dir)
    run("git -C data/sources/delving pull origin master", cwd=root_dir)

    # PHASE 1: Extraction (Source -> Raw)
    print("\n--- PHASE 1: Extraction (Raw Staging) ---")
    run("python3 scripts/01_ingest/core.py", cwd=root_dir)
    run("python3 scripts/01_ingest/bips.py", cwd=root_dir)
    run("python3 scripts/01_ingest/bips_metadata.py", cwd=root_dir)  # BIPs GitHub PR + review events
    run("python3 scripts/01_ingest/delving.py", cwd=root_dir)
    run("python3 scripts/01_ingest/mailing_list.py", cwd=root_dir)
    run("python3 scripts/01_ingest/github_metadata.py", cwd=root_dir)
    count_step("After Phase 1 — raw source files extracted", root_dir)

    # PHASE 2: Convergence (Raw -> Enriched)
    print("\n--- PHASE 2: Convergence (Enrichment) ---")
    run("python3 scripts/identity/build_github_id_map.py", cwd=root_dir)  # pre-compute github_id → email anchors
    run("python3 scripts/identity/build_identities.py", cwd=root_dir)
    # Re-stamp raw social parquets against the freshly rebuilt identities.json.
    # Both must run after build_identities.py and before merge_social.py.
    run("python3 scripts/identity/restamp_social_ids.py", cwd=root_dir)
    run("python3 scripts/identity/restamp_delving_ids.py", cwd=root_dir)
    # Audit report: only run on demand (--audit flag) — it writes metadata/audit_potential_matches.json
    # for human curation review but is not consumed by any downstream pipeline step.
    if run_audit:
        run("python3 scripts/maintenance/generate_audit_potential_matches.py", cwd=root_dir)
    count_step("After build_identities — identity graph built", root_dir)

    run("python3 scripts/02_process/reviews.py", cwd=root_dir)
    run("python3 scripts/02_process/github_social.py", cwd=root_dir)
    run("python3 scripts/02_process/core.py", cwd=root_dir)
    run("python3 scripts/02_process/merge_social.py", cwd=root_dir)
    run("python3 scripts/02_process/governance.py", cwd=root_dir)
    
    # registry.py must run before Phase 3 — influence.py and unify_contributors.py both read metadata/contributors.json
    run("python3 scripts/04_deliver/registry.py", cwd=root_dir)

    # PHASE 3: Intelligence (Heavy Analytics & Graphs)
    print("\n--- PHASE 3: Intelligence (NLP & Graphs) ---")
    run("python3 scripts/02_process/categorize.py", cwd=root_dir)
    run("python3 scripts/03_analyze/review_metrics.py", cwd=root_dir)
    count_step("After review_metrics — reviewer metrics computed", root_dir)
    run("python3 scripts/03_analyze/influence.py", cwd=root_dir)
    count_step("After influence — social stats computed", root_dir)
    run("python3 scripts/03_analyze/expertise.py", cwd=root_dir)
    run("python3 scripts/02_process/unify_contributors.py", cwd=root_dir)
    count_step("After unify_contributors — grand join complete", root_dir)

    # PHASE 4: Delivery (Enriched -> Output)
    print("\n--- PHASE 4: Artifact Generation ---")
    run("python3 scripts/04_deliver/generate_regional_evolution.py", cwd=root_dir)
    run("python3 scripts/04_deliver/registry.py", cwd=root_dir)
    run("python3 scripts/04_deliver/ui_artifacts.py", cwd=root_dir)
    run("python3 scripts/04_deliver/ecosystem_summary.py", cwd=root_dir)
    run("python3 scripts/04_deliver/discussions_pulse.py", cwd=root_dir)
    run("python3 scripts/04_deliver/network_home_snapshot.py", cwd=root_dir)
    count_step("FINAL — ecosystem summary", root_dir)

    print("\n✨ MONTHLY PIPELINE COMPLETE!")
    print("Everything is up to date and all NLP/Graphs have been recalculated.")

if __name__ == "__main__":
    main()
