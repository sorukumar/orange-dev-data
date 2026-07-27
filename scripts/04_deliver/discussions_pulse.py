import json
import os
import re
import pandas as pd
from datetime import datetime

# Strip "'NAME' via Bitcoin Development Mailing List" delivery-address artifacts
_via_pat = re.compile(r"^'(.+)'\s+via\s+", re.IGNORECASE)

# --- Configuration ---
SOCIAL_THREADS_INPUT = "data/enriched/social_threads.parquet"
BIPS_INPUT = "output/tracker/bips_ui.json"
OUTPUT_FILE = "output/shared/discussions_pulse.json"

CATEGORY_LABELS = {
    'quantum':              'Quantum Resistance',
    'covenants':            'Covenants & Vaults',
    'taproot':              'Taproot',
    'mining':               'Mining Protocol',
    'mempool-fees':         'Mempool & Fees',
    'privacy':              'Privacy',
    'lightning':            'Lightning / L2',
    'bitvm':                'BitVM',
    'p2p-network':          'P2P Network',
    'bip-process':          'BIP Process',
    'testing-devtools':     'Dev Tooling',
    'soft-fork-activation': 'Soft Fork Activation',
    'segwit':               'SegWit',
    'script-opcodes':       'Script & Opcodes',
    'ecash':                'eCash',
    'silent-payments':      'Silent Payments',
    'signatures-sighash':   'Signatures & Sighash',
    'scaling':              'Scaling',
    'spam-filtering':       'Spam Filtering',
    'utxo-sync':            'UTXO & Sync',
    'payment-protocol':     'Payment Protocol',
    'vaults':               'Vaults',
    'dlc':                  'DLCs',
    'atomic-swaps':         'Atomic Swaps',
    'multisig-threshold':   'Multisig',
    'wallet-keys':          'Wallet & Keys',
    'core-dev':             'Core Dev',
    'nostr':                'Nostr',
    'l2-bridges':           'L2 Bridges',
    'hard-fork-block-size': 'Block Size Debate',
}

_BIP_TITLES = {}


def _clean_subject(subject):
    """Strip reply prefixes and list prefixes from thread subjects."""
    s = (subject or '').strip()
    while s.lower().startswith('re:'):
        s = s[3:].strip()
    if s.startswith('[bitcoindev]'):
        s = s.replace('[bitcoindev]', '').strip()
    return s

IDENTITY_MAP = {}


try:
    with open("data/cache/thread_summaries_cache.json", "r") as f:
        THREAD_SUMMARIES = json.load(f)
except Exception:
    THREAD_SUMMARIES = {}

def _compute_window(df, window_days):
    """Return raw metrics dict for the given window. Includes _cat_shares for trend calc."""
    latest = df['date'].max()
    cutoff = latest - pd.Timedelta(days=window_days)
    w = df[df['date'] >= cutoff].copy()

    if len(w) == 0:
        return {}

    total_messages = len(w)
    total_threads = int(w['thread_id'].nunique())
    unique_voices = int(w['canonical_id'].nunique())

    # --- Themes (exclude noisy categories) ---
    _SKIP = {'other', 'Other', None}
    w_cats = w[~w['category'].isin(_SKIP)].copy()
    cat_stats = w_cats.groupby('category').agg(
        msgs=('message_id', 'count'),
        voices=('author_name', 'nunique'),
        threads=('thread_id', 'nunique')
    ).sort_values('msgs', ascending=False)

    total_cat_msgs = int(cat_stats['msgs'].sum())
    themes = []
    cat_shares = {}
    for cat, row in cat_stats.head(10).iterrows():
        share = round(row['msgs'] / total_cat_msgs * 100, 1) if total_cat_msgs > 0 else 0
        cat_shares[cat] = share
        themes.append({
            "category": cat,
            "label": CATEGORY_LABELS.get(cat, cat.replace('-', ' ').title()),
            "msgs": int(row['msgs']),
            "threads": int(row['threads']),
            "voices": int(row['voices']),
            "share": share,
            "trend": "steady",
        })

    # Get opening-post snippet (first non-reply per thread, fallback to earliest reply)
    openers = (
        df.sort_values(['is_reply', 'date'])
        .groupby('thread_id')
        .first()
        .reset_index()[['thread_id', 'body_snippet', 'author_name', 'canonical_id', 'is_reply']]
    )

    thread_stats = w.groupby('thread_id').agg(
        reply_count=('is_reply', 'sum'),
        unique_authors=('author_name', 'nunique'),
        subject=('subject', 'first'),
        source=('source', 'first'),
        category=('category', 'first'),
        link=('link', 'first'),
        last_post=('date', 'max'),
    ).reset_index()

    thread_stats['score'] = thread_stats['reply_count'] * thread_stats['unique_authors']
    thread_stats = thread_stats.sort_values('score', ascending=False)
    thread_stats = thread_stats.merge(openers, on='thread_id', how='left')

    top_overall = thread_stats.head(8)
    top_delving = thread_stats[thread_stats['source'] == 'delving'].head(8)
    top_ml = thread_stats[thread_stats['source'] == 'mailing_list'].head(8)
    
    combined = pd.concat([top_overall, top_delving, top_ml]).drop_duplicates(subset=['thread_id'])
    combined = combined.sort_values('score', ascending=False)

    hot_threads = []
    for _, row in combined.iterrows():
        cat = row.get('category') or 'other'
        snippet_raw = str(row.get('body_snippet') or '').strip()
        # Truncate cleanly at word boundary
        if len(snippet_raw) > 140:
            snippet_raw = snippet_raw[:137].rsplit(' ', 1)[0] + '…'
            
        tid = row['thread_id']
        insight = None
        summary = None
        tech_summary = None
        if f"tid_{tid}" in THREAD_SUMMARIES:
            insight = THREAD_SUMMARIES[f"tid_{tid}"].get("pulse_insight")
            summary = THREAD_SUMMARIES[f"tid_{tid}"].get("public_summary")
            tech_summary = THREAD_SUMMARIES[f"tid_{tid}"].get("technical_summary")
        else:
            subj_clean = row['subject'][:20]
            if f"thread_{subj_clean}" in THREAD_SUMMARIES:
                insight = THREAD_SUMMARIES[f"thread_{subj_clean}"].get("pulse_insight")
                summary = THREAD_SUMMARIES[f"thread_{subj_clean}"].get("public_summary")
                tech_summary = THREAD_SUMMARIES[f"thread_{subj_clean}"].get("technical_summary")

        raw_uuid = str(row.get('canonical_id', ''))
        raw_name = str(row.get('author_name', ''))
        display_name = IDENTITY_MAP.get(raw_uuid, raw_name)

        hot_threads.append({
            "thread_id": tid,
            "subject": _clean_subject(row['subject']),
            "category": cat,
            "label": CATEGORY_LABELS.get(cat, cat.replace('-', ' ').title()),
            "reply_count": int(row['reply_count']),
            "unique_authors": int(row['unique_authors']),
            "score": int(row['score']),
            "last_post": row['last_post'].strftime('%Y-%m-%d') if pd.notna(row['last_post']) else '',
            "source": row.get('source', ''),
            "link": row.get('link', '') or '',
            "snippet": snippet_raw,
            "insight": insight,
            "summary": summary,
            "technical_summary": tech_summary,
            "author": display_name,
            "author_uuid": raw_uuid,
            "is_original_author": not bool(row.get('is_reply', False))
        })

    # --- Top BIPs ---
    bip_counts = {}
    for refs in w['bip_refs'].dropna():
        # bip_refs is stored as numpy ndarray in parquet
        try:
            items = list(refs)
        except TypeError:
            items = [refs]
        for b in items:
            if b is not None and str(b).strip():
                key = str(b).strip()
                bip_counts[key] = bip_counts.get(key, 0) + 1

    top_bips = []
    for bip_id, count in sorted(bip_counts.items(), key=lambda x: x[1], reverse=True)[:6]:
        top_bips.append({
            "bip_id": bip_id,
            "title": _BIP_TITLES.get(bip_id, ''),
            "mentions": count,
        })

    # --- Top Voices ---
    # Group by canonical_id so the same person isn't split across name variants
    # (e.g. "'conduition' via Bitcoin Development Mailing List" vs "conduition").
    # For the display name, prefer a non-via author_name; fall back to stripping the via suffix.
    def _best_name(grp):
        names = grp['author_name'].dropna()
        non_via = names[~names.str.contains(' via ', na=False, regex=False)]
        raw = non_via.iloc[0] if len(non_via) > 0 else names.iloc[0] if len(names) > 0 else ''
        m = _via_pat.match(str(raw))
        return m.group(1).strip() if m else str(raw).strip()

    name_map = {cid: _best_name(grp) for cid, grp in w.groupby('canonical_id')}
    voice_counts = w.groupby('canonical_id').size().sort_values(ascending=False)
    top_voices = [
        {"name": IDENTITY_MAP.get(str(cid), name_map.get(cid, cid)), "uuid": str(cid), "posts": int(count)}
        for cid, count in voice_counts.head(8).items()
    ]

    # --- Source Split ---
    source_counts = w['source'].value_counts().to_dict()
    total_src = sum(source_counts.values())
    source_pct = {
        k: round(v / total_src * 100, 1)
        for k, v in source_counts.items()
    } if total_src > 0 else {}

    # Detect mailing list staleness: check how long ago the last ML message was
    ml_rows = df[df['source'] == 'mailing_list']
    mailing_list_stale = False
    mailing_list_days_since = None
    if not ml_rows.empty:
        last_ml = ml_rows['date'].max()
        days_since = int((df['date'].max() - last_ml).days)
        if days_since > window_days:
            mailing_list_stale = True
            mailing_list_days_since = days_since

    return {
        "window_days": window_days,
        "total_messages": total_messages,
        "total_threads": total_threads,
        "unique_voices": unique_voices,
        "themes": themes,
        "_cat_shares": cat_shares,   # internal — stripped before output
        "hot_threads": hot_threads,
        "top_bips": top_bips,
        "top_voices": top_voices,
        "source_pct": source_pct,
        "source_counts": {k: int(v) for k, v in source_counts.items()},
        "mailing_list_stale": mailing_list_stale,
        "mailing_list_days_since": mailing_list_days_since,
    }


def _add_trends(w30, w90):
    """Annotate themes in both windows with rising/steady/fading trend signals."""
    shares_30 = w30.get('_cat_shares', {})
    shares_90 = w90.get('_cat_shares', {})

    for window_data in [w30, w90]:
        for theme in window_data.get('themes', []):
            cat = theme['category']
            s30 = shares_30.get(cat, 0)
            s90 = shares_90.get(cat, 0)
            if s90 > 0:
                ratio = s30 / s90
                if ratio > 1.25:
                    theme['trend'] = 'rising'
                elif ratio < 0.75:
                    theme['trend'] = 'fading'
                else:
                    theme['trend'] = 'steady'
            elif s30 > 0:
                theme['trend'] = 'new'   # appeared in 30d, not in 90d baseline
            else:
                theme['trend'] = 'steady'


def generate_discussions_pulse():
    print("Generating Discussions Pulse...")

    if not os.path.exists(SOCIAL_THREADS_INPUT):
        print(f"  Error: {SOCIAL_THREADS_INPUT} not found.")
        return

    # Load BIP titles from tracker output if available
    if os.path.exists(BIPS_INPUT):
        try:
            with open(BIPS_INPUT) as f:
                for b in json.load(f):
                    _BIP_TITLES[str(b.get('bip_id', ''))] = b.get('title', '')
        except Exception as e:
            print(f"  Warning: Could not load BIP titles: {e}")

    # Load canonical identity map
    IDENTITY_MAP = {}
    if os.path.exists("data/enriched/contributors_unified.parquet"):
        try:
            idf = pd.read_parquet("data/enriched/contributors_unified.parquet")
            for _, row in idf.iterrows():
                if pd.notna(row.get('uuid')) and pd.notna(row.get('display_name')):
                    IDENTITY_MAP[str(row['uuid'])] = str(row['display_name'])
        except Exception as e:
            print(f"  Warning: Could not load identity map: {e}")
            
    df = pd.read_parquet(SOCIAL_THREADS_INPUT)
    df['date'] = pd.to_datetime(df['date'])
    
    # Enforce T-1 boundary to prevent partial day data
    t1_end = (datetime.now() - pd.Timedelta(days=1)).replace(hour=23, minute=59, second=59)
    df = df[df['date'] <= t1_end]

    w90 = _compute_window(df, 90)
    w30 = _compute_window(df, 30)
    w7 = _compute_window(df, 7)

    if not w90 or not w30:
        print("  Error: Not enough data to compute windows.")
        return

    _add_trends(w30, w90)
    if w7:
        _add_trends(w7, w30)

    # Load LLM editorial if available
    pulse_editorial = None
    if os.path.exists("data/raw/pulse_summary.json"):
        try:
            with open("data/raw/pulse_summary.json") as f:
                pulse_editorial = json.load(f)
        except Exception as e:
            print(f"  Warning: Could not load pulse summary: {e}")

    if pulse_editorial:
        w30["pulse_editorial"] = {
            "summary": pulse_editorial.get("summary", ""),
            "insights": pulse_editorial.get("insights", []),
        }
        
    # Enrich hot threads with insights from unified thread summaries
    thread_cache = {}
    if os.path.exists("data/cache/thread_summaries_cache.json"):
        try:
            with open("data/cache/thread_summaries_cache.json") as f:
                thread_cache = json.load(f)
        except Exception as e:
            print(f"  Warning: Could not load thread summaries cache: {e}")
            
    for thread in w30.get("hot_threads", []):
        tid_str = str(thread.get("thread_id"))
        
        # Look up by thread_id first (new robust method)
        cache_entry = thread_cache.get(f"tid_{tid_str}")
        
        # Fallback to subject-based lookup
        if not cache_entry:
            subj = str(thread.get("subject", ""))
            cache_entry = thread_cache.get(f"thread_{subj[:20]}")
            
        if cache_entry and isinstance(cache_entry, dict):
            thread["insight"] = cache_entry.get("pulse_insight", cache_entry.get("public_summary", ""))

    # Strip internal keys
    w90.pop('_cat_shares', None)
    w30.pop('_cat_shares', None)
    if w7:
        w7.pop('_cat_shares', None)

    # Use T-1 for generated_at to clearly signify the complete data boundary
    generated_at_t1 = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    output = {
        "generated_at": generated_at_t1,
        "windows": {
            "7d": w7 or {},
            "30d": w30,
            "90d": w90,
        },
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2, default=str)

    print(f"  ✅ Written: {OUTPUT_FILE}")
    print(f"     90d: {w90.get('total_messages', 0)} msgs · {w90.get('total_threads', 0)} threads · {w90.get('unique_voices', 0)} voices")
    print(f"     30d: {w30.get('total_messages', 0)} msgs · {w30.get('total_threads', 0)} threads · {w30.get('unique_voices', 0)} voices")
    if w7:
        print(f"      7d: {w7.get('total_messages', 0)} msgs · {w7.get('total_threads', 0)} threads · {w7.get('unique_voices', 0)} voices")


if __name__ == "__main__":
    generate_discussions_pulse()
