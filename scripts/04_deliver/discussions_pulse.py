import json
import os
import pandas as pd
from datetime import datetime

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

    # --- Hot Threads ---
    # Get opening-post snippet (first non-reply per thread)
    openers = (
        w[~w['is_reply']]
        .sort_values('date')
        .groupby('thread_id')
        .first()
        .reset_index()[['thread_id', 'body_snippet']]
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

    hot_threads = []
    for _, row in thread_stats.head(8).iterrows():
        cat = row.get('category') or 'other'
        snippet_raw = str(row.get('body_snippet') or '').strip()
        # Truncate cleanly at word boundary
        if len(snippet_raw) > 140:
            snippet_raw = snippet_raw[:137].rsplit(' ', 1)[0] + '…'

        hot_threads.append({
            "thread_id": row['thread_id'],
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
    voice_counts = w.groupby('author_name').size().sort_values(ascending=False)
    top_voices = [
        {"name": name, "posts": int(count)}
        for name, count in voice_counts.head(8).items()
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

    df = pd.read_parquet(SOCIAL_THREADS_INPUT)
    df['date'] = pd.to_datetime(df['date'])

    w90 = _compute_window(df, 90)
    w30 = _compute_window(df, 30)

    if not w90 or not w30:
        print("  Error: Not enough data to compute windows.")
        return

    _add_trends(w30, w90)

    # Strip internal keys
    w90.pop('_cat_shares', None)
    w30.pop('_cat_shares', None)

    output = {
        "generated_at": datetime.now().isoformat(),
        "windows": {
            "30d": w30,
            "90d": w90,
        },
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2, default=str)

    print(f"  ✅ Written: {OUTPUT_FILE}")
    print(f"     90d: {w90['total_messages']} msgs · {w90['total_threads']} threads · {w90['unique_voices']} voices")
    print(f"     30d: {w30['total_messages']} msgs · {w30['total_threads']} threads · {w30['unique_voices']} voices")


if __name__ == "__main__":
    generate_discussions_pulse()
