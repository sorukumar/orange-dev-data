import json
import os
import re
import sys
from datetime import datetime

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from scripts.utils.identity import resolver

REGISTRY_PATH = 'output/shared/contributors/registry_index.json'
BIPS_UI_PATH = 'output/tracker/bips_ui.json'
REVIEW_EVENTS_PATHS = [
    'data/raw/github_review_events.parquet',
    'data/raw/bips_review_events.parquet',
]
SOCIAL_COMBINED_PATH = 'data/raw/social_combined.parquet'
SOCIAL_THREADS_PATH = 'data/enriched/social_threads.parquet'
OUTPUT_PATH = 'output/shared/network_home_snapshot.json'

WINDOW_DAYS = 30
BOT_PATTERNS = (
    'bot',
    'automerge',
    'bitcoinpulltester',
    'drahtbot',
)
ACTIVE_REVIEW_TYPES = {'commented', 'reviewed'}


def read_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, 'r') as f:
        return json.load(f)


def is_bot_contributor(c):
    name = str(c.get('display_name') or '').lower()
    uuid = str(c.get('uuid') or '').lower()
    login = str((c.get('github') or {}).get('login') or '').lower()
    blob = ' '.join([name, uuid, login])
    return any(token in blob for token in BOT_PATTERNS)


def is_bot_text(text):
    val = str(text or '').lower()
    return any(token in val for token in BOT_PATTERNS)


def find_alias_canonical_uuid(uuid, canonical_slugs):
    text = str(uuid or '')
    if text.startswith('auto_'):
        slug = text[5:]
        candidate = f'can_{slug}'
        if slug in canonical_slugs:
            return candidate
    return text


def to_utc(series):
    return pd.to_datetime(series, utc=True, errors='coerce')


def count_window(series, start, end):
    return int(((series > start) & (series <= end)).sum())


def load_review_events():
    frames = []
    for path in REVIEW_EVENTS_PATHS:
        if not os.path.exists(path):
            continue
        try:
            df = pd.read_parquet(path)
            if df.empty or not all(c in df.columns for c in ['pr_number', 'user', 'timestamp', 'event_type']):
                continue
            frames.append(df[['pr_number', 'user', 'timestamp', 'event_type']])
        except Exception:
            continue
    if not frames:
        return pd.DataFrame(columns=['pr_number', 'user', 'timestamp', 'event_type'])
    return pd.concat(frames, ignore_index=True)


def build_author_index(contributors):
    index = {}
    for c in contributors:
        name = (c.get('display_name') or '').strip().lower()
        if not name:
            continue
        index[name] = c.get('uuid')
    return index


def pick_name(candidates, preferred_uuid):
    if preferred_uuid and preferred_uuid in candidates:
        return candidates[preferred_uuid]
    if candidates:
        return sorted(candidates.values(), key=lambda x: (len(x), x))[0]
    return preferred_uuid or 'Unknown'


def generate_snapshot():
    registry = read_json(REGISTRY_PATH, {'metadata': {}, 'contributors': []})
    bips_ui = read_json(BIPS_UI_PATH, [])

    contributors = registry.get('contributors', [])
    contributor_by_uuid = {
        str(c.get('uuid')): c
        for c in contributors
        if c.get('uuid')
    }
    human_contributors = [c for c in contributors if not is_bot_contributor(c)]

    canonical_slugs = {
        str(c.get('uuid'))[4:]
        for c in human_contributors
        if str(c.get('uuid') or '').startswith('can_')
    }

    # Anchor windows on the latest known cross-source timestamp.
    global_series = to_utc([c.get('global_last_active') for c in human_contributors])
    latest_global = global_series.max() if not global_series.dropna().empty else pd.NaT

    reviews_df = load_review_events()
    reviews_df['timestamp'] = to_utc(reviews_df['timestamp'])
    reviews_df = reviews_df.dropna(subset=['timestamp', 'user', 'pr_number'])
    reviews_df = reviews_df[reviews_df['event_type'].isin(ACTIVE_REVIEW_TYPES)]
    latest_review = reviews_df['timestamp'].max() if not reviews_df.empty else pd.NaT

    social_df = pd.DataFrame(columns=['source', 'date', 'canonical_id'])
    if os.path.exists(SOCIAL_COMBINED_PATH):
        social_df = pd.read_parquet(SOCIAL_COMBINED_PATH, columns=['source', 'date', 'canonical_id'])
        social_df['date'] = to_utc(social_df['date'])
    latest_social = social_df['date'].max() if not social_df.empty else pd.NaT

    anchors = [x for x in [latest_global, latest_review, latest_social] if pd.notna(x)]
    # Use T-1 as the effective anchor to prevent partial day data
    anchor = (pd.Timestamp.utcnow() - pd.Timedelta(days=1)).replace(hour=23, minute=59, second=59)

    current_start = anchor - pd.Timedelta(days=WINDOW_DAYS)
    previous_start = anchor - pd.Timedelta(days=WINDOW_DAYS * 2)

    # Active contributors current vs previous 30-day windows.
    active_current = count_window(global_series, current_start, anchor)
    active_previous = count_window(global_series, previous_start, current_start)
    active_delta = active_current - active_previous

    # Top reviewers (true 30-day counts from review event timestamps).
    aliases_collapsed = 0
    if not reviews_df.empty:
        reviews_df['uuid'] = reviews_df['user'].map(lambda x: resolver.resolve_github(x))

        def remap_uuid(u):
            nonlocal aliases_collapsed
            collapsed = find_alias_canonical_uuid(u, canonical_slugs)
            if collapsed != u:
                aliases_collapsed += 1
            return collapsed

        reviews_df['uuid'] = reviews_df['uuid'].map(remap_uuid)

        # Bot cleanup post-resolve.
        def keep_row(row):
            c = contributor_by_uuid.get(row['uuid'])
            if c is not None:
                return not is_bot_contributor(c)
            return not is_bot_text(row['user'])

        reviews_df = reviews_df[reviews_df.apply(keep_row, axis=1)]

    def reviewer_counts_for_window(start, end):
        if reviews_df.empty:
            return {}
        mask = (reviews_df['timestamp'] > start) & (reviews_df['timestamp'] <= end)
        scoped = reviews_df[mask]
        if scoped.empty:
            return {}
        grouped = scoped.groupby('uuid')['pr_number'].nunique()
        return {k: int(v) for k, v in grouped.items()}

    reviewer_current = reviewer_counts_for_window(current_start, anchor)
    reviewer_previous = reviewer_counts_for_window(previous_start, current_start)

    name_candidates = {}
    for _, row in reviews_df.iterrows():
        uuid = row['uuid']
        entry = name_candidates.setdefault(uuid, {})
        c = contributor_by_uuid.get(uuid)
        if c is not None and c.get('display_name'):
            entry[uuid] = c.get('display_name')
        user = str(row.get('user') or '').strip()
        if user:
            entry[user.lower()] = user

    reviewer_items = []
    all_reviewer_uuids = set(reviewer_current.keys()) | set(reviewer_previous.keys())
    for uuid in all_reviewer_uuids:
        cur = reviewer_current.get(uuid, 0)
        prev = reviewer_previous.get(uuid, 0)
        if cur <= 0:
            continue
        names = name_candidates.get(uuid, {})
        reviewer_items.append({
            'uuid': uuid,
            'display_name': pick_name(names, uuid),
            'reviews_30d': cur,
            'previous_30d': prev,
            'delta_30d': cur - prev,
        })

    reviewer_items = sorted(
        reviewer_items,
        key=lambda x: (x['reviews_30d'], x['delta_30d']),
        reverse=True,
    )[:5]

    import random
    # 3 years threshold
    retired_threshold = anchor - pd.Timedelta(days=1095)
    retired_pool = []
    for c in human_contributors:
        last_act = pd.to_datetime(c.get('global_last_active'), utc=True, errors='coerce')
        if pd.isna(last_act) or last_act < retired_threshold:
            impact = c.get('impact_score')
            if impact is not None and impact > 0:
                retired_pool.append(c)
    
    retired_pool = sorted(retired_pool, key=lambda x: x.get('impact_score', 0), reverse=True)[:50]
    
    # Pick 3-5 random legends to feature each time this runs
    num_legends = min(random.randint(3, 5), len(retired_pool))
    sampled_legends = random.sample(retired_pool, num_legends)
    
    historical_legends = []
    for c in sampled_legends:
        historical_legends.append({
            'uuid': c.get('uuid'),
            'display_name': c.get('display_name') or 'Unknown',
            'impact_score': c.get('impact_score')
        })

    # Recent BIPs with period-over-period mention deltas.
    bips_index = {str(item.get('bip_id')): item for item in bips_ui if item.get('bip_id') is not None}
    author_index = build_author_index(human_contributors)

    bip_current_counts = {}
    bip_previous_counts = {}
    topic_current_counts = {}
    topic_previous_counts = {}
    if os.path.exists(SOCIAL_THREADS_PATH):
        threads_df = pd.read_parquet(SOCIAL_THREADS_PATH, columns=['date', 'bip_refs', 'category'])
        threads_df['date'] = to_utc(threads_df['date'])
        threads_df = threads_df.dropna(subset=['date'])

        cur_df = threads_df[(threads_df['date'] > current_start) & (threads_df['date'] <= anchor)]
        prev_df = threads_df[(threads_df['date'] > previous_start) & (threads_df['date'] <= current_start)]

        def fold_bips(df):
            out = {}
            for refs in df['bip_refs'].dropna():
                try:
                    items = list(refs)
                except TypeError:
                    items = [refs]
                for raw in items:
                    key = str(raw or '').strip()
                    if not key:
                        continue
                    out[key] = out.get(key, 0) + 1
            return out

        bip_current_counts = fold_bips(cur_df)
        bip_previous_counts = fold_bips(prev_df)

        def fold_topics(df):
            out = {}
            if 'category' not in df.columns:
                return out
            for category in df['category'].dropna():
                key = str(category).strip()
                if not key or key == 'other':
                    continue
                out[key] = out.get(key, 0) + 1
            return out

        topic_current_counts = fold_topics(cur_df)
        topic_previous_counts = fold_topics(prev_df)

    recent_bips = []
    for bip_id, current_mentions in sorted(bip_current_counts.items(), key=lambda x: x[1], reverse=True)[:3]:
        bip_ref = bips_index.get(bip_id, {})
        authors_text = str(bip_ref.get('authors') or '')
        primary_author = authors_text.split(',')[0].strip() if authors_text else ''
        author_uuid = author_index.get(primary_author.lower()) if primary_author else None
        prev_mentions = int(bip_previous_counts.get(bip_id, 0))

        recent_bips.append({
            'bip_id': bip_id,
            'title': bip_ref.get('title') or f'BIP {bip_id}',
            'primary_author': primary_author or 'Unknown',
            'primary_author_uuid': author_uuid,
            'mentions_30d': int(current_mentions),
            'previous_30d': prev_mentions,
            'delta_30d': int(current_mentions) - prev_mentions,
        })

    topic_items = []
    all_topics = set(topic_current_counts.keys()) | set(topic_previous_counts.keys())
    for topic in all_topics:
        current_mentions = int(topic_current_counts.get(topic, 0))
        previous_mentions = int(topic_previous_counts.get(topic, 0))
        if current_mentions <= 0:
            continue
        topic_items.append({
            'topic': topic,
            'label': topic.replace('-', ' ').title(),
            'mentions_30d': current_mentions,
            'previous_30d': previous_mentions,
            'delta_30d': current_mentions - previous_mentions,
        })

    topic_items = sorted(
        topic_items,
        key=lambda x: (x['mentions_30d'], x['delta_30d']),
        reverse=True,
    )[:5]

    # Research/social momentum current vs previous 30 days.
    research_messages_30d = 0
    research_messages_previous_30d = 0
    voices_30d = 0
    voices_previous_30d = 0
    discussions_30d = 0
    discussions_previous_30d = 0

    if not social_df.empty:
        cur = social_df[(social_df['date'] > current_start) & (social_df['date'] <= anchor)]
        prev = social_df[(social_df['date'] > previous_start) & (social_df['date'] <= current_start)]

        research_messages_30d = int(len(cur))
        research_messages_previous_30d = int(len(prev))

        voices_30d = int(cur['canonical_id'].dropna().nunique())
        voices_previous_30d = int(prev['canonical_id'].dropna().nunique())
        discussions_30d = int(len(cur))
        discussions_previous_30d = int(len(prev))

    if research_messages_previous_30d == 0 and research_messages_30d > 0:
        momentum = 'rising'
    elif research_messages_previous_30d > 0:
        ratio = research_messages_30d / float(research_messages_previous_30d)
        if ratio > 1.15:
            momentum = 'rising'
        elif ratio < 0.85:
            momentum = 'cooling'
        else:
            momentum = 'steady'
    else:
        momentum = 'steady'


    # --- 7-Day Logic ---
    current_start_7 = anchor - pd.Timedelta(days=7)
    previous_start_7 = anchor - pd.Timedelta(days=14)

    active_current_7d = count_window(global_series, current_start_7, anchor)
    active_previous_7d = count_window(global_series, previous_start_7, current_start_7)
    active_delta_7d = active_current_7d - active_previous_7d

    reviewer_current_7d = reviewer_counts_for_window(current_start_7, anchor)
    reviewer_previous_7d = reviewer_counts_for_window(previous_start_7, current_start_7)
    reviewer_items_7d = []
    all_reviewer_uuids_7 = set(reviewer_current_7d.keys()) | set(reviewer_previous_7d.keys())
    for uuid in all_reviewer_uuids_7:
        cur = reviewer_current_7d.get(uuid, 0)
        prev = reviewer_previous_7d.get(uuid, 0)
        if cur <= 0:
            continue
        names = name_candidates.get(uuid, {})
        reviewer_items_7d.append({
            'uuid': uuid,
            'display_name': pick_name(names, uuid),
            'reviews_7d': cur,
            'previous_7d': prev,
            'delta_7d': cur - prev,
        })
    reviewer_items_7d = sorted(reviewer_items_7d, key=lambda x: (x['reviews_7d'], x['delta_7d']), reverse=True)[:5]

    bip_current_counts_7d = {}
    bip_previous_counts_7d = {}
    topic_current_counts_7d = {}
    topic_previous_counts_7d = {}
    
    if os.path.exists(SOCIAL_THREADS_PATH) and 'threads_df' in locals():
        cur_df_7 = threads_df[(threads_df['date'] > current_start_7) & (threads_df['date'] <= anchor)]
        prev_df_7 = threads_df[(threads_df['date'] > previous_start_7) & (threads_df['date'] <= current_start_7)]
        bip_current_counts_7d = fold_bips(cur_df_7)
        bip_previous_counts_7d = fold_bips(prev_df_7)
        topic_current_counts_7d = fold_topics(cur_df_7)
        topic_previous_counts_7d = fold_topics(prev_df_7)
        
    recent_bips_7d = []
    for bip_id, current_mentions in sorted(bip_current_counts_7d.items(), key=lambda x: x[1], reverse=True)[:3]:
        bip_ref = bips_index.get(bip_id, {})
        authors_text = str(bip_ref.get('authors') or '')
        primary_author = authors_text.split(',')[0].strip() if authors_text else ''
        author_uuid = author_index.get(primary_author.lower()) if primary_author else None
        prev_mentions = int(bip_previous_counts_7d.get(bip_id, 0))
        recent_bips_7d.append({
            'bip_id': bip_id,
            'title': bip_ref.get('title') or f'BIP {bip_id}',
            'primary_author': primary_author or 'Unknown',
            'primary_author_uuid': author_uuid,
            'mentions_7d': int(current_mentions),
            'previous_7d': prev_mentions,
            'delta_7d': int(current_mentions) - prev_mentions,
        })
        
    topic_items_7d = []
    all_topics_7 = set(topic_current_counts_7d.keys()) | set(topic_previous_counts_7d.keys())
    for topic in all_topics_7:
        current_mentions = int(topic_current_counts_7d.get(topic, 0))
        previous_mentions = int(topic_previous_counts_7d.get(topic, 0))
        if current_mentions <= 0:
            continue
        topic_items_7d.append({
            'topic': topic,
            'label': topic.replace('-', ' ').title(),
            'mentions_7d': current_mentions,
            'previous_7d': previous_mentions,
            'delta_7d': current_mentions - previous_mentions,
        })
    topic_items_7d = sorted(topic_items_7d, key=lambda x: (x['mentions_7d'], x['delta_7d']), reverse=True)[:5]

    research_messages_7d = 0
    research_messages_previous_7d = 0
    voices_7d = 0
    voices_previous_7d = 0
    discussions_7d = 0
    discussions_previous_7d = 0
    if not social_df.empty:
        cur_7 = social_df[(social_df['date'] > current_start_7) & (social_df['date'] <= anchor)]
        prev_7 = social_df[(social_df['date'] > previous_start_7) & (social_df['date'] <= current_start_7)]
        research_messages_7d = int(len(cur_7))
        research_messages_previous_7d = int(len(prev_7))
        voices_7d = int(cur_7['canonical_id'].dropna().nunique())
        voices_previous_7d = int(prev_7['canonical_id'].dropna().nunique())
        discussions_7d = int(len(cur_7))
        discussions_previous_7d = int(len(prev_7))

    if research_messages_previous_7d == 0 and research_messages_7d > 0:
        momentum_7d = 'rising'
    elif research_messages_previous_7d > 0:
        ratio = research_messages_7d / float(research_messages_previous_7d)
        if ratio > 1.15:
            momentum_7d = 'rising'
        elif ratio < 0.85:
            momentum_7d = 'cooling'
        else:
            momentum_7d = 'steady'
    else:
        momentum_7d = 'steady'
    # Use T-1 for generated_at to clearly signify the complete data boundary
    generated_at_t1 = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    snapshot = {
        'generated_at': generated_at_t1,
        'contributors_tracked': int(registry.get('metadata', {}).get('count') or len(contributors)),
        'window_reference': {
            'anchor_timestamp': anchor.isoformat(),
            'current_window_start': current_start.isoformat(),
            'previous_window_start': previous_start.isoformat(),
            'window_days': WINDOW_DAYS,
        },
        'widgets': {
            'active_contributors_30d': {
                'value': active_current,
                'previous_30d': active_previous,
                'delta_30d': active_delta,
                'window_days': WINDOW_DAYS,
                'definition': 'Contributors with global_last_active in current 30-day window, compared to the previous 30-day window.',
                'latest_activity_date': latest_global.date().isoformat() if pd.notna(latest_global) else None,
            },
            'top_reviewers_30d': {
                'window_days': WINDOW_DAYS,
                'definition': 'Top reviewers by unique PRs reviewed in current 30-day window (comments/reviews only), with previous 30-day comparison.',
                'items': reviewer_items,
                'aliases_collapsed': aliases_collapsed,
            },
            'recent_bips': {
                'window_days': WINDOW_DAYS,
                'definition': 'Most-mentioned BIPs in current 30-day discussion window versus previous 30-day baseline.',
                'items': recent_bips,
            },
            'research_activity_30d': {
                'messages_30d': research_messages_30d,
                'previous_30d': research_messages_previous_30d,
                'delta_30d': research_messages_30d - research_messages_previous_30d,
                'momentum': momentum,
                'definition': 'Combined Delving Bitcoin and mailing list discussion volume in current 30 days versus previous 30 days.',
            },
            'discussion_voices_30d': {
                'value': voices_30d,
                'previous_30d': voices_previous_30d,
                'delta_30d': voices_30d - voices_previous_30d,
                'messages_30d': discussions_30d,
                'messages_previous_30d': discussions_previous_30d,
            },
            'topic_momentum_30d': {
                'window_days': WINDOW_DAYS,
                'definition': 'Most active research topics in current 30-day discussion window versus previous 30-day baseline.',
                'items': topic_items,
            },
            
            'active_contributors_7d': {
                'value': active_current_7d,
                'previous_7d': active_previous_7d,
                'delta_7d': active_delta_7d,
                'window_days': 7,
                'definition': 'Contributors with global_last_active in current 7-day window, compared to the previous 7-day window.',
                'latest_activity_date': latest_global.date().isoformat() if pd.notna(latest_global) else None,
            },
            'top_reviewers_7d': {
                'window_days': 7,
                'definition': 'Top reviewers by unique PRs reviewed in current 7-day window (comments/reviews only), with previous 7-day comparison.',
                'items': reviewer_items_7d,
                'aliases_collapsed': aliases_collapsed,
            },
            'recent_bips_7d': {
                'window_days': 7,
                'definition': 'Most-mentioned BIPs in current 7-day discussion window versus previous 7-day baseline.',
                'items': recent_bips_7d,
            },
            'research_activity_7d': {
                'messages_7d': research_messages_7d,
                'previous_7d': research_messages_previous_7d,
                'delta_7d': research_messages_7d - research_messages_previous_7d,
                'momentum': momentum_7d,
                'definition': 'Combined Delving Bitcoin and mailing list discussion volume in current 7 days versus previous 7 days.',
            },
            'discussion_voices_7d': {
                'value': voices_7d,
                'previous_7d': voices_previous_7d,
                'delta_7d': voices_7d - voices_previous_7d,
                'messages_7d': discussions_7d,
                'messages_previous_7d': discussions_previous_7d,
            },
            'topic_momentum_7d': {
                'window_days': 7,
                'definition': 'Most active research topics in current 7-day discussion window versus previous 7-day baseline.',
                'items': topic_items_7d,
            },
'historical_legends': {
                'definition': 'Random selection of top retired contributors by impact score to feature as ecosystem veterans.',
                'items': historical_legends,
            },
        },
        'quality': {
            'bots_excluded': len(contributors) - len(human_contributors),
            'bot_filter_tokens': list(BOT_PATTERNS),
            'active_review_event_types': sorted(list(ACTIVE_REVIEW_TYPES)),
        },
        'sources': {
            'registry_index': REGISTRY_PATH,
            'review_events': REVIEW_EVENTS_PATHS,
            'social_combined': SOCIAL_COMBINED_PATH,
            'social_threads': SOCIAL_THREADS_PATH,
            'bips_ui': BIPS_UI_PATH,
        },
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(snapshot, f, indent=2)

    print(f'Wrote {OUTPUT_PATH}')
    print(f"  active_contributors_30d={active_current} (delta={active_delta:+d})")
    print(f"  top_reviewers_30d={[x['display_name'] for x in reviewer_items]}")
    print(f"  top_topics_30d={[x['label'] for x in topic_items]}")


if __name__ == '__main__':
    generate_snapshot()
