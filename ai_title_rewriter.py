"""AI-powered product title rewriter using Anthropic Claude.

Long Etsy/Everful titles are keyword-stuffed dumps like:

    "Stainless Steel Necklace With Copper Zirconia Inlay Pendant Luxury
     Exquisite Elegant Fine Jewelry Accessories For Women"

We ask Claude to rewrite them into short, clean Shopify-friendly titles like:

    "Zirconia Pendant Necklace"

Design:
- Single public entry point ``rewrite_title(data_dir, source_title, *, category=None)``.
- Hard fallback: any failure (no key, network, parse) returns the original
  title. A push must never be blocked by AI.
- Sqlite cache keyed on (source_title, category) so the same listing pushed
  twice does not bill twice.
- Tight system prompt + a 60-char target to keep titles SEO-tight.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import time
from typing import Optional, Tuple

log = logging.getLogger('ai_title_rewriter')

# Model + tuning knobs. Haiku is cheapest, fast, and easily handles this task.
_MODEL = 'claude-haiku-4-5'
_MAX_TOKENS = 60        # rewrites are <= ~12 words, well under 60 output tokens
_TARGET_CHAR_MAX = 60   # we'll trim post-hoc to keep titles snappy
_CACHE_TTL_SECS = 60 * 60 * 24 * 90  # 90 days

_SYSTEM_PROMPT = (
    "You rewrite long, keyword-stuffed product titles into short, clean, "
    "shop-ready titles for a Shopify storefront. Rules:\n"
    "- Output ONLY the rewritten title, no quotes, no preamble, no period.\n"
    "- Max 60 characters total. Aim for 4-8 words.\n"
    "- Keep the most useful descriptors: material, key feature, item type.\n"
    "- Drop fluff words: 'Exquisite', 'Luxury', 'Elegant', 'For Women', "
    "'Accessories', 'Fine Jewelry', 'Fashion', etc.\n"
    "- Drop SKU codes, supplier names, and color codes like '(PBR-195G)'.\n"
    "- Drop any carat/karat/weight/measurement tokens (e.g. '0.5ct', "
    "'2 CTW', '14k', '925', '18kt', '5mm', '3 grams'). Never add a 'c', "
    "'ct', 'kt', or similar suffix on your own.\n"
    "- Use Title Case.\n"
    "- Do not invent attributes that are not in the source.\n"
)

_USER_TEMPLATE = (
    "Source title:\n{source}\n\n"
    "{category_hint}"
    "Rewrite it as a clean, short Shopify product title."
)

# ---------- sqlite cache ----------

_DB_FILENAME = 'ai_title_cache.db'


def _db_path(data_dir: str) -> str:
    return os.path.join(data_dir, _DB_FILENAME)


def _open_db(data_dir: str) -> sqlite3.Connection:
    os.makedirs(data_dir, exist_ok=True)
    conn = sqlite3.connect(_db_path(data_dir))
    conn.execute(
        """CREATE TABLE IF NOT EXISTS title_cache (
            cache_key TEXT PRIMARY KEY,
            source_title TEXT NOT NULL,
            category TEXT,
            rewritten TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            model TEXT
        )"""
    )
    return conn


def _cache_key(source_title: str, category: Optional[str]) -> str:
    raw = (source_title or '').strip().lower() + '|' + (category or '').lower()
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:32]


def _cache_get(data_dir: str, key: str) -> Optional[str]:
    try:
        conn = _open_db(data_dir)
        row = conn.execute(
            'SELECT rewritten, created_at FROM title_cache WHERE cache_key=?',
            (key,),
        ).fetchone()
        conn.close()
    except Exception:
        log.exception('cache_get failed')
        return None
    if not row:
        return None
    rewritten, created_at = row
    if int(time.time()) - int(created_at) > _CACHE_TTL_SECS:
        return None
    return rewritten


def _cache_put(data_dir: str, key: str, source_title: str,
               category: Optional[str], rewritten: str) -> None:
    try:
        conn = _open_db(data_dir)
        conn.execute(
            'INSERT OR REPLACE INTO title_cache '
            '(cache_key, source_title, category, rewritten, created_at, model) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (key, source_title, category, rewritten, int(time.time()), _MODEL),
        )
        conn.commit()
        conn.close()
    except Exception:
        log.exception('cache_put failed')


# ---------- key lookup ----------

def _load_api_key(data_dir: str) -> Optional[str]:
    """Read anthropic key from data/settings.json. Returns None if missing."""
    path = os.path.join(data_dir, 'settings.json')
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            s = json.load(f) or {}
        key = (s.get('anthropic_api_key') or '').strip()
        return key or None
    except Exception:
        log.exception('settings.json read failed')
        return None


# ---------- post-processing ----------

_STRIP_PREAMBLES = [
    r'^here(?:\'s| is)[^:]*:\s*',
    r'^rewritten title[:\-]\s*',
    r'^title[:\-]\s*',
    r'^short title[:\-]\s*',
]


# Tokens that look like carat / karat / weight / measurement units. We strip
# these as whole words from the AI output as a belt-and-braces safety net.
_UNIT_TOKEN_RE = re.compile(
    r'(?<![A-Za-z])'                          # not part of a longer word
    r'(?:'
      r'\d+(?:\.\d+)?\s*'                     # optional number prefix
      r'(?:ctw|cttw|ct|cts|carat|carats|'      # carat-like units
        r'kt|k|karat|karats|'                  # karat / gold purity
        r'mm|cm|inch|inches|in|"|\″|'      # length units
        r'g|gr|gram|grams|oz)'                 # weight units
      r'|'                                     # OR a bare unit with no number,
      r'(?:ctw|cttw)'                          # only if it is one of the most
    r')'                                       # carat-specific bare forms.
    r'(?![A-Za-z])',
    re.IGNORECASE,
)


def _clean_output(text: str) -> str:
    t = (text or '').strip()
    # Anthropic occasionally echoes a "Here's the title:" prefix; strip.
    for pat in _STRIP_PREAMBLES:
        t = re.sub(pat, '', t, flags=re.IGNORECASE)
    # Drop surrounding quotes if present.
    if len(t) >= 2 and t[0] in '"\'\u201c\u2018' and t[-1] in '"\'\u201d\u2019':
        t = t[1:-1].strip()
    # Strip carat / karat / measurement tokens (e.g. '0.5ct', '2 CTW', '14k',
    # '925' alone is a sterling silver marker that user wants gone, but we
    # only strip it if attached to common phrases like '925 Sterling').
    t = _UNIT_TOKEN_RE.sub('', t)
    # Collapse whitespace; strip trailing punctuation.
    t = re.sub(r'\s+', ' ', t)
    t = t.rstrip('.,;:!?-\u2013\u2014 ').strip()
    # Hard length cap (in case Claude over-shoots).
    if len(t) > _TARGET_CHAR_MAX:
        t = t[:_TARGET_CHAR_MAX].rsplit(' ', 1)[0].rstrip('.,;:!?-\u2013\u2014 ')
    return t


# ---------- public entry ----------

def rewrite_title(data_dir: str, source_title: str, *,
                  category: Optional[str] = None,
                  force_refresh: bool = False) -> Tuple[str, str]:
    """Return ``(final_title, source)`` where source is one of
    ``'ai'``, ``'cache'``, ``'fallback'``.

    Never raises. If the rewrite fails for any reason, returns the original
    source title with source='fallback'.
    """
    src = (source_title or '').strip()
    if not src:
        return src, 'fallback'

    # If the title is already short and clean, skip the API call entirely.
    if len(src) <= 40 and src.count(' ') <= 5:
        return src, 'fallback'

    key = _cache_key(src, category)
    if not force_refresh:
        cached = _cache_get(data_dir, key)
        if cached:
            return cached, 'cache'

    api_key = _load_api_key(data_dir)
    if not api_key:
        log.info('No anthropic_api_key in settings.json; skipping title rewrite')
        return src, 'fallback'

    try:
        # Lazy import so a missing SDK doesn't break the rest of the app.
        from anthropic import Anthropic
        client = Anthropic(api_key=api_key)
        cat_hint = f"Detected category: {category}\n\n" if category else ""
        user_msg = _USER_TEMPLATE.format(source=src, category_hint=cat_hint)
        resp = client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            system=_SYSTEM_PROMPT,
            messages=[{'role': 'user', 'content': user_msg}],
        )
        # resp.content is a list of content blocks; concatenate the text ones.
        chunks = []
        for block in (resp.content or []):
            text = getattr(block, 'text', None)
            if text:
                chunks.append(text)
        raw = '\n'.join(chunks).strip()
        cleaned = _clean_output(raw)
        if not cleaned or len(cleaned) < 3:
            log.warning('Title rewrite produced empty/short output: %r', raw)
            return src, 'fallback'
        _cache_put(data_dir, key, src, category, cleaned)
        return cleaned, 'ai'
    except Exception:
        log.exception('rewrite_title API call failed; using original')
        return src, 'fallback'


def is_enabled(data_dir: str) -> bool:
    """True iff an Anthropic key is configured."""
    return bool(_load_api_key(data_dir))
