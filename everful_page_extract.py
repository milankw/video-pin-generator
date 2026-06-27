"""Everful Wholesale product page extractor.

Mirrors the public API of ``etsy_page_extract`` so the Shopify push pipeline
(``etsy_to_shopify._resolve_inventory``, ``_build_variants``,
``_build_shopify_product``) can be reused unchanged.

Why this is much cheaper than the Etsy extractor:
  * Everful's product pages return HTTP 200 to a plain ``requests.get`` with
    a desktop User-Agent — no DataDome, no Cloudflare challenge.
  * They embed a complete ``schema.org/ProductGroup`` JSON-LD blob with all
    variants, prices, images, descriptions, and properties. No HTML scraping
    required.
  * Therefore: ZERO ScrapingBee credits per listing. The plain-curl path is
    tried first, with ScrapingBee only as an optional fallback if Everful
    ever starts gating pages.

Public surface:
  fetch_listing(data_dir, product_url, *, force_refresh=False)
      -> (listing_data, inventory) tuple matching the Etsy shapes:
         listing_data = {'title','description','images','price','tags','url','sku'}
         inventory    = {'products': [{'property_values': [...], 'offerings': [...]}]}

  is_listing_cached(data_dir, product_url) -> bool
  product_url_to_handle(url) -> str   (the trailing /products/<slug> segment)
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

# Reuse the same option-label sanitizer used for Etsy variants — strips
# trailing parenthesized prices like "Gold ($1.26)" -> "Gold".
from etsy_page_extract import clean_option_label

log = logging.getLogger('everful_page_extract')

DEFAULT_UA = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
)
EVERFUL_DOMAIN = 'everfulwholesale.com'


# ---------- sqlite cache ----------

def _db_path(data_dir: str) -> str:
    # Co-locate with the Etsy shops db so backups stay simple. A separate
    # table keeps the two ecosystems independent.
    return os.path.join(data_dir, 'etsy_shops.db')


def _init_db(data_dir: str) -> None:
    os.makedirs(data_dir, exist_ok=True)
    with sqlite3.connect(_db_path(data_dir)) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS everful_listings (
                handle           TEXT PRIMARY KEY,
                url              TEXT NOT NULL,
                listing_data     TEXT NOT NULL,
                inventory        TEXT NOT NULL,
                fetched_at       INTEGER NOT NULL
            )
            """
        )


def product_url_to_handle(url: str) -> str:
    """Return the trailing /products/<slug> segment as a stable cache key."""
    if not url:
        return ''
    m = re.search(r'/products/([A-Za-z0-9\-_]+)', url)
    if m:
        return m.group(1)
    # Fallback: last non-empty path segment.
    parts = [p for p in url.split('?')[0].rstrip('/').split('/') if p]
    return parts[-1] if parts else ''


def is_listing_cached(data_dir: str, product_url: str) -> bool:
    handle = product_url_to_handle(product_url)
    if not handle:
        return False
    _init_db(data_dir)
    with sqlite3.connect(_db_path(data_dir)) as con:
        row = con.execute(
            'SELECT 1 FROM everful_listings WHERE handle=? LIMIT 1',
            (handle,),
        ).fetchone()
    return row is not None


def _load_cached(data_dir: str, handle: str) -> Optional[Tuple[dict, dict]]:
    _init_db(data_dir)
    with sqlite3.connect(_db_path(data_dir)) as con:
        row = con.execute(
            'SELECT listing_data, inventory FROM everful_listings WHERE handle=?',
            (handle,),
        ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0]), json.loads(row[1])
    except Exception:
        return None


def _store_cached(data_dir: str, handle: str, url: str,
                  listing_data: dict, inventory: dict) -> None:
    _init_db(data_dir)
    with sqlite3.connect(_db_path(data_dir)) as con:
        con.execute(
            """
            INSERT OR REPLACE INTO everful_listings
              (handle, url, listing_data, inventory, fetched_at)
            VALUES (?,?,?,?,?)
            """,
            (
                handle,
                url,
                json.dumps(listing_data, ensure_ascii=False),
                json.dumps(inventory, ensure_ascii=False),
                int(time.time()),
            ),
        )


# ---------- fetch + parse ----------

def _http_fetch_html(url: str, *, timeout: int = 25) -> Optional[str]:
    """Try a plain GET with a desktop UA. Returns None on non-200."""
    try:
        r = requests.get(
            url,
            headers={
                'User-Agent': DEFAULT_UA,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            },
            timeout=timeout,
        )
        if r.status_code == 200 and r.text:
            return r.text
        log.warning('everful direct fetch %s returned %s', url, r.status_code)
        return None
    except Exception as e:
        log.warning('everful direct fetch error %s: %s', url, e)
        return None


_LDJSON_RE = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)


def _parse_jsonld_product_group(html: str) -> Optional[dict]:
    """Find the ProductGroup entry inside <script type='application/ld+json'>."""
    for raw in _LDJSON_RE.findall(html):
        try:
            data = json.loads(raw.strip())
        except Exception:
            continue
        # Two shapes seen in the wild: top-level dict OR {'@graph': [...]}.
        candidates: List[dict] = []
        if isinstance(data, dict):
            if '@graph' in data and isinstance(data['@graph'], list):
                candidates.extend(d for d in data['@graph'] if isinstance(d, dict))
            else:
                candidates.append(data)
        elif isinstance(data, list):
            candidates.extend(d for d in data if isinstance(d, dict))
        for c in candidates:
            t = c.get('@type')
            if t == 'ProductGroup' or (isinstance(t, list) and 'ProductGroup' in t):
                return c
        # Fallback: bare Product with offers — still usable (no variants).
        for c in candidates:
            t = c.get('@type')
            if t == 'Product' or (isinstance(t, list) and 'Product' in t):
                return c
    return None


def _strip_html(s: Any) -> str:
    if not s:
        return ''
    if not isinstance(s, str):
        s = str(s)
    return re.sub(r'<[^>]+>', '', s).strip()


def _to_price_amount_minor(price: Any) -> int:
    """Convert a JSON-LD price string (e.g. '1.26', '0.75') to minor units (126, 75)."""
    if price is None:
        return 0
    s = str(price).strip().replace(',', '')
    try:
        return int(round(float(s) * 100))
    except Exception:
        return 0


def _variation_property_names(product_group: dict) -> List[str]:
    """Return the option-property names the variants vary by, e.g. ['Color','Style']."""
    raw = product_group.get('variesBy') or []
    if isinstance(raw, str):
        raw = [raw]
    out: List[str] = []
    for v in raw:
        if not v:
            continue
        s = str(v)
        # schema.org URLs -> last path segment, title-cased.
        if s.startswith('http'):
            seg = s.rsplit('/', 1)[-1]
            out.append(seg.replace('_', ' ').replace('-', ' ').title())
        else:
            out.append(s)
    # Dedupe while preserving order.
    seen = set(); uniq = []
    for n in out:
        if n.lower() not in seen:
            seen.add(n.lower()); uniq.append(n)
    return uniq[:3]  # Shopify only supports 3 options anyway.


def _value_from_variant(variant: dict, prop_name: str) -> str:
    """Pull the option value for a given property name from a variant dict.

    Tries (in order):
      1. lowercase key on variant (e.g. variant['color'])
      2. additionalProperty list with @type=PropertyValue
      3. fallback: the variant 'name' minus the parent product name
    """
    key_lc = prop_name.lower()
    # 1. Direct key.
    for k, v in variant.items():
        if k.lower() == key_lc and isinstance(v, (str, int, float)):
            return clean_option_label(str(v))
    # 2. additionalProperty.
    for p in variant.get('additionalProperty') or []:
        if not isinstance(p, dict):
            continue
        if (p.get('name') or '').strip().lower() == key_lc:
            return clean_option_label(str(p.get('value') or ''))
    return ''


def _normalize_jsonld_to_pipeline(product_group: dict,
                                   source_url: str) -> Tuple[dict, dict]:
    """Convert a ProductGroup JSON-LD payload to the Etsy-shaped listing_data
    + inventory dicts the Shopify push pipeline already understands.
    """
    name = product_group.get('name') or ''
    description = _strip_html(product_group.get('description') or '')
    images_raw = product_group.get('image') or []
    if isinstance(images_raw, str):
        images_raw = [images_raw]

    # Build listing_data.images in Etsy's shape so _build_shopify_product
    # can read .url_fullxfull / .url_570xN unchanged.
    images = []
    for u in images_raw:
        if not u:
            continue
        u = str(u)
        images.append({
            'url_fullxfull': u,
            'url_570xN': u,
            'url_300x300': u,
        })

    # additionalProperty -> tags so smart collections still match.
    tags: List[str] = []
    for p in product_group.get('additionalProperty') or []:
        if not isinstance(p, dict):
            continue
        val = (p.get('value') or '').strip()
        if not val:
            continue
        # Split multi-value entries like "Copper, Zircon, Stainless Steel".
        for piece in re.split(r'[;,]', val):
            piece = piece.strip()
            if piece and 2 <= len(piece) <= 40:
                tags.append(piece)
    # Dedupe (case-insensitive), preserve order.
    seen = set(); uniq_tags = []
    for t in tags:
        k = t.lower()
        if k not in seen:
            seen.add(k); uniq_tags.append(t)

    # Top-level price = lowest variant price (Shopify wants minor units).
    offers = product_group.get('offers') or {}
    low = offers.get('lowPrice') or offers.get('price') or '0'
    currency = offers.get('priceCurrency') or 'USD'
    base_amount_minor = _to_price_amount_minor(low)

    # Try to pull a top-level SKU (rare on a ProductGroup, common on Product).
    sku_top = product_group.get('sku') or product_group.get('mpn') or ''

    listing_data = {
        'title': name,
        'description': description,
        'images': images,
        'price': {
            'amount': base_amount_minor,
            'divisor': 100,
            'currency_code': currency,
        },
        'tags': uniq_tags[:30],  # cap tag count
        'url': source_url,
        'sku': sku_top if isinstance(sku_top, str) else '',
    }

    # ---- Inventory: hasVariant -> products[].property_values + offerings ----
    prop_names = _variation_property_names(product_group)
    variants = product_group.get('hasVariant') or []
    products: List[dict] = []
    if variants and prop_names:
        for v in variants:
            if not isinstance(v, dict):
                continue
            prop_values = []
            for n in prop_names:
                val = _value_from_variant(v, n)
                if not val:
                    val = 'Default'
                prop_values.append({
                    'property_name': n,
                    'values': [val],
                })
            offers_v = v.get('offers') or {}
            price_v = offers_v.get('price') if isinstance(offers_v, dict) else None
            amt_minor = _to_price_amount_minor(price_v) if price_v else base_amount_minor
            sku_v = v.get('sku') or v.get('mpn') or ''
            img_v = v.get('image')
            if isinstance(img_v, list):
                img_v = img_v[0] if img_v else None
            offerings = [{
                'offering_id': sku_v or None,
                'price': {
                    'amount': amt_minor,
                    'divisor': 100,
                    'currency_code': currency,
                },
                'quantity': 999,  # Everful doesn't expose per-variant qty publicly
                'is_enabled': True,
                'sku': sku_v,
                'image_url': img_v,
            }]
            products.append({
                'product_id': sku_v or None,
                'sku': sku_v,
                'property_values': prop_values,
                'offerings': offerings,
            })
    inventory = {'products': products}

    return listing_data, inventory


# ---------- public entry point ----------

def fetch_listing(data_dir: str, product_url: str,
                  *, force_refresh: bool = False) -> Optional[Tuple[dict, dict]]:
    """Fetch and parse an Everful product page.

    Returns ``(listing_data, inventory)`` on success, or ``None`` if the page
    couldn't be fetched / parsed.

    Caches forever in sqlite. ``force_refresh=True`` re-fetches.
    """
    if not product_url:
        return None
    # Normalize: ensure it points at everfulwholesale.com.
    if EVERFUL_DOMAIN not in product_url:
        log.warning('fetch_listing called with non-Everful url: %s', product_url)
        return None

    handle = product_url_to_handle(product_url)
    if not handle:
        return None

    if not force_refresh:
        cached = _load_cached(data_dir, handle)
        if cached:
            return cached

    html = _http_fetch_html(product_url)
    if not html:
        return None
    pg = _parse_jsonld_product_group(html)
    if not pg:
        log.warning('no ProductGroup JSON-LD found in %s', product_url)
        return None

    listing_data, inventory = _normalize_jsonld_to_pipeline(pg, product_url)
    if not listing_data.get('title'):
        log.warning('empty title parsed from %s', product_url)
        return None

    _store_cached(data_dir, handle, product_url, listing_data, inventory)
    return listing_data, inventory


# Convenience wrapper that returns just the inventory (for symmetry with
# the Etsy extractor's name).
def fetch_listing_variants(data_dir: str, product_url: str,
                            *, force_refresh: bool = False) -> Optional[dict]:
    result = fetch_listing(data_dir, product_url, force_refresh=force_refresh)
    return result[1] if result else None
