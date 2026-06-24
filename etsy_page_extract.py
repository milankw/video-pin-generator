"""
Etsy public listing-page extractor.

The Etsy v3 API's /listings/{id}/inventory endpoint is owner-only — it returns
404 for any listing we don't own. So for third-party listings we read the
public listing page through ScrapingBee (residential proxy + JS rendering +
Cloudflare bypass) and parse the variation <select> dropdowns ourselves.

Each extraction costs 1 ScrapingBee credit. We cache aggressively in a
sqlite table (etsy_listing_variants) so re-pushes and pushes to additional
stores never re-spend a credit on the same listing.

Returns data in the same shape as Etsy's /inventory endpoint
(`{'products': [...], ...}`) so the existing _build_variants() code in
etsy_to_shopify.py works without modification.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

log = logging.getLogger('etsy_page_extract')

SCRAPINGBEE_ENDPOINT = 'https://app.scrapingbee.com/api/v1/'


# ---------- sqlite cache ----------

def _db_path(data_dir: str) -> str:
    return os.path.join(data_dir, 'etsy_shops.db')


def _init_variant_cache(data_dir: str) -> None:
    """Create the variant cache table if missing."""
    con = sqlite3.connect(_db_path(data_dir), timeout=30)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS etsy_listing_variants (
                listing_id   INTEGER PRIMARY KEY,
                fetched_at   INTEGER NOT NULL,
                source       TEXT NOT NULL,           -- 'scrapingbee' / 'api' / 'manual'
                inventory    TEXT NOT NULL,           -- json: same shape as Etsy /inventory
                raw_page_url TEXT,                    -- the URL we read
                error        TEXT
            )
        """)
        con.commit()
    finally:
        con.close()


def is_listing_cached(data_dir: str, listing_id: int) -> bool:
    """True if the listing already has an entry in the variant cache.

    Used by the UI to estimate how many ScrapingBee credits a push would cost.
    """
    try:
        _init_variant_cache(data_dir)
        con = sqlite3.connect(_db_path(data_dir), timeout=30)
        try:
            row = con.execute(
                'SELECT 1 FROM etsy_listing_variants WHERE listing_id=?',
                (int(listing_id),),
            ).fetchone()
        finally:
            con.close()
        return row is not None
    except Exception:
        return False


def _get_cached_inventory(data_dir: str, listing_id: int) -> Optional[Dict[str, Any]]:
    """Return cached inventory dict if we've already extracted this listing."""
    con = sqlite3.connect(_db_path(data_dir), timeout=30)
    con.row_factory = sqlite3.Row
    try:
        row = con.execute(
            'SELECT inventory, error FROM etsy_listing_variants WHERE listing_id=?',
            (listing_id,)
        ).fetchone()
    finally:
        con.close()
    if not row:
        return None
    if row['error']:  # Don't reuse error-state rows; let caller retry.
        return None
    try:
        return json.loads(row['inventory'])
    except Exception:
        return None


def _store_cached_inventory(data_dir: str, listing_id: int, inventory: Dict[str, Any],
                            source: str = 'scrapingbee', page_url: Optional[str] = None,
                            error: Optional[str] = None) -> None:
    con = sqlite3.connect(_db_path(data_dir), timeout=30)
    try:
        con.execute("""
            INSERT INTO etsy_listing_variants
                (listing_id, fetched_at, source, inventory, raw_page_url, error)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(listing_id) DO UPDATE SET
                fetched_at = excluded.fetched_at,
                source = excluded.source,
                inventory = excluded.inventory,
                raw_page_url = excluded.raw_page_url,
                error = excluded.error
        """, (listing_id, int(time.time()), source,
              json.dumps(inventory or {}), page_url, error))
        con.commit()
    finally:
        con.close()


# ---------- ScrapingBee fetch ----------

def _load_scrapingbee_key(data_dir: str) -> Optional[str]:
    """Load the ScrapingBee API key from etsy_settings.json."""
    path = os.path.join(data_dir, 'etsy_settings.json')
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            settings = json.load(f)
        return (settings.get('scrapingbeeApiKey') or '').strip() or None
    except Exception as e:
        log.warning('failed to load scrapingbee key: %s', e)
        return None


def _scrapingbee_fetch_html(api_key: str, url: str, timeout: int = 120) -> Optional[str]:
    """Fetch a URL through ScrapingBee with JS rendering. Returns HTML or None."""
    # Etsy uses DataDome bot defense — basic proxy gets 403. Premium proxy (10-25 credits)
    # rotates through residential IPs and clears DataDome reliably. Stealth is overkill (75c).
    params = {
        'api_key': api_key,
        'url': url,
        'render_js': 'true',
        'wait': '2500',                # wait 2.5s after page load for JS-rendered selects
        'premium_proxy': 'true',       # required to bypass Etsy's DataDome challenge
        'country_code': 'us',          # US IPs see full variant UI; some EU IPs get redirects
    }
    try:
        r = requests.get(SCRAPINGBEE_ENDPOINT, params=params, timeout=timeout)
    except requests.RequestException as e:
        log.warning('scrapingbee request failed: %s', e)
        return None
    if r.status_code != 200:
        log.warning('scrapingbee returned %s for %s (body: %s)',
                    r.status_code, url, r.text[:200])
        return None
    return r.text


# ---------- HTML parsing ----------

def _parse_variations_from_html(html: str) -> List[Dict[str, Any]]:
    """Find all variation <select> dropdowns and their options.

    Returns:
        [{property_name: str, options: [{value_id, label, price?, image_id?}]}, ...]
    """
    soup = BeautifulSoup(html, 'html.parser')
    out: List[Dict[str, Any]] = []
    for sel in soup.select('select[id^="variation-selector-"]'):
        sid = sel.get('id', '')
        # Etsy's accessible label is on a sibling <label for="...">.
        label_el = soup.find('label', attrs={'for': sid})
        prop_name = (label_el.get_text(strip=True) if label_el else '').strip() or 'Option'

        options: List[Dict[str, Any]] = []
        for o in sel.find_all('option'):
            val = (o.get('value') or '').strip()
            if not val or o.has_attr('disabled'):
                continue
            label = o.get_text(strip=True)
            opt: Dict[str, Any] = {'value_id': val, 'label': label}
            # Etsy sometimes embeds per-option price/image hints as data-* attrs.
            for attr in ('data-price', 'data-image-id', 'data-quantity',
                         'data-is-available'):
                if o.get(attr) is not None:
                    opt[attr.replace('data-', '')] = o.get(attr)
            options.append(opt)
        if options:
            out.append({'property_name': prop_name, 'options': options})
    return out


def _expand_to_inventory_shape(variations: List[Dict[str, Any]],
                               base_price_amount: int,
                               base_price_divisor: int,
                               base_currency: str) -> Dict[str, Any]:
    """Turn the parsed variation list into the same `products[]` shape
    that Etsy's /inventory endpoint returns.

    We do a cartesian product across all variation properties so each
    combination becomes one product entry. _build_variants() in
    etsy_to_shopify.py iterates these and emits one Shopify variant per row.

    Note: Etsy public pages don't expose per-combination pricing in the
    dropdowns (per-variant prices are an owner-only field), so every
    product gets the listing's base price. Category pricing in the push
    flow then overrides all of these anyway.
    """
    if not variations:
        return {'products': []}

    # Cap at 3 properties to match Shopify's 3-option limit and Etsy's 2026 cap.
    variations = variations[:3]

    # Cartesian product across properties.
    combos: List[List[Dict[str, Any]]] = [[]]
    for v in variations:
        new_combos: List[List[Dict[str, Any]]] = []
        for c in combos:
            for opt in v['options']:
                new_combos.append(c + [{
                    'property_name': v['property_name'],
                    'option': opt,
                }])
        combos = new_combos

    # Shopify hard-caps at 100 variants per product. If we'd exceed, truncate.
    MAX_VARIANTS = 100
    truncated = False
    if len(combos) > MAX_VARIANTS:
        truncated = True
        combos = combos[:MAX_VARIANTS]

    products = []
    for combo in combos:
        property_values = []
        for slot in combo:
            property_values.append({
                'property_name': slot['property_name'],
                'value_ids': [slot['option']['value_id']],
                'values': [slot['option']['label']],
            })
        # Per-variant price: use the option's data-price if present, else base.
        price_amount = base_price_amount
        for slot in combo:
            dp = slot['option'].get('price')
            if dp:
                try:
                    # data-price is usually a string in major units, e.g. "27.00"
                    price_amount = int(round(float(dp) * base_price_divisor))
                    break
                except Exception:
                    pass
        products.append({
            'property_values': property_values,
            'offerings': [{
                'price': {
                    'amount': price_amount,
                    'divisor': base_price_divisor,
                    'currency_code': base_currency,
                },
                'quantity': 1,
                'is_enabled': True,
            }],
            'sku': '',
        })

    return {
        'products': products,
        '_extract_source': 'scrapingbee_page',
        '_truncated': truncated,
        '_property_names': [v['property_name'] for v in variations],
    }


# ---------- public entry ----------

def fetch_listing_variants(
    data_dir: str,
    listing_id: int,
    listing_url: str,
    base_price_amount: int = 0,
    base_price_divisor: int = 100,
    base_currency: str = 'USD',
    *,
    force_refresh: bool = False,
) -> Optional[Dict[str, Any]]:
    """Return inventory-shaped dict for a third-party Etsy listing.

    Reads from cache first (free). On cache miss, calls ScrapingBee (1 credit)
    and stores the result. Returns None if there are no variations or the
    fetch failed.
    """
    _init_variant_cache(data_dir)

    if not force_refresh:
        cached = _get_cached_inventory(data_dir, listing_id)
        if cached is not None:
            log.info('etsy_page_extract: cache hit for listing %s (%d products)',
                     listing_id, len(cached.get('products') or []))
            return cached

    api_key = _load_scrapingbee_key(data_dir)
    if not api_key:
        log.warning('etsy_page_extract: no ScrapingBee API key configured; '
                    'cannot extract variants for listing %s', listing_id)
        return None

    log.info('etsy_page_extract: fetching listing %s via ScrapingBee', listing_id)
    html = _scrapingbee_fetch_html(api_key, listing_url)
    if not html:
        _store_cached_inventory(data_dir, listing_id, {}, source='scrapingbee',
                                page_url=listing_url, error='fetch_failed')
        return None

    variations = _parse_variations_from_html(html)
    if not variations:
        log.info('etsy_page_extract: no variations found on listing %s', listing_id)
        # Cache empty result so we don't keep spending credits on no-variant listings.
        inv = {'products': [], '_extract_source': 'scrapingbee_page',
               '_property_names': []}
        _store_cached_inventory(data_dir, listing_id, inv,
                                source='scrapingbee', page_url=listing_url)
        return inv

    inventory = _expand_to_inventory_shape(
        variations,
        base_price_amount=base_price_amount,
        base_price_divisor=base_price_divisor,
        base_currency=base_currency,
    )
    log.info('etsy_page_extract: listing %s -> %d variant combos across %s',
             listing_id, len(inventory.get('products') or []),
             inventory.get('_property_names'))
    _store_cached_inventory(data_dir, listing_id, inventory,
                            source='scrapingbee', page_url=listing_url)
    return inventory
