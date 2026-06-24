"""Etsy listing -> Shopify product pusher.

This is a separate module so the Etsy Shops tab stays focused on browsing,
and Shopify push logic lives in one place that can evolve independently.

What it does
------------
* Given (etsy listing_id, target Shopify store, push options) it:
  1. Loads the cached Etsy listing data (no extra API call needed).
  2. Optionally fetches Etsy ``getListingInventory`` (1 extra call) to
     get variations + offerings.
  3. Maps Etsy data -> Shopify ``POST /admin/api/2024-10/products.json``
     body (title, body_html, vendor, tags, status, options, variants,
     images).
  4. Links variation images to variants by setting ``variant.image_id``
     using the per-variation image references Etsy provides.
  5. Records the push in ``etsy_shopify_push`` so we can detect duplicates
     and assign unique Shopify handles on intentional re-pushes.

Public API
----------
* ``push_listing(...)``  — sync push of one listing.
* ``register_routes(app, ...)`` — Flask wiring.

Design notes
------------
* No background-thread queue. The user said "I'll click cards then push" —
  pushes are interactive and short. We run them in the request handler
  with a small in-memory job map for progress polling when pushing >1.
* We never auto-skip a duplicate; the route returns ``conflict=True`` and
  the UI asks the user to confirm before the second push.
* We use Shopify REST 2024-10 because it's the version every store in
  this app already supports (verified against stored access tokens).
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import threading
import time
import uuid
from typing import Any

import requests as http_requests

log = logging.getLogger(__name__)

SHOPIFY_API_VERSION = '2024-10'


# ---------- tiny DB helper (same WAL pattern as etsy_shops_module) ----------

def _db_path(data_dir):
    return os.path.join(data_dir, 'etsy_shops.db')


class _Conn:
    """Auto-closing sqlite connection — same pattern as etsy_shops_module."""
    __slots__ = ('_path', '_c')
    def __init__(self, path): self._path = path; self._c = None
    def __enter__(self):
        c = sqlite3.connect(self._path, timeout=30)
        c.row_factory = sqlite3.Row
        try:
            c.execute('PRAGMA journal_mode=WAL')
            c.execute('PRAGMA busy_timeout=30000')
        except Exception:
            pass
        self._c = c
        return c
    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None: self._c.commit()
            else: self._c.rollback()
        finally:
            try: self._c.close()
            except Exception: pass


def _init_push_table(data_dir):
    """Tracks every successful push so the UI can show 'already pushed'
    and we can generate unique handles on intentional re-pushes."""
    with _Conn(_db_path(data_dir)) as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS etsy_shopify_push (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id INTEGER NOT NULL,
                shop_id INTEGER NOT NULL,
                target_store_id TEXT NOT NULL,
                target_domain TEXT NOT NULL,
                shopify_product_id INTEGER,
                shopify_handle TEXT,
                shopify_admin_url TEXT,
                pushed_at INTEGER NOT NULL,
                push_options TEXT,
                error TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_eshp_listing ON etsy_shopify_push(listing_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_eshp_target ON etsy_shopify_push(target_store_id, listing_id)")


# ---------- store loading ----------

def _load_stores(data_dir):
    """Return list of Shopify-capable stores from data/stores.json.

    The host server.py also reads this file (single source of truth for
    Shopify credentials). We never duplicate writes here.
    """
    path = os.path.join(data_dir, 'stores.json')
    if not os.path.isfile(path):
        return []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        log.warning('etsy-to-shopify: cannot read stores.json: %s', e)
        return []
    out = []
    seen = set()
    for s in data:
        token = (s.get('shopifyAccessToken') or '').strip()
        domain = (s.get('domain') or s.get('shopifyDomain') or '').strip()
        if not token or not domain:
            continue
        # De-dupe by domain (the file has duplicates from prior migrations).
        if domain in seen:
            continue
        seen.add(domain)
        out.append({
            'id': s.get('id') or domain,
            'name': s.get('name') or domain,
            'domain': domain,
            'access_token': token,
            'currency': s.get('currency') or '',
            'category': s.get('storeCategory') or '',
        })
    out.sort(key=lambda x: x['name'].lower())
    return out


def _find_store(data_dir, store_id):
    for s in _load_stores(data_dir):
        if s['id'] == store_id or s['domain'] == store_id:
            return s
    return None


# ---------- Shopify REST helpers ----------

class ShopifyError(RuntimeError):
    """Raised when Shopify returns 4xx/5xx. Wraps body for surfacing to UI."""
    def __init__(self, status, body):
        super().__init__(f'Shopify {status}: {body[:500] if isinstance(body, str) else body}')
        self.status = status
        self.body = body


def _shopify_request(store, method, path, body=None, params=None, retries=3):
    url = f"https://{store['domain']}/admin/api/{SHOPIFY_API_VERSION}{path}"
    headers = {
        'X-Shopify-Access-Token': store['access_token'],
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }
    last_err = None
    for attempt in range(retries):
        try:
            r = http_requests.request(method, url, params=params,
                                      data=json.dumps(body) if body is not None else None,
                                      headers=headers, timeout=45)
            if r.status_code in (200, 201):
                return r.json()
            if r.status_code == 429:
                # Shopify rate limit. Honour Retry-After.
                wait = float(r.headers.get('Retry-After') or 2)
                log.info('Shopify 429, waiting %.1fs', wait)
                time.sleep(wait)
                continue
            if r.status_code >= 500 and attempt < retries - 1:
                time.sleep(1 + attempt)
                continue
            raise ShopifyError(r.status_code, r.text)
        except http_requests.RequestException as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(1 + attempt)
                continue
            raise
    if last_err:
        raise last_err
    raise RuntimeError('Shopify request exhausted retries with no result')


# ---------- title / handle helpers ----------

_HANDLE_STRIP = re.compile(r'[^a-z0-9]+')


def _slugify_handle(text, suffix=''):
    s = (text or '').lower()
    s = _HANDLE_STRIP.sub('-', s).strip('-')
    s = s[:200]  # Shopify handle max length safety
    if suffix:
        s = f'{s}-{suffix}'
    return s or f'product-{int(time.time())}'


def _apply_title_mode(original_title, mode, prefix='', suffix=''):
    t = (original_title or '').strip()
    if mode == 'prefix':
        return f'{prefix} {t}'.strip()
    if mode == 'suffix':
        return f'{t} {suffix}'.strip()
    if mode == 'clean':
        # Remove common Etsy SEO bloat: "| Gift for Her", trailing punctuation,
        # excess whitespace. Conservative — keep first clause.
        t = re.split(r'\s[\|\u2013\u2014\-]\s', t)[0]
        return t[:255].strip()
    # 'as_is'
    return t[:255]


# ---------- pricing ----------

# Category detection keyword map. Order matters: more specific first so we
# don't mis-classify 'earring' as 'ring'. Each entry is (category, keyword_regex).
# Keywords are matched case-insensitive against the title with word boundaries.
_CATEGORY_RULES = [
    ('Earrings',  re.compile(r'\b(earring|earrings|studs?|hoops?|ear[\s-]?cuff|ear[\s-]?climber|dangles?|huggies?|jhumka|tassel earring|threader)\b', re.I)),
    ('Bracelets', re.compile(r'\b(bracelet|bracelets|bangle|bangles|cuff bracelet|tennis bracelet|charm bracelet|anklet|anklets)\b', re.I)),
    ('Necklaces', re.compile(r'\b(necklace|necklaces|pendant|pendants|choker|chokers|chain|chains|locket|lockets|lariat)\b', re.I)),
    # 'ring' last so the earring regex above wins first.
    ('Rings',     re.compile(r'\b(ring|rings|signet|band|bands|wrap ring|stacking ring|midi ring|toe ring)\b', re.I)),
]


def _detect_category(title):
    """Return one of Earrings/Rings/Bracelets/Necklaces by matching title
    keywords. Returns None if nothing matches."""
    t = title or ''
    for cat, rx in _CATEGORY_RULES:
        if rx.search(t):
            return cat
    return None


def _apply_pricing(amount_minor, divisor, mode, value=None,
                   category=None, category_prices=None):
    """amount_minor / divisor = base price. Returns Shopify variant price string.

    For mode='category', category_prices is a dict like
    ``{'Earrings': 26.99, 'Rings': 29.99, 'Bracelets': 32.99, 'Necklaces': 36.99}``
    and ``category`` selects the price. If category is None, returns None
    so the caller can skip the listing.
    """
    base = (float(amount_minor) / float(max(1, divisor))) if amount_minor else 0.0
    if mode == 'multiplier' and value:
        base = base * float(value)
    elif mode == 'fixed_add' and value:
        base = base + float(value)
    elif mode == 'fixed':
        base = float(value or 0)
    elif mode == 'category':
        if not category or not category_prices:
            return None
        price = category_prices.get(category)
        if price is None:
            return None
        base = float(price)
    # 'as_is' falls through
    return f'{base:.2f}'


def _compare_at_for_category(category, category_prices):
    """Compare-at price for a category = 2× sale price rounded to whole dollar.
    Returns string or None."""
    if not category or not category_prices:
        return None
    p = category_prices.get(category)
    if p is None:
        return None
    return f'{round(float(p) * 2):.2f}'


# ---------- Etsy data extraction ----------

def _load_cached_listing(data_dir, listing_id):
    """Read the listing's raw_json that the Etsy Shops import stored.

    raw_json contains the merged listing + image array, so we don't need
    a fresh API call for the base data.
    """
    with _Conn(_db_path(data_dir)) as c:
        row = c.execute(
            'SELECT shop_id, raw_json FROM etsy_listing WHERE listing_id=?',
            (listing_id,)
        ).fetchone()
    if not row:
        return None
    try:
        return {'shop_id': row['shop_id'], 'data': json.loads(row['raw_json'])}
    except Exception:
        return None


def _fetch_inventory(etsy_request_fn, listing_id):
    """Call Etsy getListingInventory. Returns dict or None.

    Etsy returns: { products: [ { property_values, offerings, sku, ... }, ... ],
                    price_on_property, quantity_on_property, sku_on_property }
    """
    try:
        return etsy_request_fn(f'/v3/application/listings/{listing_id}/inventory') or None
    except Exception as e:
        log.warning('etsy inventory fetch failed for %s: %s', listing_id, e)
        return None


def _resolve_inventory(data_dir, etsy_request_fn, listing_id, listing_data):
    """Get inventory for a listing using the same cascade as push_listing:
    Etsy API first (free, owner-only), then ScrapingBee page extract (cached).

    Returns None if no variations could be resolved.
    """
    inventory = _fetch_inventory(etsy_request_fn, listing_id)
    if inventory and inventory.get('products'):
        return inventory
    try:
        from etsy_page_extract import fetch_listing_variants
        price_obj = listing_data.get('price') or {}
        listing_url = (
            listing_data.get('url')
            or f'https://www.etsy.com/listing/{listing_id}/'
        )
        page_inv = fetch_listing_variants(
            data_dir,
            int(listing_id),
            listing_url=listing_url,
            base_price_amount=int(price_obj.get('amount') or 0),
            base_price_divisor=int(price_obj.get('divisor') or 100),
            base_currency=str(price_obj.get('currency_code') or 'USD'),
        )
        if page_inv and page_inv.get('products'):
            return page_inv
    except Exception as e:
        log.warning('page-extract fallback failed for listing %s: %s',
                    listing_id, e)
    return None


# ---------- ScrapingBee usage / credit tracking ----------

_SCRAPINGBEE_USAGE_CACHE = {'data': None, 'fetched_at': 0.0}
_SCRAPINGBEE_USAGE_TTL = 300  # 5 minutes
_SCRAPINGBEE_USAGE_LOCK = threading.Lock()


def _load_scrapingbee_key_from_data_dir(data_dir):
    path = os.path.join(data_dir, 'etsy_settings.json')
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return (json.load(f).get('scrapingbeeApiKey') or '').strip() or None
    except Exception:
        return None


def get_scrapingbee_usage(data_dir, force_refresh=False):
    """Return the current ScrapingBee /usage payload, augmented with derived
    fields the UI can show without recomputing.

    Cached in-process for 5 minutes so polling from the header pill stays
    cheap. Pass force_refresh=True to bypass the cache (e.g. after a push).
    """
    now = time.time()
    if not force_refresh:
        with _SCRAPINGBEE_USAGE_LOCK:
            cached = _SCRAPINGBEE_USAGE_CACHE.get('data')
            ts = _SCRAPINGBEE_USAGE_CACHE.get('fetched_at', 0.0)
            if cached and (now - ts) < _SCRAPINGBEE_USAGE_TTL:
                return cached
    api_key = _load_scrapingbee_key_from_data_dir(data_dir)
    if not api_key:
        out = {'ok': False, 'configured': False,
               'error': 'ScrapingBee API key not configured.'}
        with _SCRAPINGBEE_USAGE_LOCK:
            _SCRAPINGBEE_USAGE_CACHE['data'] = out
            _SCRAPINGBEE_USAGE_CACHE['fetched_at'] = now
        return out
    try:
        r = http_requests.get(
            'https://app.scrapingbee.com/api/v1/usage',
            headers={'Authorization': f'Bearer {api_key}'},
            timeout=10,
        )
        if r.status_code != 200:
            return {'ok': False, 'configured': True,
                    'error': f'ScrapingBee usage returned {r.status_code}',
                    'body': r.text[:200]}
        d = r.json()
    except Exception as e:
        return {'ok': False, 'configured': True,
                'error': f'{type(e).__name__}: {e}'}
    used = int(d.get('used_api_credit') or 0)
    cap = int(d.get('max_api_credit') or 0)
    remaining = max(0, cap - used)
    pct_used = (used / cap * 100.0) if cap else 0.0
    out = {
        'ok': True,
        'configured': True,
        'used_api_credit': used,
        'max_api_credit': cap,
        'remaining_api_credit': remaining,
        'percent_used': round(pct_used, 1),
        'max_concurrency': d.get('max_concurrency'),
        'current_concurrency': d.get('current_concurrency'),
        'renewal_subscription_date': d.get('renewal_subscription_date'),
        # Threshold flags so the UI doesn't have to redo the math.
        'low': remaining > 0 and pct_used >= 80,
        'critical': remaining > 0 and pct_used >= 95,
        'exhausted': remaining <= 0,
        # Estimated listings remaining at ~15 credits each (mid of 10-25 range).
        'estimated_listings_remaining': remaining // 15 if remaining else 0,
    }
    with _SCRAPINGBEE_USAGE_LOCK:
        _SCRAPINGBEE_USAGE_CACHE['data'] = out
        _SCRAPINGBEE_USAGE_CACHE['fetched_at'] = now
    return out


def summarize_inventory_options(inventory):
    """Return a UI-friendly summary of the option groups in an inventory dict.

    Output shape:
        [
          {'name': 'Gemstone', 'values': ['Rose quartz', 'Aventurine', ...]},
          {'name': 'Finish',   'values': ['Antiqued Brass', 'Antiqued Silver']},
        ]
    Preserves first-seen order from inventory.products[].property_values.
    """
    if not inventory or not inventory.get('products'):
        return []
    order = []
    seen_names = set()
    values_by_name = {}
    for p in inventory.get('products') or []:
        for pv in p.get('property_values') or []:
            name = (pv.get('property_name') or '').strip()
            if not name:
                continue
            if name not in seen_names:
                seen_names.add(name)
                order.append(name)
                values_by_name[name] = []
            vals = pv.get('values') or []
            v = (vals[0] if vals else '').strip()
            if v and v not in values_by_name[name]:
                values_by_name[name].append(v)
    return [{'name': n, 'values': values_by_name.get(n, [])} for n in order[:3]]


def _filter_inventory_by_selections(inventory, selections):
    """Keep only inventory products whose property_values fall within the
    user's selections.

    Args:
        inventory: dict, the resolved Etsy/page inventory shape
        selections: dict mapping property_name -> list[allowed value strings]
            Empty list or missing key means "keep all values for that property".

    Returns a new inventory dict with .products filtered.
    """
    if not inventory:
        return inventory
    selections = selections or {}
    # Normalize keys to stripped strings; values to sets for O(1) lookup.
    allowed = {}
    for k, vs in selections.items():
        key = (k or '').strip()
        if not key:
            continue
        if not vs:
            continue  # empty list = keep all
        allowed[key] = {str(v).strip() for v in vs if str(v).strip()}
    if not allowed:
        return inventory  # no constraints
    kept = []
    for p in inventory.get('products') or []:
        ok = True
        for pv in p.get('property_values') or []:
            name = (pv.get('property_name') or '').strip()
            if name not in allowed:
                continue
            vals = pv.get('values') or []
            v = (vals[0] if vals else '').strip()
            if v not in allowed[name]:
                ok = False
                break
        if ok:
            kept.append(p)
    new_inv = dict(inventory)
    new_inv['products'] = kept
    new_inv['_filtered'] = True
    return new_inv


# ---------- mapping: Etsy -> Shopify product payload ----------

def _build_shopify_product(listing_data, inventory, push_opts, store, handle_suffix=''):
    """Map Etsy data to the body for POST /admin/api/.../products.json.

    Returns ``({'product': {...}}, variant_image_links, category)``.
    ``category`` is one of Earrings/Rings/Bracelets/Necklaces or None and is
    returned so the caller can use it for diagnostics.
    """
    raw_title = listing_data.get('title') or ''
    title = _apply_title_mode(raw_title,
                              push_opts.get('title_mode') or 'as_is',
                              push_opts.get('title_prefix') or '',
                              push_opts.get('title_suffix') or '')

    pricing_mode = push_opts.get('pricing_mode') or 'as_is'
    category_prices = push_opts.get('category_prices') or None
    # Detect from the original Etsy title, which has the keywords.
    category = _detect_category(raw_title)
    compare_at_str = None
    if pricing_mode == 'category':
        compare_at_str = _compare_at_for_category(category, category_prices)

    description = listing_data.get('description') or ''
    # Etsy descriptions are plain text with \n. Shopify body_html wants HTML.
    # Convert blank-line paragraphs to <p>, single newlines to <br>.
    body_html = _description_to_html(description)

    tags = list(listing_data.get('tags') or [])
    extra_tags = (push_opts.get('extra_tags') or '').split(',')
    tags += [t.strip() for t in extra_tags if t.strip()]
    # Always tag with source so user can filter "imported from Etsy" later.
    tags.append('etsy-import')
    # Add lowercase category tag so tag-based smart collections match.
    if category:
        tags.append(category.lower())  # 'earrings' / 'rings' / 'bracelets' / 'necklaces'
    tags = ','.join(sorted(set(t for t in tags if t)))

    handle = _slugify_handle(title, suffix=handle_suffix)

    # Images: collect Etsy image URLs from cached listing data.
    # NOTE: Shopify will fetch each image URL itself, so we just pass URLs.
    images_in = listing_data.get('images') or []
    product_images = []
    image_idx_by_url = {}  # url -> 0-based index for variant image_id resolution
    for i, im in enumerate(images_in):
        u = im.get('url_fullxfull') or im.get('url_570xN') or im.get('url_300x300')
        if not u:
            continue
        product_images.append({
            'src': u,
            'position': i + 1,
            'alt': title[:255],
        })
        image_idx_by_url[u] = i

    # Pricing for the base (no-variant) case.
    price_obj = listing_data.get('price') or {}
    base_price_str = _apply_pricing(
        price_obj.get('amount') or 0,
        price_obj.get('divisor') or 100,
        pricing_mode,
        push_opts.get('pricing_value'),
        category=category,
        category_prices=category_prices,
    )

    # product_type: prefer explicit override, else use detected category so
    # smart collections that match by Product Type work.
    product_type = push_opts.get('product_type') or category or ''

    product = {
        'title': title,
        'body_html': body_html,
        'vendor': push_opts.get('vendor') or store['name'],
        'product_type': product_type,
        'tags': tags,
        'status': 'active' if (push_opts.get('status') or 'active') == 'active' else 'draft',
        'handle': handle,
        'images': product_images,
    }

    # Variants: derive from Etsy inventory if requested + available.
    options, variants, variant_image_links = _build_variants(
        inventory, listing_data, push_opts, base_price_str, image_idx_by_url,
        category=category, compare_at_str=compare_at_str,
    )
    if options:
        product['options'] = options
    if variants:
        product['variants'] = variants
    else:
        # Single-variant product.
        single_variant = {
            'price': base_price_str,
            'inventory_management': None,  # untracked (user explicitly said no inventory tracking)
            'requires_shipping': True,
            'taxable': True,
            'sku': (listing_data.get('sku') or [''])[0] if isinstance(listing_data.get('sku'), list) else (listing_data.get('sku') or ''),
        }
        if compare_at_str:
            single_variant['compare_at_price'] = compare_at_str
        product['variants'] = [single_variant]

    return {'product': product}, variant_image_links, category


def _description_to_html(text):
    if not text:
        return ''
    # Escape HTML-sensitive chars, then convert newlines.
    import html as _html
    s = _html.escape(text)
    # Split on 2+ newlines -> paragraphs. Single newlines -> <br>.
    paras = re.split(r'\n\s*\n', s.strip())
    return ''.join(f'<p>{p.replace(chr(10), "<br>")}</p>' for p in paras if p.strip())


def _build_variants(inventory, listing_data, push_opts, base_price_str, image_idx_by_url,
                    category=None, compare_at_str=None):
    """Translate Etsy inventory -> Shopify options/variants/image links.

    Returns (options_list, variants_list, variant_image_links).
    variant_image_links is a list of dicts {'variant_option_signature': str,
                                            'image_position': int}
    used after the POST to assign variant.image_id.
    """
    if not inventory:
        return [], [], []
    products = inventory.get('products') or []
    if not products:
        return [], [], []

    # Discover the distinct property names in the order Etsy lists them.
    # Shopify supports up to 3 options (option1, option2, option3) which
    # aligns with Etsy's June-2026 cap of 3 variations.
    prop_order = []
    prop_seen = set()
    for p in products:
        for pv in p.get('property_values') or []:
            name = (pv.get('property_name') or '').strip()
            if name and name not in prop_seen:
                prop_seen.add(name)
                prop_order.append(name)
    prop_order = prop_order[:3]
    if not prop_order:
        return [], [], []

    options = [{'name': n, 'position': i + 1, 'values': []} for i, n in enumerate(prop_order)]
    seen_values = [set() for _ in prop_order]

    pricing_mode = push_opts.get('pricing_mode') or 'as_is'
    pricing_value = push_opts.get('pricing_value')
    category_prices = push_opts.get('category_prices') or None

    variants = []
    variant_image_links = []  # to apply after creation
    for p in products:
        # Build option1/2/3 from property_values in declared order.
        opt_vals = [None] * len(prop_order)
        for pv in p.get('property_values') or []:
            name = (pv.get('property_name') or '').strip()
            if name not in prop_order:
                continue
            idx = prop_order.index(name)
            vals = pv.get('values') or []
            opt_vals[idx] = (vals[0] if vals else '').strip()
        # Fill missing slots with 'Default' so Shopify doesn't reject the variant.
        for i, v in enumerate(opt_vals):
            if not v:
                opt_vals[i] = 'Default'
            seen_values[i].add(opt_vals[i])

        # Price: Etsy offerings[0].price.amount/divisor.
        # In category mode every variant is the flat category price, so we
        # don't even look at the per-variant Etsy price.
        if pricing_mode == 'category':
            variant_price = base_price_str
        else:
            offerings = p.get('offerings') or []
            if offerings and offerings[0].get('price'):
                pr = offerings[0]['price']
                variant_price = _apply_pricing(
                    pr.get('amount') or 0,
                    pr.get('divisor') or 100,
                    pricing_mode, pricing_value,
                    category=category, category_prices=category_prices,
                )
            else:
                variant_price = base_price_str

        variant_payload = {
            'price': variant_price,
            'inventory_management': None,  # untracked per user spec
            'requires_shipping': True,
            'taxable': True,
            'sku': (p.get('sku') or '').strip() or None,
        }
        if compare_at_str:
            variant_payload['compare_at_price'] = compare_at_str
        # Position the option values into option1/2/3.
        for i, v in enumerate(opt_vals):
            variant_payload[f'option{i+1}'] = v

        variants.append(variant_payload)

        # If this product references a variation image, remember the link.
        # Etsy exposes variation images via getListingVariationImages — that's
        # a separate call we don't make here to keep push fast. As a
        # heuristic, if the product itself has an `image` field with a URL,
        # use it; otherwise the variant will inherit the product's primary
        # image (Shopify default). This is fine for most jewelry/clothing
        # shops where variations share photos.
        v_img = (p.get('image') or {}).get('url_fullxfull') if isinstance(p.get('image'), dict) else None
        if v_img and v_img in image_idx_by_url:
            variant_image_links.append({
                'option_signature': '|'.join(opt_vals),
                'image_position': image_idx_by_url[v_img] + 1,
            })

    # Populate options.values from collected sets, preserving first-seen order
    # in `products` to be deterministic.
    for opt in options:
        idx = options.index(opt)
        ordered = []
        seen = set()
        for p in products:
            for pv in p.get('property_values') or []:
                if (pv.get('property_name') or '').strip() == opt['name']:
                    vals = pv.get('values') or []
                    v = (vals[0] if vals else '').strip() or 'Default'
                    if v not in seen:
                        seen.add(v); ordered.append(v)
        if not ordered:
            ordered = ['Default']
        opt['values'] = ordered

    return options, variants, variant_image_links


# ---------- the push itself ----------

def push_listing(data_dir, etsy_request_fn, listing_id, target_store_id,
                 push_opts, *, force_duplicate=False):
    """Push one Etsy listing to one Shopify store.

    Returns dict with keys:
        ok: bool
        conflict: bool   — set when already pushed and force_duplicate=False
        shopify_product_id: int|None
        shopify_admin_url: str|None
        shopify_handle: str|None
        error: str|None
    """
    _init_push_table(data_dir)

    cached = _load_cached_listing(data_dir, listing_id)
    if not cached:
        return {'ok': False, 'error': f'Listing {listing_id} not found in cache. Import the shop first.'}

    store = _find_store(data_dir, target_store_id)
    if not store:
        return {'ok': False, 'error': f'Shopify store {target_store_id!r} not found or not connected.'}

    # Duplicate detection.
    existing = _find_existing_push(data_dir, listing_id, store['id'])
    if existing and not force_duplicate:
        return {
            'ok': False,
            'conflict': True,
            'existing': {
                'shopify_product_id': existing['shopify_product_id'],
                'shopify_admin_url': existing['shopify_admin_url'],
                'pushed_at': existing['pushed_at'],
            },
        }

    # On forced re-push, append -2, -3, ... so the new handle doesn't collide.
    handle_suffix = ''
    if existing and force_duplicate:
        n = 1 + _count_existing_pushes(data_dir, listing_id, store['id'])
        handle_suffix = str(n + 1)  # first re-push => -2

    listing_data = cached['data']

    # In category-pricing mode we must be able to detect a category from the
    # title. If we can't, skip — the user explicitly asked unmatched listings
    # to be skipped (not silently priced).
    pricing_mode = (push_opts.get('pricing_mode') or 'as_is')
    if pricing_mode == 'category':
        cat = _detect_category(listing_data.get('title') or '')
        if not cat:
            return {
                'ok': False,
                'skipped': True,
                'reason': 'no_category_match',
                'error': 'Title did not match Earrings/Rings/Bracelets/Necklaces keywords; skipped.',
                'title': listing_data.get('title') or '',
            }
        cat_prices = push_opts.get('category_prices') or {}
        if cat not in cat_prices:
            return {
                'ok': False,
                'skipped': True,
                'reason': 'category_price_missing',
                'error': f'No price configured for category {cat}; skipped.',
                'title': listing_data.get('title') or '',
                'category': cat,
            }

    # Resolve variations based on variants_mode.
    #   'all'  -> fetch via cascade (Etsy API -> ScrapingBee page) and use all
    #   'pick' -> fetch via cascade, then filter to the user's selections
    #   'none' -> skip variations entirely; single-variant product at base price
    #
    # Back-compat: legacy push payloads that only set include_variants=true/false
    # map to 'all' / 'none' respectively.
    variants_mode = (push_opts.get('variants_mode') or '').strip().lower()
    if not variants_mode:
        variants_mode = 'all' if push_opts.get('include_variants', True) else 'none'

    inventory = None
    if variants_mode in ('all', 'pick'):
        inventory = _resolve_inventory(data_dir, etsy_request_fn, listing_id, listing_data)
        if variants_mode == 'pick' and inventory:
            selections_by_listing = push_opts.get('variant_selections') or {}
            # variant_selections may be keyed by listing_id (multi-push) or
            # a flat {property: [values]} dict (single-listing convenience).
            sel = selections_by_listing.get(str(listing_id))
            if sel is None:
                sel = selections_by_listing.get(listing_id)
            if sel is None and selections_by_listing and not any(
                str(k).isdigit() for k in selections_by_listing.keys()
            ):
                sel = selections_by_listing
            inventory = _filter_inventory_by_selections(inventory, sel or {})
            if not (inventory and inventory.get('products')):
                return {
                    'ok': False,
                    'skipped': True,
                    'reason': 'no_variant_selection_match',
                    'error': 'No variants matched the selected options; nothing to push.',
                    'title': listing_data.get('title') or '',
                }

    body, variant_image_links, detected_category = _build_shopify_product(
        listing_data, inventory, push_opts, store, handle_suffix=handle_suffix,
    )

    try:
        resp = _shopify_request(store, 'POST', '/products.json', body=body)
    except ShopifyError as e:
        log.warning('Shopify push failed for listing %s: %s', listing_id, e)
        _record_push(data_dir, listing_id, cached['shop_id'], store,
                     push_opts, error=str(e))
        return {'ok': False, 'error': str(e)}

    product = resp.get('product') or {}
    product_id = product.get('id')
    handle = product.get('handle')
    admin_url = f"https://{store['domain']}/admin/products/{product_id}" if product_id else None

    # Variant image assignment (if we have links).
    if product_id and variant_image_links and product.get('variants') and product.get('images'):
        try:
            _apply_variant_images(store, product, variant_image_links)
        except Exception as e:
            log.warning('variant image link failed for %s: %s', listing_id, e)
            # Non-fatal.

    _record_push(data_dir, listing_id, cached['shop_id'], store,
                 push_opts, shopify_product_id=product_id, handle=handle,
                 admin_url=admin_url)

    return {
        'ok': True,
        'shopify_product_id': product_id,
        'shopify_admin_url': admin_url,
        'shopify_handle': handle,
        'category': detected_category,
        'title': listing_data.get('title') or '',
    }


def _apply_variant_images(store, product, variant_image_links):
    """After product creation, set variant.image_id for each linked variant.

    product.variants is the Shopify response (has option1..3 + variant id).
    product.images is the Shopify response (has id + position).
    """
    pos_to_image_id = {img['position']: img['id'] for img in product.get('images') or []}
    # Build variant signature lookup (option1|option2|option3) -> variant_id
    sig_to_variant_id = {}
    for v in product.get('variants') or []:
        sig = '|'.join([(v.get('option1') or ''), (v.get('option2') or ''), (v.get('option3') or '')]).rstrip('|')
        sig_to_variant_id[sig] = v['id']

    product_id = product['id']
    for link in variant_image_links:
        sig = link['option_signature']
        # Normalize trailing empty option slots so signatures align.
        sig_norm = sig.rstrip('|')
        variant_id = sig_to_variant_id.get(sig_norm) or sig_to_variant_id.get(sig)
        image_id = pos_to_image_id.get(link['image_position'])
        if not variant_id or not image_id:
            continue
        _shopify_request(store, 'PUT', f'/variants/{variant_id}.json',
                         body={'variant': {'id': variant_id, 'image_id': image_id}})


def _record_push(data_dir, listing_id, shop_id, store, push_opts,
                 shopify_product_id=None, handle=None, admin_url=None, error=None):
    with _Conn(_db_path(data_dir)) as c:
        c.execute("""
            INSERT INTO etsy_shopify_push
                (listing_id, shop_id, target_store_id, target_domain,
                 shopify_product_id, shopify_handle, shopify_admin_url,
                 pushed_at, push_options, error)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            listing_id, shop_id, store['id'], store['domain'],
            shopify_product_id, handle, admin_url,
            int(time.time()), json.dumps(push_opts or {}), error,
        ))


def _find_existing_push(data_dir, listing_id, store_id):
    with _Conn(_db_path(data_dir)) as c:
        r = c.execute("""
            SELECT shopify_product_id, shopify_admin_url, pushed_at
            FROM etsy_shopify_push
            WHERE listing_id=? AND target_store_id=? AND error IS NULL
            ORDER BY pushed_at DESC LIMIT 1
        """, (listing_id, store_id)).fetchone()
    return dict(r) if r else None


def _count_existing_pushes(data_dir, listing_id, store_id):
    with _Conn(_db_path(data_dir)) as c:
        r = c.execute("""
            SELECT COUNT(*) AS n FROM etsy_shopify_push
            WHERE listing_id=? AND target_store_id=? AND error IS NULL
        """, (listing_id, store_id)).fetchone()
    return r['n'] if r else 0


def list_pushes_for_listing(data_dir, listing_id):
    with _Conn(_db_path(data_dir)) as c:
        rows = c.execute("""
            SELECT target_store_id, target_domain, shopify_product_id,
                   shopify_admin_url, pushed_at, error
            FROM etsy_shopify_push WHERE listing_id=?
            ORDER BY pushed_at DESC
        """, (listing_id,)).fetchall()
    return [dict(r) for r in rows]


# ---------- Shopify collections lookup ----------

def list_collections(store, limit=250):
    """Return list of {id, title, type} for both smart + custom collections."""
    out = []
    try:
        r = _shopify_request(store, 'GET', '/custom_collections.json',
                             params={'limit': limit})
        for c in r.get('custom_collections', []):
            out.append({'id': c['id'], 'title': c.get('title') or '', 'type': 'custom'})
    except Exception as e:
        log.warning('list custom_collections failed: %s', e)
    try:
        r = _shopify_request(store, 'GET', '/smart_collections.json',
                             params={'limit': limit})
        for c in r.get('smart_collections', []):
            out.append({'id': c['id'], 'title': c.get('title') or '', 'type': 'smart'})
    except Exception as e:
        log.warning('list smart_collections failed: %s', e)
    out.sort(key=lambda x: (x['title'] or '').lower())
    return out


def add_product_to_collections(store, product_id, collection_ids):
    """Add a product to custom collections via /collects.json. Smart collections
    auto-include products by rule, so we only collect for custom ones —
    but Shopify's /collects endpoint silently accepts both, so we try all."""
    ok = 0
    for cid in collection_ids or []:
        try:
            _shopify_request(store, 'POST', '/collects.json',
                             body={'collect': {'product_id': product_id, 'collection_id': cid}})
            ok += 1
        except ShopifyError as e:
            # Smart collections will 422; that's expected.
            log.info('collect %s -> %s skipped: %s', product_id, cid, e.status)
    return ok


# ---------- Flask wiring ----------

def register_routes(app, data_dir, etsy_request_fn, login_required):
    """Mount routes under /api/etsy-shops/shopify/* so they live alongside
    the existing Etsy Shops endpoints."""
    from flask import jsonify, request

    _init_push_table(data_dir)

    @app.route('/api/etsy-shops/shopify/stores', methods=['GET'])
    @login_required
    def _ets_shopify_stores():
        stores = _load_stores(data_dir)
        # Strip token before returning.
        public = [{'id': s['id'], 'name': s['name'], 'domain': s['domain'],
                   'currency': s['currency'], 'category': s['category']}
                  for s in stores]
        return jsonify({'ok': True, 'success': True, 'stores': public})

    @app.route('/api/etsy-shops/shopify/push', methods=['POST'])
    @login_required
    def _ets_shopify_push():
        body = request.get_json(force=True, silent=True) or {}
        listing_ids = body.get('listing_ids') or []
        if not isinstance(listing_ids, list) or not listing_ids:
            return jsonify({'ok': False, 'error': 'listing_ids required'}), 400
        store_id = body.get('target_store_id') or ''
        if not store_id:
            return jsonify({'ok': False, 'error': 'target_store_id required'}), 400
        push_opts = body.get('options') or {}
        force_duplicate = bool(body.get('force_duplicate'))

        results = []
        store = _find_store(data_dir, store_id)
        if not store:
            return jsonify({'ok': False, 'error': 'store not found'}), 404
        for lid in listing_ids:
            try:
                lid_int = int(lid)
            except Exception:
                results.append({'listing_id': lid, 'ok': False, 'error': 'bad listing_id'})
                continue
            try:
                r = push_listing(data_dir, etsy_request_fn, lid_int, store_id,
                                 push_opts, force_duplicate=force_duplicate)
            except Exception as e:
                log.exception('push_listing crash for %s', lid_int)
                r = {'ok': False, 'error': f'{type(e).__name__}: {e}'}
            r['listing_id'] = lid_int
            results.append(r)

        return jsonify({
            'ok': True,
            'success': True,
            'results': results,
            'pushed': sum(1 for r in results if r.get('ok')),
            'conflicts': sum(1 for r in results if r.get('conflict')),
            'skipped': sum(1 for r in results if r.get('skipped')),
            'failed': sum(1 for r in results if not r.get('ok') and not r.get('conflict') and not r.get('skipped')),
        })

    @app.route('/api/etsy-shops/shopify/pushes/<int:listing_id>', methods=['GET'])
    @login_required
    def _ets_shopify_pushes_for_listing(listing_id):
        return jsonify({'ok': True, 'success': True,
                        'pushes': list_pushes_for_listing(data_dir, listing_id)})

    @app.route('/api/etsy-shops/shopify/preview-variants', methods=['POST'])
    @login_required
    def _ets_shopify_preview_variants():
        """Return discovered variant options per listing, for the picker UI.

        Cached listings are served free; the first preview for a listing
        costs 1 ScrapingBee credit (same as the push fallback).
        """
        body = request.get_json(force=True, silent=True) or {}
        listing_ids = body.get('listing_ids') or []
        if not isinstance(listing_ids, list) or not listing_ids:
            return jsonify({'ok': False, 'error': 'listing_ids required'}), 400
        out = []
        for lid in listing_ids:
            try:
                lid_int = int(lid)
            except Exception:
                out.append({'listing_id': lid, 'ok': False, 'error': 'bad listing_id'})
                continue
            cached = _load_cached_listing(data_dir, lid_int)
            if not cached:
                out.append({'listing_id': lid_int, 'ok': False,
                            'error': 'Listing not in cache. Import the shop first.'})
                continue
            try:
                inv = _resolve_inventory(data_dir, etsy_request_fn, lid_int, cached['data'])
            except Exception as e:
                log.exception('preview-variants crash for %s', lid_int)
                out.append({'listing_id': lid_int, 'ok': False,
                            'error': f'{type(e).__name__}: {e}'})
                continue
            options = summarize_inventory_options(inv) if inv else []
            out.append({
                'listing_id': lid_int,
                'ok': True,
                'title': (cached['data'].get('title') or '')[:160],
                'has_variants': bool(options),
                'options': options,
            })
        return jsonify({'ok': True, 'success': True, 'listings': out})

    @app.route('/api/etsy-shops/scrapingbee/usage', methods=['GET'])
    @login_required
    def _ets_scrapingbee_usage():
        force = request.args.get('refresh') in ('1', 'true', 'yes')
        return jsonify(get_scrapingbee_usage(data_dir, force_refresh=force))

    @app.route('/api/etsy-shops/scrapingbee/estimate', methods=['POST'])
    @login_required
    def _ets_scrapingbee_estimate():
        """Given a list of listing_ids, report how many are already cached
        (free) vs uncached (will cost a ScrapingBee credit each on first
        fetch). Used by the modal to warn before a big push.
        """
        try:
            from etsy_page_extract import is_listing_cached
        except Exception as e:
            return jsonify({'ok': False, 'error': f'extractor unavailable: {e}'}), 500
        body = request.get_json(force=True, silent=True) or {}
        listing_ids = body.get('listing_ids') or []
        if not isinstance(listing_ids, list):
            return jsonify({'ok': False, 'error': 'listing_ids required'}), 400
        cached_ids, uncached_ids = [], []
        for lid in listing_ids:
            try:
                lid_int = int(lid)
            except Exception:
                continue
            if is_listing_cached(data_dir, lid_int):
                cached_ids.append(lid_int)
            else:
                uncached_ids.append(lid_int)
        usage = get_scrapingbee_usage(data_dir)
        # ~15 credits per uncached listing (mid of 10-25 range).
        est_credits = len(uncached_ids) * 15
        remaining = usage.get('remaining_api_credit', 0) if usage.get('ok') else 0
        return jsonify({
            'ok': True,
            'cached_count': len(cached_ids),
            'uncached_count': len(uncached_ids),
            'estimated_credits': est_credits,
            'remaining_credits': remaining,
            'would_exceed': est_credits > remaining,
            'usage': usage,
        })

    log.info('Etsy -> Shopify push routes registered')
