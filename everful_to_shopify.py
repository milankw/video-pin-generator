"""Everful Wholesale -> Shopify push pipeline.

Thin wrapper around the existing Etsy push machinery in
``etsy_to_shopify``. Reuses:

  * _build_shopify_product   (title/desc/images/tags/options/variants -> Shopify body)
  * _build_variants          (variant options + price + image_id resolution)
  * _detect_category         (Earrings/Rings/Bracelets/Necklaces routing)
  * _apply_pricing           (category-based fixed prices, compare-at = 2x)
  * _apply_title_mode        (prefix/suffix support)
  * _filter_inventory_by_selections  (variant picker)
  * summarize_inventory_options       (for picker preview UI)
  * _shopify_request, _find_store, ShopifyError

The Everful-specific bits:
  * everful_page_extract.fetch_listing parses JSON-LD into the same shapes.
  * Separate push-history table (everful_shopify_push) keyed by URL handle.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from typing import Any, Dict, List, Optional

# Reuse Etsy push primitives unchanged.
from etsy_to_shopify import (
    _build_shopify_product,
    _filter_inventory_by_selections,
    _find_store,
    _shopify_request,
    summarize_inventory_options,
    _detect_category,
    ShopifyError,
)
from everful_page_extract import (
    EVERFUL_DOMAIN,
    fetch_listing,
    is_listing_cached,
    product_url_to_handle,
)

log = logging.getLogger('everful_to_shopify')


# ---------- sqlite push-history table (parallel to etsy_shopify_push) ----------

def _db_path(data_dir: str) -> str:
    return os.path.join(data_dir, 'etsy_shops.db')


def _init_push_table(data_dir: str) -> None:
    os.makedirs(data_dir, exist_ok=True)
    with sqlite3.connect(_db_path(data_dir)) as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS everful_shopify_push (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                product_handle    TEXT NOT NULL,
                product_url       TEXT NOT NULL,
                target_store_id   TEXT NOT NULL,
                target_domain     TEXT NOT NULL,
                shopify_product_id INTEGER,
                shopify_handle    TEXT,
                shopify_admin_url TEXT,
                pushed_at         INTEGER NOT NULL,
                push_options      TEXT,
                error             TEXT
            )
            """
        )
        c.execute(
            'CREATE INDEX IF NOT EXISTS idx_evshp_handle '
            'ON everful_shopify_push(product_handle)'
        )
        c.execute(
            'CREATE INDEX IF NOT EXISTS idx_evshp_target '
            'ON everful_shopify_push(target_store_id, product_handle)'
        )


def _find_existing_push(data_dir: str, handle: str, store_id: str) -> Optional[dict]:
    with sqlite3.connect(_db_path(data_dir)) as c:
        c.row_factory = sqlite3.Row
        row = c.execute(
            """SELECT * FROM everful_shopify_push
               WHERE product_handle=? AND target_store_id=?
                 AND shopify_product_id IS NOT NULL
               ORDER BY pushed_at DESC LIMIT 1""",
            (handle, store_id),
        ).fetchone()
    return dict(row) if row else None


def _count_existing_pushes(data_dir: str, handle: str, store_id: str) -> int:
    with sqlite3.connect(_db_path(data_dir)) as c:
        return c.execute(
            """SELECT COUNT(*) FROM everful_shopify_push
               WHERE product_handle=? AND target_store_id=?
                 AND shopify_product_id IS NOT NULL""",
            (handle, store_id),
        ).fetchone()[0] or 0


def _record_push(data_dir: str, handle: str, product_url: str, store: dict,
                  push_opts: dict, *, shopify_product_id=None,
                  shopify_handle=None, shopify_admin_url=None,
                  error: Optional[str] = None) -> None:
    with sqlite3.connect(_db_path(data_dir)) as c:
        c.execute(
            """INSERT INTO everful_shopify_push
               (product_handle, product_url, target_store_id, target_domain,
                shopify_product_id, shopify_handle, shopify_admin_url,
                pushed_at, push_options, error)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                handle, product_url, store['id'], store['domain'],
                shopify_product_id, shopify_handle, shopify_admin_url,
                int(time.time()), json.dumps(push_opts, ensure_ascii=False),
                error,
            ),
        )


def list_pushes_for_product(data_dir: str, handle: str) -> List[dict]:
    with sqlite3.connect(_db_path(data_dir)) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute(
            """SELECT * FROM everful_shopify_push
               WHERE product_handle=?
               ORDER BY pushed_at DESC""",
            (handle,),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------- input normalization ----------

_URL_LINE_RE = re.compile(r'https?://[^\s,;]+', re.IGNORECASE)


def parse_urls_input(raw: Any) -> List[str]:
    """Accept a list of URLs or a multi-line string; return clean URL list."""
    out: List[str] = []
    if isinstance(raw, list):
        for s in raw:
            if isinstance(s, str):
                out.extend(_URL_LINE_RE.findall(s))
    elif isinstance(raw, str):
        out.extend(_URL_LINE_RE.findall(raw))
    # Filter to Everful domain only and dedupe, preserving order.
    seen = set(); uniq = []
    for u in out:
        u = u.strip().rstrip('.,;')
        if EVERFUL_DOMAIN not in u:
            continue
        if u in seen:
            continue
        seen.add(u); uniq.append(u)
    return uniq


# ---------- push entry point ----------

def push_product_url(data_dir: str, product_url: str, target_store_id: str,
                     push_opts: dict, *, force_duplicate: bool = False) -> dict:
    """Push a single Everful product URL into a Shopify store.

    Returns the same shape ``etsy_to_shopify.push_listing`` returns.
    """
    _init_push_table(data_dir)

    handle_key = product_url_to_handle(product_url)
    if not handle_key:
        return {'ok': False, 'error': f'Could not derive a handle from URL: {product_url!r}'}

    store = _find_store(data_dir, target_store_id)
    if not store:
        return {'ok': False, 'error': f'Shopify store {target_store_id!r} not found or not connected.'}

    # Duplicate detection — same handle + same target store.
    existing = _find_existing_push(data_dir, handle_key, store['id'])
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
    handle_suffix = ''
    if existing and force_duplicate:
        n = 1 + _count_existing_pushes(data_dir, handle_key, store['id'])
        handle_suffix = str(n + 1)

    # Fetch + parse (free; cached forever after first fetch).
    parsed = fetch_listing(data_dir, product_url)
    if not parsed:
        return {'ok': False, 'error': f'Could not fetch or parse Everful product page: {product_url}'}
    listing_data, inventory = parsed

    # Enforce category match when in category-pricing mode (same as Etsy path).
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

    # Variant mode handling.
    variants_mode = (push_opts.get('variants_mode') or '').strip().lower()
    if not variants_mode:
        variants_mode = 'all' if push_opts.get('include_variants', True) else 'none'

    used_inventory: Optional[dict] = None
    if variants_mode in ('all', 'pick'):
        used_inventory = inventory
        if variants_mode == 'pick':
            sels = push_opts.get('variant_selections') or {}
            # Accept keyed-by-handle or flat {prop: [values]}.
            sel = sels.get(handle_key)
            if sel is None and sels and not any(isinstance(k, str) and ('/' in k or '-' in k) for k in sels.keys()):
                sel = sels  # treat as flat dict
            # Also support keying by product_url.
            if sel is None:
                sel = sels.get(product_url)
            used_inventory = _filter_inventory_by_selections(inventory, sel or {})
            if not (used_inventory and used_inventory.get('products')):
                return {
                    'ok': False,
                    'skipped': True,
                    'reason': 'no_variant_selection_match',
                    'error': 'No variants matched the selected options; nothing to push.',
                    'title': listing_data.get('title') or '',
                }

    # Hard cap at Shopify's 100-variant limit. If user picked too many in
    # 'all' or 'pick' mode, truncate and surface a warning.
    truncated = False
    if used_inventory and used_inventory.get('products'):
        prods = used_inventory['products']
        if len(prods) > 100:
            used_inventory = dict(used_inventory)
            used_inventory['products'] = prods[:100]
            truncated = True

    body, _variant_image_links, detected_category = _build_shopify_product(
        listing_data, used_inventory, push_opts, store, handle_suffix=handle_suffix,
    )

    # Swap the source tag from 'etsy-import' to 'everful-import' so the user
    # can filter by source in Shopify. _build_shopify_product hardcodes
    # 'etsy-import'; we patch it after construction to keep that helper untouched.
    product = body.get('product') or {}
    tags = product.get('tags') or ''
    tag_list = [t.strip() for t in tags.split(',') if t.strip()]
    tag_list = [t for t in tag_list if t.lower() != 'etsy-import']
    if 'everful-import' not in {t.lower() for t in tag_list}:
        tag_list.append('everful-import')
    product['tags'] = ','.join(sorted(set(tag_list)))

    try:
        resp = _shopify_request(store, 'POST', '/products.json', body=body)
    except ShopifyError as e:
        log.warning('Shopify push failed for %s: %s', product_url, e)
        _record_push(data_dir, handle_key, product_url, store, push_opts,
                     error=str(e))
        return {'ok': False, 'error': str(e)}
    except Exception as e:
        log.exception('Unexpected Shopify push error for %s', product_url)
        _record_push(data_dir, handle_key, product_url, store, push_opts,
                     error=f'{type(e).__name__}: {e}')
        return {'ok': False, 'error': f'{type(e).__name__}: {e}'}

    sp = resp.get('product') or {}
    product_id = sp.get('id')
    shopify_handle = sp.get('handle')
    admin_url = (
        f"https://{store['domain']}/admin/products/{product_id}"
        if product_id else None
    )

    _record_push(
        data_dir, handle_key, product_url, store, push_opts,
        shopify_product_id=product_id,
        shopify_handle=shopify_handle,
        shopify_admin_url=admin_url,
    )

    out = {
        'ok': True,
        'shopify_product_id': product_id,
        'shopify_admin_url': admin_url,
        'shopify_handle': shopify_handle,
        'title': listing_data.get('title') or '',
        'category': detected_category,
        'variant_count': len((used_inventory or {}).get('products') or []),
    }
    if truncated:
        out['warning'] = 'Variant count exceeded Shopify\'s 100 limit; truncated to first 100.'
    return out


# ---------- Flask routes ----------

def register_routes(app, data_dir: str, login_required) -> None:
    """Mount Everful routes under /api/everful/*.

    Endpoints:
      POST /api/everful/preview-variants  body: {urls: [...]}
      POST /api/everful/shopify/push      body: {urls, target_store_id, options, force_duplicate}
      GET  /api/everful/shopify/pushes    ?handle=<handle>
    """
    from flask import jsonify, request

    _init_push_table(data_dir)

    @app.route('/api/everful/preview-variants', methods=['POST'])
    @login_required
    def _ev_preview_variants():
        body = request.get_json(force=True, silent=True) or {}
        urls = parse_urls_input(body.get('urls') or body.get('product_urls') or [])
        if not urls:
            return jsonify({'ok': False, 'error': 'No valid Everful URLs in input.'}), 400
        out = []
        for u in urls:
            handle = product_url_to_handle(u)
            try:
                parsed = fetch_listing(data_dir, u)
            except Exception as e:
                log.exception('preview-variants crash for %s', u)
                out.append({'product_url': u, 'handle': handle, 'ok': False,
                            'error': f'{type(e).__name__}: {e}'})
                continue
            if not parsed:
                out.append({'product_url': u, 'handle': handle, 'ok': False,
                            'error': 'Could not fetch/parse product page.'})
                continue
            ld, inv = parsed
            options = summarize_inventory_options(inv) if inv else []
            out.append({
                'product_url': u,
                'handle': handle,
                'ok': True,
                'title': (ld.get('title') or '')[:160],
                'image': (ld.get('images') or [{}])[0].get('url_fullxfull'),
                'has_variants': bool(options),
                'options': options,
                'variant_count': len((inv or {}).get('products') or []),
                'price_low_minor': (ld.get('price') or {}).get('amount'),
                'category': _detect_category(ld.get('title') or ''),
            })
        return jsonify({'ok': True, 'success': True, 'listings': out})

    @app.route('/api/everful/shopify/push', methods=['POST'])
    @login_required
    def _ev_shopify_push():
        body = request.get_json(force=True, silent=True) or {}
        urls = parse_urls_input(body.get('urls') or body.get('product_urls') or [])
        if not urls:
            return jsonify({'ok': False, 'error': 'No valid Everful URLs in input.'}), 400
        store_id = body.get('target_store_id') or ''
        if not store_id:
            return jsonify({'ok': False, 'error': 'target_store_id required'}), 400
        push_opts = body.get('options') or {}
        force_duplicate = bool(body.get('force_duplicate'))

        results = []
        for u in urls:
            try:
                r = push_product_url(data_dir, u, store_id, push_opts,
                                     force_duplicate=force_duplicate)
            except Exception as e:
                log.exception('push_product_url crash for %s', u)
                r = {'ok': False, 'error': f'{type(e).__name__}: {e}'}
            r['product_url'] = u
            r['handle'] = product_url_to_handle(u)
            results.append(r)

        return jsonify({
            'ok': True,
            'success': True,
            'results': results,
            'pushed': sum(1 for r in results if r.get('ok')),
            'conflicts': sum(1 for r in results if r.get('conflict')),
            'skipped': sum(1 for r in results if r.get('skipped')),
            'failed': sum(
                1 for r in results
                if not r.get('ok') and not r.get('conflict') and not r.get('skipped')
            ),
        })

    @app.route('/api/everful/shopify/pushes', methods=['GET'])
    @login_required
    def _ev_shopify_pushes():
        from flask import request as _req
        handle = (_req.args.get('handle') or '').strip()
        if not handle:
            return jsonify({'ok': False, 'error': 'handle required'}), 400
        return jsonify({
            'ok': True, 'success': True,
            'pushes': list_pushes_for_product(data_dir, handle),
        })

    log.info('Everful -> Shopify push routes registered')
