"""Bulk rename existing Shopify product titles using the AI title rewriter.

Workflow exposed to the UI:
  1. POST /api/shopify/bulk-rename/preview  body: {store_id}
     -> fetches all products, runs the AI rewriter on each title, returns a
        diff list (id, old, new, changed).  No writes.
  2. POST /api/shopify/bulk-rename/apply    body: {store_id, items: [{id,new_title}]}
     -> PATCHes each product's title via Shopify Admin API in batches.

The preview step is what costs Claude credits; apply only writes. Because the
AI rewriter has a 90-day sqlite cache, re-running preview on the same products
costs almost nothing.

Concurrency: Shopify Admin REST allows 2 req/s (4 on Plus). We push one product
update at a time with a small sleep to be safe; _shopify_request already handles
429 backoff.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import List, Dict, Optional, Tuple

import requests as http_requests
from flask import jsonify, request

from etsy_to_shopify import (
    SHOPIFY_API_VERSION,
    ShopifyError,
    _find_store,
    _shopify_request,
)
from ai_title_rewriter import rewrite_title, is_enabled as ai_is_enabled

log = logging.getLogger('shopify_bulk_rename')

# Shopify Admin REST page size cap is 250. Use it to minimise round-trips.
_PAGE_LIMIT = 250

# Tiny sleep between writes; _shopify_request already handles 429 retries, but
# this keeps us comfortably under the 2 req/s baseline so we don't hammer.
_WRITE_DELAY_SECS = 0.4


# ---------- list with cursor pagination ----------

def _list_all_products(store: dict) -> List[Dict]:
    """Walk every page of /products.json and return [{id,title,handle,...}].

    Uses Shopify's Link header cursor pagination. Honors 429 via direct retry
    rather than _shopify_request because we need the raw Response to read
    headers.
    """
    base = f"https://{store['domain']}/admin/api/{SHOPIFY_API_VERSION}/products.json"
    headers = {
        'X-Shopify-Access-Token': store['access_token'],
        'Accept': 'application/json',
    }
    url = base
    params = {'limit': _PAGE_LIMIT, 'fields': 'id,title,handle,status,product_type'}
    results: List[Dict] = []

    while url:
        # 429-aware GET loop.
        for attempt in range(6):
            r = http_requests.get(url, params=params if url == base else None,
                                  headers=headers, timeout=45)
            if r.status_code == 429:
                wait = float(r.headers.get('Retry-After') or 2)
                log.info('Shopify 429 on products list; sleeping %.1fs', wait)
                time.sleep(wait)
                continue
            if r.status_code >= 500 and attempt < 5:
                time.sleep(1 + attempt)
                continue
            if r.status_code != 200:
                raise ShopifyError(r.status_code, r.text)
            break
        else:
            raise ShopifyError(429, 'Exhausted retries listing products')

        data = r.json() or {}
        for p in data.get('products') or []:
            results.append({
                'id': p.get('id'),
                'title': p.get('title') or '',
                'handle': p.get('handle') or '',
                'status': p.get('status') or '',
                'product_type': p.get('product_type') or '',
            })

        # Parse next cursor from Link header.
        link = r.headers.get('Link') or r.headers.get('link') or ''
        next_url = _extract_next_link(link)
        url = next_url
        params = None  # cursor URLs are self-contained
    return results


_LINK_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


def _extract_next_link(link_header: str) -> Optional[str]:
    if not link_header:
        return None
    m = _LINK_RE.search(link_header)
    return m.group(1) if m else None


# ---------- title update ----------

def _update_product_title(store: dict, product_id: int, new_title: str) -> Dict:
    """PUT /products/{id}.json with the new title only."""
    body = {'product': {'id': product_id, 'title': new_title}}
    return _shopify_request(store, 'PUT', f'/products/{product_id}.json', body=body)


# ---------- preview ----------

def preview_rename(data_dir: str, store_id: str) -> Dict:
    store = _find_store(data_dir, store_id)
    if not store:
        return {'ok': False, 'error': f'Store not found: {store_id}'}
    if not ai_is_enabled(data_dir):
        return {'ok': False, 'error': 'AI title rewriter not configured (anthropic_api_key missing).'}

    try:
        products = _list_all_products(store)
    except ShopifyError as e:
        return {'ok': False, 'error': f'Shopify error listing products: {e}'}

    items = []
    cache_hits = 0
    api_calls = 0
    unchanged = 0
    for p in products:
        old_title = p.get('title') or ''
        if not old_title:
            continue
        try:
            new_title, source = rewrite_title(data_dir, old_title)
        except Exception:
            log.exception('rewrite_title failed for product %s', p.get('id'))
            new_title, source = old_title, 'fallback'
        if source == 'cache':
            cache_hits += 1
        elif source == 'ai':
            api_calls += 1
        changed = bool(new_title) and new_title.strip() != old_title.strip()
        if not changed:
            unchanged += 1
        items.append({
            'id': p.get('id'),
            'handle': p.get('handle'),
            'status': p.get('status'),
            'old_title': old_title,
            'new_title': new_title,
            'changed': changed,
            'source': source,  # 'ai' | 'cache' | 'fallback'
        })

    return {
        'ok': True,
        'count': len(items),
        'changed_count': sum(1 for i in items if i['changed']),
        'cache_hits': cache_hits,
        'api_calls': api_calls,
        'unchanged': unchanged,
        'items': items,
    }


# ---------- apply ----------

def apply_rename(data_dir: str, store_id: str,
                 items: List[Dict]) -> Dict:
    """Apply a list of {id, new_title} pairs. Returns per-item status."""
    store = _find_store(data_dir, store_id)
    if not store:
        return {'ok': False, 'error': f'Store not found: {store_id}'}

    results = []
    success = 0
    failed = 0
    for item in (items or []):
        pid = item.get('id')
        new_title = (item.get('new_title') or '').strip()
        if not pid or not new_title:
            results.append({'id': pid, 'ok': False, 'error': 'missing id or new_title'})
            failed += 1
            continue
        try:
            _update_product_title(store, int(pid), new_title)
            results.append({'id': pid, 'ok': True, 'new_title': new_title})
            success += 1
        except ShopifyError as e:
            log.warning('Title update failed for %s: %s', pid, e)
            results.append({'id': pid, 'ok': False, 'error': str(e)})
            failed += 1
        except Exception as e:
            log.exception('Title update crash for %s', pid)
            results.append({'id': pid, 'ok': False, 'error': f'{type(e).__name__}: {e}'})
            failed += 1
        time.sleep(_WRITE_DELAY_SECS)

    return {
        'ok': True, 'success_count': success, 'failed_count': failed,
        'results': results,
    }


# ---------- flask routes ----------

def register_routes(app, data_dir: str, login_required) -> None:
    @app.route('/api/shopify/bulk-rename/preview', methods=['POST'])
    @login_required
    def _bulk_rename_preview():
        body = request.get_json(force=True, silent=True) or {}
        store_id = (body.get('store_id') or '').strip()
        if not store_id:
            return jsonify({'ok': False, 'error': 'store_id required'}), 400
        try:
            return jsonify(preview_rename(data_dir, store_id))
        except Exception as e:
            log.exception('preview_rename crash')
            return jsonify({'ok': False, 'error': f'{type(e).__name__}: {e}'}), 500

    @app.route('/api/shopify/bulk-rename/apply', methods=['POST'])
    @login_required
    def _bulk_rename_apply():
        body = request.get_json(force=True, silent=True) or {}
        store_id = (body.get('store_id') or '').strip()
        items = body.get('items') or []
        if not store_id:
            return jsonify({'ok': False, 'error': 'store_id required'}), 400
        if not isinstance(items, list) or not items:
            return jsonify({'ok': False, 'error': 'items required'}), 400
        try:
            return jsonify(apply_rename(data_dir, store_id, items))
        except Exception as e:
            log.exception('apply_rename crash')
            return jsonify({'ok': False, 'error': f'{type(e).__name__}: {e}'}), 500

    log.info('Shopify bulk-rename routes registered')
