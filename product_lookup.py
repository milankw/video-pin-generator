"""Product sales lookup by URL.

Takes a product URL from a Shopify or Shoplazza storefront (custom domain or
*.myshopify.com / *.myshoplaza.com) and returns aggregate sales data pulled
from the winner-cache built by /api/shopify/winners/<store>/sync.

Match strategy:
  1. Parse host and handle from the URL.
  2. Try to match host directly against store.domain / store.shoplazzaDomain.
  3. If no host match, iterate every connected store and resolve the handle
     via the platform API (Shopify /products.json?handle=X, Shoplazza search).
     The first store whose API returns a product with this handle wins.
  4. Look up product_id in that store's winner_cache. If not present, the
     product exists but has zero paid-order sales in the cached window.

Endpoint returns:
  {
    ok: True,
    store: {id, name, platform},
    product: {id, title, handle, url, quantity, revenue, currency,
              variant_sales: [...]},
    matched_by: 'host' | 'handle_lookup',
    in_cache: bool,
  }
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from flask import jsonify, request

log = logging.getLogger(__name__)


# --- URL parsing ------------------------------------------------------------

_HANDLE_RE = re.compile(r'/products/([^/?#]+)')


def _parse_product_url(url: str) -> Tuple[str, str]:
    """Return (host, handle) from a product URL. Empty strings if unparseable."""
    if not url:
        return '', ''
    url = url.strip()
    # Accept bare "thegothsociety.com/products/foo" too
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    try:
        parsed = urlparse(url)
    except Exception:
        return '', ''
    host = (parsed.hostname or '').lower().lstrip('www.')
    if host.startswith('www.'):
        host = host[4:]
    m = _HANDLE_RE.search(parsed.path or '')
    handle = (m.group(1) if m else '').strip().lower()
    return host, handle


# --- Store discovery --------------------------------------------------------

def _store_hosts(store: Dict[str, Any]) -> list:
    """Every host we can safely associate with a store."""
    hosts = []
    for key in ('domain', 'shoplazzaDomain', 'customDomain', 'primaryDomain', 'storefrontDomain'):
        val = (store.get(key) or '').strip().lower()
        if not val:
            continue
        if val.startswith('http'):
            val = urlparse(val).hostname or ''
        if val.startswith('www.'):
            val = val[4:]
        if val:
            hosts.append(val)
    return hosts


def _find_store_by_host(stores: list, host: str) -> Optional[Dict[str, Any]]:
    if not host:
        return None
    for s in stores:
        if host in _store_hosts(s):
            return s
    # Also allow suffix match: passed host is 'thegothsociety.com',
    # some setups store 'shop.thegothsociety.com'. Try endswith.
    for s in stores:
        for sh in _store_hosts(s):
            if sh.endswith(host) or host.endswith(sh):
                return s
    return None


# --- Shopify / Shoplazza handle -> product_id lookup ------------------------

def _resolve_shopify_product_by_handle(domain: str, token: str, handle: str, http_requests) -> Optional[Dict[str, Any]]:
    """Return {id, title, handle, image} for a Shopify product by handle, or None."""
    if not (domain and token and handle):
        return None
    base_url = f'https://{domain}/admin/api/2024-01'
    try:
        r = http_requests.get(
            f'{base_url}/products.json',
            params={'handle': handle, 'fields': 'id,title,handle,image,variants'},
            headers={'X-Shopify-Access-Token': token},
            timeout=10,
        )
        if r.status_code != 200:
            return None
        products = (r.json() or {}).get('products') or []
        if not products:
            return None
        p = products[0]
        return {
            'id': str(p.get('id') or ''),
            'title': p.get('title') or '',
            'handle': p.get('handle') or handle,
            'image': (p.get('image') or {}).get('src', '') if isinstance(p.get('image'), dict) else '',
            'variants': p.get('variants') or [],
        }
    except Exception as e:  # noqa: BLE001
        log.debug('Shopify handle lookup failed for %s: %s', domain, e)
        return None


def _resolve_shoplazza_product_by_handle(domain: str, token: str, handle: str, http_requests, api_version: str) -> Optional[Dict[str, Any]]:
    if not (domain and token and handle):
        return None
    base_url = f'https://{domain}/openapi/{api_version}'
    try:
        r = http_requests.get(
            f'{base_url}/products',
            params={'handle': handle, 'limit': 1},
            headers={'Access-Token': token},
            timeout=10,
        )
        if r.status_code != 200:
            return None
        data = (r.json() or {}).get('data') or {}
        products = data.get('products') or data.get('list') or []
        if not products:
            return None
        p = products[0]
        return {
            'id': str(p.get('id') or ''),
            'title': p.get('title') or '',
            'handle': p.get('handle') or handle,
            'image': ((p.get('image') or {}).get('src') if isinstance(p.get('image'), dict) else '') or '',
            'variants': p.get('variants') or [],
        }
    except Exception as e:  # noqa: BLE001
        log.debug('Shoplazza handle lookup failed for %s: %s', domain, e)
        return None


# --- Route registration -----------------------------------------------------

def register_routes(app, data_dir, login_required, http_requests, shoplazza_api_version='2022-01'):
    """Wire /api/product-lookup into the Flask app.

    Args:
        app: Flask app
        data_dir: server DATA_DIR
        login_required: existing auth decorator
        http_requests: shared requests-like module (server.py's http_requests)
        shoplazza_api_version: e.g. '2022-01'
    """
    stores_path = os.path.join(data_dir, 'stores.json')
    winner_cache_dir = os.path.join(data_dir, 'winner_cache')

    def _load_stores():
        try:
            with open(stores_path) as f:
                return json.load(f) or []
        except Exception:
            return []

    def _load_cache(store_id):
        try:
            with open(os.path.join(winner_cache_dir, f'{store_id}.json')) as f:
                return json.load(f) or {}
        except Exception:
            return {}

    def _get_creds(store):
        platform = (store.get('platform') or 'shopify').lower()
        if platform == 'shoplazza':
            return 'shoplazza', store.get('shoplazzaDomain', ''), store.get('shoplazzaAccessToken', '')
        return 'shopify', store.get('domain', ''), store.get('shopifyAccessToken', '')

    @app.route('/api/product-lookup', methods=['POST'])
    @login_required
    def product_lookup():
        body = request.get_json(silent=True) or {}
        url = (body.get('url') or '').strip()
        forced_store_id = (body.get('store_id') or '').strip()

        if not url:
            return jsonify({'ok': False, 'error': 'url required'}), 400

        host, handle = _parse_product_url(url)
        if not handle:
            return jsonify({'ok': False, 'error': 'Could not extract product handle from URL. Expected /products/<handle>.'}), 400

        stores = _load_stores()
        matched_store = None
        matched_by = ''

        # 1) forced store_id wins if provided
        if forced_store_id:
            matched_store = next((s for s in stores if s.get('id') == forced_store_id), None)
            matched_by = 'forced'

        # 2) try host match
        if not matched_store:
            matched_store = _find_store_by_host(stores, host)
            if matched_store:
                matched_by = 'host'

        # 3) fall back to handle lookup across every connected store
        resolved_product = None
        if not matched_store:
            for s in stores:
                platform, domain, token = _get_creds(s)
                if not domain or not token:
                    continue
                if platform == 'shopify':
                    p = _resolve_shopify_product_by_handle(domain, token, handle, http_requests)
                elif platform == 'shoplazza':
                    p = _resolve_shoplazza_product_by_handle(domain, token, handle, http_requests, shoplazza_api_version)
                else:
                    p = None
                if p:
                    matched_store = s
                    resolved_product = p
                    matched_by = 'handle_lookup'
                    break

        if not matched_store:
            return jsonify({
                'ok': False,
                'error': f'No connected store matched host "{host}" or handle "{handle}". Make sure the store is connected.',
                'host': host,
                'handle': handle,
            }), 404

        # If we matched via host but didn't yet resolve the product, do it now
        # so we know the numeric product_id used as the cache key.
        if not resolved_product:
            platform, domain, token = _get_creds(matched_store)
            if platform == 'shopify':
                resolved_product = _resolve_shopify_product_by_handle(domain, token, handle, http_requests)
            elif platform == 'shoplazza':
                resolved_product = _resolve_shoplazza_product_by_handle(domain, token, handle, http_requests, shoplazza_api_version)

        cache = _load_cache(matched_store['id'])
        cache_entry = None
        product_id = ''
        if resolved_product:
            product_id = resolved_product.get('id', '')
            # cache is keyed by product_id as string in most places, but may be int
            cache_entry = cache.get(product_id) or cache.get(str(product_id))
            # As a last resort, search cache by title match
            if not cache_entry and resolved_product.get('title'):
                title_lower = resolved_product['title'].strip().lower()
                for cid, cent in cache.items():
                    if isinstance(cent, dict) and (cent.get('title') or '').strip().lower() == title_lower:
                        cache_entry = cent
                        product_id = str(cid)
                        break

        # Build canonical URL from matched store
        canonical_host = _store_hosts(matched_store)[0] if _store_hosts(matched_store) else host
        canonical_url = f'https://{canonical_host}/products/{handle}'

        # Compose response
        if cache_entry:
            variant_sales_raw = cache_entry.get('variant_sales') or {}
            if isinstance(variant_sales_raw, dict):
                variant_sales_list = list(variant_sales_raw.values())
            else:
                variant_sales_list = list(variant_sales_raw)
            variant_sales_list.sort(key=lambda v: v.get('quantity', 0), reverse=True)

            product_payload = {
                'id': str(cache_entry.get('product_id') or product_id or ''),
                'title': cache_entry.get('title') or (resolved_product or {}).get('title') or '',
                'handle': handle,
                'url': canonical_url,
                'quantity': int(cache_entry.get('quantity') or 0),
                'revenue': float(cache_entry.get('revenue') or 0.0),
                'image': cache_entry.get('fallback_image') or (resolved_product or {}).get('image', ''),
                'variant_sales': variant_sales_list,
            }
            in_cache = True
        else:
            product_payload = {
                'id': product_id,
                'title': (resolved_product or {}).get('title') or '',
                'handle': handle,
                'url': canonical_url,
                'quantity': 0,
                'revenue': 0.0,
                'image': (resolved_product or {}).get('image', ''),
                'variant_sales': [],
            }
            in_cache = False

        # Sync metadata so the UI can tell the user how fresh the number is
        meta_path = os.path.join(winner_cache_dir, f'{matched_store["id"]}_meta.json')
        sync_meta = {}
        try:
            with open(meta_path) as f:
                sync_meta = json.load(f) or {}
        except Exception:
            sync_meta = {}

        return jsonify({
            'ok': True,
            'store': {
                'id': matched_store.get('id'),
                'name': matched_store.get('name'),
                'platform': (matched_store.get('platform') or 'shopify').lower(),
            },
            'product': product_payload,
            'matched_by': matched_by,
            'in_cache': in_cache,
            'product_exists': bool(resolved_product),
            'sync_meta': {
                'status': sync_meta.get('status', ''),
                'last_sync_at': sync_meta.get('last_sync_at') or sync_meta.get('completed_at'),
                'pages_scanned': sync_meta.get('pages_scanned', 0),
                'total_orders': sync_meta.get('total_orders', 0),
            },
        })

    log.info('Product-lookup route registered at /api/product-lookup')
