"""Single-listing Etsy importer.

Lets the user paste an Etsy listing URL (or ID) and import that one listing
into the local etsy_listing sqlite cache without needing to import the whole
shop first. After this, push_listing() in etsy_to_shopify works unchanged.

Endpoint:
  POST /api/etsy-shops/import-listing-by-url
  body: {url: "https://www.etsy.com/listing/1234567890/..."}

Response:
  {ok: True, listing_id: 1234567890, listing: {id,title,image,price_display,
                                               num_favorers,views,shop_name,url}}

Reuses the same /listings/batch?listing_ids=X&includes=Images endpoint that
the shop importer uses, so quality/shape of data is identical. Rows are
inserted into etsy_listing (or updated on conflict), with shop_id filled in
from the Etsy response so the shop card still lines up if the shop is later
imported. Freshly-inserted rows are marked with a synthetic shop_id when
the shop is not yet in etsy_shop — no FK dependency needed since the
existing schema doesn't enforce one.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from typing import Any, Dict, Optional

from flask import jsonify, request

log = logging.getLogger(__name__)

# The etsy_shops_module writes to data/etsy_shops.db in its own _conn() helper.
_DB_FILENAME = 'etsy_shops.db'


def _db_path(data_dir: str) -> str:
    return os.path.join(data_dir, _DB_FILENAME)


def _conn(data_dir: str):
    conn = sqlite3.connect(_db_path(data_dir), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


_LISTING_ID_RE = re.compile(r'/listing/(\d+)')


def _extract_listing_id(url_or_id: str) -> Optional[int]:
    """Accept full Etsy URL, /listing/<id>/ path, or bare numeric id."""
    if not url_or_id:
        return None
    s = url_or_id.strip()
    # bare numeric id
    if s.isdigit():
        return int(s)
    # regex match on URL
    m = _LISTING_ID_RE.search(s)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def _upsert_shop_if_missing(data_dir: str, shop_id: int, shop_name: str, shop_url: str):
    """Insert a lightweight etsy_shop row so foreign UI code that JOINs on
    shop_id doesn't 404. Full shop import can enrich it later."""
    if not shop_id:
        return
    now = int(time.time())
    with _conn(data_dir) as c:
        exists = c.execute('SELECT 1 FROM etsy_shop WHERE shop_id=?', (shop_id,)).fetchone()
        if exists:
            return
        # Only fill the columns we know exist across all schema versions.
        try:
            c.execute("""
                INSERT INTO etsy_shop(shop_id, shop_name, url, imported_at, is_placeholder)
                VALUES (?,?,?,?,1)
                ON CONFLICT(shop_id) DO NOTHING
            """, (shop_id, shop_name or f'shop_{shop_id}', shop_url or '', now))
        except sqlite3.OperationalError:
            # Older schema without is_placeholder column: try without it.
            try:
                c.execute("""
                    INSERT INTO etsy_shop(shop_id, shop_name, url, imported_at)
                    VALUES (?,?,?,?)
                    ON CONFLICT(shop_id) DO NOTHING
                """, (shop_id, shop_name or f'shop_{shop_id}', shop_url or '', now))
            except sqlite3.OperationalError as e:
                log.warning('placeholder shop insert failed (%s) — skipping', e)


def _persist_listing(data_dir: str, listing: Dict[str, Any]) -> Dict[str, Any]:
    """Insert/update one listing row from an Etsy /listings/batch payload
    (includes Images). Returns a compact dict for the UI."""
    lid = int(listing.get('listing_id') or 0)
    shop_id = int(listing.get('shop_id') or 0)
    price = listing.get('price') or {}
    images = listing.get('images') or []

    main_url = None
    if images:
        im0 = images[0]
        main_url = (im0.get('url_570xN') or im0.get('url_fullxfull')
                    or im0.get('url_300x300'))
    secondary = [im.get('url_570xN') or im.get('url_fullxfull')
                 for im in images if im.get('url_570xN') or im.get('url_fullxfull')]

    now = int(time.time())
    raw_json = json.dumps(listing)

    with _conn(data_dir) as c:
        c.execute("""
            INSERT INTO etsy_listing(listing_id, shop_id, title, url,
                price_amount, price_divisor, price_currency,
                num_favorers, views, quantity, state,
                created_timestamp, updated_timestamp,
                tags, materials, image_main_url, image_local_path, image_urls,
                imported_at, raw_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(listing_id) DO UPDATE SET
                shop_id=excluded.shop_id,
                title=excluded.title, url=excluded.url,
                price_amount=excluded.price_amount, price_divisor=excluded.price_divisor,
                price_currency=excluded.price_currency,
                num_favorers=excluded.num_favorers, views=excluded.views,
                quantity=excluded.quantity, state=excluded.state,
                updated_timestamp=excluded.updated_timestamp,
                tags=excluded.tags, materials=excluded.materials,
                image_main_url=excluded.image_main_url,
                image_urls=excluded.image_urls,
                raw_json=excluded.raw_json
        """, (
            lid, shop_id, listing.get('title'), listing.get('url'),
            int(price.get('amount') or 0), int(price.get('divisor') or 100),
            price.get('currency_code'),
            int(listing.get('num_favorers') or 0),
            int(listing.get('views') or 0),
            int(listing.get('quantity') or 0),
            listing.get('state'),
            int(listing.get('created_timestamp') or 0),
            int(listing.get('updated_timestamp') or listing.get('last_modified_timestamp') or 0),
            json.dumps(listing.get('tags') or []),
            json.dumps(listing.get('materials') or []),
            main_url,
            None,   # image_local_path — hotlink mode
            json.dumps(secondary),
            now,
            raw_json,
        ))

    # Build display price like the shop-import path does.
    price_display = ''
    if price.get('amount'):
        div = int(price.get('divisor') or 100) or 100
        price_display = f'{price.get("currency_code") or "USD"} {int(price["amount"]) / div:.2f}'

    return {
        'id': lid,
        'shop_id': shop_id,
        'title': listing.get('title'),
        'url': listing.get('url'),
        'image': main_url,
        'price_display': price_display,
        'num_favorers': int(listing.get('num_favorers') or 0),
        'views': int(listing.get('views') or 0),
    }


def register_routes(app, data_dir, etsy_request_fn, login_required):
    """Wire /api/etsy-shops/import-listing-by-url into the Flask app."""

    @app.route('/api/etsy-shops/import-listing-by-url', methods=['POST'])
    @login_required
    def _import_listing_by_url():
        body = request.get_json(force=True, silent=True) or {}
        raw = (body.get('url') or body.get('listing_id') or '').strip()
        lid = _extract_listing_id(raw)
        if not lid:
            return jsonify({
                'ok': False,
                'error': 'Could not extract listing ID. Paste an Etsy URL like '
                         'https://www.etsy.com/listing/1234567890/... or the numeric ID.'
            }), 400

        # Fetch listing (with images) via the same batch endpoint the shop
        # importer uses — 1 Etsy API call, no ScrapingBee credits.
        try:
            res = etsy_request_fn('/listings/batch', {
                'listing_ids': str(lid),
                'includes': 'Images',
            })
        except Exception as e:  # noqa: BLE001
            log.exception('etsy fetch failed for %s', lid)
            return jsonify({'ok': False, 'error': f'Etsy API error: {e}'}), 502

        results = ((res or {}).get('results') or [])
        if not results:
            return jsonify({
                'ok': False,
                'error': f'Etsy returned no data for listing {lid}. '
                         'It may be sold-out, deleted, or in a private draft state.'
            }), 404

        listing = results[0]

        # Persist a lightweight shop row so downstream JOINs work.
        _upsert_shop_if_missing(
            data_dir,
            int(listing.get('shop_id') or 0),
            listing.get('shop_name') or listing.get('Shop', {}).get('shop_name', '')
            if isinstance(listing.get('Shop'), dict) else (listing.get('shop_name') or ''),
            f'https://www.etsy.com/shop/{listing.get("shop_id")}' if listing.get('shop_id') else '',
        )

        row = _persist_listing(data_dir, listing)
        log.info('single-listing import ok: listing_id=%s shop_id=%s title=%r',
                 row['id'], row['shop_id'], row['title'])
        return jsonify({'ok': True, 'success': True, 'listing_id': lid, 'listing': row})

    log.info('Etsy single-listing importer registered at '
             '/api/etsy-shops/import-listing-by-url')
