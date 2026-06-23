"""
Etsy Shops tab — import any Etsy shop once and keep it forever.

Imports the full active-listing catalog for a shop via Etsy Open API v3,
saves it permanently to a sqlite DB (etsy_shops.db) and downloads images
to data/etsy_shop_images/<shop_id>/ so they're served from our own host
and never go cold. Once imported, shops are never re-fetched unless the
user explicitly clicks Refresh.

Public surface — all routes are added to the given Flask `app`:

    GET    /api/etsy-shops                       — list saved shops
    POST   /api/etsy-shops/import                — kick off import for {shop_url}
    GET    /api/etsy-shops/<shop_id>             — shop meta + listings (sorted)
    GET    /api/etsy-shops/<shop_id>/progress    — import progress
    POST   /api/etsy-shops/<shop_id>/refresh     — re-fetch from Etsy
    DELETE /api/etsy-shops/<shop_id>             — remove shop (keeps images)
    POST   /api/etsy-shops/<shop_id>/favorite    — toggle heart on listing
    GET    /api/etsy-shops/favorites             — all hearted listings
"""
import json, os, re, sqlite3, threading, time, logging
from urllib.parse import urlparse
import requests as http_requests

log = logging.getLogger('etsy_shops')


# ---------- Storage ----------

def _db_path(data_dir):
    return os.path.join(data_dir, 'etsy_shops.db')

def _images_dir(data_dir):
    p = os.path.join(data_dir, 'etsy_shop_images')
    os.makedirs(p, exist_ok=True)
    return p

class _Conn:
    """Context manager that guarantees connection close.

    Python's ``with sqlite3.connect(...) as c:`` only commits/rolls-back on
    exit — it does NOT close the connection. Used at scale (one import =
    hundreds of DB calls) that leaks file descriptors and SQLite handles,
    serialises writes via OS locks, and surfaces as 'database is locked'.
    This wrapper closes the connection on exit.
    """
    __slots__ = ('_path', '_c')
    def __init__(self, path):
        self._path = path
        self._c = None
    def __enter__(self):
        c = sqlite3.connect(self._path, timeout=30)
        c.row_factory = sqlite3.Row
        # WAL mode lets readers and writers proceed concurrently. Set once;
        # cheap when already enabled. busy_timeout handles transient lock
        # contention without raising.
        try:
            c.execute('PRAGMA journal_mode=WAL')
            c.execute('PRAGMA busy_timeout=30000')
            c.execute('PRAGMA synchronous=NORMAL')
        except Exception:
            pass
        self._c = c
        return c
    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self._c.commit()
            else:
                self._c.rollback()
        finally:
            try: self._c.close()
            except Exception: pass

def _conn(data_dir):
    return _Conn(_db_path(data_dir))

def init_db(data_dir):
    """Create tables on first run. Idempotent."""
    os.makedirs(data_dir, exist_ok=True)
    with _conn(data_dir) as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS etsy_shop (
            shop_id INTEGER PRIMARY KEY,
            shop_name TEXT NOT NULL,
            url TEXT,
            title TEXT,
            announcement TEXT,
            icon_url TEXT,
            num_favorers INTEGER DEFAULT 0,
            review_count INTEGER DEFAULT 0,
            review_average REAL DEFAULT 0,
            transaction_sold_count INTEGER DEFAULT 0,
            listing_active_count INTEGER DEFAULT 0,
            currency_code TEXT,
            country_iso TEXT,
            created_timestamp INTEGER,
            imported_at INTEGER NOT NULL,
            last_refreshed_at INTEGER NOT NULL,
            raw_json TEXT
        );

        CREATE TABLE IF NOT EXISTS etsy_listing (
            listing_id INTEGER PRIMARY KEY,
            shop_id INTEGER NOT NULL REFERENCES etsy_shop(shop_id) ON DELETE CASCADE,
            title TEXT,
            url TEXT,
            price_amount INTEGER,
            price_divisor INTEGER,
            price_currency TEXT,
            num_favorers INTEGER DEFAULT 0,
            views INTEGER DEFAULT 0,
            quantity INTEGER,
            state TEXT,
            created_timestamp INTEGER,
            updated_timestamp INTEGER,
            tags TEXT,
            materials TEXT,
            image_main_url TEXT,
            image_local_path TEXT,
            image_urls TEXT,
            is_favorite INTEGER DEFAULT 0,
            favorited_at INTEGER,
            imported_at INTEGER NOT NULL,
            raw_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_listing_shop ON etsy_listing(shop_id);
        CREATE INDEX IF NOT EXISTS idx_listing_favorers ON etsy_listing(num_favorers DESC);
        CREATE INDEX IF NOT EXISTS idx_listing_fav ON etsy_listing(is_favorite);

        CREATE TABLE IF NOT EXISTS etsy_import_job (
            shop_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,           -- queued|running|done|error
            phase TEXT,                     -- resolve_shop|listings|images|done
            total INTEGER DEFAULT 0,
            done INTEGER DEFAULT 0,
            error TEXT,
            started_at INTEGER,
            finished_at INTEGER,
            shop_name TEXT
        );
        """)
        c.commit()


# ---------- URL/shop-name parsing ----------

_SHOP_URL_RE = re.compile(r'etsy\.com/(?:[a-z]{2}/)?shop/([A-Za-z0-9_-]+)', re.I)

def parse_shop_input(s):
    """Accept a full Etsy shop URL or bare shop name, return the shop_name."""
    s = (s or '').strip()
    if not s:
        return None
    m = _SHOP_URL_RE.search(s)
    if m:
        return m.group(1)
    # bare name
    if re.fullmatch(r'[A-Za-z0-9_-]{1,80}', s):
        return s
    return None


# ---------- Image download ----------

def _safe_ext(url):
    p = urlparse(url).path.lower()
    for ext in ('.jpg', '.jpeg', '.png', '.webp', '.gif'):
        if p.endswith(ext):
            return ext
    return '.jpg'

def _download_image(url, shop_id, listing_id, data_dir, timeout=20):
    """Save image to data/etsy_shop_images/<shop_id>/<listing_id><ext>.
    Returns (local_relative_path, local_url_path). Skips if already present."""
    if not url:
        return None, None
    shop_dir = os.path.join(_images_dir(data_dir), str(shop_id))
    os.makedirs(shop_dir, exist_ok=True)
    ext = _safe_ext(url)
    fname = f'{listing_id}{ext}'
    fpath = os.path.join(shop_dir, fname)
    # served URL — matches the existing /product-images/ route convention
    served = f'/etsy-shop-images/{shop_id}/{fname}'
    if os.path.exists(fpath) and os.path.getsize(fpath) > 100:
        return fpath, served
    try:
        r = http_requests.get(url, timeout=timeout, stream=True,
                              headers={'User-Agent': 'Mozilla/5.0 VideoPin/1.0'})
        if r.status_code != 200:
            return None, None
        with open(fpath, 'wb') as f:
            for chunk in r.iter_content(64 * 1024):
                f.write(chunk)
        return fpath, served
    except Exception as e:
        log.warning('etsy-shop-image dl failed %s: %s', url, e)
        return None, None


# ---------- Import worker ----------

# job_id -> threading.Thread
_running_jobs = {}
_jobs_lock = threading.Lock()


def _job_update(data_dir, shop_id, **fields):
    cols = ', '.join(f'{k}=?' for k in fields)
    vals = list(fields.values()) + [shop_id]
    with _conn(data_dir) as c:
        c.execute(f'UPDATE etsy_import_job SET {cols} WHERE shop_id=?', vals)
        c.commit()


def _job_upsert(data_dir, shop_id, shop_name, status='queued'):
    now = int(time.time())
    with _conn(data_dir) as c:
        c.execute("""
            INSERT INTO etsy_import_job(shop_id, shop_name, status, phase, total, done, started_at)
            VALUES (?, ?, ?, 'resolve_shop', 0, 0, ?)
            ON CONFLICT(shop_id) DO UPDATE SET
                status=excluded.status, phase='resolve_shop',
                total=0, done=0, error=NULL,
                started_at=excluded.started_at, finished_at=NULL,
                shop_name=excluded.shop_name
        """, (shop_id or 0, shop_name, status, now))
        c.commit()


def _import_worker(shop_input, data_dir, etsy_request_fn):
    """Background worker that does the full import.

    etsy_request_fn(path, params=None) -> dict | None
    A callback so we can reuse server.py's auth/rate-limit logic without
    duplicating it. Returns parsed JSON dict on success, None on error.
    """
    shop_name = parse_shop_input(shop_input)
    if not shop_name:
        log.error('etsy-shops: invalid input %r', shop_input)
        return

    # 1) Resolve shop_id by name
    _job_upsert(data_dir, 0, shop_name, status='running')
    try:
        res = etsy_request_fn('/shops', {'shop_name': shop_name})
    except Exception as e:
        log.exception('resolve shop failed: %s', e)
        return
    if not res or not res.get('results'):
        # No such shop
        with _conn(data_dir) as c:
            c.execute("""UPDATE etsy_import_job SET status='error', error=?, finished_at=?
                         WHERE shop_id=0""",
                      (f'Shop "{shop_name}" not found', int(time.time())))
            c.commit()
        return

    shop = res['results'][0]
    shop_id = int(shop['shop_id'])

    # Migrate the placeholder job row (shop_id=0) to the real shop_id, if any
    with _conn(data_dir) as c:
        c.execute('DELETE FROM etsy_import_job WHERE shop_id=?', (shop_id,))
        c.execute('UPDATE etsy_import_job SET shop_id=? WHERE shop_id=0', (shop_id,))
        c.commit()

    _job_update(data_dir, shop_id, phase='listings', total=int(shop.get('listing_active_count') or 0))

    # 2) Upsert shop
    now = int(time.time())
    with _conn(data_dir) as c:
        c.execute("""
            INSERT INTO etsy_shop(shop_id, shop_name, url, title, announcement, icon_url,
                num_favorers, review_count, review_average, transaction_sold_count,
                listing_active_count, currency_code, country_iso, created_timestamp,
                imported_at, last_refreshed_at, raw_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(shop_id) DO UPDATE SET
                shop_name=excluded.shop_name, url=excluded.url, title=excluded.title,
                announcement=excluded.announcement, icon_url=excluded.icon_url,
                num_favorers=excluded.num_favorers, review_count=excluded.review_count,
                review_average=excluded.review_average,
                transaction_sold_count=excluded.transaction_sold_count,
                listing_active_count=excluded.listing_active_count,
                currency_code=excluded.currency_code, country_iso=excluded.country_iso,
                created_timestamp=excluded.created_timestamp,
                last_refreshed_at=excluded.last_refreshed_at,
                raw_json=excluded.raw_json
        """, (
            shop_id, shop.get('shop_name'), shop.get('url'), shop.get('title'),
            shop.get('announcement'), shop.get('icon_url_fullxfull') or shop.get('image_url_760x100'),
            int(shop.get('num_favorers') or 0),
            int(shop.get('review_count') or 0),
            float(shop.get('review_average') or 0),
            int(shop.get('transaction_sold_count') or 0),
            int(shop.get('listing_active_count') or 0),
            shop.get('currency_code'),
            shop.get('shop_location_country_iso') or shop.get('shipping_from_country_iso'),
            int(shop.get('created_timestamp') or 0),
            now, now,
            json.dumps(shop),
        ))
        c.commit()

    # 3) Paginate active listings (100/page)
    all_listings = []
    offset = 0
    PAGE = 100
    while True:
        res = etsy_request_fn(f'/shops/{shop_id}/listings/active',
                              {'limit': PAGE, 'offset': offset})
        if not res:
            break
        batch = res.get('results') or []
        all_listings.extend(batch)
        _job_update(data_dir, shop_id, done=len(all_listings))
        if len(batch) < PAGE:
            break
        offset += PAGE
        if offset > 20000:  # hard cap
            break

    if not all_listings:
        _job_update(data_dir, shop_id, status='done', phase='done',
                    finished_at=int(time.time()))
        return

    # 4) Batch-fetch with images (100 IDs per call)
    _job_update(data_dir, shop_id, phase='images', total=len(all_listings), done=0)
    by_id = {int(L['listing_id']): L for L in all_listings}
    ids = list(by_id.keys())
    enriched = {}
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        # Etsy accepts comma-separated values via repeated 'listing_ids' or
        # as a single 'listing_ids' with a comma list. Use comma form.
        params = {'listing_ids': ','.join(str(x) for x in chunk), 'includes': 'Images'}
        res = etsy_request_fn('/listings/batch', params)
        for L in (res or {}).get('results') or []:
            enriched[int(L['listing_id'])] = L
        _job_update(data_dir, shop_id, done=min(i + 100, len(ids)))

    # 5) Persist listings + download images.
    # Clear stale rows up-front (own short transaction).
    with _conn(data_dir) as c:
        c.execute('DELETE FROM etsy_listing WHERE shop_id=?', (shop_id,))

    _job_update(data_dir, shop_id, phase='persisting', total=len(by_id), done=0)

    # Per-listing short transaction. Holding ONE transaction open across
    # thousands of HTTP image downloads (~300ms each) was locking the DB
    # for 10+ minutes and starving every reader. Now each listing commits
    # immediately, so the API/UI can see progress in near real-time.
    #
    # HOTLINK MODE: we no longer download images to disk. Etsy's CDN URLs
    # are public + permanent + faster than re-serving from this VPS.
    # Saves ~100 KB per listing on disk (a 6747-listing shop = 0 MB vs
    # ~675 MB) and turns a 13-min import into ~30 sec.
    done_count = 0
    for lid, base in by_id.items():
        try:
            rich = enriched.get(lid) or {}
            merged = {**base, **rich}
            images = merged.get('images') or []
            # 570px wide is plenty for grid cards (<=400px).
            main_display_url = None
            if images:
                im0 = images[0]
                main_display_url = (im0.get('url_570xN') or im0.get('url_fullxfull')
                                    or im0.get('url_300x300'))
            secondary_urls = [im.get('url_570xN') or im.get('url_fullxfull')
                              for im in images if im.get('url_570xN') or im.get('url_fullxfull')]

            # Hotlink only — no on-disk copy.
            local_path, served_url = (None, None)

            price = merged.get('price') or {}
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
                        title=excluded.title, url=excluded.url,
                        price_amount=excluded.price_amount, price_divisor=excluded.price_divisor,
                        price_currency=excluded.price_currency,
                        num_favorers=excluded.num_favorers, views=excluded.views,
                        quantity=excluded.quantity, state=excluded.state,
                        updated_timestamp=excluded.updated_timestamp,
                        tags=excluded.tags, materials=excluded.materials,
                        image_main_url=excluded.image_main_url,
                        image_local_path=excluded.image_local_path,
                        image_urls=excluded.image_urls,
                        raw_json=excluded.raw_json
                """, (
                    lid, shop_id, merged.get('title'), merged.get('url'),
                    int(price.get('amount') or 0), int(price.get('divisor') or 100),
                    price.get('currency_code'),
                    int(merged.get('num_favorers') or 0),
                    int(merged.get('views') or 0),
                    int(merged.get('quantity') or 0),
                    merged.get('state'),
                    int(merged.get('created_timestamp') or 0),
                    int(merged.get('updated_timestamp') or merged.get('last_modified_timestamp') or 0),
                    json.dumps(merged.get('tags') or []),
                    json.dumps(merged.get('materials') or []),
                    served_url or main_display_url,
                    local_path,
                    json.dumps(secondary_urls),
                    int(time.time()),
                    json.dumps(merged),
                ))
        except Exception as e:
            log.warning('etsy-shops: failed to persist listing %s: %s', lid, e)
        done_count += 1
        # Update job progress every 25 listings to limit DB churn.
        if done_count % 25 == 0 or done_count == len(by_id):
            _job_update(data_dir, shop_id, done=done_count)

    _job_update(data_dir, shop_id, status='done', phase='done',
                finished_at=int(time.time()),
                total=len(all_listings), done=len(all_listings))
    log.info('etsy-shops: imported shop_id=%s (%s) with %d listings',
             shop_id, shop_name, len(all_listings))


def start_import(shop_input, data_dir, etsy_request_fn):
    """Kick off background import; idempotent — returns existing job if running."""
    shop_name = parse_shop_input(shop_input)
    if not shop_name:
        return {'ok': False, 'error': 'Provide an Etsy shop URL or shop name'}

    # If we already have this shop imported, return immediately
    with _conn(data_dir) as c:
        row = c.execute('SELECT shop_id FROM etsy_shop WHERE LOWER(shop_name)=LOWER(?)',
                        (shop_name,)).fetchone()
        existing_shop_id = row['shop_id'] if row else None
        # Active job?
        if existing_shop_id:
            j = c.execute('SELECT status FROM etsy_import_job WHERE shop_id=?',
                          (existing_shop_id,)).fetchone()
            if j and j['status'] == 'running':
                return {'ok': True, 'shop_id': existing_shop_id, 'status': 'already_running'}

    with _jobs_lock:
        t = threading.Thread(
            target=_import_worker,
            args=(shop_input, data_dir, etsy_request_fn),
            name=f'etsy-shop-import-{shop_name[:30]}',
            daemon=True,
        )
        t.start()
        _running_jobs[shop_name] = t

    return {'ok': True, 'shop_name': shop_name, 'status': 'started'}


# ---------- Read API ----------

def list_shops(data_dir):
    with _conn(data_dir) as c:
        rows = c.execute("""
            SELECT s.shop_id, s.shop_name, s.url, s.title, s.icon_url,
                   s.num_favorers, s.review_count, s.review_average,
                   s.transaction_sold_count, s.listing_active_count,
                   s.currency_code, s.country_iso, s.created_timestamp,
                   s.imported_at, s.last_refreshed_at,
                   (SELECT COUNT(*) FROM etsy_listing l WHERE l.shop_id=s.shop_id) AS imported_count,
                   (SELECT COUNT(*) FROM etsy_listing l WHERE l.shop_id=s.shop_id AND l.is_favorite=1) AS favorites_count
            FROM etsy_shop s
            ORDER BY s.last_refreshed_at DESC
        """).fetchall()
    return [dict(r) for r in rows]


def get_shop_detail(data_dir, shop_id, sort='favorers', favorites_only=False,
                    search='', limit=None, offset=0):
    with _conn(data_dir) as c:
        shop = c.execute('SELECT * FROM etsy_shop WHERE shop_id=?', (shop_id,)).fetchone()
        if not shop:
            return None
        sort_col = {
            'favorers':  'num_favorers DESC',
            'views':     'views DESC',
            'newest':    'created_timestamp DESC',
            'oldest':    'created_timestamp ASC',
            'price_asc': 'CAST(price_amount AS REAL)/COALESCE(price_divisor,100) ASC',
            'price_desc':'CAST(price_amount AS REAL)/COALESCE(price_divisor,100) DESC',
            'title':     'title ASC',
        }.get(sort, 'num_favorers DESC')
        q = 'SELECT * FROM etsy_listing WHERE shop_id=?'
        args = [shop_id]
        if favorites_only:
            q += ' AND is_favorite=1'
        if search:
            q += ' AND (LOWER(title) LIKE ? OR LOWER(tags) LIKE ?)'
            s = '%' + search.lower() + '%'
            args += [s, s]
        q += f' ORDER BY {sort_col}'
        if limit:
            q += ' LIMIT ? OFFSET ?'
            args += [int(limit), int(offset)]
        listings = c.execute(q, args).fetchall()

        # Job status
        job = c.execute('SELECT * FROM etsy_import_job WHERE shop_id=?',
                        (shop_id,)).fetchone()

    def listing_dict(r):
        d = dict(r)
        amt = d.pop('price_amount', 0) or 0
        div = d.pop('price_divisor', 100) or 100
        d['price'] = amt / div if div else amt
        # parse JSON columns
        for k in ('tags', 'materials', 'image_urls'):
            try:
                d[k] = json.loads(d.get(k) or '[]')
            except Exception:
                d[k] = []
        d.pop('raw_json', None)
        return d

    return {
        'shop': dict(shop),
        'listings': [listing_dict(r) for r in listings],
        'job': dict(job) if job else None,
    }


def get_progress(data_dir, shop_id):
    with _conn(data_dir) as c:
        row = c.execute('SELECT * FROM etsy_import_job WHERE shop_id=?',
                        (shop_id,)).fetchone()
    return dict(row) if row else None


def delete_shop(data_dir, shop_id):
    """Remove all data for this shop: DB rows AND on-disk image files."""
    with _conn(data_dir) as c:
        c.execute('DELETE FROM etsy_listing WHERE shop_id=?', (shop_id,))
        c.execute('DELETE FROM etsy_shop WHERE shop_id=?', (shop_id,))
        c.execute('DELETE FROM etsy_import_job WHERE shop_id=?', (shop_id,))
    # Delete cached image files to free disk space.
    try:
        import shutil
        img_dir = os.path.join(_image_dir(data_dir), str(shop_id))
        if os.path.isdir(img_dir):
            shutil.rmtree(img_dir, ignore_errors=True)
    except Exception as e:
        log.warning('Could not remove image dir for shop %s: %s', shop_id, e)
    return True


def toggle_favorite(data_dir, listing_id, value=None):
    """If value is None, toggle. Returns new state (1 or 0) and listing dict."""
    with _conn(data_dir) as c:
        cur = c.execute('SELECT is_favorite FROM etsy_listing WHERE listing_id=?',
                        (listing_id,)).fetchone()
        if not cur:
            return None
        new_val = (1 - int(cur['is_favorite'] or 0)) if value is None else (1 if value else 0)
        c.execute('UPDATE etsy_listing SET is_favorite=?, favorited_at=? WHERE listing_id=?',
                  (new_val, int(time.time()) if new_val else None, listing_id))
        c.commit()
    return new_val


def list_favorites(data_dir):
    with _conn(data_dir) as c:
        rows = c.execute("""
            SELECT l.*, s.shop_name, s.url AS shop_url
            FROM etsy_listing l
            JOIN etsy_shop s ON s.shop_id = l.shop_id
            WHERE l.is_favorite=1
            ORDER BY l.favorited_at DESC
        """).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        amt = d.pop('price_amount', 0) or 0
        div = d.pop('price_divisor', 100) or 100
        d['price'] = amt / div if div else amt
        for k in ('tags', 'materials', 'image_urls'):
            try:
                d[k] = json.loads(d.get(k) or '[]')
            except Exception:
                d[k] = []
        d.pop('raw_json', None)
        out.append(d)
    return out


# ---------- Flask wiring ----------

def _recover_zombie_jobs(data_dir):
    """Any job still marked status='running' at startup is a zombie — the
    worker thread died with the process. Mark as 'error' so the UI can
    show a clean retry path instead of polling forever.
    """
    try:
        with _conn(data_dir) as c:
            n = c.execute(
                """UPDATE etsy_import_job
                   SET status='error',
                       error=COALESCE(error,'')||'Import interrupted by server restart. Click Refresh to retry.',
                       finished_at=strftime('%s','now')
                   WHERE status IN ('running','queued')"""
            ).rowcount
            if n:
                log.info('etsy-shops: recovered %d zombie import job(s)', n)
    except Exception as e:
        log.warning('etsy-shops: zombie recovery failed: %s', e)


def register_routes(app, data_dir, etsy_request_fn, login_required, send_from_directory):
    """Attach all /api/etsy-shops/* routes and the image-serve route to the Flask app."""
    from flask import jsonify, request

    init_db(data_dir)
    _recover_zombie_jobs(data_dir)

    @app.route('/api/etsy-shops', methods=['GET'])
    @login_required
    def _es_list():
        return jsonify({'ok': True, 'shops': list_shops(data_dir)})

    @app.route('/api/etsy-shops/import', methods=['POST'])
    @login_required
    def _es_import():
        body = request.get_json(silent=True) or {}
        shop_url = body.get('shop_url') or body.get('shop_name') or ''
        result = start_import(shop_url, data_dir, etsy_request_fn)
        if not result.get('ok'):
            return jsonify({'ok': False, 'error': result.get('error')}), 400
        return jsonify(result)

    @app.route('/api/etsy-shops/<int:shop_id>', methods=['GET'])
    @login_required
    def _es_get(shop_id):
        sort = request.args.get('sort', 'favorers')
        fav_only = request.args.get('favorites_only') in ('1', 'true', 'yes')
        search = (request.args.get('search') or '').strip()
        limit = request.args.get('limit')
        limit = int(limit) if (limit and limit.isdigit()) else None
        offset = int(request.args.get('offset') or 0)
        d = get_shop_detail(data_dir, shop_id, sort=sort,
                            favorites_only=fav_only, search=search,
                            limit=limit, offset=offset)
        if not d:
            return jsonify({'ok': False, 'error': 'Shop not found'}), 404
        return jsonify({'ok': True, **d})

    @app.route('/api/etsy-shops/<int:shop_id>/progress', methods=['GET'])
    @login_required
    def _es_progress(shop_id):
        p = get_progress(data_dir, shop_id)
        if not p:
            return jsonify({'ok': True, 'progress': None})
        return jsonify({'ok': True, 'progress': p})

    @app.route('/api/etsy-shops/progress/by-name', methods=['GET'])
    @login_required
    def _es_progress_name():
        """Look up progress by shop_name during the pre-resolve window when shop_id is still 0."""
        name = (request.args.get('shop_name') or '').strip()
        if not name:
            return jsonify({'ok': False, 'error': 'shop_name required'}), 400
        with _conn(data_dir) as c:
            row = c.execute("""SELECT * FROM etsy_import_job
                               WHERE LOWER(shop_name)=LOWER(?)
                               ORDER BY COALESCE(finished_at, started_at) DESC LIMIT 1""",
                             (name,)).fetchone()
        return jsonify({'ok': True, 'progress': dict(row) if row else None})

    @app.route('/api/etsy-shops/<int:shop_id>/refresh', methods=['POST'])
    @login_required
    def _es_refresh(shop_id):
        with _conn(data_dir) as c:
            row = c.execute('SELECT shop_name FROM etsy_shop WHERE shop_id=?',
                            (shop_id,)).fetchone()
        if not row:
            return jsonify({'ok': False, 'error': 'Shop not found'}), 404
        result = start_import(row['shop_name'], data_dir, etsy_request_fn)
        return jsonify(result)

    @app.route('/api/etsy-shops/<int:shop_id>', methods=['DELETE'])
    @login_required
    def _es_delete(shop_id):
        delete_shop(data_dir, shop_id)
        return jsonify({'ok': True})

    @app.route('/api/etsy-shops/<int:shop_id>/favorite/<int:listing_id>', methods=['POST'])
    @login_required
    def _es_fav(shop_id, listing_id):
        body = request.get_json(silent=True) or {}
        value = body.get('value')
        new_val = toggle_favorite(data_dir, listing_id, value=value)
        if new_val is None:
            return jsonify({'ok': False, 'error': 'Listing not found'}), 404
        return jsonify({'ok': True, 'is_favorite': new_val})

    @app.route('/api/etsy-shops/favorites', methods=['GET'])
    @login_required
    def _es_favs():
        return jsonify({'ok': True, 'favorites': list_favorites(data_dir)})

    @app.route('/etsy-shop-images/<int:shop_id>/<path:filename>')
    def _es_img(shop_id, filename):
        return send_from_directory(os.path.join(_images_dir(data_dir), str(shop_id)),
                                   filename)
