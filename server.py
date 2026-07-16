#!/usr/bin/env python3
"""Video Pin Generator — AI-powered product video creation for Pinterest.
Single-file Flask backend. Port 5110.
"""

from flask import Flask, send_file, send_from_directory, request, jsonify, redirect, session, make_response, url_for
from functools import wraps
import bcrypt
import secrets
import os

os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
import json
import time
import uuid
import threading
import requests as http_requests
import re
import html as html_mod
try:
    from urllib3.exceptions import InsecureRequestWarning
    http_requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except Exception:
    pass
import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

app = Flask(__name__)

# Trust reverse proxy headers (nginx) so request.host_url uses https://
from werkzeug.middleware.proxy_fix import ProxyFix
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
VIDEOS_DIR = os.path.join(DATA_DIR, 'videos')
os.makedirs(VIDEOS_DIR, exist_ok=True)
PRODUCT_IMAGES_DIR = os.path.join(DATA_DIR, 'product_images')
os.makedirs(PRODUCT_IMAGES_DIR, exist_ok=True)

# --- Persistent secret key ---
SECRET_KEY_FILE = os.path.join(BASE_DIR, '.flask_secret')
if os.path.exists(SECRET_KEY_FILE):
    with open(SECRET_KEY_FILE, 'r') as f:
        app.secret_key = f.read().strip()
else:
    _key = secrets.token_hex(32)
    with open(SECRET_KEY_FILE, 'w') as f:
        f.write(_key)
    app.secret_key = _key

app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# ===== Auth =====
PASSWORD_HASH_FILE = os.path.join(BASE_DIR, '.password_hash')
VIEWER_HASH_FILE = os.path.join(BASE_DIR, '.viewer_password_hash')
DEFAULT_PASSWORD = os.environ.get('VPG_PASSWORD', 'videopins2026!')
DEFAULT_VIEWER_PASSWORD = os.environ.get('VPG_VIEWER_PASSWORD', 'analytics2026!')

def _get_password_hash():
    if os.path.exists(PASSWORD_HASH_FILE):
        with open(PASSWORD_HASH_FILE, 'r') as f:
            return f.read().strip().encode('utf-8')
    hashed = bcrypt.hashpw(DEFAULT_PASSWORD.encode('utf-8'), bcrypt.gensalt(rounds=12))
    with open(PASSWORD_HASH_FILE, 'w') as f:
        f.write(hashed.decode('utf-8'))
    return hashed

def _get_viewer_hash():
    if os.path.exists(VIEWER_HASH_FILE):
        with open(VIEWER_HASH_FILE, 'r') as f:
            return f.read().strip().encode('utf-8')
    hashed = bcrypt.hashpw(DEFAULT_VIEWER_PASSWORD.encode('utf-8'), bcrypt.gensalt(rounds=12))
    with open(VIEWER_HASH_FILE, 'w') as f:
        f.write(hashed.decode('utf-8'))
    return hashed

PASSWORD_HASH = _get_password_hash()
VIEWER_PASSWORD_HASH = _get_viewer_hash()

def verify_password(password):
    return bcrypt.checkpw(password.encode('utf-8'), PASSWORD_HASH)

_login_attempts = {}
MAX_ATTEMPTS = 5
LOCKOUT_WINDOW = 300
LOCKOUT_DURATION = 900

def _get_client_ip():
    return request.headers.get('X-Forwarded-For', request.remote_addr or '0.0.0.0').split(',')[0].strip()

def _check_rate_limit(ip):
    now = time.time()
    rec = _login_attempts.get(ip)
    if not rec:
        return True, 0
    if rec.get('locked_until', 0) > now:
        return False, int(rec['locked_until'] - now)
    if now - rec['first_attempt'] > LOCKOUT_WINDOW:
        del _login_attempts[ip]
        return True, 0
    return True, 0

def _record_failed_attempt(ip):
    now = time.time()
    rec = _login_attempts.get(ip)
    if not rec or (now - rec['first_attempt'] > LOCKOUT_WINDOW):
        _login_attempts[ip] = {'count': 1, 'first_attempt': now}
        return
    rec['count'] += 1
    if rec['count'] >= MAX_ATTEMPTS:
        rec['locked_until'] = now + LOCKOUT_DURATION

def _clear_attempts(ip):
    _login_attempts.pop(ip, None)

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('authenticated'):
            if request.path.startswith('/api/'):
                return jsonify({'success': False, 'error': 'Not authenticated'}), 401
            return redirect('/login')
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('authenticated'):
            if request.path.startswith('/api/'):
                return jsonify({'success': False, 'error': 'Not authenticated'}), 401
            return redirect('/login')
        if session.get('role', 'admin') != 'admin':
            return jsonify({'success': False, 'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated


# ===== Data helpers =====
def _load_json(filename, default=None):
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except:
            pass
    return default if default is not None else {}

def _save_json(filename, data):
    path = os.path.join(DATA_DIR, filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Backup before overwriting critical files
    if filename in ('stores.json', 'settings.json') and os.path.exists(path):
        try:
            existing = os.path.getsize(path)
            if existing > 0:
                import shutil
                shutil.copy2(path, path + '.bak')
        except Exception:
            pass
    # Atomic write: write to temp file then rename
    tmp_path = path + '.tmp'
    with open(tmp_path, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, path)

def _load_stores():
    return _load_json('stores.json', [])

def _save_stores(stores):
    if not stores:
        log.warning('Refusing to save empty stores list')
        return
    _save_json('stores.json', stores)

# ===== Exchange rate helpers =====
def _get_eur_usd_rate():
    """Fetch EUR/USD rate, cached daily in data/exchange_rates.json."""
    cache = _load_json('exchange_rates.json', {})
    today = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
    if cache.get('date') == today and cache.get('EUR_USD'):
        return cache['EUR_USD']
    try:
        resp = http_requests.get('https://api.frankfurter.dev/v1/latest?from=EUR&to=USD', timeout=10)
        if resp.status_code == 200:
            rate = resp.json().get('rates', {}).get('USD', 1.08)
            _save_json('exchange_rates.json', {'EUR_USD': rate, 'date': today})
            log.info(f'Fetched EUR/USD rate: {rate}')
            return rate
    except Exception as e:
        log.warning(f'Failed to fetch EUR/USD rate: {e}')
    return cache.get('EUR_USD', 1.08)  # fallback to cached or hardcoded

def _ensure_store_currency(store):
    """Fetch and cache store currency from Shopify if not set."""
    if store.get('currency'):
        return store['currency']
    domain = store.get('domain', '')
    token = store.get('shopifyAccessToken', '')
    if not domain or not token:
        return 'USD'
    try:
        resp = http_requests.get(
            f'https://{domain}/admin/api/2024-01/shop.json?fields=currency',
            headers={'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'},
            timeout=10
        )
        if resp.status_code == 200:
            currency = resp.json().get('shop', {}).get('currency', 'USD')
            store['currency'] = currency
            stores = _load_stores()
            for s in stores:
                if s['id'] == store['id']:
                    s['currency'] = currency
                    break
            _save_stores(stores)
            log.info(f'Detected currency for {store.get("name", "")}: {currency}')
            return currency
    except Exception as e:
        log.warning(f'Failed to fetch currency for {store.get("name", "")}: {e}')
    return 'USD'

# Default prompt batches — each category gets 4 prompts
_DEFAULT_PROMPT_BATCHES = {
    'sweaters': [
        {'label': 'Studio', 'template': 'Elegant slow-motion showcase of {product_name} sweater, soft knit texture detail, professional studio lighting, clean background, fashion editorial style'},
        {'label': 'Lifestyle', 'template': 'Cozy lifestyle shot of {product_name}, model in warm indoor setting, natural lighting, autumn/winter editorial feel'},
        {'label': 'Detail', 'template': 'Close-up of {product_name} knit pattern and fabric texture, soft focus background, premium quality feel, slow camera drift'},
        {'label': 'Dynamic', 'template': 'Dynamic showcase of {product_name}, smooth camera movement around the sweater, modern styling, commercial quality'},
    ],
    'dresses': [
        {'label': 'Studio', 'template': 'Elegant slow-motion showcase of {product_name} dress, flowing fabric movement, professional studio lighting, fashion editorial style'},
        {'label': 'Lifestyle', 'template': 'Beautiful lifestyle shot of {product_name}, model in stylish setting, golden hour lighting, dress in motion'},
        {'label': 'Detail', 'template': 'Close-up detail shot of {product_name} fabric and stitching, soft bokeh background, premium fashion commercial feel'},
        {'label': 'Dynamic', 'template': 'Dynamic fashion showcase of {product_name}, dress flowing with movement, smooth cinematic camera, editorial quality'},
    ],
    'tops': [
        {'label': 'Studio', 'template': 'Professional studio showcase of {product_name} top, clean lighting, crisp details, fashion commercial style'},
        {'label': 'Lifestyle', 'template': 'Casual lifestyle shot of {product_name}, styled with modern accessories, natural warm lighting, editorial feel'},
        {'label': 'Detail', 'template': 'Close-up on {product_name} fabric and collar detail, shallow depth of field, premium quality commercial'},
        {'label': 'Dynamic', 'template': 'Dynamic presentation of {product_name}, smooth camera pan, fresh modern styling, commercial production quality'},
    ],
    'shirts': [
        {'label': 'Studio', 'template': 'Clean studio showcase of {product_name} shirt, crisp fabric detail, professional lighting, fashion editorial style'},
        {'label': 'Lifestyle', 'template': 'Lifestyle shot of {product_name}, casually styled, warm natural indoor lighting, editorial commercial feel'},
        {'label': 'Detail', 'template': 'Close-up of {product_name} collar and button details, soft background blur, premium shirt commercial'},
        {'label': 'Dynamic', 'template': 'Dynamic showcase of {product_name} shirt, smooth rotating camera movement, clean modern styling, commercial quality'},
    ],
    'bikinis': [
        {'label': 'Studio', 'template': 'Vibrant studio showcase of {product_name} bikini, bright clean lighting, summer editorial style, professional product photography'},
        {'label': 'Lifestyle', 'template': 'Sunny lifestyle shot of {product_name}, beach or poolside setting, golden natural lighting, summer editorial feel'},
        {'label': 'Detail', 'template': 'Close-up detail of {product_name} fabric and design pattern, bright soft lighting, premium swimwear commercial'},
        {'label': 'Dynamic', 'template': 'Dynamic summer showcase of {product_name}, vibrant colors, smooth camera movement, tropical editorial quality'},
    ],
    'coats': [
        {'label': 'Studio', 'template': 'Elegant slow-motion showcase of {product_name} coat, rich fabric texture, dramatic studio lighting, luxury fashion editorial'},
        {'label': 'Lifestyle', 'template': 'Lifestyle shot of {product_name}, urban outdoor setting, moody natural lighting, winter fashion editorial'},
        {'label': 'Detail', 'template': 'Close-up of {product_name} fabric texture, buttons and lining details, soft focus background, luxury commercial feel'},
        {'label': 'Dynamic', 'template': 'Dynamic cinematic showcase of {product_name} coat, flowing movement, smooth camera work, premium fashion commercial'},
    ],
    'rings': [
        {'label': 'Studio', 'template': 'Luxurious close-up of {product_name} ring, sparkling reflections, dark elegant background, jewelry commercial lighting'},
        {'label': 'Lifestyle', 'template': 'Elegant lifestyle shot of {product_name} ring on hand, soft natural lighting, romantic setting, jewelry editorial'},
        {'label': 'Detail', 'template': 'Extreme close-up macro shot of {product_name}, gemstone detail and metal finish, dramatic reflections, luxury feel'},
        {'label': 'Dynamic', 'template': 'Slow rotating showcase of {product_name} ring, catching light from every angle, dark velvet background, premium commercial'},
    ],
    'necklaces': [
        {'label': 'Studio', 'template': 'Luxurious showcase of {product_name} necklace, elegant draping, soft studio lighting, jewelry commercial style'},
        {'label': 'Lifestyle', 'template': 'Lifestyle shot of {product_name} necklace worn elegantly, soft natural lighting, fashion editorial feel'},
        {'label': 'Detail', 'template': 'Close-up detail of {product_name} chain and pendant, sparkling reflections, shallow depth of field, luxury commercial'},
        {'label': 'Dynamic', 'template': 'Slow-motion showcase of {product_name} necklace, light catching each link, smooth camera drift, premium jewelry commercial'},
    ],
    'bracelets': [
        {'label': 'Studio', 'template': 'Elegant studio shot of {product_name} bracelet, soft reflections, professional jewelry lighting, clean dark background'},
        {'label': 'Lifestyle', 'template': 'Lifestyle shot of {product_name} bracelet on wrist, natural daylight, casual elegant styling, editorial feel'},
        {'label': 'Detail', 'template': 'Macro detail shot of {product_name} bracelet clasp and links, sparkling finish, premium quality commercial'},
        {'label': 'Dynamic', 'template': 'Dynamic showcase of {product_name} bracelet, smooth rotating view, light reflections, luxury jewelry commercial'},
    ],
    'earrings': [
        {'label': 'Studio', 'template': 'Luxurious close-up of {product_name} earrings, dramatic lighting, soft bokeh background, jewelry commercial style'},
        {'label': 'Lifestyle', 'template': 'Elegant lifestyle shot of {product_name} earrings being worn, soft natural lighting, fashion editorial'},
        {'label': 'Detail', 'template': 'Extreme close-up of {product_name} earring detail and gemstones, sparkling reflections, luxury commercial feel'},
        {'label': 'Dynamic', 'template': 'Slow-motion showcase of {product_name} earrings catching light, gentle movement, premium jewelry commercial'},
    ],
    'sandals': [
        {'label': 'Studio', 'template': 'Clean studio showcase of {product_name} sandals, bright lighting, crisp product detail, footwear commercial style'},
        {'label': 'Lifestyle', 'template': 'Lifestyle shot of {product_name} sandals, outdoor summer setting, warm natural lighting, casual editorial feel'},
        {'label': 'Detail', 'template': 'Close-up of {product_name} sandal straps and sole detail, soft background, premium footwear commercial'},
        {'label': 'Dynamic', 'template': 'Dynamic showcase of {product_name} sandals, walking motion, smooth camera tracking, summer commercial quality'},
    ],
    'sneakers': [
        {'label': 'Studio', 'template': 'Bold studio showcase of {product_name} sneakers, dramatic lighting, clean background, modern footwear commercial'},
        {'label': 'Lifestyle', 'template': 'Street-style lifestyle shot of {product_name} sneakers, urban setting, dynamic natural lighting, sneaker culture editorial'},
        {'label': 'Detail', 'template': 'Close-up of {product_name} sneaker sole, stitching and material detail, modern commercial feel, shallow depth of field'},
        {'label': 'Dynamic', 'template': 'Dynamic showcase of {product_name} sneakers, smooth 360 rotating view, dramatic lighting, premium footwear commercial'},
    ],
    'heels': [
        {'label': 'Studio', 'template': 'Elegant studio showcase of {product_name} heels, dramatic lighting, glossy reflection, luxury footwear commercial'},
        {'label': 'Lifestyle', 'template': 'Sophisticated lifestyle shot of {product_name} heels, elegant indoor setting, warm lighting, fashion editorial style'},
        {'label': 'Detail', 'template': 'Close-up of {product_name} heel and sole craftsmanship, soft bokeh background, luxury commercial feel'},
        {'label': 'Dynamic', 'template': 'Slow-motion cinematic showcase of {product_name} heels, smooth camera movement, dramatic angles, premium fashion commercial'},
    ],
    'default': [
        {'label': 'Studio', 'template': 'Professional product showcase of {product_name}, clean studio lighting, smooth rotating view, commercial quality, elegant presentation'},
        {'label': 'Lifestyle', 'template': 'Lifestyle shot of {product_name}, warm natural lighting, modern setting, editorial style'},
        {'label': 'Detail', 'template': 'Close-up detail shot of {product_name}, soft bokeh background, premium commercial feel'},
        {'label': 'Dynamic', 'template': 'Dynamic showcase of {product_name}, smooth camera movement, professional product film'},
    ],
}

def _load_settings():
    defaults = {
        'xai_api_key': '',
        'xai_video_model': 'grok-imagine-video',
        'video_duration': 8,
        'video_resolution': '720p',
        'aspect_ratio': '9:16',
        'prompt_batches': {},
        'custom_prompt_batches': {},
        'google_ai_key': '',
        'google_ai_model': 'gemini-2.0-flash',
        'gdrive_client_id': '',
        'gdrive_client_secret': '',
        'gdrive_access_token': '',
        'gdrive_refresh_token': '',
        'gdrive_token_expiry': '',
        'gdrive_root_folder_id': '',
        'gdrive_status': '',
        'shopify_client_id': '',
        'shopify_client_secret': '',
        'shopify_scopes': 'read_products,read_orders,read_apps,read_reports',
    }
    settings = _load_json('settings.json', defaults)
    # Ensure all default keys exist
    for k, v in defaults.items():
        if k not in settings:
            settings[k] = v

    # Migrate old prompt_templates to prompt_batches if needed
    if 'prompt_templates' in settings and settings['prompt_templates'] and not settings.get('prompt_batches'):
        old = settings['prompt_templates']
        migrated = {}
        for cat, tmpl in old.items():
            if isinstance(tmpl, str):
                # Convert single template to batch of 4
                migrated[cat] = [
                    {'label': 'Studio', 'template': tmpl},
                    {'label': 'Lifestyle', 'template': 'Lifestyle shot of {product_name}, warm natural lighting, modern setting, editorial style'},
                    {'label': 'Detail', 'template': 'Close-up detail shot of {product_name}, soft bokeh background, premium commercial feel'},
                    {'label': 'Dynamic', 'template': 'Dynamic showcase of {product_name}, smooth camera movement, professional product film'},
                ]
        settings['prompt_batches'] = migrated
        settings.pop('prompt_templates', None)
        _save_json('settings.json', settings)

    # Ensure prompt_batches has all default categories
    batches = settings.get('prompt_batches', {})
    for cat, prompts in _DEFAULT_PROMPT_BATCHES.items():
        if cat not in batches:
            batches[cat] = prompts
    settings['prompt_batches'] = batches

    # Ensure custom_prompt_batches exists
    if 'custom_prompt_batches' not in settings:
        settings['custom_prompt_batches'] = {}

    return settings

def _save_settings(settings):
    _save_json('settings.json', settings)

def _load_jobs():
    return _load_json('video_jobs.json', [])

def _save_jobs(jobs):
    _save_json('video_jobs.json', jobs)

def _load_archive():
    return _load_json('video_jobs_archive.json', [])

def _save_archive(jobs):
    _save_json('video_jobs_archive.json', jobs)

def _archive_old_jobs():
    """Move completed/failed/cancelled jobs older than 1 hour to archive."""
    now = datetime.datetime.now(datetime.timezone.utc)
    with _jobs_lock:
        jobs = _load_jobs()
        active = []
        to_archive = []
        for j in jobs:
            status = j.get('status', '')
            if status in ('done', 'failed', 'cancelled'):
                # Use completedAt for done, or createdAt as fallback
                ts_str = j.get('completedAt', '') or j.get('createdAt', '')
                if ts_str:
                    try:
                        dt = datetime.datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                        age_seconds = (now - dt).total_seconds()
                        if age_seconds > 3600:  # 1 hour
                            j['archivedAt'] = now.isoformat().replace('+00:00', 'Z')
                            to_archive.append(j)
                            continue
                    except (ValueError, TypeError):
                        pass
            active.append(j)

        if to_archive:
            archive = _load_archive()
            archive.extend(to_archive)
            _save_archive(archive)
            _save_jobs(active)
            log.info(f"Archived {len(to_archive)} old jobs ({len(active)} active remain)")

def _load_all_jobs():
    """Load jobs from both active and archive files."""
    with _jobs_lock:
        active = _load_jobs()
        archive = _load_archive()
    return active + archive


# ===== Google Drive helper =====
def _get_drive_service():
    """Build a Google Drive API service using OAuth credentials."""
    settings = _load_settings()
    access_token = settings.get('gdrive_access_token', '')
    refresh_token = settings.get('gdrive_refresh_token', '')
    client_id = settings.get('gdrive_client_id', '')
    client_secret = settings.get('gdrive_client_secret', '')

    if not access_token or not refresh_token:
        return None, 'Google Drive not connected. Connect in Settings.'

    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build as build_service
    except ImportError:
        return None, 'google-auth / google-api-python-client not installed'

    try:
        creds = Credentials(
            token=access_token,
            refresh_token=refresh_token,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=client_id,
            client_secret=client_secret,
            scopes=['https://www.googleapis.com/auth/drive']
        )

        # Auto-refresh if expired
        if creds.expired and creds.refresh_token:
            from google.auth.transport.requests import Request
            creds.refresh(Request())
            # Save new tokens
            settings['gdrive_access_token'] = creds.token
            if creds.expiry:
                settings['gdrive_token_expiry'] = creds.expiry.isoformat()
            _save_settings(settings)

        service = build_service('drive', 'v3', credentials=creds, cache_discovery=False)
        return service, None
    except Exception as e:
        return None, str(e)


def _get_or_create_root_folder(service):
    """Find or create the 'Video Pin Generator' root folder in Drive."""
    settings = _load_settings()
    root_id = settings.get('gdrive_root_folder_id', '')

    # If we have a stored root folder ID, verify it still exists
    if root_id:
        try:
            f = service.files().get(fileId=root_id, fields='id,name,trashed').execute()
            if not f.get('trashed'):
                return root_id
        except:
            pass  # Folder deleted or inaccessible, create new one

    # Search for existing folder
    q = "name='Video Pin Generator' and mimeType='application/vnd.google-apps.folder' and trashed=false and 'root' in parents"
    results = service.files().list(q=q, fields='files(id,name)', pageSize=1).execute()
    if results.get('files'):
        root_id = results['files'][0]['id']
    else:
        # Create it
        folder_metadata = {
            'name': 'Video Pin Generator',
            'mimeType': 'application/vnd.google-apps.folder'
        }
        folder = service.files().create(body=folder_metadata, fields='id').execute()
        root_id = folder['id']

    # Save for future use
    settings['gdrive_root_folder_id'] = root_id
    _save_settings(settings)
    return root_id


def _find_or_create_drive_folder(service, folder_name, parent_id):
    """Find a folder by name under parent, or create it. Returns folder ID."""
    # Escape single quotes in folder name for Drive API query
    safe_name = folder_name.replace("'", "\\'")
    q = f"name='{safe_name}' and '{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    existing = service.files().list(q=q, fields='files(id,name)', pageSize=1).execute()

    if existing.get('files'):
        return existing['files'][0]['id']

    folder_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }
    folder = service.files().create(body=folder_metadata, fields='id').execute()
    return folder['id']


def _find_or_create_numbered_product_folder(service, product_handle, store_folder_id):
    """Find existing numbered product folder or create next one like #001-handle, #002-handle."""
    # List all folders in the store folder
    q = f"'{store_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(q=q, fields='files(id,name)', pageSize=500).execute()
    folders = results.get('files', [])

    # Check if this product already has a numbered folder
    for f in folders:
        name = f['name']
        # Match pattern like #001-product-handle or #002-product-handle
        if name.split('-', 1)[-1] == product_handle or name.lstrip('#').split('-', 1)[-1] == product_handle:
            return f['id']
        # Also check without the number prefix for backwards compatibility
        if name == product_handle:
            return f['id']

    # Find the highest existing number
    max_num = 0
    for f in folders:
        name = f['name']
        if name.startswith('#') and '-' in name:
            try:
                num_str = name.split('-', 1)[0].lstrip('#')
                num = int(num_str)
                if num > max_num:
                    max_num = num
            except (ValueError, IndexError):
                pass

    # Create new numbered folder
    next_num = max_num + 1
    numbered_name = f'#{next_num:03d}-{product_handle}'

    folder_metadata = {
        'name': numbered_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [store_folder_id]
    }
    folder = service.files().create(body=folder_metadata, fields='id').execute()
    return folder['id']


# ===== Job queue + background worker =====
from concurrent.futures import ThreadPoolExecutor
_jobs_lock = threading.Lock()
_worker_running = False
_executor = ThreadPoolExecutor(max_workers=2)
_in_flight = set()  # job IDs currently being processed
_in_flight_lock = threading.Lock()

def _get_prompt(product_name, store_category, settings):
    """Get a single prompt (fallback for old code paths)."""
    batches = settings.get('prompt_batches', {})
    category = (store_category or 'default').lower()
    batch = batches.get(category, batches.get('default', _DEFAULT_PROMPT_BATCHES.get('default', [])))
    if batch and isinstance(batch, list) and len(batch) > 0:
        template = batch[0].get('template', 'Professional product showcase of {product_name}')
    else:
        template = 'Professional product showcase of {product_name}'
    return template.replace('{product_name}', product_name).replace('{store_category}', category)

def _get_prompt_batch(category_key, settings):
    """Get a batch of 4 prompts for a given category key (built-in or custom)."""
    # Check built-in batches first
    batches = settings.get('prompt_batches', {})
    if category_key in batches:
        return batches[category_key]
    # Check custom batches
    custom = settings.get('custom_prompt_batches', {})
    if category_key in custom:
        return custom[category_key].get('prompts', [])
    # Fallback to default
    return batches.get('default', _DEFAULT_PROMPT_BATCHES.get('default', []))

def _get_default_store_prompts(store):
    """Return 4 default prompts for a store, based on its category."""
    settings = _load_settings()
    category = (store.get('storeCategory', '') or 'default').lower()
    return _get_prompt_batch(category, settings)

def _download_video(url, local_path):
    """Download a video file from URL to local path."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    resp = http_requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()
    with open(local_path, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    return local_path

def _process_job(job):
    """Process a single video generation job."""
    settings = _load_settings()
    api_key = settings.get('xai_api_key', '')
    if not api_key:
        job['status'] = 'failed'
        job['error'] = 'No xAI API key configured'
        return job

    prompt = job.get('prompt', '')
    if not prompt:
        prompt = _get_prompt(job['productName'], job.get('storeCategory', ''), settings)
        job['prompt'] = prompt

    duration = settings.get('video_duration', 8)
    resolution = settings.get('video_resolution', '720p')
    aspect_ratio = job.get('aspectRatio') or settings.get('aspect_ratio', '9:16')

    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }

    # Step 1: Submit video generation request
    video_model = job.get('videoModel') or settings.get('xai_video_model', 'grok-imagine-video')
    payload = {
        'model': video_model,
        'prompt': prompt,
        'image': {'url': job['imageUrl']},
        'duration': duration,
        'aspect_ratio': aspect_ratio,
        'resolution': resolution
    }

    try:
        job['status'] = 'generating'
        _save_jobs_safe(job)

        resp = http_requests.post(
            'https://api.x.ai/v1/videos/generations',
            headers=headers,
            json=payload,
            timeout=60
        )

        if resp.status_code != 200:
            job['status'] = 'failed'
            job['error'] = f'xAI API error {resp.status_code}: {resp.text[:500]}'
            return job

        data = resp.json()
        request_id = data.get('request_id')
        if not request_id:
            job['status'] = 'failed'
            job['error'] = f'No request_id in response: {json.dumps(data)[:500]}'
            return job

        job['xaiRequestId'] = request_id
        job['status'] = 'polling'
        _save_jobs_safe(job)

        # Step 2: Poll for completion
        max_polls = 36  # 3 minutes max (5s * 36)
        for poll_i in range(max_polls):
            time.sleep(5)

            # Check if job was skipped/cancelled externally
            with _jobs_lock:
                current_jobs = _load_jobs()
                current = next((jj for jj in current_jobs if jj['id'] == job['id']), None)
                if current and current['status'] == 'failed':
                    job['status'] = 'failed'
                    job['error'] = current.get('error', 'Skipped')
                    return job

            try:
                poll_resp = http_requests.get(
                    f'https://api.x.ai/v1/videos/{request_id}',
                    headers={'Authorization': f'Bearer {api_key}'},
                    timeout=(10, 30)
                )
            except http_requests.exceptions.Timeout:
                log.warning(f"Poll timeout for {job['id']}, attempt {poll_i+1}/{max_polls}")
                continue

            if poll_resp.status_code != 200:
                continue

            poll_data = poll_resp.json()
            status = poll_data.get('status', '')

            if status == 'done':
                video_url = poll_data.get('video', {}).get('url', '')
                if not video_url:
                    job['status'] = 'failed'
                    job['error'] = 'Video completed but no URL returned'
                    return job

                # Download video locally
                safe_store = re.sub(r'[^\w\-]', '_', job.get('storeName', 'unknown'))
                safe_handle = re.sub(r'[^\w\-]', '_', job.get('productHandle', 'unknown'))
                ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                prompt_idx = job.get('promptIndex', 0)
                job_suffix = job.get('id', '')[-6:]  # unique suffix from job ID
                local_dir = os.path.join(VIDEOS_DIR, safe_store, safe_handle)
                local_path = os.path.join(local_dir, f'video_{ts}_p{prompt_idx}_{job_suffix}.mp4')

                try:
                    _download_video(video_url, local_path)
                    # Store relative path from VIDEOS_DIR
                    job['localPath'] = os.path.relpath(local_path, VIDEOS_DIR)
                except Exception as e:
                    job['status'] = 'failed'
                    job['error'] = f'Failed to download video: {str(e)}'
                    return job

                job['videoUrl'] = video_url
                job['status'] = 'done'
                job['completedAt'] = datetime.datetime.utcnow().isoformat() + 'Z'

                # Record actual duration and cost from xAI response
                actual_duration = poll_data.get('video', {}).get('duration')
                if actual_duration:
                    job['videoDuration'] = actual_duration
                    job['actualCost'] = round(actual_duration * 0.05, 2)
                else:
                    # Fall back to estimated cost as actual
                    job['actualCost'] = job.get('estimatedCost', round(duration * 0.05, 2))

                return job

            elif status == 'failed':
                job['status'] = 'failed'
                job['error'] = poll_data.get('error', 'Video generation failed')
                return job

            elif status == 'expired':
                job['status'] = 'failed'
                job['error'] = 'Video generation request expired'
                return job

            # Still pending — continue polling
            _save_jobs_safe(job)

        # Timed out
        job['status'] = 'failed'
        job['error'] = 'Generation timed out after 3 minutes — retry later'
        return job

    except Exception as e:
        job['status'] = 'failed'
        job['error'] = str(e)
        return job

def _save_jobs_safe(updated_job):
    """Thread-safe save of a single job update."""
    with _jobs_lock:
        jobs = _load_jobs()
        for i, j in enumerate(jobs):
            if j['id'] == updated_job['id']:
                jobs[i] = updated_job
                break
        _save_jobs(jobs)

def _auto_upload_to_drive(job):
    """Automatically upload a completed video to Google Drive if Drive is configured."""
    if not job.get('localPath'):
        return

    full_path = os.path.join(VIDEOS_DIR, job['localPath'])
    if not os.path.exists(full_path):
        return

    service, err = _get_drive_service()
    if not service:
        log.warning(f"Drive auto-upload skipped: {err}")
        return

    try:
        from googleapiclient.http import MediaFileUpload

        root_folder_id = _get_or_create_root_folder(service)
        store_name = job.get('storeName', 'Unknown Store')
        product_handle = job.get('productHandle', '') or job.get('productName', 'unknown-product')

        # Create folder structure: Root > Store Name > #NNN-product-handle
        store_folder_id = _find_or_create_drive_folder(service, store_name, root_folder_id)
        product_folder_id = _find_or_create_numbered_product_folder(service, product_handle, store_folder_id)

        # Upload
        file_name = os.path.basename(full_path)
        file_metadata = {'name': file_name, 'parents': [product_folder_id]}
        media = MediaFileUpload(full_path, mimetype='video/mp4', resumable=True)
        uploaded = service.files().create(
            body=file_metadata, media_body=media, fields='id,webViewLink'
        ).execute()

        drive_url = uploaded.get('webViewLink', '')
        drive_file_id = uploaded.get('id', '')
        with _jobs_lock:
            found_in_active = False
            jobs = _load_jobs()
            for j in jobs:
                if j['id'] == job['id']:
                    j['driveUrl'] = drive_url
                    j['driveFileId'] = drive_file_id
                    found_in_active = True
                    break
            if found_in_active:
                _save_jobs(jobs)
            else:
                archive = _load_archive()
                for j in archive:
                    if j['id'] == job['id']:
                        j['driveUrl'] = drive_url
                        j['driveFileId'] = drive_file_id
                        break
                _save_archive(archive)

        log.info(f"Auto-uploaded {job['id']} to Drive: {drive_url}")
    except ImportError:
        log.warning("Drive auto-upload skipped: google-api-python-client not installed")
    except Exception as e:
        log.warning(f"Drive auto-upload error: {e}")


def _process_and_save(job):
    """Wrapper that processes a job and saves result. Runs in thread pool."""
    job_id = job['id']
    try:
        result = _process_job(job)
        _save_jobs_safe(result)
        if result['status'] == 'done':
            log.info(f"Job {job_id} completed: {result.get('localPath', 'N/A')}")
            # Auto-upload to Google Drive
            try:
                _auto_upload_to_drive(result)
            except Exception as ue:
                log.warning(f"Auto-upload failed for {job_id}: {ue}")
        else:
            log.warning(f"Job {job_id} failed: {result.get('error', 'unknown')}")
    except Exception as e:
        job['status'] = 'failed'
        job['error'] = str(e)
        _save_jobs_safe(job)
        log.error(f"Job {job_id} error: {e}")
    finally:
        with _in_flight_lock:
            _in_flight.discard(job_id)


def _dispatcher_loop():
    """Picks up queued jobs and submits to thread pool (2 parallel workers)."""
    global _worker_running
    _worker_running = True
    log.info("Video dispatcher started (2 parallel workers)")

    _archive_counter = 0

    while True:
        try:
            # Periodically archive old completed jobs
            _archive_counter += 1
            if _archive_counter >= 10:
                _archive_counter = 0
                try:
                    _archive_old_jobs()
                except Exception as ae:
                    log.warning(f"Archive error: {ae}")

            # Find queued jobs
            with _jobs_lock:
                jobs = _load_jobs()
                queued = [j for j in jobs if j['status'] == 'queued']

            if not queued:
                time.sleep(3)
                continue

            # Submit up to available slots
            with _in_flight_lock:
                available_slots = 2 - len(_in_flight)
                to_submit = []
                for j in queued:
                    if j['id'] not in _in_flight and available_slots > 0:
                        to_submit.append(j)
                        _in_flight.add(j['id'])
                        available_slots -= 1

            for j in to_submit:
                log.info(f"Submitting job {j['id']}: {j['productName']}")
                _executor.submit(_process_and_save, j)

            time.sleep(3)

        except Exception as e:
            log.error(f"Dispatcher error: {e}")
            time.sleep(5)


def _ensure_worker():
    global _worker_running
    if not _worker_running:
        t = threading.Thread(target=_dispatcher_loop, daemon=True)
        t.start()


# ===== Routes: Auth =====
@app.route('/login', methods=['GET'])
def login_page():
    if session.get('authenticated'):
        return redirect('/')
    return send_file('login.html')

@app.route('/login', methods=['POST'])
def login_submit():
    ip = _get_client_ip()
    allowed, remaining = _check_rate_limit(ip)
    if not allowed:
        return redirect(f'/login?locked={remaining}')

    password = request.form.get('password', '')
    import random
    time.sleep(random.uniform(0.2, 0.5))

    # Check admin password first, then viewer password
    role = None
    if verify_password(password):
        role = 'admin'
    elif bcrypt.checkpw(password.encode('utf-8'), VIEWER_PASSWORD_HASH):
        role = 'viewer'

    if role:
        _clear_attempts(ip)
        session['authenticated'] = True
        session['role'] = role
        session.permanent = True
        app.permanent_session_lifetime = datetime.timedelta(days=30)
        session.modified = True
        return redirect('/')

    _record_failed_attempt(ip)
    allowed_after, remaining_after = _check_rate_limit(ip)
    if not allowed_after:
        return redirect(f'/login?locked={remaining_after}')
    return redirect('/login?error=1')

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')


# ===== Routes: Pages =====
@app.route('/api/session-role')
@login_required
def session_role():
    return jsonify({'success': True, 'role': session.get('role', 'admin')})

@app.route('/')
@login_required
def index():
    return send_file('index.html')

@app.route('/favicon.svg')
def favicon():
    return send_file('favicon.svg', mimetype='image/svg+xml')

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'service': 'video-pin-generator', 'port': 5110})


# ===== Routes: Store Prompts =====
@app.route('/api/stores/<store_id>/platforms', methods=['PUT'])
@admin_required
def update_store_platforms(store_id):
    """Update which ad platforms a store is live on."""
    stores = _load_stores()
    store = next((s for s in stores if s['id'] == store_id), None)
    if not store:
        return jsonify({'success': False, 'error': 'Store not found'}), 404
    body = request.get_json(force=True)
    platforms = body.get('platforms', [])
    # Validate: only allow known platform values
    valid = {'google', 'pinterest', 'native'}
    platforms = [p for p in platforms if p in valid]
    store['platforms'] = platforms
    _save_stores(stores)
    return jsonify({'success': True, 'platforms': platforms})

@app.route('/api/stores/<store_id>/prompts', methods=['GET'])
@admin_required
def get_store_prompts(store_id):
    """Get prompts for a store. If none exist, return defaults."""
    stores = _load_stores()
    store = next((s for s in stores if s['id'] == store_id), None)
    if not store:
        return jsonify({'success': False, 'error': 'Store not found'}), 404

    prompts = store.get('prompts')
    if not prompts:
        prompts = _get_default_store_prompts(store)
    return jsonify({'success': True, 'prompts': prompts})


@app.route('/api/stores/<store_id>/prompts', methods=['PUT'])
@admin_required
def save_store_prompts(store_id):
    """Save prompts for a store."""
    data = request.json
    prompts = data.get('prompts', [])
    if not prompts or not isinstance(prompts, list):
        return jsonify({'success': False, 'error': 'At least one prompt is required'}), 400

    # Validate each prompt has label and template
    for p in prompts:
        if not p.get('label') or not p.get('template'):
            return jsonify({'success': False, 'error': 'Each prompt must have a label and template'}), 400

    stores = _load_stores()
    found = False
    for s in stores:
        if s['id'] == store_id:
            s['prompts'] = prompts
            found = True
            break

    if not found:
        return jsonify({'success': False, 'error': 'Store not found'}), 404

    _save_stores(stores)
    return jsonify({'success': True})


# ===== Routes: Stores =====
@app.route('/api/stores', methods=['GET'])
@login_required
def get_stores():
    stores = _load_stores()
    # Strip tokens from response
    safe = []
    for s in stores:
        platform = _get_store_platform(s)
        safe.append({
            'id': s.get('id', ''),
            'name': s.get('name', ''),
            'domain': s.get('domain', ''),
            'shopifyStatus': s.get('shopifyStatus', ''),
            'shoplazzaStatus': s.get('shoplazzaStatus', ''),
            'shoplazzaDomain': s.get('shoplazzaDomain', ''),
            'platform': platform,
            'storeCategory': s.get('storeCategory', ''),
            'productCount': s.get('productCount', None),
            'hasToken': bool(s.get('shopifyAccessToken', '') or s.get('shoplazzaAccessToken', '')),
            'promptCount': len(s.get('prompts', [])),
            'platforms': s.get('platforms', [])
        })
    return jsonify({'success': True, 'stores': safe})

@app.route('/api/stores/sync', methods=['POST'])
@admin_required
def sync_stores():
    """Sync stores from Pinterest Autopilot or Google Ads Hub.
    Body: {source: 'pinterest' | 'gads'}  (default: 'pinterest')
    Merges into existing stores — does not overwrite stores from other sources.
    """
    data = request.get_json(silent=True) or {}
    source = data.get('source', 'pinterest')

    # Source-specific paths
    if source == 'gads':
        store_candidates = [
            '/root/gads-hub/data/stores.json',
            os.path.join(BASE_DIR, '..', 'gads-hub', 'data', 'stores.json'),
        ]
        config_candidates = []  # No Gemini config from gads
        source_label = 'Google Ads Hub'
    else:
        store_candidates = [
            '/root/dropship-autopilot/stores.json',
            os.path.join(BASE_DIR, '..', 'dropship-autopilot', 'stores.json'),
        ]
        config_candidates = [
            '/root/dropship-autopilot/config.json',
            os.path.join(BASE_DIR, '..', 'dropship-autopilot', 'config.json'),
        ]
        source_label = 'Pinterest Autopilot'

    # --- Load source stores ---
    source_data = None
    source_path = None
    for path in store_candidates:
        resolved = os.path.abspath(path)
        if os.path.exists(resolved):
            try:
                with open(resolved, 'r') as f:
                    source_data = json.load(f)
                source_path = resolved
                break
            except:
                continue

    if source_data is None:
        return jsonify({'success': False, 'error': f'Could not find stores.json for {source_label}. Tried: ' + ', '.join(store_candidates)}), 404

    # Preserve existing stores and their prompts
    existing_stores = _load_stores()
    existing_by_id = {s.get('id'): s for s in existing_stores}

    raw_stores = source_data if isinstance(source_data, list) else source_data.get('stores', [])
    new_count = 0
    updated_count = 0
    shoplazza_count = 0
    for s in raw_stores:
        store_id = s.get('id', '')
        if not store_id:
            continue
        # g-ads uses 'shopifyDomain', pinterest uses 'domain'
        domain = s.get('domain', '') or s.get('shopifyDomain', '')
        token = s.get('shopifyAccessToken', '')

        # Shoplazza credentials (only present in Pinterest Autopilot source)
        shoplazza_domain = s.get('shoplazzaDomain', '')
        shoplazza_token = s.get('shoplazzaAccessToken', '')
        explicit_platform = (s.get('platform') or '').strip().lower()

        # Determine which platform this store should be tracked as.
        # Priority: explicit platform field > shopify creds > shoplazza creds.
        if explicit_platform == 'shoplazza' and shoplazza_domain and shoplazza_token:
            platform = 'shoplazza'
        elif domain and token:
            platform = 'shopify'
        elif shoplazza_domain and shoplazza_token:
            platform = 'shoplazza'
        else:
            continue  # No usable credentials — skip

        if platform == 'shoplazza':
            shoplazza_count += 1

        if store_id in existing_by_id:
            # Update existing store — refresh creds but keep prompts
            existing = existing_by_id[store_id]
            existing['name'] = s.get('name', existing.get('name', ''))
            existing['platform'] = platform
            if platform == 'shopify':
                existing['domain'] = domain
                existing['shopifyAccessToken'] = token
                existing['shopifyStatus'] = s.get('shopifyStatus', existing.get('shopifyStatus', ''))
            else:  # shoplazza
                existing['shoplazzaDomain'] = shoplazza_domain
                existing['shoplazzaAccessToken'] = shoplazza_token
                existing['shoplazzaStatus'] = s.get('shoplazzaStatus', existing.get('shoplazzaStatus', 'connected'))
                # Also persist shopify creds if they happen to exist (some stores have both)
                if domain and token:
                    existing['domain'] = domain
                    existing['shopifyAccessToken'] = token
                    existing['shopifyStatus'] = s.get('shopifyStatus', existing.get('shopifyStatus', ''))
            existing['storeCategory'] = s.get('storeCategory', existing.get('storeCategory', 'default'))
            existing['productCount'] = s.get('products', s.get('productCount', existing.get('productCount')))
            updated_count += 1
        else:
            # New store
            new_entry = {
                'id': store_id,
                'name': s.get('name', ''),
                'platform': platform,
                'storeCategory': s.get('storeCategory', 'default'),
                'productCount': s.get('products', s.get('productCount', None)),
            }
            if platform == 'shopify':
                new_entry['domain'] = domain
                new_entry['shopifyAccessToken'] = token
                new_entry['shopifyStatus'] = s.get('shopifyStatus', '')
            else:  # shoplazza
                new_entry['shoplazzaDomain'] = shoplazza_domain
                new_entry['shoplazzaAccessToken'] = shoplazza_token
                new_entry['shoplazzaStatus'] = s.get('shoplazzaStatus', 'connected')
                # Also keep Shopify creds if present
                if domain and token:
                    new_entry['domain'] = domain
                    new_entry['shopifyAccessToken'] = token
                    new_entry['shopifyStatus'] = s.get('shopifyStatus', '')
            existing_by_id[store_id] = new_entry
            new_count += 1

    # Backfill platform on any pre-existing store that's missing it (so older data is consistent)
    for s in existing_by_id.values():
        if not s.get('platform'):
            if s.get('shoplazzaDomain') and s.get('shoplazzaAccessToken') and not s.get('shopifyAccessToken'):
                s['platform'] = 'shoplazza'
            else:
                s['platform'] = 'shopify'

    merged = list(existing_by_id.values())
    _save_stores(merged)
    connected_count = sum(
        1 for s in merged
        if s.get('shopifyStatus') == 'connected' or s.get('shoplazzaStatus') == 'connected'
    )

    # --- Import Gemini config (Pinterest only) ---
    gemini_imported = False
    for path in config_candidates:
        resolved = os.path.abspath(path)
        if os.path.exists(resolved):
            try:
                with open(resolved, 'r') as f:
                    config_data = json.load(f)
                gemini = config_data.get('gemini', {})
                if gemini.get('apiKey'):
                    settings = _load_settings()
                    settings['google_ai_key'] = gemini['apiKey']
                    if gemini.get('model'):
                        settings['google_ai_model'] = gemini['model']
                    _save_settings(settings)
                    gemini_imported = True
                break
            except:
                continue

    return jsonify({
        'success': True,
        'count': len(merged),
        'newCount': new_count,
        'updatedCount': updated_count,
        'shoplazzaCount': shoplazza_count,
        'connectedCount': connected_count,
        'source': source_label,
        'sourcePath': source_path,
        'geminiImported': gemini_imported
    })


# ===== Routes: Settings =====
@app.route('/api/settings', methods=['GET'])
@admin_required
def get_settings():
    settings = _load_settings()
    safe = dict(settings)
    # Remove sensitive Drive tokens from response (but keep client_id/secret for display)
    safe.pop('gdrive_access_token', None)
    safe.pop('gdrive_refresh_token', None)
    safe.pop('gdrive_token_expiry', None)
    return jsonify({'success': True, 'settings': safe})

@app.route('/api/settings', methods=['POST'])
@admin_required
def save_settings():
    data = request.json
    settings = _load_settings()

    if 'xai_api_key' in data and data['xai_api_key']:
        settings['xai_api_key'] = data['xai_api_key']
    if 'xai_video_model' in data:
        settings['xai_video_model'] = data['xai_video_model']
    if 'video_duration' in data:
        settings['video_duration'] = int(data['video_duration'])
    if 'video_resolution' in data:
        settings['video_resolution'] = data['video_resolution']
    if 'google_ai_key' in data and data['google_ai_key']:
        settings['google_ai_key'] = data['google_ai_key']
    if 'google_ai_model' in data:
        settings['google_ai_model'] = data['google_ai_model']
    if 'shopify_client_id' in data:
        settings['shopify_client_id'] = data['shopify_client_id'].strip()
    if 'shopify_client_secret' in data:
        settings['shopify_client_secret'] = data['shopify_client_secret'].strip()
    if 'shopify_scopes' in data and data['shopify_scopes'].strip():
        settings['shopify_scopes'] = data['shopify_scopes'].strip()
    if 'prompt_batches' in data and isinstance(data['prompt_batches'], dict):
        for k, v in data['prompt_batches'].items():
            if v is None:
                # Reset to default: remove so _load_settings fills from defaults
                settings['prompt_batches'].pop(k, None)
            else:
                settings['prompt_batches'][k] = v
    if 'custom_prompt_batches' in data and isinstance(data['custom_prompt_batches'], dict):
        settings['custom_prompt_batches'] = data['custom_prompt_batches']

    _save_settings(settings)
    return jsonify({'success': True})

@app.route('/api/settings/prompt-batches', methods=['GET'])
@admin_required
def get_prompt_batches():
    """Get all prompt batch options for the category selector."""
    settings = _load_settings()
    batches = settings.get('prompt_batches', {})
    custom = settings.get('custom_prompt_batches', {})

    # Build ordered list: built-in categories first, then custom
    built_in_order = ['sweaters', 'dresses', 'tops', 'shirts', 'bikinis', 'coats',
                      'rings', 'necklaces', 'bracelets', 'earrings',
                      'sandals', 'sneakers', 'heels', 'default']
    options = []
    for key in built_in_order:
        if key in batches:
            options.append({'key': key, 'label': key.title(), 'type': 'builtin', 'prompts': batches[key]})
    # Add any built-in that somehow isn't in the ordered list
    for key in batches:
        if key not in built_in_order:
            options.append({'key': key, 'label': key.title(), 'type': 'builtin', 'prompts': batches[key]})
    # Add custom batches
    for key, val in custom.items():
        options.append({'key': key, 'label': val.get('label', key), 'type': 'custom', 'prompts': val.get('prompts', [])})

    return jsonify({'success': True, 'batches': options})

@app.route('/api/settings/prompt-batches/custom', methods=['POST'])
@admin_required
def save_custom_prompt_batch():
    """Create or update a custom prompt batch."""
    data = request.json or {}
    key = data.get('key', '').strip().lower().replace(' ', '_')
    label = data.get('label', '').strip()
    prompts = data.get('prompts', [])

    if not key or not label:
        return jsonify({'success': False, 'error': 'Name is required'}), 400
    if not prompts or len(prompts) != 4:
        return jsonify({'success': False, 'error': 'Exactly 4 prompts are required'}), 400

    settings = _load_settings()
    custom = settings.get('custom_prompt_batches', {})
    custom[key] = {'label': label, 'prompts': prompts}
    settings['custom_prompt_batches'] = custom
    _save_settings(settings)
    return jsonify({'success': True})

@app.route('/api/settings/prompt-batches/custom/<key>', methods=['DELETE'])
@admin_required
def delete_custom_prompt_batch(key):
    """Delete a custom prompt batch."""
    settings = _load_settings()
    custom = settings.get('custom_prompt_batches', {})
    if key in custom:
        del custom[key]
        settings['custom_prompt_batches'] = custom
        _save_settings(settings)
    return jsonify({'success': True})

@app.route('/api/settings/test-xai', methods=['POST'])
@admin_required
def test_xai_connection():
    """Test xAI API key by listing models and video generation models."""
    settings = _load_settings()
    api_key = settings.get('xai_api_key', '')
    if not api_key:
        return jsonify({'success': False, 'error': 'No API key configured'})

    try:
        resp = http_requests.get(
            'https://api.x.ai/v1/models',
            headers={'Authorization': f'Bearer {api_key}'},
            timeout=10
        )
        if resp.status_code == 200:
            models = [m.get('id', '') for m in resp.json().get('data', [])]
            has_video = any('video' in m.lower() for m in models)

            # Also fetch video generation models
            video_models = []
            try:
                vresp = http_requests.get(
                    'https://api.x.ai/v1/video-generation-models',
                    headers={'Authorization': f'Bearer {api_key}'},
                    timeout=10
                )
                if vresp.status_code == 200:
                    video_models = [m.get('id', '') for m in vresp.json().get('data', [])]
            except:
                pass

            return jsonify({'success': True, 'models': models, 'hasVideoModel': has_video, 'videoModels': video_models})
        else:
            return jsonify({'success': False, 'error': f'API returned {resp.status_code}: {resp.text[:200]}'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ===== Routes: Google AI (Gemini) =====
@app.route('/api/settings/test-google-ai', methods=['POST'])
@admin_required
def test_google_ai():
    """Test Google AI (Gemini) API key."""
    data = request.json
    api_key = data.get('apiKey', '')
    model = data.get('model', 'gemini-2.0-flash')

    if not api_key:
        return jsonify({'success': False, 'error': 'No API key provided'})

    url = f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}'
    payload = {'contents': [{'parts': [{'text': 'Say "connected" in one word.'}]}]}

    try:
        resp = http_requests.post(url, json=payload, timeout=15)
        if resp.status_code == 200:
            # Save key on success
            settings = _load_settings()
            settings['google_ai_key'] = api_key
            settings['google_ai_model'] = model
            _save_settings(settings)
            response_text = ''
            try:
                response_text = resp.json().get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')
            except:
                pass
            return jsonify({'success': True, 'response': response_text, 'model': model})
        elif resp.status_code == 429:
            # Rate limited but key is valid
            settings = _load_settings()
            settings['google_ai_key'] = api_key
            settings['google_ai_model'] = model
            _save_settings(settings)
            return jsonify({'success': True, 'response': 'Rate limited but key is valid', 'model': model, 'rateLimited': True})
        elif resp.status_code == 403 or resp.status_code == 400:
            return jsonify({'success': False, 'error': f'Invalid API key or permission denied ({resp.status_code})'})
        else:
            return jsonify({'success': False, 'error': f'API returned {resp.status_code}: {resp.text[:300]}'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ===== Routes: Shopify OAuth =====
@app.route('/api/shopify/auth-url', methods=['POST'])
@admin_required
def shopify_auth_url():
    """Generate Shopify OAuth authorization URL for a specific store."""
    data = request.json or {}
    store_id = data.get('storeId', '').strip()

    if not store_id:
        return jsonify({'success': False, 'error': 'Store ID is required'})

    settings = _load_settings()
    client_id = settings.get('shopify_client_id', '').strip()
    scopes = settings.get('shopify_scopes', 'read_products,read_orders,read_apps').strip()

    if not client_id:
        return jsonify({'success': False, 'error': 'Shopify Client ID not configured. Set it in Settings first.'})

    stores = _load_stores()
    store = next((s for s in stores if s.get('id') == store_id), None)
    if not store:
        return jsonify({'success': False, 'error': 'Store not found'})

    domain = store.get('domain', '')
    if not domain:
        return jsonify({'success': False, 'error': 'Store has no domain'})

    # Build redirect URI
    redirect_uri = f'{request.host_url.rstrip("/")}/api/shopify/callback'

    # Generate a random state with store_id embedded for CSRF + store identification
    state = f'{store_id}:{secrets.token_urlsafe(32)}'
    session['shopify_oauth_state'] = state

    # Shopify OAuth URL — use the shop's myshopify domain
    shop = domain.replace('.myshopify.com', '') if '.myshopify.com' in domain else domain
    auth_url = (
        f'https://{domain}/admin/oauth/authorize'
        f'?client_id={client_id}'
        f'&scope={scopes}'
        f'&redirect_uri={redirect_uri}'
        f'&state={state}'
    )
    return jsonify({'success': True, 'authUrl': auth_url, 'store': store.get('name', domain)})


@app.route('/api/shopify/callback', methods=['GET'])
def shopify_oauth_callback():
    """Shopify OAuth callback — exchanges code for permanent access token."""
    code = request.args.get('code', '')
    state = request.args.get('state', '')
    shop = request.args.get('shop', '')  # Shopify sends this

    if not code or not state:
        return redirect('/?tab=stores&shopify=error&msg=missing_params')

    # Validate state
    saved_state = session.get('shopify_oauth_state', '')
    if not saved_state or state != saved_state:
        log.warning('Shopify OAuth state mismatch')
        return redirect('/?tab=stores&shopify=error&msg=state_mismatch')

    # Extract store_id from state
    store_id = state.split(':')[0] if ':' in state else ''
    if not store_id:
        return redirect('/?tab=stores&shopify=error&msg=invalid_state')

    settings = _load_settings()
    client_id = settings.get('shopify_client_id', '')
    client_secret = settings.get('shopify_client_secret', '')

    if not client_id or not client_secret:
        return redirect('/?tab=stores&shopify=error&msg=no_credentials')

    # Find store
    stores = _load_stores()
    store = next((s for s in stores if s.get('id') == store_id), None)
    if not store:
        return redirect('/?tab=stores&shopify=error&msg=store_not_found')

    domain = store.get('domain', '') or shop

    try:
        # Exchange code for permanent access token
        token_url = f'https://{domain}/admin/oauth/access_token'
        resp = http_requests.post(token_url, json={
            'client_id': client_id,
            'client_secret': client_secret,
            'code': code,
        }, timeout=15)

        if resp.status_code != 200:
            log.error(f'Shopify token exchange failed: {resp.status_code} {resp.text[:500]}')
            return redirect(f'/?tab=stores&shopify=error&msg=token_failed')

        tokens = resp.json()
        access_token = tokens.get('access_token', '')
        granted_scopes = tokens.get('scope', '')

        if not access_token:
            return redirect('/?tab=stores&shopify=error&msg=no_token')

        # Update store with new token
        store['shopifyAccessToken'] = access_token
        store['shopifyStatus'] = 'connected'
        store['shopifyScopes'] = granted_scopes
        _save_stores(stores)

        # Clear OAuth state
        session.pop('shopify_oauth_state', None)

        store_name = store.get('name', domain)
        log.info(f'Shopify re-auth success for {store_name} — scopes: {granted_scopes}')
        return redirect(f'/?tab=stores&shopify=connected&store={store_name}')
    except Exception as e:
        log.error(f'Shopify OAuth callback error: {e}')
        return redirect('/?tab=stores&shopify=error&msg=exception')


# ===== Routes: Google Drive =====
@app.route('/api/drive/auth-url', methods=['POST'])
@admin_required
def drive_auth_url():
    """Generate OAuth authorization URL for Google Drive."""
    data = request.json or {}
    client_id = data.get('clientId', '').strip()
    client_secret = data.get('clientSecret', '').strip()

    if not client_id or not client_secret:
        return jsonify({'success': False, 'error': 'Client ID and Client Secret are required'})

    # Save client credentials to settings
    settings = _load_settings()
    settings['gdrive_client_id'] = client_id
    settings['gdrive_client_secret'] = client_secret
    _save_settings(settings)

    try:
        from google_auth_oauthlib.flow import Flow

        redirect_uri = f'{request.host_url.rstrip("/")}/api/drive/callback'
        flow = Flow.from_client_config(
            {
                "web": {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                }
            },
            scopes=['https://www.googleapis.com/auth/drive'],
            redirect_uri=redirect_uri
        )
        authorization_url, state = flow.authorization_url(
            access_type='offline',
            prompt='consent'
        )
        # Store state in session for CSRF protection
        session['oauth_state'] = state
        return jsonify({'success': True, 'authUrl': authorization_url})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/drive/callback', methods=['GET'])
def drive_oauth_callback():
    """OAuth callback handler — NO @login_required since Google redirects here."""
    code = request.args.get('code', '')
    state = request.args.get('state', '')

    if not code:
        return redirect('/?tab=settings&drive=error')

    # Validate state for CSRF protection
    saved_state = session.get('oauth_state', '')
    if not saved_state or state != saved_state:
        log.warning('OAuth state mismatch — possible CSRF')
        return redirect('/?tab=settings&drive=error')

    settings = _load_settings()
    client_id = settings.get('gdrive_client_id', '')
    client_secret = settings.get('gdrive_client_secret', '')

    if not client_id or not client_secret:
        return redirect('/?tab=settings&drive=error')

    try:
        # Exchange authorization code for tokens directly (avoids scope mismatch
        # errors when Google returns additional previously-granted scopes)
        redirect_uri = f'{request.host_url.rstrip("/")}/api/drive/callback'
        token_resp = http_requests.post('https://oauth2.googleapis.com/token', data={
            'code': code,
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri,
            'grant_type': 'authorization_code',
        }, timeout=15)

        if token_resp.status_code != 200:
            log.error(f'Token exchange failed: {token_resp.status_code} {token_resp.text[:500]}')
            return redirect('/?tab=settings&drive=error')

        tokens = token_resp.json()
        settings['gdrive_access_token'] = tokens.get('access_token', '')
        settings['gdrive_refresh_token'] = tokens.get('refresh_token', '')
        expires_in = tokens.get('expires_in')
        if expires_in:
            import datetime as _dt
            settings['gdrive_token_expiry'] = (_dt.datetime.utcnow() + _dt.timedelta(seconds=int(expires_in))).isoformat()
        settings['gdrive_status'] = 'connected'
        _save_settings(settings)

        # Clear OAuth state from session
        session.pop('oauth_state', None)

        return redirect('/?tab=settings&drive=connected')
    except Exception as e:
        log.error(f'OAuth callback error: {e}')
        return redirect('/?tab=settings&drive=error')


@app.route('/api/drive/disconnect', methods=['POST'])
@admin_required
def drive_disconnect():
    """Disconnect Google Drive by clearing OAuth tokens."""
    settings = _load_settings()
    settings['gdrive_access_token'] = ''
    settings['gdrive_refresh_token'] = ''
    settings['gdrive_token_expiry'] = ''
    settings['gdrive_status'] = ''
    settings['gdrive_root_folder_id'] = ''
    _save_settings(settings)
    return jsonify({'success': True})

@app.route('/api/drive/folders', methods=['GET'])
@admin_required
def drive_folders():
    """List contents of the root Drive folder."""
    service, err = _get_drive_service()
    if not service:
        return jsonify({'success': False, 'error': err})

    try:
        folder_id = _get_or_create_root_folder(service)
        # Get root folder info
        root = service.files().get(fileId=folder_id, fields='id,name,mimeType').execute()

        # List children
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields='files(id,name,mimeType,size,createdTime,webViewLink,thumbnailLink)',
            orderBy='name',
            pageSize=200
        ).execute()

        items = results.get('files', [])
        folders = []
        files = []
        for item in items:
            entry = {
                'id': item['id'],
                'name': item['name'],
                'mimeType': item.get('mimeType', ''),
                'size': item.get('size'),
                'createdTime': item.get('createdTime', ''),
                'webViewLink': item.get('webViewLink', ''),
                'thumbnailLink': item.get('thumbnailLink', ''),
            }
            if item.get('mimeType') == 'application/vnd.google-apps.folder':
                folders.append(entry)
            else:
                files.append(entry)

        return jsonify({
            'success': True,
            'folder': {'id': root['id'], 'name': root['name']},
            'folders': folders,
            'files': files
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/drive/folders/<folder_id>', methods=['GET'])
@admin_required
def drive_folder_contents(folder_id):
    """List contents of a specific Drive folder."""
    service, err = _get_drive_service()
    if not service:
        return jsonify({'success': False, 'error': err})

    try:
        # Get folder info
        folder = service.files().get(fileId=folder_id, fields='id,name,mimeType').execute()

        # List children
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields='files(id,name,mimeType,size,createdTime,webViewLink,thumbnailLink)',
            orderBy='name',
            pageSize=200
        ).execute()

        items = results.get('files', [])
        folders = []
        files = []
        for item in items:
            entry = {
                'id': item['id'],
                'name': item['name'],
                'mimeType': item.get('mimeType', ''),
                'size': item.get('size'),
                'createdTime': item.get('createdTime', ''),
                'webViewLink': item.get('webViewLink', ''),
                'thumbnailLink': item.get('thumbnailLink', ''),
            }
            if item.get('mimeType') == 'application/vnd.google-apps.folder':
                folders.append(entry)
            else:
                files.append(entry)

        return jsonify({
            'success': True,
            'folder': {'id': folder['id'], 'name': folder['name']},
            'folders': folders,
            'files': files
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/drive/upload/<job_id>', methods=['POST'])
@admin_required
def upload_to_drive(job_id):
    """Upload a completed video to Google Drive."""
    all_jobs = _load_all_jobs()
    job = next((j for j in all_jobs if j['id'] == job_id), None)
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404
    if not job.get('localPath'):
        return jsonify({'success': False, 'error': 'No video file available'}), 400

    full_path = os.path.join(VIDEOS_DIR, job['localPath'])
    if not os.path.exists(full_path):
        return jsonify({'success': False, 'error': 'Video file not found on disk'}), 404

    service, err = _get_drive_service()
    if not service:
        return jsonify({'success': False, 'error': err}), 500

    try:
        from googleapiclient.http import MediaFileUpload

        root_folder_id = _get_or_create_root_folder(service)

        # Create folder structure: Root > Store Name > #NNN-product-handle
        store_name = job.get('storeName', 'Unknown Store')
        product_handle = job.get('productHandle', '') or job.get('productName', 'unknown-product')

        # Find or create store folder
        store_folder_id = _find_or_create_drive_folder(service, store_name, root_folder_id)

        # Find or create numbered product subfolder inside store folder
        product_folder_id = _find_or_create_numbered_product_folder(service, product_handle, store_folder_id)

        # Upload file into product folder
        file_name = os.path.basename(full_path)
        file_metadata = {
            'name': file_name,
            'parents': [product_folder_id]
        }
        media = MediaFileUpload(full_path, mimetype='video/mp4', resumable=True)
        uploaded = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,webViewLink'
        ).execute()

        # Update job with drive URL (check both active and archive)
        drive_url = uploaded.get('webViewLink', '')
        with _jobs_lock:
            found_in_active = False
            jobs = _load_jobs()
            for j in jobs:
                if j['id'] == job_id:
                    j['driveUrl'] = drive_url
                    found_in_active = True
                    break
            if found_in_active:
                _save_jobs(jobs)
            else:
                archive = _load_archive()
                for j in archive:
                    if j['id'] == job_id:
                        j['driveUrl'] = drive_url
                        break
                _save_archive(archive)

        return jsonify({'success': True, 'driveUrl': drive_url, 'fileId': uploaded['id']})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== Routes: Drive Management (rename, delete, create folder, move) =====
@app.route('/api/drive/files/<file_id>/rename', methods=['PATCH'])
@admin_required
def drive_rename(file_id):
    data = request.json
    new_name = data.get('name', '').strip()
    if not new_name:
        return jsonify({'success': False, 'error': 'Name is required'}), 400

    service, err = _get_drive_service()
    if not service:
        return jsonify({'success': False, 'error': err}), 500

    try:
        updated = service.files().update(
            fileId=file_id,
            body={'name': new_name},
            fields='id,name'
        ).execute()
        return jsonify({'success': True, 'file': updated})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/drive/files/<file_id>', methods=['DELETE'])
@admin_required
def drive_delete(file_id):
    service, err = _get_drive_service()
    if not service:
        return jsonify({'success': False, 'error': err}), 500

    try:
        service.files().update(
            fileId=file_id,
            body={'trashed': True}
        ).execute()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/drive/folders', methods=['POST'])
@admin_required
def drive_create_folder():
    data = request.json
    name = data.get('name', '').strip()
    parent_id = data.get('parentId', '')

    if not name:
        return jsonify({'success': False, 'error': 'Name is required'}), 400

    service, err = _get_drive_service()
    if not service:
        return jsonify({'success': False, 'error': err}), 500

    try:
        metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder',
        }
        if parent_id:
            metadata['parents'] = [parent_id]

        folder = service.files().create(body=metadata, fields='id,name').execute()
        return jsonify({'success': True, 'folder': folder})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/drive/files/<file_id>/move', methods=['PATCH'])
@admin_required
def drive_move(file_id):
    data = request.json
    new_parent_id = data.get('parentId', '')
    if not new_parent_id:
        return jsonify({'success': False, 'error': 'parentId is required'}), 400

    service, err = _get_drive_service()
    if not service:
        return jsonify({'success': False, 'error': err}), 500

    try:
        file_info = service.files().get(fileId=file_id, fields='parents').execute()
        previous_parents = ','.join(file_info.get('parents', []))

        updated = service.files().update(
            fileId=file_id,
            addParents=new_parent_id,
            removeParents=previous_parents,
            fields='id,name,parents'
        ).execute()
        return jsonify({'success': True, 'file': updated})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== Routes: Order Fulfillment Summary =====
_orders_day_labels = []  # computed once per request
_orders_today = None

def _fetch_orders_for_store(s):
    """Fetch order fulfillment counts for a single store (used in parallel).
    Only 4 API calls per store: 3 counts + 1 order list for daily breakdown."""
    domain = s.get('domain', '')
    token = s.get('shopifyAccessToken', '')
    if not domain or not token:
        return None
    base_url = f'https://{domain}/admin/api/2024-01'
    headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
    today = _orders_today
    try:
        r1 = http_requests.get(f'{base_url}/orders/count.json?status=open&fulfillment_status=unfulfilled', headers=headers, timeout=15)
        unfulfilled = r1.json().get('count', 0) if r1.status_code == 200 else 0
        r2 = http_requests.get(f'{base_url}/orders/count.json?status=any&fulfillment_status=shipped', headers=headers, timeout=15)
        fulfilled = r2.json().get('count', 0) if r2.status_code == 200 else 0
        r3 = http_requests.get(f'{base_url}/orders/count.json?status=any&fulfillment_status=partial', headers=headers, timeout=15)
        partial = r3.json().get('count', 0) if r3.status_code == 200 else 0

        # Daily unfulfilled: single API call fetching orders from last 5 days, then bucket by date
        five_days_ago = today - datetime.timedelta(days=4)
        daily_unfulfilled = {(today - datetime.timedelta(days=i)).isoformat(): 0 for i in range(5)}
        recent_total = 0
        try:
            page_url = f'{base_url}/orders.json?status=open&fulfillment_status=unfulfilled&created_at_min={five_days_ago.isoformat()}T00:00:00Z&limit=250&fields=id,created_at'
            pages = 0
            while page_url and pages < 20:
                ro = http_requests.get(page_url, headers=headers, timeout=15)
                if ro.status_code != 200:
                    break
                orders = ro.json().get('orders', [])
                if not orders:
                    break
                for o in orders:
                    recent_total += 1
                    ca = o.get('created_at', '')
                    if ca:
                        order_date = ca[:10]  # YYYY-MM-DD
                        if order_date in daily_unfulfilled:
                            daily_unfulfilled[order_date] += 1
                # Pagination
                page_url = None
                link_header = ro.headers.get('Link', '')
                if 'rel="next"' in link_header:
                    for part in link_header.split(','):
                        if 'rel="next"' in part:
                            page_url = part.split('<')[1].split('>')[0]
                            break
                pages += 1
        except Exception:
            pass

        return {
            'id': s.get('id', ''),
            'name': s.get('name', ''),
            'domain': domain,
            'unfulfilled': unfulfilled,
            'fulfilled': fulfilled,
            'partial': partial,
            'dailyUnfulfilled': daily_unfulfilled,
            'recentUnfulfilled': recent_total
        }
    except Exception as e:
        log.warning(f"Order summary failed for {s.get('name','')}: {e}")
        return None

@app.route('/api/orders/summary', methods=['GET'])
@admin_required
def orders_summary():
    """Get fulfilled/unfulfilled/partial order counts per store + daily unfulfilled (parallel)."""
    global _orders_today, _orders_day_labels
    stores = _load_stores()
    _orders_today = datetime.datetime.now(datetime.timezone.utc).date()
    _orders_day_labels = [(_orders_today - datetime.timedelta(days=i)).isoformat() for i in range(5)]

    valid_stores = [s for s in stores if s.get('domain') and s.get('shopifyAccessToken')]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(_fetch_orders_for_store, valid_stores))
    results = [r for r in results if r is not None]
    return jsonify({'success': True, 'stores': results, 'days': _orders_day_labels})


# ===== Routes: Installed Apps Per Store =====
def _fetch_apps_for_store(s):
    """Fetch installed apps for a single store (used in parallel)."""
    domain = s.get('domain', '')
    token = s.get('shopifyAccessToken', '')
    if not domain or not token:
        return None
    graphql_url = f'https://{domain}/admin/api/2024-01/graphql.json'
    headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
    query = '{"query": "{ appInstallations(first: 100) { nodes { id app { title } } } }"}'
    try:
        resp = http_requests.post(graphql_url, headers=headers, data=query, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            nodes = data.get('data', {}).get('appInstallations', {}).get('nodes', [])
            apps = [n.get('app', {}).get('title', 'Unknown') for n in nodes if n.get('app')]
        else:
            apps = []
        return {
            'id': s.get('id', ''),
            'name': s.get('name', ''),
            'domain': domain,
            'apps': sorted(apps),
            'appCount': len(apps)
        }
    except Exception as e:
        return {
            'id': s.get('id', ''),
            'name': s.get('name', ''),
            'domain': domain,
            'apps': [],
            'appCount': 0,
            'error': str(e)
        }

@app.route('/api/stores/apps', methods=['GET'])
@admin_required
def get_store_apps():
    """Fetch installed Shopify apps for each store using GraphQL (parallel)."""
    stores = _load_stores()
    valid_stores = [s for s in stores if s.get('domain') and s.get('shopifyAccessToken')]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(_fetch_apps_for_store, valid_stores))
    results = [r for r in results if r is not None]
    return jsonify({'success': True, 'stores': results})


# ===== Routes: Shopify Winners =====
# ===== Winner cache (per-store persistent sales aggregation) =====
WINNER_CACHE_DIR = os.path.join(DATA_DIR, 'winner_cache')
os.makedirs(WINNER_CACHE_DIR, exist_ok=True)
_winner_sync_locks = {}  # store_id -> threading.Lock to prevent double-runs

def _winner_cache_path(store_id):
    return os.path.join(WINNER_CACHE_DIR, f'{store_id}.json')

def _winner_meta_path(store_id):
    return os.path.join(WINNER_CACHE_DIR, f'{store_id}_meta.json')

def _load_winner_cache(store_id):
    path = _winner_cache_path(store_id)
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except:
        return None

def _save_winner_cache(store_id, product_sales):
    # product_sales keys are product_ids (may be ints) — JSON keys must be strings
    serialisable = {str(k): v for k, v in product_sales.items()}
    path = _winner_cache_path(store_id)
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(serialisable, f)
    os.replace(tmp, path)

def _load_winner_meta(store_id):
    path = _winner_meta_path(store_id)
    if not os.path.exists(path):
        return {'status': 'never', 'pages_scanned': 0, 'total_orders': 0, 'total_products': 0, 'last_synced': None, 'error': None}
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except:
        return {'status': 'never'}

def _save_winner_meta(store_id, meta):
    path = _winner_meta_path(store_id)
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(meta, f)
    os.replace(tmp, path)

# ===== Platform dispatch =====
# Each store has a 'platform' field set during /api/stores/sync.
# Older stores without it default to 'shopify' for backwards compat.
SHOPLAZZA_API_VERSION = '2025-06'

def _get_store_platform(store):
    """Return 'shopify' or 'shoplazza' for a store dict."""
    p = (store.get('platform') or '').strip().lower()
    if p in ('shopify', 'shoplazza'):
        return p
    # Backwards compat: infer from credentials
    if store.get('shoplazzaDomain') and store.get('shoplazzaAccessToken') and not store.get('shopifyAccessToken'):
        return 'shoplazza'
    return 'shopify'

def _get_store_credentials(store):
    """Return (platform, domain, token) for whichever platform the store uses."""
    platform = _get_store_platform(store)
    if platform == 'shoplazza':
        return platform, store.get('shoplazzaDomain', ''), store.get('shoplazzaAccessToken', '')
    return platform, store.get('domain', ''), store.get('shopifyAccessToken', '')

def _shoplazza_headers(token):
    return {'access-token': token, 'Content-Type': 'application/json', 'accept': 'application/json'}

def _shoplazza_get_with_retry(url, headers, params=None, timeout=30, max_retries=3):
    """GET with 429 + transient-error handling for Shoplazza."""
    last_resp = None
    for attempt in range(max_retries + 1):
        try:
            resp = http_requests.get(url, headers=headers, params=params, timeout=timeout)
        except Exception:
            if attempt >= max_retries:
                raise
            time.sleep(2 ** attempt)
            continue
        last_resp = resp
        if resp.status_code == 429:
            retry_after = int(resp.headers.get('Retry-After', '2'))
            time.sleep(retry_after)
            continue
        return resp
    return last_resp


def _run_shoplazza_winner_sync(store_id, domain, token):
    """Background worker: scans ALL paid orders from a Shoplazza store and caches per-product sales.

    Mirrors the Shopify version but uses cursor-based pagination and Shoplazza's response shape.
    Saves into the same on-disk cache layout (data/winner_cache/<store_id>.json + _meta.json) so the
    GET endpoint can read uniformly.
    """
    lock = _winner_sync_locks.setdefault(store_id, threading.Lock())
    if not lock.acquire(blocking=False):
        return  # Already syncing

    meta = {
        'status': 'running',
        'platform': 'shoplazza',
        'pages_scanned': 0,
        'total_orders': 0,
        'total_products': 0,
        'started_at': time.time(),
        'last_synced': None,
        'error': None,
    }
    _save_winner_meta(store_id, meta)

    try:
        headers = _shoplazza_headers(token)
        base_url = f'https://{domain}/openapi/{SHOPLAZZA_API_VERSION}'
        product_sales = {}
        cursor = None
        pages_fetched = 0
        max_pages = 10000  # effectively unlimited

        while pages_fetched < max_pages:
            params = {'page_size': 250, 'financial_status': 'paid'}
            if cursor:
                params['cursor'] = cursor

            resp = _shoplazza_get_with_retry(f'{base_url}/orders', headers, params=params, timeout=60)
            if resp is None:
                raise Exception('Shoplazza request failed (no response)')
            if resp.status_code != 200:
                raise Exception(f'Shoplazza orders {resp.status_code}: {resp.text[:200]}')

            resp_data = resp.json() or {}
            # Shoplazza response shape: { code, data: { orders, cursor, pre_cursor } }
            # Some endpoints / versions return data at the top level; support both.
            data_block = resp_data.get('data') if isinstance(resp_data.get('data'), dict) else resp_data
            orders = data_block.get('orders') or resp_data.get('orders') or []
            if not orders:
                break

            for order in orders:
                for item in (order.get('line_items') or order.get('items') or []):
                    pid = item.get('product_id')
                    if not pid:
                        continue
                    try:
                        qty = int(item.get('quantity', 1) or 1)
                    except:
                        qty = 1
                    try:
                        price = float(item.get('price', '0') or '0')
                    except:
                        price = 0.0
                    revenue = qty * price

                    # Product title — Shoplazza uses 'product_title' on line items
                    li_product_title = (
                        item.get('product_title') or
                        item.get('title') or
                        item.get('name') or
                        ''
                    )

                    # Capture line-item image as fallback for products that may later
                    # be deleted from the store (so cards can still render).
                    li_image = ''
                    img_obj = item.get('image')
                    if isinstance(img_obj, dict):
                        li_image = img_obj.get('src', '') or img_obj.get('url', '')
                    elif isinstance(img_obj, str):
                        li_image = img_obj
                    # Normalize protocol-relative URLs (//img.staticdj.com/...)
                    if li_image and li_image.startswith('//'):
                        li_image = 'https:' + li_image

                    if pid not in product_sales:
                        product_sales[pid] = {
                            'product_id': pid,
                            'title': li_product_title or 'Unknown',
                            'quantity': 0,
                            'revenue': 0.0,
                            'variant_sales': {},
                            'fallback_image': li_image,
                            'product_url': item.get('product_url', ''),
                        }
                    else:
                        # Backfill / improve title if we now have a better one
                        if (not product_sales[pid].get('title') or product_sales[pid]['title'] == 'Unknown') and li_product_title:
                            product_sales[pid]['title'] = li_product_title
                        # Backfill image if missing
                        if not product_sales[pid].get('fallback_image') and li_image:
                            product_sales[pid]['fallback_image'] = li_image
                    product_sales[pid]['quantity'] += qty
                    product_sales[pid]['revenue'] += revenue

                    vid = str(item.get('variant_id', '') or '')
                    # Shoplazza line items expose variant info as variant_title; fall back gracefully.
                    vtitle = (
                        item.get('variant_title') or
                        item.get('variant_name') or
                        item.get('option_title') or
                        'Default'
                    )
                    if vid not in product_sales[pid]['variant_sales']:
                        product_sales[pid]['variant_sales'][vid] = {
                            'variant_id': vid,
                            'title': vtitle,
                            'quantity': 0,
                            'revenue': 0.0,
                            'image': li_image,
                        }
                    product_sales[pid]['variant_sales'][vid]['quantity'] += qty
                    product_sales[pid]['variant_sales'][vid]['revenue'] += revenue
                    if not product_sales[pid]['variant_sales'][vid].get('image') and li_image:
                        product_sales[pid]['variant_sales'][vid]['image'] = li_image

            # Cursor pagination (under data.* in modern Shoplazza API)
            next_cursor = data_block.get('cursor') or resp_data.get('cursor') or ''
            has_more = bool(data_block.get('has_more') if data_block.get('has_more') is not None else resp_data.get('has_more', False))
            pages_fetched += 1

            # Periodic progress save every 10 pages
            if pages_fetched % 10 == 0:
                meta['pages_scanned'] = pages_fetched
                meta['total_orders'] = sum(p['quantity'] for p in product_sales.values())
                meta['total_products'] = len(product_sales)
                _save_winner_meta(store_id, meta)
                _save_winner_cache(store_id, product_sales)

            if not has_more or not next_cursor:
                break
            cursor = next_cursor

            # Shoplazza rate limit: 2 req/sec baseline
            time.sleep(0.5)

        # Final save
        _save_winner_cache(store_id, product_sales)
        meta['status'] = 'done'
        meta['pages_scanned'] = pages_fetched
        meta['total_orders'] = sum(p['quantity'] for p in product_sales.values())
        meta['total_products'] = len(product_sales)
        meta['last_synced'] = time.time()
        meta['duration_sec'] = round(time.time() - meta['started_at'], 1)
        _save_winner_meta(store_id, meta)

    except Exception as e:
        meta['status'] = 'error'
        meta['error'] = str(e)
        _save_winner_meta(store_id, meta)
    finally:
        lock.release()


def _run_winner_sync(store_id, domain, token):
    """Background worker: scans ALL paid orders for a store and saves per-product sales to disk."""
    lock = _winner_sync_locks.setdefault(store_id, threading.Lock())
    if not lock.acquire(blocking=False):
        return  # Already syncing

    meta = {
        'status': 'running',
        'pages_scanned': 0,
        'total_orders': 0,
        'total_products': 0,
        'started_at': time.time(),
        'last_synced': None,
        'error': None,
    }
    _save_winner_meta(store_id, meta)

    try:
        headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
        base_url = f'https://{domain}/admin/api/2024-01'
        product_sales = {}
        page_url = f'{base_url}/orders.json?status=any&financial_status=paid&limit=250&fields=line_items,created_at'
        pages_fetched = 0
        max_pages = 10000  # effectively unlimited

        while page_url and pages_fetched < max_pages:
            try:
                resp = http_requests.get(page_url, headers=headers, timeout=60)
            except Exception as e:
                # Transient error — retry up to 3 times with backoff
                retry_ok = False
                for attempt in range(3):
                    time.sleep(2 ** attempt)
                    try:
                        resp = http_requests.get(page_url, headers=headers, timeout=60)
                        retry_ok = True
                        break
                    except:
                        continue
                if not retry_ok:
                    raise

            # Handle 429 (rate limit) — Shopify returns Retry-After header
            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', '2'))
                time.sleep(retry_after)
                continue  # retry same page_url

            if resp.status_code != 200:
                raise Exception(f'Shopify API error {resp.status_code}: {resp.text[:200]}')

            orders = resp.json().get('orders', [])
            if not orders:
                break

            for order in orders:
                for item in order.get('line_items', []):
                    pid = item.get('product_id')
                    if not pid:
                        continue
                    qty = item.get('quantity', 1)
                    try:
                        price = float(item.get('price', '0'))
                    except:
                        price = 0.0
                    revenue = qty * price

                    if pid not in product_sales:
                        product_sales[pid] = {
                            'product_id': pid,
                            'title': item.get('title', 'Unknown'),
                            'quantity': 0,
                            'revenue': 0.0,
                            'variant_sales': {},
                        }
                    product_sales[pid]['quantity'] += qty
                    product_sales[pid]['revenue'] += revenue

                    vid = str(item.get('variant_id', '') or '')
                    vtitle = item.get('variant_title', '') or 'Default'
                    if vid not in product_sales[pid]['variant_sales']:
                        product_sales[pid]['variant_sales'][vid] = {
                            'variant_id': vid,
                            'title': vtitle,
                            'quantity': 0,
                            'revenue': 0.0,
                        }
                    product_sales[pid]['variant_sales'][vid]['quantity'] += qty
                    product_sales[pid]['variant_sales'][vid]['revenue'] += revenue

            link_header = resp.headers.get('Link', '')
            page_url = None
            if 'rel="next"' in link_header:
                for part in link_header.split(','):
                    if 'rel="next"' in part:
                        page_url = part.split('<')[1].split('>')[0]
                        break
            pages_fetched += 1

            # Periodic progress save every 10 pages + final save at end
            if pages_fetched % 10 == 0:
                meta['pages_scanned'] = pages_fetched
                meta['total_orders'] = sum(p['quantity'] for p in product_sales.values())
                meta['total_products'] = len(product_sales)
                _save_winner_meta(store_id, meta)
                _save_winner_cache(store_id, product_sales)

            # Respect Shopify rate limit (2 req/s baseline for REST)
            time.sleep(0.3)

        # Final save
        _save_winner_cache(store_id, product_sales)
        meta['status'] = 'done'
        meta['pages_scanned'] = pages_fetched
        meta['total_orders'] = sum(p['quantity'] for p in product_sales.values())
        meta['total_products'] = len(product_sales)
        meta['last_synced'] = time.time()
        meta['duration_sec'] = round(time.time() - meta['started_at'], 1)
        _save_winner_meta(store_id, meta)

    except Exception as e:
        meta['status'] = 'error'
        meta['error'] = str(e)
        _save_winner_meta(store_id, meta)
    finally:
        lock.release()


# ===== Shopify collection cache (per-store) =====
# Stores: {collections: [{id, title, handle, products_count, type}],
#          product_collections: {product_id_str: [coll_id_int, ...]}}
_collection_sync_locks = {}  # store_id -> threading.Lock

def _collection_cache_path(store_id):
    return os.path.join(WINNER_CACHE_DIR, f'{store_id}_collections.json')

def _load_collection_cache(store_id):
    path = _collection_cache_path(store_id)
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except:
        return None

def _save_collection_cache(store_id, data):
    path = _collection_cache_path(store_id)
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(data, f)
    os.replace(tmp, path)

def _shopify_get_with_retry(url, headers, timeout=30, max_retries=3):
    """GET helper that handles 429 (Retry-After) + transient errors."""
    for attempt in range(max_retries + 1):
        try:
            resp = http_requests.get(url, headers=headers, timeout=timeout)
        except Exception:
            if attempt >= max_retries:
                raise
            time.sleep(2 ** attempt)
            continue
        if resp.status_code == 429:
            retry_after = int(resp.headers.get('Retry-After', '2'))
            time.sleep(retry_after)
            continue
        return resp
    return resp

def _next_link(link_header):
    if not link_header or 'rel="next"' not in link_header:
        return None
    for part in link_header.split(','):
        if 'rel="next"' in part:
            try:
                return part.split('<')[1].split('>')[0]
            except:
                return None
    return None

def _run_shoplazza_collection_sync(store_id, domain, token):
    """Background worker: fetches all Shoplazza collections + product->collection mapping via /collects.

    Output shape matches the Shopify version so the GET endpoint and frontend treat both uniformly:
      collections: [{id, title, handle, products_count, type}]
      product_collections: {pid_str: [coll_id_str, ...]}
    """
    lock = _collection_sync_locks.setdefault(store_id, threading.Lock())
    if not lock.acquire(blocking=False):
        return

    state = {
        'status': 'running',
        'platform': 'shoplazza',
        'started_at': time.time(),
        'last_synced': None,
        'error': None,
        'collections': [],
        'product_collections': {},
    }
    _save_collection_cache(store_id, state)

    try:
        headers = _shoplazza_headers(token)
        base_url = f'https://{domain}/openapi/{SHOPLAZZA_API_VERSION}'

        # 1) Fetch all collections (single endpoint, no smart/custom split)
        collections = []
        cursor = None
        pages = 0
        while pages < 200:
            params = {'page_size': 100}
            if cursor:
                params['cursor'] = cursor
            resp = _shoplazza_get_with_retry(f'{base_url}/collections', headers, params=params, timeout=30)
            if resp is None or resp.status_code != 200:
                raise Exception(f'Shoplazza /collections {getattr(resp, "status_code", "?")}: {getattr(resp, "text", "")[:200]}')
            resp_data = resp.json()
            items = resp_data.get('collections', []) or resp_data.get('data', {}).get('collections', [])
            if not items:
                break
            for c in items:
                collections.append({
                    'id': str(c.get('id', '')),
                    'title': c.get('title', ''),
                    'handle': c.get('handle', ''),
                    'products_count': 0,  # backfilled from /collects below
                    'type': 'smart' if c.get('smart', False) else 'custom',
                })
            next_cursor = resp_data.get('cursor', '') or resp_data.get('data', {}).get('cursor', '')
            has_more = resp_data.get('has_more', False) or resp_data.get('data', {}).get('has_more', False)
            if not has_more or not next_cursor:
                break
            cursor = next_cursor
            pages += 1
            time.sleep(0.5)

        # 2) For each collection, fetch its product IDs via /collects?collection_id=<id>
        product_collections = {}
        for idx, c in enumerate(collections):
            cid = c['id']
            if not cid:
                continue
            cursor = None
            inner_pages = 0
            while inner_pages < 200:
                params = {'collection_id': cid, 'page_size': 250}
                if cursor:
                    params['cursor'] = cursor
                resp = _shoplazza_get_with_retry(f'{base_url}/collects', headers, params=params, timeout=30)
                if resp is None or resp.status_code != 200:
                    break  # don't fail the whole sync on one bad collection
                resp_data = resp.json()
                collects = resp_data.get('collects', []) or resp_data.get('data', {}).get('collects', [])
                if not collects:
                    break
                for col in collects:
                    pid = col.get('product_id', '')
                    if not pid:
                        continue
                    pid_str = str(pid)
                    if pid_str not in product_collections:
                        product_collections[pid_str] = []
                    product_collections[pid_str].append(cid)
                next_cursor = resp_data.get('cursor', '') or resp_data.get('data', {}).get('cursor', '')
                has_more = resp_data.get('has_more', False) or resp_data.get('data', {}).get('has_more', False)
                if not has_more or not next_cursor:
                    break
                cursor = next_cursor
                inner_pages += 1
                time.sleep(0.5)

            # Save partial progress every 5 collections
            if (idx + 1) % 5 == 0:
                state['collections'] = collections
                state['product_collections'] = product_collections
                state['progress'] = {'collections_done': idx + 1, 'collections_total': len(collections)}
                _save_collection_cache(store_id, state)

        # Compute authoritative products_count from the mapping
        from collections import Counter as _Counter
        actual = _Counter()
        for _pid, _cids in product_collections.items():
            for _cid in _cids:
                actual[_cid] += 1
        for c in collections:
            mapped = actual.get(c['id'], 0)
            c['products_count'] = mapped
            c['mapped_products_count'] = mapped

        state['status'] = 'done'
        state['collections'] = collections
        state['product_collections'] = product_collections
        state['last_synced'] = time.time()
        state['progress'] = {'collections_done': len(collections), 'collections_total': len(collections)}
        state['duration_sec'] = round(time.time() - state['started_at'], 1)
        _save_collection_cache(store_id, state)

    except Exception as e:
        state['status'] = 'error'
        state['error'] = str(e)
        _save_collection_cache(store_id, state)
    finally:
        lock.release()


def _run_collection_sync(store_id, domain, token):
    """Background worker: fetches all collections + product->collection mapping."""
    lock = _collection_sync_locks.setdefault(store_id, threading.Lock())
    if not lock.acquire(blocking=False):
        return

    state = {
        'status': 'running',
        'started_at': time.time(),
        'last_synced': None,
        'error': None,
        'collections': [],
        'product_collections': {},
    }
    _save_collection_cache(store_id, state)

    try:
        headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
        base_url = f'https://{domain}/admin/api/2024-01'

        collections = []
        # 1) Custom collections
        url = f'{base_url}/custom_collections.json?limit=250&fields=id,title,handle,products_count'
        while url:
            resp = _shopify_get_with_retry(url, headers, timeout=30)
            if resp.status_code != 200:
                raise Exception(f'custom_collections.json {resp.status_code}: {resp.text[:200]}')
            for c in resp.json().get('custom_collections', []):
                collections.append({
                    'id': c.get('id'),
                    'title': c.get('title', ''),
                    'handle': c.get('handle', ''),
                    'products_count': c.get('products_count', 0),
                    'type': 'custom',
                })
            url = _next_link(resp.headers.get('Link', ''))
            time.sleep(0.3)

        # 2) Smart collections
        url = f'{base_url}/smart_collections.json?limit=250&fields=id,title,handle,products_count'
        while url:
            resp = _shopify_get_with_retry(url, headers, timeout=30)
            if resp.status_code != 200:
                raise Exception(f'smart_collections.json {resp.status_code}: {resp.text[:200]}')
            for c in resp.json().get('smart_collections', []):
                collections.append({
                    'id': c.get('id'),
                    'title': c.get('title', ''),
                    'handle': c.get('handle', ''),
                    'products_count': c.get('products_count', 0),
                    'type': 'smart',
                })
            url = _next_link(resp.headers.get('Link', ''))
            time.sleep(0.3)

        # 3) For each collection, fetch product IDs.
        # /collections/<id>/products.json works for both custom + smart collections,
        # and only returns active products (matches what's shown on storefront).
        product_collections = {}  # pid_str -> [coll_id, ...]
        for idx, c in enumerate(collections):
            cid = c['id']
            if not cid:
                continue
            url = f'{base_url}/collections/{cid}/products.json?limit=250&fields=id'
            page = 0
            while url and page < 200:  # safety cap
                resp = _shopify_get_with_retry(url, headers, timeout=30)
                if resp.status_code != 200:
                    # Don't fail the whole sync on one bad collection — log + skip
                    break
                for prod in resp.json().get('products', []):
                    pid = prod.get('id')
                    if pid is None:
                        continue
                    pid_str = str(pid)
                    if pid_str not in product_collections:
                        product_collections[pid_str] = []
                    product_collections[pid_str].append(cid)
                url = _next_link(resp.headers.get('Link', ''))
                page += 1
                time.sleep(0.3)

            # Save partial progress every 5 collections so big stores don't lose work
            if (idx + 1) % 5 == 0:
                state['collections'] = collections
                state['product_collections'] = product_collections
                state['progress'] = {'collections_done': idx + 1, 'collections_total': len(collections)}
                _save_collection_cache(store_id, state)

        # Recompute products_count from the actual mapping — Shopify's products_count
        # field on collections is unreliable (often 0 in newer API versions).
        from collections import Counter as _Counter
        actual_counts = _Counter()
        for _pid, _cids in product_collections.items():
            for _cid in _cids:
                actual_counts[_cid] += 1
        for c in collections:
            cid = c.get('id')
            mapped = actual_counts.get(cid, 0)
            # Use mapped count if Shopify's reported count is 0 or missing
            if not c.get('products_count'):
                c['products_count'] = mapped
            # Also expose the authoritative count separately
            c['mapped_products_count'] = mapped

        state['status'] = 'done'
        state['collections'] = collections
        state['product_collections'] = product_collections
        state['last_synced'] = time.time()
        state['progress'] = {'collections_done': len(collections), 'collections_total': len(collections)}
        state['duration_sec'] = round(time.time() - state['started_at'], 1)
        _save_collection_cache(store_id, state)

    except Exception as e:
        state['status'] = 'error'
        state['error'] = str(e)
        _save_collection_cache(store_id, state)
    finally:
        lock.release()


@app.route('/api/shopify/collections/<store_id>', methods=['GET'])
@admin_required
def shopify_collections_get(store_id):
    """Return cached collections for a store, or needsSync flag if no cache exists."""
    stores = _load_stores()
    store = next((s for s in stores if s['id'] == store_id), None)
    if not store:
        return jsonify({'success': False, 'error': 'Store not found'}), 404

    cache = _load_collection_cache(store_id)
    if cache is None:
        return jsonify({
            'success': True, 'needsSync': True,
            'collections': [], 'status': 'never', 'last_synced': None,
        })

    status = cache.get('status', 'unknown')
    if status == 'running':
        prog = cache.get('progress', {})
        return jsonify({
            'success': True, 'syncing': True,
            'collections': cache.get('collections', []),  # partial list ok
            'status': 'running',
            'progress': prog,
            'last_synced': cache.get('last_synced'),
        })
    if status == 'error':
        return jsonify({
            'success': True, 'error': cache.get('error', 'Unknown error'),
            'status': 'error',
            'collections': cache.get('collections', []),
            'last_synced': cache.get('last_synced'),
        })

    # Backfill products_count from the actual product->collection mapping.
    # Shopify's reported products_count is unreliable (often 0 on newer API versions).
    raw_collections = cache.get('collections', []) or []
    pc_map = cache.get('product_collections', {}) or {}
    actual_counts = {}
    for _pid, _cids in pc_map.items():
        for _cid in _cids:
            actual_counts[_cid] = actual_counts.get(_cid, 0) + 1
    enriched = []
    needs_resave = False
    for c in raw_collections:
        cid = c.get('id')
        mapped = actual_counts.get(cid, 0)
        if not c.get('products_count') and mapped:
            c['products_count'] = mapped
            needs_resave = True
        c['mapped_products_count'] = mapped
        enriched.append(c)
    if needs_resave:
        cache['collections'] = enriched
        try:
            _save_collection_cache(store_id, cache)
        except Exception:
            pass  # not critical, will re-derive on next request

    # Sort by title for stable UI
    collections = sorted(enriched, key=lambda c: (c.get('title') or '').lower())
    return jsonify({
        'success': True,
        'collections': collections,
        'status': status,
        'last_synced': cache.get('last_synced'),
        'fromCache': True,
    })


@app.route('/api/shopify/collections/<store_id>/sync', methods=['POST'])
@admin_required
def shopify_collections_sync_start(store_id):
    """Start background sync of collections + product->collection mapping.
    Dispatches to Shopify or Shoplazza based on the store's platform field."""
    stores = _load_stores()
    store = next((s for s in stores if s['id'] == store_id), None)
    if not store:
        return jsonify({'success': False, 'error': 'Store not found'}), 404

    platform, domain, token = _get_store_credentials(store)
    if not domain or not token:
        return jsonify({
            'success': False,
            'error': f'{platform.title()} not connected for this store'
        }), 400

    cache = _load_collection_cache(store_id)
    if cache and cache.get('status') == 'running':
        return jsonify({'success': True, 'status': 'already_running'})

    worker = _run_shoplazza_collection_sync if platform == 'shoplazza' else _run_collection_sync
    thread = threading.Thread(target=worker, args=(store_id, domain, token), daemon=True)
    thread.start()
    return jsonify({'success': True, 'status': 'started', 'platform': platform})


@app.route('/api/shopify/winners/<store_id>/sync', methods=['POST'])
@admin_required
def shopify_winners_sync_start(store_id):
    """Start a background sync of ALL paid orders for this store.
    Dispatches to Shopify or Shoplazza based on the store's platform field."""
    stores = _load_stores()
    store = next((s for s in stores if s['id'] == store_id), None)
    if not store:
        return jsonify({'success': False, 'error': 'Store not found'}), 404

    platform, domain, token = _get_store_credentials(store)
    if not domain or not token:
        return jsonify({
            'success': False,
            'error': f'{platform.title()} not connected for this store'
        }), 400

    # Check if already running
    meta = _load_winner_meta(store_id)
    if meta.get('status') == 'running':
        return jsonify({'success': True, 'status': 'already_running', 'meta': meta})

    worker = _run_shoplazza_winner_sync if platform == 'shoplazza' else _run_winner_sync
    thread = threading.Thread(target=worker, args=(store_id, domain, token), daemon=True)
    thread.start()
    return jsonify({'success': True, 'status': 'started', 'platform': platform})


@app.route('/api/shopify/winners/<store_id>/sync/status', methods=['GET'])
@admin_required
def shopify_winners_sync_status(store_id):
    """Poll sync progress."""
    meta = _load_winner_meta(store_id)
    return jsonify({'success': True, 'meta': meta})


@app.route('/api/shopify/winners/<store_id>', methods=['GET'])
@admin_required
def shopify_winners(store_id):
    """Fetch top-selling products from the CACHED sales data. Fast.

    If no cache exists, returns a flag telling the client to trigger a sync first.
    If a sync is running, returns current progress.
    """
    stores = _load_stores()
    store = next((s for s in stores if s['id'] == store_id), None)
    if not store:
        return jsonify({'success': False, 'error': 'Store not found'}), 404

    platform, domain, token = _get_store_credentials(store)
    if not domain or not token:
        return jsonify({'success': False, 'error': f'{platform.title()} not connected'}), 400

    threshold = int(request.args.get('threshold', 5))
    collection_id_arg = (request.args.get('collection_id') or '').strip()

    meta = _load_winner_meta(store_id)
    cache = _load_winner_cache(store_id)

    # No cache ever built: tell client to start sync
    if cache is None and meta.get('status') != 'running':
        return jsonify({
            'success': True,
            'needsSync': True,
            'store': store.get('name', ''),
            'meta': meta,
            'products': [],
            'totalOrders': 0,
            'totalProducts': 0,
            'qualifiedCount': 0,
            'thresholdUsed': threshold,
            'pagesScanned': 0,
        })

    # Sync running, no cache yet
    if cache is None:
        return jsonify({
            'success': True,
            'syncing': True,
            'store': store.get('name', ''),
            'meta': meta,
            'products': [],
            'totalOrders': meta.get('total_orders', 0),
            'totalProducts': meta.get('total_products', 0),
            'qualifiedCount': 0,
            'thresholdUsed': threshold,
            'pagesScanned': meta.get('pages_scanned', 0),
        })

    # We have a cache — filter by threshold and return
    product_sales = cache
    pages_fetched = meta.get('pages_scanned', 0)

    try:
        # Filter and sort
        sorted_products = sorted(product_sales.values(), key=lambda x: x.get('quantity', 0), reverse=True)
        if threshold > 0:
            sorted_products = [p for p in sorted_products if p.get('quantity', 0) >= threshold]

        # Optional: filter to a specific Shopify collection (uses cached map)
        collection_filter_status = 'none'  # none | applied | needs_sync | unknown_collection
        collection_filter_title = None
        if collection_id_arg:
            coll_cache = _load_collection_cache(store_id)
            if not coll_cache or coll_cache.get('status') == 'never':
                collection_filter_status = 'needs_sync'
            else:
                # Find collection title for response
                for c in coll_cache.get('collections', []) or []:
                    if str(c.get('id')) == collection_id_arg:
                        collection_filter_title = c.get('title')
                        break
                if collection_filter_title is None:
                    collection_filter_status = 'unknown_collection'
                else:
                    pc_map = coll_cache.get('product_collections', {}) or {}
                    target_cid = collection_id_arg
                    def _in_target_coll(pid):
                        ids = pc_map.get(str(pid), [])
                        return any(str(x) == target_cid for x in ids)
                    sorted_products = [p for p in sorted_products if _in_target_coll(p.get('product_id'))]
                    collection_filter_status = 'applied'

        # Batch-fetch product details (only for qualified products) — platform-aware
        product_details = {}
        all_pids = [p['product_id'] for p in sorted_products]

        if platform == 'shopify':
            headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
            base_url = f'https://{domain}/admin/api/2024-01'
            for i in range(0, len(all_pids), 250):
                batch_ids = ','.join(str(pid) for pid in all_pids[i:i+250])
                try:
                    pr = http_requests.get(
                        f'{base_url}/products.json?ids={batch_ids}&limit=250&fields=id,handle,images,variants,product_type,status',
                        headers=headers, timeout=30
                    )
                    if pr.status_code == 200:
                        for prod in pr.json().get('products', []):
                            pid = prod.get('id')
                            if pid:
                                imgs = prod.get('images', [])
                                img_by_id = {img['id']: img.get('src', '') for img in imgs if img.get('id')}
                                default_img = imgs[0].get('src', '') if imgs else ''
                                variants = []
                                seen_imgs = set()
                                for v in prod.get('variants', []):
                                    v_img = img_by_id.get(v.get('image_id'), default_img)
                                    variants.append({
                                        'id': str(v.get('id', '')),
                                        'title': v.get('title', 'Default'),
                                        'image': v_img
                                    })
                                    seen_imgs.add(v_img)
                                product_details[pid] = {
                                    'handle': prod.get('handle', ''),
                                    'image': default_img,
                                    'variants': variants if len(seen_imgs) > 1 else [],
                                    'product_type': prod.get('product_type', ''),
                                    'status': prod.get('status', 'unknown')
                                }
                    time.sleep(0.3)
                except:
                    pass
        else:
            # Shoplazza product-details enrichment.
            # Note: cached product IDs come from past orders and may no longer exist
            # in the live store (deleted/unpublished products). For those we fall back
            # to the line-item image and title we cached during order sync.
            headers = _shoplazza_headers(token)
            base_url = f'https://{domain}/openapi/{SHOPLAZZA_API_VERSION}'
            for i in range(0, len(all_pids), 100):  # smaller batches to avoid URL-length limits
                batch_ids = ','.join(str(pid) for pid in all_pids[i:i+100])
                try:
                    pr = _shoplazza_get_with_retry(
                        f'{base_url}/products',
                        headers,
                        params={'ids': batch_ids, 'page_size': 100},
                        timeout=30,
                    )
                    if pr is not None and pr.status_code == 200:
                        body = pr.json() or {}
                        # Shoplazza response: {code, data: {products: [...]}} — fall back to top-level for safety.
                        data_block = body.get('data') if isinstance(body.get('data'), dict) else body
                        prods = data_block.get('products') or body.get('products') or []
                        for prod in prods:
                            pid = prod.get('id')
                            if not pid:
                                continue
                            imgs = prod.get('images', []) or []
                            default_img = ''
                            if imgs:
                                first = imgs[0] or {}
                                default_img = first.get('src') or first.get('url') or ''
                            # Normalize protocol-relative URLs (//img.staticdj.com/...)
                            if default_img and default_img.startswith('//'):
                                default_img = 'https:' + default_img
                            product_details[pid] = {
                                'handle': prod.get('handle', '') or prod.get('seo_handle', ''),
                                'image': default_img,
                                # Shoplazza has no variant->image mapping, so leave empty
                                'variants': [],
                                'product_type': prod.get('product_type', '') or prod.get('category', ''),
                                'status': (prod.get('status') or ('active' if prod.get('published') else 'unknown')),
                                'title': prod.get('title', ''),
                            }
                    time.sleep(0.5)
                except Exception:
                    pass

        # Check which products already have video jobs
        all_jobs = _load_all_jobs()
        product_video_status = {}
        for j in all_jobs:
            if j.get('storeId') != store_id:
                continue
            pid_str = str(j.get('productId', ''))
            jstatus = j.get('status', '')
            existing = product_video_status.get(pid_str)
            if jstatus == 'done':
                product_video_status[pid_str] = 'done'
            elif jstatus in ('generating', 'polling') and existing != 'done':
                product_video_status[pid_str] = 'in_progress'
            elif jstatus == 'queued' and existing not in ('done', 'in_progress'):
                product_video_status[pid_str] = 'queued'

        # Build results
        results = []
        for p in sorted_products:
            pid = p['product_id']
            # Look up details — cached pid may be int or str, Shopify returns int
            detail = product_details.get(pid)
            if detail is None and isinstance(pid, str) and pid.isdigit():
                detail = product_details.get(int(pid))
            if detail is None and isinstance(pid, int):
                detail = product_details.get(str(pid))
            if detail is None:
                detail = {}
            handle = detail.get('handle', '')
            # Image: prefer live product image, fall back to cached line-item image (for deleted products)
            image_url = detail.get('image', '') or p.get('fallback_image', '')
            if image_url and image_url.startswith('//'):
                image_url = 'https:' + image_url
            product_type = detail.get('product_type', '')
            # If we got a live product, it's still active. If not found, mark 'archived'
            # (it existed at order time but isn't in the live store anymore).
            if detail:
                shopify_status = detail.get('status', 'unknown')
            else:
                shopify_status = 'archived' if platform == 'shoplazza' else 'unknown'
            video_status = product_video_status.get(str(pid), 'none')

            detail_variants = detail.get('variants', [])
            vid_to_img = {}
            for dv in detail_variants:
                vid_to_img[str(dv.get('id', ''))] = dv.get('image', '')

            colour_sales = {}
            for vb in p.get('variant_sales', {}).values():
                vtitle = vb.get('title', '') or 'Default'
                colour = vtitle.split(' / ')[0].strip() if ' / ' in vtitle else vtitle.strip()
                if not colour:
                    colour = 'Default'
                if colour not in colour_sales:
                    colour_sales[colour] = {'title': colour, 'quantity': 0, 'revenue': 0.0, 'image': ''}
                colour_sales[colour]['quantity'] += vb['quantity']
                colour_sales[colour]['revenue'] += vb['revenue']
                if not colour_sales[colour]['image']:
                    # Prefer Shopify variant->image map; fall back to Shoplazza cached line-item image.
                    variant_image = vid_to_img.get(vb.get('variant_id', ''), '') or vb.get('image', '')
                    if variant_image and variant_image.startswith('//'):
                        variant_image = 'https:' + variant_image
                    colour_sales[colour]['image'] = variant_image

            enriched_breakdown = sorted(
                colour_sales.values(),
                key=lambda v: v['quantity'], reverse=True
            )
            for eb in enriched_breakdown:
                eb['revenue'] = round(eb['revenue'], 2)

            # Build a public product URL the frontend can link to.
            # Shopify:  https://<domain>/products/<handle>
            # Shoplazza: prefer cached line-item product_url (works even for deleted products),
            #            else https://<shoplazzaDomain>/products/<handle>
            product_url = ''
            if platform == 'shoplazza':
                cached_path = (p.get('product_url') or '').strip()
                if cached_path:
                    if cached_path.startswith('http'):
                        product_url = cached_path
                    else:
                        # Shoplazza stores cached_path like '/products/some-handle'
                        product_url = f'https://{domain}{cached_path}'
                elif handle:
                    product_url = f'https://{domain}/products/{handle}'
            else:
                if handle and domain:
                    product_url = f'https://{domain}/products/{handle}'

            results.append({
                'id': str(pid),
                'name': p['title'],
                'store': store.get('name', ''),
                'storeId': store_id,
                'sales': p['quantity'],
                'revenue': round(p['revenue'], 2),
                'image': image_url,
                'variants': detail.get('variants', []),
                'variantSales': enriched_breakdown,
                'handle': handle,
                'productUrl': product_url,
                'platform': platform,
                'productType': product_type,
                'shopifyStatus': shopify_status,
                'hasVideo': video_status == 'done',
                'videoStatus': video_status,
            })

        return jsonify({
            'success': True,
            'products': results,
            'totalOrders': sum(p.get('quantity', 0) for p in product_sales.values()),
            'totalProducts': len(product_sales),
            'qualifiedCount': len(results),
            'thresholdUsed': threshold,
            'pagesScanned': pages_fetched,
            'store': store.get('name', ''),
            'meta': meta,  # includes last_synced, status
            'fromCache': True,
            'collectionFilter': {
                'id': collection_id_arg or None,
                'title': collection_filter_title,
                'status': collection_filter_status,
            },
        })

    except http_requests.exceptions.Timeout:
        return jsonify({'success': False, 'error': 'Shopify request timed out'}), 504
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== Routes: Video Generation =====
@app.route('/api/videos/generate', methods=['POST'])
@admin_required
def generate_videos():
    """Queue video generation for selected products.
    Creates N jobs per product (one per store prompt) if store has prompts configured.
    Falls back to 1 job with global prompt if no store prompts exist.
    """
    data = request.json
    products = data.get('products', [])
    if not products:
        return jsonify({'success': False, 'error': 'No products provided'}), 400

    settings = _load_settings()
    if not settings.get('xai_api_key'):
        return jsonify({'success': False, 'error': 'xAI API key not configured. Go to Settings tab.'}), 400

    # Optional per-request video model override
    video_model = data.get('video_model', '').strip() or None
    # Optional per-request aspect ratio override
    aspect_ratio = data.get('aspect_ratio', '').strip() or settings.get('aspect_ratio', '9:16')

    duration = settings.get('video_duration', 8)

    # Determine prompt source: 'store' (default) or 'account:<category_key>'
    prompt_source = data.get('promptSource', 'store').strip()

    # Resolve the 4 prompts to use
    use_prompts = []
    if prompt_source.startswith('account:'):
        # Account-level category batch
        category_key = prompt_source.split(':', 1)[1]
        use_prompts = _get_prompt_batch(category_key, settings)
    else:
        # Store-level prompts (original behavior)
        if products:
            first_store_id = products[0].get('storeId', '')
            if first_store_id:
                all_stores = _load_stores()
                store = next((s for s in all_stores if s['id'] == first_store_id), None)
                if store:
                    use_prompts = store.get('prompts', [])

    new_jobs = []
    with _jobs_lock:
        jobs = _load_jobs()
        for p in products:
            product_name = p.get('productName', '')
            store_category = p.get('storeCategory', '')
            group_id = uuid.uuid4().hex  # shared across all jobs for this product

            if use_prompts:
                # Create one job per prompt in the batch
                for idx, sp in enumerate(use_prompts):
                    job_id = f'job_{uuid.uuid4().hex[:12]}'
                    prompt_template = sp.get('template', '')
                    prompt = prompt_template.replace('{product_name}', product_name).replace('{store_category}', store_category)
                    job = {
                        'id': job_id,
                        'storeId': p.get('storeId', ''),
                        'storeName': p.get('storeName', ''),
                        'storeDomain': p.get('storeDomain', ''),
                        'productId': str(p.get('productId', '')),
                        'productName': product_name,
                        'productHandle': p.get('handle', ''),
                        'imageUrl': p.get('imageUrl', ''),
                        'storeCategory': store_category,
                        'status': 'queued',
                        'xaiRequestId': None,
                        'videoUrl': None,
                        'localPath': None,
                        'driveUrl': None,
                        'error': None,
                        'createdAt': datetime.datetime.utcnow().isoformat() + 'Z',
                        'completedAt': None,
                        'prompt': prompt,
                        'promptIndex': idx,
                        'promptLabel': sp.get('label', f'Prompt {idx + 1}'),
                        'groupId': group_id,
                        'videoModel': video_model,
                        'aspectRatio': aspect_ratio,
                        'videoDuration': duration,
                        'estimatedCost': round(duration * 0.05, 2),
                        'actualCost': None
                    }
                    jobs.append(job)
                    new_jobs.append(job)
            else:
                # Fallback: 1 job with global prompt (old behavior)
                job_id = f'job_{uuid.uuid4().hex[:12]}'
                prompt = _get_prompt(product_name, store_category, settings)
                job = {
                    'id': job_id,
                    'storeId': p.get('storeId', ''),
                    'storeName': p.get('storeName', ''),
                    'storeDomain': p.get('storeDomain', ''),
                    'productId': str(p.get('productId', '')),
                    'productName': product_name,
                    'productHandle': p.get('handle', ''),
                    'imageUrl': p.get('imageUrl', ''),
                    'storeCategory': store_category,
                    'status': 'queued',
                    'xaiRequestId': None,
                    'videoUrl': None,
                    'localPath': None,
                    'driveUrl': None,
                    'error': None,
                    'createdAt': datetime.datetime.utcnow().isoformat() + 'Z',
                    'completedAt': None,
                    'prompt': prompt,
                    'promptIndex': 0,
                    'promptLabel': '',
                    'groupId': group_id,
                    'videoModel': video_model,
                    'aspectRatio': aspect_ratio,
                    'videoDuration': duration,
                    'estimatedCost': round(duration * 0.05, 2),
                    'actualCost': None
                }
                jobs.append(job)
                new_jobs.append(job)
        _save_jobs(jobs)

    _ensure_worker()
    return jsonify({'success': True, 'queued': len(new_jobs), 'jobs': new_jobs})

@app.route('/api/videos/queue', methods=['GET'])
@admin_required
def get_queue():
    """Get video jobs with pagination.
    Query params:
      page (int, default 1)
      per_page (int, default 50, max 200)
      include_archive (bool, default false)
      status (str, default 'all') - filter by status
    """
    page = max(1, int(request.args.get('page', 1)))
    per_page = min(200, max(1, int(request.args.get('per_page', 50))))
    include_archive = request.args.get('include_archive', 'false').lower() == 'true'
    status_filter = request.args.get('status', 'all').lower()

    with _jobs_lock:
        jobs = _load_jobs()
        if include_archive:
            jobs = jobs + _load_archive()

    # Filter by status
    if status_filter != 'all':
        jobs = [j for j in jobs if j.get('status', '') == status_filter]

    # Most recent first
    jobs.sort(key=lambda j: j.get('createdAt', ''), reverse=True)

    total = len(jobs)
    start = (page - 1) * per_page
    end = start + per_page
    page_jobs = jobs[start:end]
    has_more = end < total

    return jsonify({
        'success': True,
        'jobs': page_jobs,
        'total': total,
        'page': page,
        'per_page': per_page,
        'has_more': has_more
    })


@app.route('/api/videos/queue/stats', methods=['GET'])
@admin_required
def get_queue_stats():
    """Get queue summary stats from both active and archive without returning all jobs."""
    with _jobs_lock:
        active_jobs = _load_jobs()
        archive_jobs = _load_archive()

    all_jobs = active_jobs + archive_jobs

    counts = {'queued': 0, 'generating': 0, 'polling': 0, 'done': 0, 'failed': 0, 'cancelled': 0}
    total_spent = 0.0
    total_pending = 0.0

    for j in all_jobs:
        status = j.get('status', '')
        if status in counts:
            counts[status] += 1

        if status == 'done':
            cost = j.get('actualCost')
            if cost is None:
                cost = j.get('estimatedCost', 0) or 0
            total_spent += cost
        elif status in ('queued', 'generating', 'polling'):
            total_pending += j.get('estimatedCost', 0) or 0

    return jsonify({
        'success': True,
        'total': len(all_jobs),
        'active_total': len(active_jobs),
        'archive_total': len(archive_jobs),
        'counts': counts,
        'totalSpent': round(total_spent, 2),
        'totalPending': round(total_pending, 2)
    })


@app.route('/api/videos/queue/clear-completed', methods=['POST'])
@admin_required
def clear_completed():
    """Move all done/failed/cancelled jobs from active to archive immediately."""
    now = datetime.datetime.now(datetime.timezone.utc)
    with _jobs_lock:
        jobs = _load_jobs()
        active = []
        to_archive = []
        for j in jobs:
            if j.get('status', '') in ('done', 'failed', 'cancelled'):
                j['archivedAt'] = now.isoformat().replace('+00:00', 'Z')
                to_archive.append(j)
            else:
                active.append(j)

        if to_archive:
            archive = _load_archive()
            archive.extend(to_archive)
            _save_archive(archive)
            _save_jobs(active)

    return jsonify({'success': True, 'cleared': len(to_archive)})

@app.route('/api/videos/<job_id>/retry', methods=['POST'])
@admin_required
def retry_job(job_id):
    data = request.get_json(silent=True) or {}
    new_prompt = data.get('prompt', '').strip() if data.get('prompt') else ''

    found = False
    with _jobs_lock:
        jobs = _load_jobs()
        for j in jobs:
            if j['id'] == job_id and j['status'] in ('failed', 'done'):
                # Clean up old video file if redoing a done job
                if j['status'] == 'done' and j.get('localPath'):
                    try:
                        old_path = j['localPath']
                        if os.path.exists(old_path):
                            os.remove(old_path)
                    except Exception:
                        pass
                j['status'] = 'queued'
                j['error'] = None
                j['xaiRequestId'] = None
                j['videoUrl'] = None
                j['localPath'] = None
                j['completedAt'] = None
                j['driveUrl'] = None
                j['driveFileId'] = None
                if new_prompt:
                    j['prompt'] = new_prompt
                    j['promptLabel'] = j.get('promptLabel', '') or 'Retried'
                found = True
                break
        if found:
            _save_jobs(jobs)
        else:
            # Check archive — move back to active if found
            archive = _load_archive()
            new_archive = []
            for j in archive:
                if j['id'] == job_id and j['status'] in ('failed', 'done'):
                    # Clean up old video file if redoing a done job
                    if j['status'] == 'done' and j.get('localPath'):
                        try:
                            old_path = j['localPath']
                            if os.path.exists(old_path):
                                os.remove(old_path)
                        except Exception:
                            pass
                    j['status'] = 'queued'
                    j['error'] = None
                    j['xaiRequestId'] = None
                    j['videoUrl'] = None
                    j['localPath'] = None
                    j['completedAt'] = None
                    j['driveUrl'] = None
                    j['driveFileId'] = None
                    j.pop('archivedAt', None)
                    if new_prompt:
                        j['prompt'] = new_prompt
                        j['promptLabel'] = j.get('promptLabel', '') or 'Retried'
                    jobs.append(j)
                    found = True
                else:
                    new_archive.append(j)
            if found:
                _save_archive(new_archive)
                _save_jobs(jobs)
    _ensure_worker()
    return jsonify({'success': True})

@app.route('/api/drive/upload-group', methods=['POST'])
@admin_required
def upload_group_to_drive():
    data = request.json
    group_id = data.get('groupId', '') if data else ''
    if not group_id:
        return jsonify({'success': False, 'error': 'No groupId provided'}), 400

    all_jobs = _load_all_jobs()
    group_jobs = [j for j in all_jobs if j.get('groupId') == group_id and j['status'] == 'done' and j.get('localPath')]

    if not group_jobs:
        return jsonify({'success': False, 'error': 'No completed videos in this group'}), 400

    service, err = _get_drive_service()
    if not service:
        return jsonify({'success': False, 'error': err}), 500

    results = []
    for job in group_jobs:
        if job.get('driveUrl'):
            results.append({'id': job['id'], 'status': 'already_uploaded', 'driveUrl': job['driveUrl']})
            continue

        full_path = os.path.join(VIDEOS_DIR, job['localPath'])
        if not os.path.exists(full_path):
            results.append({'id': job['id'], 'status': 'file_missing'})
            continue

        try:
            from googleapiclient.http import MediaFileUpload
            root_folder_id = _get_or_create_root_folder(service)
            store_name = job.get('storeName', 'Unknown Store')
            product_handle = job.get('productHandle', '') or job.get('productName', 'unknown-product')
            store_folder_id = _find_or_create_drive_folder(service, store_name, root_folder_id)
            product_folder_id = _find_or_create_numbered_product_folder(service, product_handle, store_folder_id)

            file_name = os.path.basename(full_path)
            file_metadata = {'name': file_name, 'parents': [product_folder_id]}
            media = MediaFileUpload(full_path, mimetype='video/mp4', resumable=True)
            uploaded = service.files().create(body=file_metadata, media_body=media, fields='id,webViewLink').execute()
            drive_url = uploaded.get('webViewLink', '')

            # Update job driveUrl
            with _jobs_lock:
                found_active = False
                jobs_list = _load_jobs()
                for j in jobs_list:
                    if j['id'] == job['id']:
                        j['driveUrl'] = drive_url
                        found_active = True
                        break
                if found_active:
                    _save_jobs(jobs_list)
                else:
                    archive = _load_archive()
                    for j in archive:
                        if j['id'] == job['id']:
                            j['driveUrl'] = drive_url
                            break
                    _save_archive(archive)

            results.append({'id': job['id'], 'status': 'uploaded', 'driveUrl': drive_url})
        except Exception as e:
            results.append({'id': job['id'], 'status': 'error', 'error': str(e)})

    return jsonify({'success': True, 'results': results})


@app.route('/api/videos/<job_id>/cancel', methods=['POST'])
@admin_required
def cancel_job(job_id):
    with _jobs_lock:
        jobs = _load_jobs()
        for j in jobs:
            if j['id'] == job_id and j['status'] == 'queued':
                j['status'] = 'cancelled'
                break
        _save_jobs(jobs)
    return jsonify({'success': True})

@app.route('/api/videos/<job_id>/skip', methods=['POST'])
@admin_required
def skip_job(job_id):
    """Skip a generating/polling job — marks it failed so the worker moves on."""
    with _jobs_lock:
        jobs = _load_jobs()
        for j in jobs:
            if j['id'] == job_id and j['status'] in ('generating', 'polling', 'queued'):
                j['status'] = 'failed'
                j['error'] = 'Skipped by user'
                break
        _save_jobs(jobs)
    return jsonify({'success': True})

@app.route('/api/videos/<job_id>/download', methods=['GET'])
@admin_required
def download_video(job_id):
    all_jobs = _load_all_jobs()
    job = next((j for j in all_jobs if j['id'] == job_id), None)
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404
    if not job.get('localPath'):
        return jsonify({'success': False, 'error': 'No video file available'}), 404

    full_path = os.path.join(VIDEOS_DIR, job['localPath'])
    if not os.path.exists(full_path):
        return jsonify({'success': False, 'error': 'Video file not found on disk'}), 404

    return send_file(full_path, mimetype='video/mp4', as_attachment=True,
                     download_name=f"{job.get('productHandle', 'video')}_{job['id']}.mp4")

@app.route('/data/videos/<path:filepath>')
@login_required
def serve_video(filepath):
    """Serve video files from data/videos/ directory."""
    full_path = os.path.join(VIDEOS_DIR, filepath)
    if not os.path.exists(full_path):
        return jsonify({'success': False, 'error': 'Not found'}), 404
    return send_file(full_path, mimetype='video/mp4')



# ===== Routes: Analytics =====
def _shopifyql_query(domain, token, query_str):
    """Execute a ShopifyQL query via Shopify GraphQL Admin API (2026-01)."""
    graphql_url = f'https://{domain}/admin/api/2026-01/graphql.json'
    headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
    gql = json.dumps({
        'query': 'query($q: String!) { shopifyqlQuery(query: $q) { tableData { columns { name dataType displayName } rows } parseErrors } }',
        'variables': {'q': query_str}
    })
    resp = http_requests.post(graphql_url, data=gql, headers=headers, timeout=30)
    if resp.status_code != 200:
        return None, f'HTTP {resp.status_code}'
    body = resp.json()
    data = body.get('data', {}).get('shopifyqlQuery', {})
    errors = data.get('parseErrors') or []
    if errors:
        return None, str(errors)
    # Check for GraphQL-level errors
    if body.get('errors'):
        return None, str(body['errors'])
    td = data.get('tableData')
    if not td:
        return None, 'No tableData'
    return td, None

def _analytics_via_shopifyql(domain, token, start, end, tz=None):
    """Fetch analytics using ShopifyQL queries (revenue + funnel)."""
    tz_clause = f" WITH TIMEZONE '{tz}'" if tz else ''
    revenue_q = f'FROM sales SHOW total_sales, orders GROUP BY day SINCE {start} UNTIL {end} ORDER BY day{tz_clause}'
    funnel_q = f'FROM sessions SHOW sessions, conversion_rate, pageviews, bounce_rate SINCE {start} UNTIL {end}{tz_clause}'

    rev_result = [None, None]
    fun_result = [None, None]
    def run_rev():
        try:
            rev_result[0], rev_result[1] = _shopifyql_query(domain, token, revenue_q)
        except Exception as e:
            rev_result[1] = str(e)
    def run_fun():
        try:
            fun_result[0], fun_result[1] = _shopifyql_query(domain, token, funnel_q)
        except Exception as e:
            fun_result[1] = str(e)

    with ThreadPoolExecutor(max_workers=2) as pool:
        pool.submit(run_rev)
        pool.submit(run_fun)
        pool.shutdown(wait=True)

    if rev_result[1]:
        return None, rev_result[1]

    # Parse revenue data — rows are dicts with column names as keys
    td = rev_result[0]
    if not td:
        return None, 'ShopifyQL returned no data'
    rows = td.get('rows', [])
    labels = []
    rev_values = []
    ord_values = []
    for row in rows:
        day_str = row.get('day', '')
        if day_str:
            labels.append(day_str[:10])  # YYYY-MM-DD
        ts = float(row.get('total_sales', '0') or '0')
        oc = int(float(row.get('orders', '0') or '0'))
        rev_values.append(round(ts, 2))
        ord_values.append(oc)

    total_rev = round(sum(rev_values), 2)
    total_ord = sum(ord_values)
    aov = round(total_rev / total_ord, 2) if total_ord > 0 else 0

    # Parse funnel / session metrics data — rows are dicts
    funnel = None
    if fun_result[0] and not fun_result[1]:
        ftd = fun_result[0]
        frows = ftd.get('rows', [])
        s_sessions = 0
        s_conversion_rate = 0.0
        s_pageviews = 0
        s_bounce_rate = 0.0
        for row in frows:
            s_sessions += int(float(row.get('sessions', '0') or '0'))
            s_conversion_rate = float(row.get('conversion_rate', '0') or '0')
            s_pageviews += int(float(row.get('pageviews', '0') or '0'))
            s_bounce_rate = float(row.get('bounce_rate', '0') or '0')
        # Compute completed checkouts from conversion rate
        s_converted = int(round(s_sessions * s_conversion_rate))
        funnel = {
            'sessions': s_sessions,
            'pageviews': s_pageviews,
            'conversionRate': round(s_conversion_rate * 100, 2),
            'bounceRate': round(s_bounce_rate * 100, 2),
            'completedCheckout': s_converted,
            'orders': total_ord  # from revenue data above
        }

    return {
        'revenue': {'labels': labels, 'values': rev_values, 'total': total_rev},
        'orders': {'values': ord_values, 'total': total_ord},
        'aov': aov,
        'funnel': funnel,
        'source': 'shopifyql'
    }, None

def _extract_utm_source(landing_site, referring_site, note_attributes=None):
    """Extract UTM source from landing_site, referring_site, then note_attributes as fallback.

    Priority order matters: landing URL and referrer reflect the current purchasing
    session. note_attributes are theme cookies that can be stale from a previous visit.
    """
    from urllib.parse import urlparse, parse_qs
    landing = landing_site or ''
    landing_lower = landing.lower()
    referring = (referring_site or '').lower()
    notes = note_attributes or []

    # Priority 1: Explicit utm_source in landing URL (current session)
    if 'utm_source=' in landing:
        try:
            qs = parse_qs(urlparse(landing).query)
            src = qs.get('utm_source', [''])[0]
            if src:
                return src.lower()
        except Exception:
            pass

    # Priority 2: Click IDs in landing URL + referring_site domain
    if 'pins_campaign_id' in landing_lower or 'pinterest' in referring:
        return 'pinterest'
    if 'gclid' in landing_lower or 'gad_source' in landing_lower or 'google' in referring:
        return 'google'
    if 'fbclid' in landing_lower or 'facebook' in referring or 'instagram' in referring:
        return 'facebook'
    if 'outbrain' in landing_lower or 'outbrain' in referring:
        return 'outbrain'
    if 'taboola' in landing_lower or 'taboola' in referring:
        return 'taboola'
    if 'msclkid' in landing_lower or 'bing' in referring:
        return 'bing'
    if 'ttclid' in landing_lower or 'tiktok' in referring:
        return 'tiktok'
    if 'sclid' in landing_lower or 'snapchat' in referring:
        return 'snapchat'
    if 'klaviyo' in landing_lower or 'klaviyo' in referring:
        return 'klaviyo'
    # Unknown referrer — extract domain name as source
    if referring and referring != 'null':
        try:
            host = urlparse(referring).hostname or ''
            host = host.replace('www.', '').replace('android-app://', '').replace('com.', '')
            if host:
                return host.split('.')[0]
        except Exception:
            pass

    # Priority 3: note_attributes cookie (fallback — may be stale from previous visit)
    for attr in notes:
        name = attr.get('name', '').lower()
        val = attr.get('value', '')
        if name == 'utm_source' and val:
            return val.lower()
    # Check note_attributes for click ID keys
    note_map = {a.get('name', '').lower(): a.get('value', '') for a in notes}
    if note_map.get('gclid'):
        return 'google'
    if note_map.get('epik'):
        return 'pinterest'
    if note_map.get('fbclid'):
        return 'facebook'

    return 'direct'

def _fetch_orders_with_utm(domain, token, start, end):
    """Fetch all orders in date range with landing_site and referring_site for UTM parsing."""
    headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
    base_url = f'https://{domain}/admin/api/2024-01'
    start_dt = f'{start}T00:00:00Z'
    end_dt = f'{end}T23:59:59Z'
    all_orders = []
    page_url = (f'{base_url}/orders.json?status=any'
                f'&created_at_min={start_dt}&created_at_max={end_dt}'
                f'&limit=250&fields=id,created_at,current_total_price,landing_site,referring_site,financial_status,note_attributes')
    pages = 0
    while page_url and pages < 50:
        resp = http_requests.get(page_url, headers=headers, timeout=20)
        if resp.status_code != 200:
            break
        orders = resp.json().get('orders', [])
        if not orders:
            break
        all_orders.extend(orders)
        page_url = None
        link = resp.headers.get('Link', '')
        if 'rel="next"' in link:
            for part in link.split(','):
                if 'rel="next"' in part:
                    page_url = part.split('<')[1].split('>')[0]
                    break
        pages += 1
        time.sleep(0.3)
    return all_orders

def _analytics_via_orders(domain, token, start, end):
    """Fallback: compute revenue from REST Orders API."""
    headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
    base_url = f'https://{domain}/admin/api/2024-01'
    start_dt = f'{start}T00:00:00Z'
    end_dt = f'{end}T23:59:59Z'

    daily = {}  # date -> {rev, orders}
    page_url = (f'{base_url}/orders.json?status=any&financial_status=paid'
                f'&created_at_min={start_dt}&created_at_max={end_dt}'
                f'&limit=250&fields=id,created_at,current_total_price')
    pages = 0
    while page_url and pages < 50:
        resp = http_requests.get(page_url, headers=headers, timeout=20)
        if resp.status_code != 200:
            break
        orders = resp.json().get('orders', [])
        if not orders:
            break
        for o in orders:
            ca = o.get('created_at', '')[:10]
            price = float(o.get('current_total_price', '0') or '0')
            if ca not in daily:
                daily[ca] = {'rev': 0.0, 'orders': 0}
            daily[ca]['rev'] += price
            daily[ca]['orders'] += 1
        page_url = None
        link = resp.headers.get('Link', '')
        if 'rel="next"' in link:
            for part in link.split(','):
                if 'rel="next"' in part:
                    page_url = part.split('<')[1].split('>')[0]
                    break
        pages += 1
        time.sleep(0.3)

    # Build daily arrays filling gaps
    d_start = datetime.date.fromisoformat(start)
    d_end = datetime.date.fromisoformat(end)
    labels = []
    rev_values = []
    ord_values = []
    d = d_start
    while d <= d_end:
        ds = d.isoformat()
        labels.append(ds)
        entry = daily.get(ds, {'rev': 0.0, 'orders': 0})
        rev_values.append(round(entry['rev'], 2))
        ord_values.append(entry['orders'])
        d += datetime.timedelta(days=1)

    total_rev = round(sum(rev_values), 2)
    total_ord = sum(ord_values)
    aov = round(total_rev / total_ord, 2) if total_ord > 0 else 0

    return {
        'revenue': {'labels': labels, 'values': rev_values, 'total': total_rev},
        'orders': {'values': ord_values, 'total': total_ord},
        'aov': aov,
        'funnel': None,
        'source': 'orders_api'
    }, None

@app.route('/api/analytics/utm-breakdown/<store_id>', methods=['GET'])
@login_required
def analytics_utm_breakdown(store_id):
    """Fetch UTM source breakdown for a store over a date range."""
    stores = _load_stores()
    store = next((s for s in stores if s['id'] == store_id), None)
    if not store:
        return jsonify({'success': False, 'error': 'Store not found'}), 404
    domain = store.get('domain', '')
    token = store.get('shopifyAccessToken', '')
    if not domain or not token:
        return jsonify({'success': False, 'error': 'Shopify not connected'}), 400

    today = datetime.datetime.now(datetime.timezone.utc).date()
    start = request.args.get('start', (today - datetime.timedelta(days=6)).isoformat())
    end = request.args.get('end', today.isoformat())

    try:
        orders = _fetch_orders_with_utm(domain, token, start, end)
        # Group by utm_source
        sources = {}  # source -> {revenue, orders, daily: {date -> {rev, ord}}}
        for o in orders:
            src = _extract_utm_source(o.get('landing_site'), o.get('referring_site'), o.get('note_attributes'))
            price = float(o.get('current_total_price', '0') or '0')
            day = o.get('created_at', '')[:10]
            if src not in sources:
                sources[src] = {'revenue': 0.0, 'orders': 0, 'daily': {}}
            sources[src]['revenue'] += price
            sources[src]['orders'] += 1
            if day not in sources[src]['daily']:
                sources[src]['daily'][day] = {'rev': 0.0, 'ord': 0}
            sources[src]['daily'][day]['rev'] += price
            sources[src]['daily'][day]['ord'] += 1

        # Build date labels for the period
        d_start = datetime.date.fromisoformat(start)
        d_end = datetime.date.fromisoformat(end)
        labels = []
        d = d_start
        while d <= d_end:
            labels.append(d.isoformat())
            d += datetime.timedelta(days=1)

        # EUR → USD conversion if needed
        currency = _ensure_store_currency(store)
        rate = _get_eur_usd_rate() if currency == 'EUR' else 1.0

        # Format response
        source_list = []
        for src, data in sorted(sources.items(), key=lambda x: -x[1]['revenue']):
            daily_rev = [round(data['daily'].get(lbl, {}).get('rev', 0.0) * rate, 2) for lbl in labels]
            daily_ord = [data['daily'].get(lbl, {}).get('ord', 0) for lbl in labels]
            rev = round(data['revenue'] * rate, 2)
            source_list.append({
                'source': src,
                'revenue': rev,
                'orders': data['orders'],
                'aov': round(rev / data['orders'], 2) if data['orders'] > 0 else 0,
                'daily_revenue': daily_rev,
                'daily_orders': daily_ord
            })

        resp = {
            'success': True,
            'labels': labels,
            'sources': source_list,
            'total_orders': len(orders)
        }
        if currency == 'EUR':
            resp['original_currency'] = 'EUR'
            resp['conversion_rate'] = rate
        return jsonify(resp)
    except Exception as e:
        log.error(f'UTM breakdown error for {store.get("name","")}: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/store-timezone/<store_id>', methods=['GET'])
@login_required
def store_timezone(store_id):
    """Fetch store timezone info from Shopify."""
    stores = _load_stores()
    store = next((s for s in stores if s['id'] == store_id), None)
    if not store:
        return jsonify({'success': False, 'error': 'Store not found'}), 404
    domain = store.get('domain', '')
    token = store.get('shopifyAccessToken', '')
    if not domain or not token:
        return jsonify({'success': False, 'error': 'Shopify not connected'}), 400
    try:
        graphql_url = f'https://{domain}/admin/api/2026-01/graphql.json'
        headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
        gql = json.dumps({'query': '{ shop { ianaTimezone timezoneAbbreviation timezoneOffset } }'})
        resp = http_requests.post(graphql_url, data=gql, headers=headers, timeout=15)
        if resp.status_code != 200:
            return jsonify({'success': False, 'error': f'HTTP {resp.status_code}'}), 500
        shop = resp.json().get('data', {}).get('shop', {})
        return jsonify({
            'success': True,
            'ianaTimezone': shop.get('ianaTimezone', ''),
            'abbreviation': shop.get('timezoneAbbreviation', ''),
            'offset': shop.get('timezoneOffset', '')
        })
    except Exception as e:
        log.error(f'Timezone fetch error for {store.get("name","")}: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/analytics/<store_id>', methods=['GET'])
@login_required
def store_analytics(store_id):
    """Fetch analytics data for a store (revenue, AOV, conversion funnel)."""
    stores = _load_stores()
    store = next((s for s in stores if s['id'] == store_id), None)
    if not store:
        return jsonify({'success': False, 'error': 'Store not found'}), 404

    domain = store.get('domain', '')
    token = store.get('shopifyAccessToken', '')
    if not domain or not token:
        return jsonify({'success': False, 'error': 'Shopify not connected'}), 400

    today = datetime.datetime.now(datetime.timezone.utc).date()
    start = request.args.get('start', (today - datetime.timedelta(days=6)).isoformat())
    end = request.args.get('end', today.isoformat())

    # Timezone override — validate IANA format (contains '/' or is 'UTC')
    tz = request.args.get('tz', None)
    if tz and tz != 'UTC' and '/' not in tz:
        tz = None  # Invalid, ignore

    try:
        # Try ShopifyQL first, fall back to Orders API
        result, err = _analytics_via_shopifyql(domain, token, start, end, tz=tz)
        if err:
            log.info(f'ShopifyQL failed for {store.get("name","")}: {err} — falling back to Orders API')
            result, err2 = _analytics_via_orders(domain, token, start, end)
            if err2:
                return jsonify({'success': False, 'error': f'Analytics failed: {err2}'}), 500

        # Detect currency and convert EUR → USD if needed
        currency = _ensure_store_currency(store)
        conversion_rate = None
        if currency == 'EUR':
            rate = _get_eur_usd_rate()
            conversion_rate = rate
            # Convert revenue values
            if 'revenue' in result:
                result['revenue']['values'] = [round(v * rate, 2) for v in result['revenue']['values']]
                result['revenue']['total'] = round(result['revenue']['total'] * rate, 2)
            if 'aov' in result:
                result['aov'] = round(result['aov'] * rate, 2)

        resp_data = {
            'success': True,
            'store': store.get('name', ''),
            'currency': 'USD',
            'period': {'start': start, 'end': end},
            **result
        }
        if conversion_rate:
            resp_data['original_currency'] = 'EUR'
            resp_data['conversion_rate'] = conversion_rate
        if tz:
            resp_data['tz'] = tz

        return jsonify(resp_data)
    except Exception as e:
        import traceback
        log.error(f'Analytics error for {store.get("name","")}: {e}\n{traceback.format_exc()}')
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== Collections =====
COLLECTIONS_FILE = os.path.join(DATA_DIR, 'collections.json')

def _load_collections():
    if os.path.exists(COLLECTIONS_FILE):
        try:
            with open(COLLECTIONS_FILE, 'r') as f:
                return json.load(f)
        except: pass
    return {'tree': []}

def _save_collections(data):
    tmp = COLLECTIONS_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, COLLECTIONS_FILE)

def _find_collection_node(tree, node_id):
    for node in tree:
        if node['id'] == node_id:
            return node
        found = _find_collection_node(node.get('children', []), node_id)
        if found:
            return found
    return None

def _delete_collection_node(tree, node_id):
    for i, node in enumerate(tree):
        if node['id'] == node_id:
            tree.pop(i)
            return True
        if _delete_collection_node(node.get('children', []), node_id):
            return True
    return False

def _parse_product_url(url):
    """Parse a product URL and return (source, product_id) or (None, None)."""
    # AliExpress: standard item URL
    m = re.search(r'aliexpress\.[a-z.]+/item/(\d+)', url)
    if m:
        return 'AliExpress', m.group(1)
    # AliExpress: short URL with productId param
    m = re.search(r'aliexpress.*[?&]productId=(\d+)', url)
    if m:
        return 'AliExpress', m.group(1)
    # 1688
    m = re.search(r'1688\.com/offer/(\d+)', url)
    if m:
        return '1688', m.group(1)
    # Shopify / Shoplazza
    m = re.search(r'(https?://[^/]+)(?:/[^/]+)*/products/([^/?#]+)', url)
    if m:
        canonical = f"{m.group(1)}/products/{m.group(2)}"
        return 'shopify_url', canonical
    return None, None

def _get_tmapi_key():
    try:
        with open('/root/product-research/config.json', 'r') as f:
            return json.load(f).get('tmapi_key', '')
    except:
        return ''

def _fetch_product_data(source, product_id):
    """Fetch product data. Returns dict on success, or (None, error_string) on failure."""
    if source == 'AliExpress':
        key = _get_tmapi_key()
        if not key:
            return None, 'TMAPI key not configured'
        try:
            resp = http_requests.get('http://api.tmapi.top/aliexpress/item_detail',
                                params={'apiToken': key, 'item_id': product_id, 'country': 'us'},
                                timeout=30, verify=False)
            data = resp.json()
            if data.get('code') and data['code'] != 200:
                return None, f"AliExpress API error: {data.get('msg', 'Unknown error')} (code {data['code']})"
            item = data.get('data', {})
            if not item:
                return None, 'AliExpress API returned no product data'
            title = item.get('title', '')
            # Price — try price_info.price, then sale_price, then raw price
            price_info = item.get('price_info', {})
            price = ''
            if price_info.get('price'):
                price = price_info['price']
            elif price_info.get('sale_price'):
                sp = price_info['sale_price']
                price = sp.get('min_amount', sp) if isinstance(sp, dict) else sp
            elif item.get('sale_price'):
                price = item['sale_price']
            else:
                price = item.get('price', '')
            currency = price_info.get('currency', 'USD')
            # Images
            images = item.get('main_imgs', item.get('images', []))
            if isinstance(images, str):
                images = [images]
            image = images[0] if images else ''
            # Colors and sizes from sku_props (API uses 'prop_name' not 'name')
            COLOR_KW_AE = ('color', 'colour', 'colors', 'color classification', 'colour name',
                           'style', 'type', 'design', 'pattern', 'model')
            SIZE_KW_AE = ('size', 'sizes', 'us size', 'eu size', 'uk size', 'shoe size',
                          'length', 'ring size', 'diameter', 'specification')
            colors = []
            sizes = []
            for prop in item.get('sku_props', []):
                prop_name = (prop.get('prop_name', '') or prop.get('name', '') or '').lower().strip()
                values = prop.get('values', [])
                has_images = any(v.get('imageUrl') or v.get('image') or v.get('img') for v in values)
                if prop_name in SIZE_KW_AE:
                    for v in values:
                        name = v.get('name', '')
                        if name: sizes.append(name)
                elif prop_name in COLOR_KW_AE or has_images:
                    for v in values:
                        name = v.get('name', '')
                        img = v.get('imageUrl', '') or v.get('image', '') or v.get('img', '') or ''
                        if name: colors.append({'name': name, 'image': img})
            return {
                'title': title,
                'image': image,
                'images': images,
                'price': price,
                'currency': currency,
                'colors': colors,
                'sizes': sizes,
                'source': 'AliExpress'
            }
        except Exception as e:
            return None, f'AliExpress API error: {str(e)}'

    elif source == '1688':
        key = _get_tmapi_key()
        if not key:
            return None, 'TMAPI key not configured'
        try:
            resp = http_requests.get('http://api.tmapi.top/1688/item_detail',
                                params={'apiToken': key, 'item_id': product_id, 'language': 'en'},
                                timeout=30, verify=False)
            data = resp.json()
            if data.get('code') and data['code'] != 200:
                return None, f"1688 API error: {data.get('msg', 'Unknown error')} (code {data['code']})"
            item = data.get('data', {})
            if not item:
                return None, '1688 API returned no product data'
            title = item.get('title', '')
            # Price
            price_range = item.get('price_range', [])
            if price_range and isinstance(price_range, list):
                price = price_range[0].get('price', '')
            else:
                price = item.get('price', '')
            currency = 'CNY'
            # Images
            images = item.get('main_imgs', item.get('images', []))
            if isinstance(images, str):
                images = [images]
            # Normalize 1688 image URLs
            normalized = []
            for img in images:
                if img.startswith('//'):
                    img = 'https:' + img
                img = re.sub(r'\.\d+x\d+\.', '.', img)
                normalized.append(img)
            images = normalized
            image = images[0] if images else ''
            # Colors and sizes
            color_kw = ['color', 'colour', '颜色', '颜色分类', '色', 'style', '款式', '款', '图案', 'pattern']
            size_kw = ['size', '尺码', '尺寸', '码', '号', 'length', 'ring size', 'diameter']
            colors = []
            sizes = []
            for prop in item.get('sku_props', item.get('skuProps', [])):
                prop_name = str(prop.get('prop_name', prop.get('propName', prop.get('name', '')))).lower()
                values = prop.get('values', prop.get('propValues', []))
                has_images = any(v.get('image') or v.get('imageUrl') for v in values)
                if any(k in prop_name for k in size_kw) and not has_images:
                    for v in values:
                        name = v.get('name', v.get('valueName', ''))
                        if name: sizes.append(name)
                elif any(k in prop_name for k in color_kw) or has_images:
                    for v in values:
                        name = v.get('name', v.get('valueName', ''))
                        img = v.get('image', v.get('imageUrl', '')) or ''
                        if img and not img.startswith('http'):
                            img = 'https:' + img if img.startswith('//') else img
                        if img:
                            img = re.sub(r'\.\d+x\d+\.', '.', img)
                        if name: colors.append({'name': name, 'image': img})
            return {
                'title': title,
                'image': image,
                'images': images,
                'price': price,
                'currency': currency,
                'colors': colors,
                'sizes': sizes,
                'source': '1688'
            }
        except Exception as e:
            return None, f'1688 API error: {str(e)}'

    elif source == 'shopify_url':
        url = product_id  # canonical URL
        _browser_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }

        def _parse_shopify_json_product(product):
            """Extract structured data from a Shopify product JSON object."""
            title = product.get('title', '')
            images = [img.get('src', '') for img in product.get('images', [])] if isinstance(product.get('images'), list) and product['images'] and isinstance(product['images'][0], dict) else product.get('images', [])
            image = images[0] if images else ''
            variants = product.get('variants', [])
            price = variants[0].get('price', '') if variants else ''
            currency = 'USD'
            option_names = {}
            for idx, opt in enumerate(product.get('options', [])):
                option_names[f'option{idx+1}'] = (opt.get('name', '') or '').lower()
            color_kw_shop = ('color', 'colour', 'style', 'design')
            size_kw_shop = ('size', 'length', 'width')
            colors = []
            sizes = []
            seen_colors = set()
            seen_sizes = set()
            for v in variants:
                for opt_key in ['option1', 'option2', 'option3']:
                    val = v.get(opt_key, '')
                    if not val: continue
                    opt_name = option_names.get(opt_key, '')
                    if any(k in opt_name for k in color_kw_shop) and val.lower() not in seen_colors:
                        seen_colors.add(val.lower())
                        var_img = ''
                        if v.get('featured_image') and v['featured_image'].get('src'):
                            var_img = v['featured_image']['src']
                        colors.append({'name': val, 'image': var_img})
                    elif any(k in opt_name for k in size_kw_shop) and val.lower() not in seen_sizes:
                        seen_sizes.add(val.lower())
                        sizes.append(val)
            return {
                'title': title, 'image': image, 'images': images,
                'price': price, 'currency': currency,
                'colors': colors, 'sizes': sizes,
            }

        # Step 1: Try .json endpoint
        try:
            resp = http_requests.get(url + '.json', timeout=15, verify=False, headers=_browser_headers)
            if resp.status_code == 200:
                product = resp.json().get('product', {})
                if product.get('title'):
                    result = _parse_shopify_json_product(product)
                    result['source'] = 'Shopify'
                    return result
        except:
            pass
        # Step 2: Try .js endpoint (some stores block .json but allow .js)
        try:
            resp = http_requests.get(url + '.js', timeout=15, verify=False, headers=_browser_headers)
            if resp.status_code == 200:
                product = resp.json()
                if product.get('title'):
                    # .js returns product directly, images are objects with src
                    title = product.get('title', '')
                    raw_images = product.get('images', product.get('media', []))
                    images = []
                    for im in raw_images:
                        if isinstance(im, dict):
                            images.append(im.get('src', ''))
                        elif isinstance(im, str):
                            images.append(im)
                    image = images[0] if images else product.get('featured_image', '')
                    variants = product.get('variants', [])
                    price = ''
                    if variants:
                        price = variants[0].get('price', '')
                        # .js returns price in cents sometimes
                        if isinstance(price, (int, float)) and price > 1000:
                            price = price / 100
                    currency = 'USD'
                    colors = []
                    sizes = []
                    option_names = {}
                    for idx, opt_name in enumerate(product.get('options', [])):
                        if isinstance(opt_name, str):
                            option_names[f'option{idx+1}'] = opt_name.lower()
                        elif isinstance(opt_name, dict):
                            option_names[f'option{idx+1}'] = (opt_name.get('name', '') or '').lower()
                    color_kw_shop = ('color', 'colour', 'style', 'design')
                    size_kw_shop = ('size', 'length', 'width')
                    seen_colors = set()
                    seen_sizes = set()
                    for v in variants:
                        for opt_key in ['option1', 'option2', 'option3']:
                            val = v.get(opt_key, '')
                            if not val: continue
                            opt_name = option_names.get(opt_key, '')
                            if any(k in opt_name for k in color_kw_shop) and val.lower() not in seen_colors:
                                seen_colors.add(val.lower())
                                var_img = ''
                                if v.get('featured_image') and isinstance(v['featured_image'], dict) and v['featured_image'].get('src'):
                                    var_img = v['featured_image']['src']
                                elif v.get('featured_image') and isinstance(v['featured_image'], str):
                                    var_img = v['featured_image']
                                colors.append({'name': val, 'image': var_img})
                            elif any(k in opt_name for k in size_kw_shop) and val.lower() not in seen_sizes:
                                seen_sizes.add(val.lower())
                                sizes.append(val)
                    return {
                        'title': title, 'image': image, 'images': images,
                        'price': price, 'currency': currency,
                        'colors': colors, 'sizes': sizes,
                        'source': 'Shopify'
                    }
        except:
            pass
        # Step 3: Fetch HTML page for Shoplazza detection / fallback
        try:
            resp = http_requests.get(url, timeout=15, verify=False, headers=_browser_headers)
            if resp.status_code == 404:
                return None, 'Product not found (404) — it may have been removed from the store'
            html = resp.text[:30000]
            is_shoplazza = 'shoplazza' in html.lower() or 'staticdj.com' in html
            detected_source = 'Shoplazza' if is_shoplazza else 'Shopify'
            # Try to extract JSON product data from script tags
            title = ''
            image = ''
            images = []
            price = ''
            currency = 'USD'
            colors = []
            sizes = []
            # Look for product JSON in script tags
            for pattern in [r'"product"\s*:\s*(\{[^<]+?\})\s*[,}]', r'var\s+product\s*=\s*(\{.+?\});']:
                m = re.search(pattern, html, re.DOTALL)
                if m:
                    try:
                        pdata = json.loads(m.group(1))
                        title = pdata.get('title', '')
                        p_images = pdata.get('images', [])
                        if p_images:
                            images = [im.get('src', im) if isinstance(im, dict) else im for im in p_images]
                            image = images[0] if images else ''
                        p_variants = pdata.get('variants', [])
                        if p_variants:
                            price = p_variants[0].get('price', '')
                        break
                    except:
                        pass
            # Fallback: extract title from <title> tag
            if not title:
                m = re.search(r'<title>([^<]+)</title>', html)
                if m:
                    raw = m.group(1).strip()
                    # Reject error / 404 pages
                    raw_lower = raw.lower()
                    if any(err in raw_lower for err in ('404', 'not found', 'page not found', 'something went wrong', 'error', 'unavailable', 'access denied')):
                        return None, f'Store returned an error page ("{raw}") — the product may be unavailable or the store is blocking requests'
                    title = html_mod.unescape(raw)
            # Fallback: og:image
            if not image:
                m = re.search(r'property="og:image"\s+content="([^"]+)"', html)
                if m:
                    image = m.group(1)
                    images = [image]
            if not title:
                return None, 'Could not extract product data from page'
            return {
                'title': title, 'image': image, 'images': images,
                'price': price, 'currency': currency,
                'colors': colors, 'sizes': sizes,
                'source': detected_source
            }
        except Exception as e:
            return None, f'Shopify/Shoplazza fetch error: {str(e)}'

    return None, 'Unsupported product source'


@app.route('/api/collections', methods=['GET'])
@admin_required
def api_get_collections():
    data = _load_collections()
    return jsonify({'success': True, 'tree': data.get('tree', [])})

@app.route('/api/collections', methods=['POST'])
@admin_required
def api_save_collections():
    body = request.get_json(force=True)
    _save_collections(body)
    return jsonify({'success': True})

@app.route('/api/collections/node', methods=['POST'])
@admin_required
def api_add_collection_node():
    body = request.get_json(force=True)
    parent_id = body.get('parent_id')
    name = body.get('name', '').strip()
    if not name:
        return jsonify({'success': False, 'error': 'Name required'}), 400
    data = _load_collections()
    new_node = {'id': 'col_' + str(uuid.uuid4())[:12], 'name': name, 'children': [], 'products': []}
    if parent_id:
        parent = _find_collection_node(data['tree'], parent_id)
        if not parent:
            return jsonify({'success': False, 'error': 'Parent not found'}), 404
        parent.setdefault('children', []).append(new_node)
    else:
        data['tree'].append(new_node)
    _save_collections(data)
    return jsonify({'success': True, 'tree': data['tree'], 'node': new_node})

@app.route('/api/collections/node/<node_id>', methods=['PUT'])
@admin_required
def api_rename_collection_node(node_id):
    body = request.get_json(force=True)
    name = body.get('name', '').strip()
    if not name:
        return jsonify({'success': False, 'error': 'Name required'}), 400
    data = _load_collections()
    node = _find_collection_node(data['tree'], node_id)
    if not node:
        return jsonify({'success': False, 'error': 'Node not found'}), 404
    node['name'] = name
    _save_collections(data)
    return jsonify({'success': True, 'tree': data['tree']})

@app.route('/api/collections/node/<node_id>', methods=['DELETE'])
@admin_required
def api_delete_collection_node(node_id):
    data = _load_collections()
    if _delete_collection_node(data['tree'], node_id):
        _save_collections(data)
        return jsonify({'success': True, 'tree': data['tree']})
    return jsonify({'success': False, 'error': 'Node not found'}), 404

COLLECTION_IMAGES_DIR = os.path.join(DATA_DIR, 'collection_images')
os.makedirs(COLLECTION_IMAGES_DIR, exist_ok=True)

def _save_collection_image(image_url, product_id):
    """Download an image and save it locally. Returns local URL path or original URL on failure."""
    if not image_url:
        return ''
    try:
        resp = http_requests.get(image_url, timeout=15, verify=False,
                                 headers={'User-Agent': 'Mozilla/5.0'}, stream=True)
        if resp.status_code != 200:
            return image_url
        content_type = resp.headers.get('Content-Type', '')
        ext = '.jpg'
        if 'png' in content_type:
            ext = '.png'
        elif 'webp' in content_type:
            ext = '.webp'
        elif 'gif' in content_type:
            ext = '.gif'
        filename = f"{product_id}_{uuid.uuid4().hex[:8]}{ext}"
        filepath = os.path.join(COLLECTION_IMAGES_DIR, filename)
        with open(filepath, 'wb') as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        return f'/api/collections/images/{filename}'
    except Exception:
        return image_url

@app.route('/api/collections/images/<filename>')
def serve_collection_image(filename):
    """Serve saved collection product images."""
    safe = os.path.basename(filename)
    return send_from_directory(COLLECTION_IMAGES_DIR, safe)

@app.route('/api/collections/upload-image', methods=['POST'])
@admin_required
def api_upload_collection_image():
    """Upload an image file for a collection product (manual entry fallback)."""
    if 'image' not in request.files:
        return jsonify({'success': False, 'error': 'No image file provided'}), 400
    f = request.files['image']
    if not f.filename:
        return jsonify({'success': False, 'error': 'Empty filename'}), 400
    ext = os.path.splitext(f.filename)[1].lower() or '.jpg'
    if ext not in ('.jpg', '.jpeg', '.png', '.webp', '.gif'):
        ext = '.jpg'
    filename = f"manual_{uuid.uuid4().hex[:12]}{ext}"
    filepath = os.path.join(COLLECTION_IMAGES_DIR, filename)
    f.save(filepath)
    local_url = f'/api/collections/images/{filename}'
    return jsonify({'success': True, 'url': local_url})

@app.route('/api/collections/fetch-product', methods=['POST'])
@admin_required
def api_fetch_product():
    body = request.get_json(force=True)
    url = body.get('url', '').strip()
    if not url:
        return jsonify({'success': False, 'error': 'URL required'}), 400
    source, product_id = _parse_product_url(url)
    if not source:
        return jsonify({'success': False, 'error': 'Could not parse URL. Supported: AliExpress, 1688, Shopify, Shoplazza'}), 400
    result = _fetch_product_data(source, product_id)
    if isinstance(result, tuple):
        return jsonify({'success': False, 'error': result[1]})
    prod_id = 'prod_' + str(uuid.uuid4())[:12]
    result['url'] = url
    result['id'] = prod_id
    result['added'] = int(time.time())
    # Save images locally so they persist even if external URLs die
    if result.get('image'):
        result['image'] = _save_collection_image(result['image'], prod_id)
    if result.get('images'):
        result['images'] = [_save_collection_image(img, prod_id) for img in result['images']]
    if result.get('colors'):
        for c in result['colors']:
            if c.get('image'):
                c['image'] = _save_collection_image(c['image'], prod_id)
    return jsonify({'success': True, 'product': result})

@app.route('/api/collections/node/<node_id>/products', methods=['POST'])
@admin_required
def api_add_product_to_node(node_id):
    body = request.get_json(force=True)
    product = body.get('product')
    if not product:
        return jsonify({'success': False, 'error': 'Product data required'}), 400
    data = _load_collections()
    node = _find_collection_node(data['tree'], node_id)
    if not node:
        return jsonify({'success': False, 'error': 'Collection not found'}), 404
    node.setdefault('products', []).append(product)
    _save_collections(data)
    return jsonify({'success': True, 'tree': data['tree']})

@app.route('/api/collections/node/<node_id>/products/<product_id>', methods=['DELETE'])
@admin_required
def api_remove_product_from_node(node_id, product_id):
    data = _load_collections()
    node = _find_collection_node(data['tree'], node_id)
    if not node:
        return jsonify({'success': False, 'error': 'Collection not found'}), 404
    products = node.get('products', [])
    node['products'] = [p for p in products if p.get('id') != product_id]
    _save_collections(data)
    return jsonify({'success': True, 'tree': data['tree']})

@app.route('/api/collections/node/<node_id>/move', methods=['POST'])
@admin_required
def api_move_collection_node(node_id):
    body = request.get_json(force=True)
    new_parent_id = body.get('new_parent_id')
    data = _load_collections()
    # Find and remove the node
    node = _find_collection_node(data['tree'], node_id)
    if not node:
        return jsonify({'success': False, 'error': 'Node not found'}), 404
    node_copy = dict(node)
    _delete_collection_node(data['tree'], node_id)
    # Insert into new parent
    if new_parent_id:
        parent = _find_collection_node(data['tree'], new_parent_id)
        if not parent:
            return jsonify({'success': False, 'error': 'Target parent not found'}), 404
        parent.setdefault('children', []).append(node_copy)
    else:
        data['tree'].append(node_copy)
    _save_collections(data)
    return jsonify({'success': True, 'tree': data['tree']})


# ============================================================
# ===== Jewelry Winners: multi-store winners -> collection ===
# ============================================================
# State for the in-progress scan so the frontend can poll progress.
_jewelry_scan_state = {
    'running': False,
    'started_at': None,
    'current_store': None,
    'stores_total': 0,
    'stores_done': 0,
    'products_added': 0,
    'images_saved': 0,
    'errors': [],
    'collection_id': None,
    'collection_name': None,
    'finished_at': None,
}
_jewelry_scan_lock = threading.Lock()

def _download_product_image(image_url, store_id, product_id):
    """Download a product image to PRODUCT_IMAGES_DIR. Returns the public URL
    path (e.g. '/product-images/<store>/<filename>') on success, or None.
    Idempotent: skips download if the file already exists.
    """
    if not image_url:
        return None
    try:
        # Filename derivation
        ext = '.jpg'
        if '.' in image_url.split('/')[-1].split('?')[0]:
            candidate = '.' + image_url.split('/')[-1].split('?')[0].split('.')[-1].lower()
            if candidate in ('.jpg', '.jpeg', '.png', '.webp', '.gif'):
                ext = candidate
        # Sanitize store_id / product_id for filesystem
        safe_store = ''.join(c for c in str(store_id) if c.isalnum() or c in '-_')[:60] or 'store'
        safe_pid = ''.join(c for c in str(product_id) if c.isalnum() or c in '-_')[:60] or 'product'
        store_dir = os.path.join(PRODUCT_IMAGES_DIR, safe_store)
        os.makedirs(store_dir, exist_ok=True)
        filename = safe_pid + ext
        local_path = os.path.join(store_dir, filename)
        public_url = f'/product-images/{safe_store}/{filename}'
        if os.path.exists(local_path) and os.path.getsize(local_path) > 1024:
            return public_url
        # Fetch
        resp = http_requests.get(image_url, timeout=20, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        })
        if resp.status_code != 200:
            return None
        data = resp.content
        if not data or len(data) < 1024:
            return None
        tmp = local_path + '.tmp'
        with open(tmp, 'wb') as f:
            f.write(data)
        os.replace(tmp, local_path)
        return public_url
    except Exception as e:
        log.warning('Image download failed for %s: %s', image_url[:80], e)
        return None


@app.route('/product-images/<path:subpath>')
def serve_product_image(subpath):
    """Serve cached product images. No auth: these are public CDN-style assets."""
    return send_from_directory(PRODUCT_IMAGES_DIR, subpath)


def _jewelry_get_or_create_collection(name):
    """Find a top-level collection by name (case-insensitive). Create it if missing.
    Returns the node dict."""
    data = _load_collections()
    name_norm = (name or '').strip()
    if not name_norm:
        raise ValueError('Collection name required')
    for node in data['tree']:
        if (node.get('name') or '').strip().lower() == name_norm.lower():
            return node
    # Create new top-level node
    new_node = {
        'id': 'col_' + uuid.uuid4().hex[:12],
        'name': name_norm,
        'type': 'collection',
        'children': [],
        'products': [],
        'createdAt': int(time.time()),
    }
    data['tree'].append(new_node)
    _save_collections(data)
    return new_node


def _jewelry_load_store_winners(store_id, min_sales):
    """Read cached winners for a store from the same cache the Winners tab uses
    (data/winner_cache/<store_id>.json). The cache is a dict keyed by product_id
    with quantity/revenue. Returns a list of normalized product dicts that
    hit the min_sales threshold. Image and handle are filled in by enrichment."""
    cache = _load_winner_cache(store_id)
    if not cache:
        return []
    out = []
    for pid, p in cache.items():
        qty = int(p.get('quantity') or 0)
        if qty < min_sales:
            continue
        out.append({
            'id': str(p.get('product_id') or pid),
            'name': p.get('title') or '',
            'handle': '',  # filled by _jewelry_enrich_products
            'sales': qty,
            'revenue': float(p.get('revenue') or 0),
            'image': '',   # filled by _jewelry_enrich_products
        })
    return out


def _jewelry_enrich_products(store, products):
    """Batch-fetch live product details (handle + main image) for a list of
    normalized product dicts. Mutates each dict in-place. Best-effort; products
    that no longer exist on the platform keep empty handle/image."""
    if not products:
        return
    platform, domain, token = _get_store_credentials(store)
    if not domain or not token:
        return
    pid_to_prod = {p['id']: p for p in products}
    all_pids = list(pid_to_prod.keys())
    if platform == 'shopify':
        headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
        base_url = f'https://{domain}/admin/api/2024-01'
        for i in range(0, len(all_pids), 250):
            batch_ids = ','.join(str(pid) for pid in all_pids[i:i+250])
            try:
                pr = http_requests.get(
                    f'{base_url}/products.json?ids={batch_ids}&limit=250&fields=id,handle,images',
                    headers=headers, timeout=30
                )
                if pr.status_code == 200:
                    for prod in pr.json().get('products', []):
                        pid = str(prod.get('id'))
                        target = pid_to_prod.get(pid)
                        if not target:
                            continue
                        target['handle'] = prod.get('handle') or ''
                        imgs = prod.get('images') or []
                        if imgs:
                            src = imgs[0].get('src') or ''
                            if src.startswith('//'):
                                src = 'https:' + src
                            target['image'] = src
                time.sleep(0.3)
            except Exception as e:
                log.warning('Jewelry enrich batch failed: %s', e)
    elif platform == 'shoplazza':
        headers = _shoplazza_headers(token)
        base_url = f'https://{domain}/openapi/{SHOPLAZZA_API_VERSION}'
        for i in range(0, len(all_pids), 100):
            batch_ids = ','.join(str(pid) for pid in all_pids[i:i+100])
            try:
                pr = _shoplazza_get_with_retry(
                    f'{base_url}/products', headers,
                    params={'ids': batch_ids, 'page_size': 100}, timeout=30,
                )
                if pr is not None and pr.status_code == 200:
                    body = pr.json() or {}
                    data_block = body.get('data') if isinstance(body.get('data'), dict) else body
                    prods = data_block.get('products') or body.get('products') or []
                    for prod in prods:
                        pid = str(prod.get('id'))
                        target = pid_to_prod.get(pid)
                        if not target:
                            continue
                        target['handle'] = prod.get('handle') or prod.get('seo_handle') or ''
                        imgs = prod.get('images') or []
                        if imgs:
                            src = (imgs[0] or {}).get('src') or (imgs[0] or {}).get('url') or ''
                            if src.startswith('//'):
                                src = 'https:' + src
                            target['image'] = src
                time.sleep(0.5)
            except Exception as e:
                log.warning('Jewelry enrich (shoplazza) batch failed: %s', e)


def _jewelry_scan_worker(store_ids, min_sales, collection_name, store_names_by_id, download_images):
    """Background worker that walks every selected store, filters by min_sales,
    downloads images, and appends to the target collection."""
    state = _jewelry_scan_state
    try:
        with _jewelry_scan_lock:
            state['running'] = True
            state['started_at'] = int(time.time())
            state['stores_total'] = len(store_ids)
            state['stores_done'] = 0
            state['products_added'] = 0
            state['images_saved'] = 0
            state['errors'] = []
            state['current_store'] = None
            state['finished_at'] = None
        # Get or create the collection
        node = _jewelry_get_or_create_collection(collection_name)
        state['collection_id'] = node['id']
        state['collection_name'] = node['name']

        # Deduplicate against existing products in the node
        data = _load_collections()
        live_node = _find_collection_node(data['tree'], node['id'])
        existing = live_node.setdefault('products', [])
        existing_ids = set(p.get('id') for p in existing if p.get('id'))

        # Cache store records for credential lookup
        all_stores = _load_stores()
        store_by_id = {s.get('id'): s for s in all_stores}
        for sid in store_ids:
            state['current_store'] = store_names_by_id.get(sid, sid)
            try:
                winners = _jewelry_load_store_winners(sid, min_sales)
                log.info('Jewelry scan: %s -> %d products at >= %d sales',
                         state['current_store'], len(winners), min_sales)
                # Enrich with handle + image from live platform API
                store_rec_local = store_by_id.get(sid)
                if store_rec_local and winners:
                    try:
                        _jewelry_enrich_products(store_rec_local, winners)
                    except Exception as e:
                        log.warning('Enrichment failed for %s: %s', sid, e)
                for p in winners:
                    pid_raw = p.get('id') or p.get('handle')
                    if not pid_raw:
                        continue
                    # Namespace product IDs by store so the same handle across
                    # stores doesn't collide.
                    pid = f'{sid}:{pid_raw}'
                    if pid in existing_ids:
                        continue
                    local_image_url = None
                    remote_image = p.get('image') or ''
                    if download_images and remote_image:
                        local_image_url = _download_product_image(remote_image, sid, pid_raw)
                        if local_image_url:
                            state['images_saved'] += 1
                    # Build the product card record
                    handle = p.get('handle') or ''
                    store_rec = store_by_id.get(sid) or {}
                    store_domain = store_rec.get('domain') or ''
                    product_url = ''
                    if store_domain and handle:
                        product_url = f'https://{store_domain}/products/{handle}'
                    card = {
                        'id': pid,
                        'storeId': sid,
                        'storeName': state['current_store'],
                        'name': p.get('name') or p.get('title') or handle,
                        'handle': handle,
                        'sales': int(p.get('sales') or 0),
                        'revenue': float(p.get('revenue') or 0),
                        'remoteImage': remote_image,
                        'image': local_image_url or remote_image,
                        'localImage': local_image_url,
                        'productUrl': product_url,
                        'source': '1688' if (p.get('source') == '1688') else 'shopify',
                        'addedAt': int(time.time()),
                    }
                    existing.append(card)
                    existing_ids.add(pid)
                    state['products_added'] += 1
                # Persist after every store so progress isn't lost
                _save_collections(data)
            except Exception as e:
                log.exception('Jewelry scan store failed: %s', sid)
                state['errors'].append({
                    'store': state['current_store'], 'error': str(e)[:200]
                })
            finally:
                state['stores_done'] += 1
        state['current_store'] = None
    finally:
        with _jewelry_scan_lock:
            state['running'] = False
            state['finished_at'] = int(time.time())


@app.route('/api/jewelry-winners/scan', methods=['POST'])
@login_required
def jewelry_winners_scan():
    """Kick off a multi-store winners scan.
    Body: {
      stores: ['store_id', ...],
      minSales: 3,
      collectionName: 'Jewelry winners (3+ sales)',
      downloadImages: true
    }
    """
    body = request.get_json(force=True) or {}
    store_ids = body.get('stores') or []
    if not isinstance(store_ids, list) or not store_ids:
        return jsonify({'success': False, 'error': 'stores required (non-empty list)'}), 400
    try:
        min_sales = int(body.get('minSales') or 3)
    except Exception:
        return jsonify({'success': False, 'error': 'minSales must be integer'}), 400
    collection_name = (body.get('collectionName') or '').strip()
    if not collection_name:
        return jsonify({'success': False, 'error': 'collectionName required'}), 400
    download_images = bool(body.get('downloadImages', True))
    if _jewelry_scan_state['running']:
        return jsonify({'success': True, 'status': 'already_running', 'state': _jewelry_scan_state})
    # Build a name map for status display
    try:
        stores = _load_stores()
        name_map = {s['id']: s.get('name', s['id']) for s in stores}
    except Exception:
        name_map = {}
    t = threading.Thread(
        target=_jewelry_scan_worker,
        args=(store_ids, min_sales, collection_name, name_map, download_images),
        daemon=True, name='jewelry-scan')
    t.start()
    return jsonify({'success': True, 'status': 'started',
                    'stores': len(store_ids), 'minSales': min_sales,
                    'collectionName': collection_name})


@app.route('/api/jewelry-winners/status', methods=['GET'])
@login_required
def jewelry_winners_status():
    """Poll-able status endpoint for the running (or last) scan.
    Returns a camelCase payload the frontend expects, plus a tail of the
    most recent products added to the destination collection."""
    s = _jewelry_scan_state
    recent_products = []
    last_error = None
    if s.get('errors'):
        try:
            last = s['errors'][-1]
            last_error = f"{last.get('store','')}: {last.get('error','')}".strip(': ')
        except Exception:
            last_error = str(s['errors'][-1])[:200]
    col_id = s.get('collection_id')
    if col_id:
        try:
            data = _load_collections()
            node = _find_collection_node(data['tree'], col_id)
            if node:
                prods = node.get('products') or []
                # most recent 60, newest-last so frontend reverse() shows newest first
                recent_products = prods[-60:]
        except Exception:
            pass
    payload = {
        'running': bool(s.get('running')),
        'startedAt': s.get('started_at'),
        'currentStoreName': s.get('current_store'),
        'totalStores': s.get('stores_total') or 0,
        'currentStoreIndex': s.get('stores_done') or 0,
        'productsAdded': s.get('products_added') or 0,
        'imagesSaved': s.get('images_saved') or 0,
        'collectionId': s.get('collection_id'),
        'collectionName': s.get('collection_name'),
        'lastFinishedAt': s.get('finished_at'),
        'lastError': last_error,
        'errors': s.get('errors') or [],
        'recentProducts': recent_products,
    }
    return jsonify({'success': True, 'state': payload})


@app.route('/api/jewelry-winners/store-summary', methods=['GET'])
@login_required
def jewelry_winners_store_summary():
    """For each connected store, return how many products would be picked up
    at a given minSales threshold. Lets the UI preview the result before scanning."""
    try:
        min_sales = int(request.args.get('minSales', '3'))
    except Exception:
        min_sales = 3
    stores = _load_stores()
    out = []
    for s in stores:
        sid = s.get('id')
        if not sid: continue
        cache_path = _winner_cache_path(sid)
        has_cache = os.path.exists(cache_path)
        qualified = 0
        total = 0
        synced_at = None
        if has_cache:
            try:
                cache = _load_winner_cache(sid) or {}
                total = len(cache)
                qualified = sum(1 for p in cache.values() if int(p.get('quantity') or 0) >= min_sales)
                meta = _load_winner_meta(sid) or {}
                synced_at = meta.get('synced_at') or meta.get('updated_at') or meta.get('finished_at')
            except Exception:
                pass
        out.append({
            'id': sid,
            'storeId': sid,
            'name': s.get('name', sid),
            'storeName': s.get('name', sid),
            'platform': s.get('platform', 'shopify'),
            'category': s.get('storeCategory', ''),
            'totalProducts': total,
            'qualifiedProducts': qualified,
            'qualifiedCount': qualified,
            'hasCache': has_cache,
            'syncedAt': synced_at,
            'lastSyncedAt': synced_at,
        })
    # Sort: jewelry first, then by qualified count desc
    out.sort(key=lambda x: (
        0 if (x.get('category') or '').lower() == 'jewelry' else 1,
        -(x.get('qualifiedCount') or 0)
    ))
    return jsonify({'success': True, 'stores': out, 'minSales': min_sales})


# ===== Creative Studio =====
STUDIO_DIR = os.path.join(DATA_DIR, 'studio')
os.makedirs(STUDIO_DIR, exist_ok=True)
STUDIO_JOBS_FILE = os.path.join(DATA_DIR, 'studio_jobs.json')

def _load_studio_jobs():
    """Load persisted studio jobs from disk."""
    if os.path.exists(STUDIO_JOBS_FILE):
        try:
            with open(STUDIO_JOBS_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _save_studio_jobs(jobs):
    """Persist studio jobs to disk (atomic write)."""
    tmp = STUDIO_JOBS_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(jobs, f, indent=2)
    os.replace(tmp, STUDIO_JOBS_FILE)

_studio_jobs = _load_studio_jobs()

@app.route('/api/studio/jobs', methods=['GET'])
@admin_required
def studio_list_jobs():
    """Return all studio jobs (newest first) for the frontend to display on load."""
    jobs = []
    for job_id, job in _studio_jobs.items():
        # Check if the video file still exists on disk for done jobs
        if job.get('status') == 'done' and job.get('video_url'):
            fname = job['video_url'].split('/')[-1]
            if not os.path.exists(os.path.join(STUDIO_DIR, fname)):
                job['status'] = 'expired'
                job['video_url'] = None
        jobs.append({
            'job_id': job_id,
            'status': job.get('status', 'unknown'),
            'prompt': job.get('prompt', ''),
            'aspect_ratio': job.get('aspect_ratio', ''),
            'duration': job.get('duration', 5),
            'resolution': job.get('resolution', '720p'),
            'video_url': job.get('video_url'),
            'error': job.get('error'),
            'created': job.get('created', 0)
        })
    jobs.sort(key=lambda j: j['created'], reverse=True)
    return jsonify({'success': True, 'jobs': jobs})

@app.route('/api/studio/jobs/<job_id>', methods=['DELETE'])
@admin_required
def studio_delete_job(job_id):
    """Delete a studio job and its video file."""
    job = _studio_jobs.pop(job_id, None)
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404
    # Delete video file if it exists
    if job.get('video_url'):
        fname = job['video_url'].split('/')[-1]
        fpath = os.path.join(STUDIO_DIR, fname)
        if os.path.exists(fpath):
            try: os.remove(fpath)
            except Exception: pass
    _save_studio_jobs(_studio_jobs)
    return jsonify({'success': True})

@app.route('/api/studio/generate', methods=['POST'])
@admin_required
def studio_generate():
    """Generate a video from an uploaded image + prompt via Grok API."""
    settings = _load_settings()
    api_key = settings.get('xai_api_key', '')
    if not api_key:
        return jsonify({'success': False, 'error': 'No xAI API key configured'}), 400

    image_file = request.files.get('image')
    prompt = request.form.get('prompt', '').strip()
    aspect_ratio = request.form.get('aspect_ratio', '9:16')
    duration = int(request.form.get('duration', 5))
    resolution = request.form.get('resolution', '720p')
    if resolution not in ('480p', '720p'):
        resolution = '720p'

    if not image_file or not prompt:
        return jsonify({'success': False, 'error': 'Image and prompt are required'}), 400

    # Read image and encode as base64 data URL
    import base64
    img_data = image_file.read()
    ext = image_file.filename.rsplit('.', 1)[-1].lower() if '.' in image_file.filename else 'jpeg'
    mime = f'image/{"jpeg" if ext in ("jpg", "jpeg") else ext}'
    b64 = base64.b64encode(img_data).decode('utf-8')
    data_url = f'data:{mime};base64,{b64}'

    # Build API payload — omit aspect_ratio when 'auto' so API uses image's native ratio
    api_payload = {
        'model': settings.get('xai_video_model', 'grok-imagine-video'),
        'prompt': prompt,
        'image': {'url': data_url},
        'duration': duration,
        'resolution': resolution
    }
    if aspect_ratio and aspect_ratio != 'auto':
        api_payload['aspect_ratio'] = aspect_ratio

    # Submit to Grok API
    try:
        resp = http_requests.post(
            'https://api.x.ai/v1/videos/generations',
            headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
            json=api_payload,
            timeout=60
        )
        if resp.status_code != 200:
            return jsonify({'success': False, 'error': f'xAI API error {resp.status_code}: {resp.text[:300]}'}), 500

        data = resp.json()
        request_id = data.get('request_id')
        if not request_id:
            return jsonify({'success': False, 'error': 'No request_id returned'}), 500

        job_id = str(uuid.uuid4())
        _studio_jobs[job_id] = {
            'request_id': request_id,
            'status': 'generating',
            'prompt': prompt,
            'aspect_ratio': aspect_ratio,
            'duration': duration,
            'resolution': resolution,
            'video_url': None,
            'error': None,
            'created': time.time()
        }
        _save_studio_jobs(_studio_jobs)
        return jsonify({'success': True, 'job_id': job_id})

    except Exception as e:
        log.error(f'Studio generate error: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/studio/status/<job_id>', methods=['GET'])
@admin_required
def studio_status(job_id):
    """Poll Grok API for studio job status."""
    job = _studio_jobs.get(job_id)
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404

    if job['status'] in ('done', 'failed', 'expired'):
        return jsonify({'success': True, 'status': job['status'], 'video_url': job.get('video_url'), 'error': job.get('error')})

    settings = _load_settings()
    api_key = settings.get('xai_api_key', '')
    if not api_key:
        job['status'] = 'failed'
        job['error'] = 'No xAI API key'
        _save_studio_jobs(_studio_jobs)
        return jsonify({'success': True, 'status': 'failed', 'error': job['error']})

    try:
        resp = http_requests.get(
            f'https://api.x.ai/v1/videos/{job["request_id"]}',
            headers={'Authorization': f'Bearer {api_key}'},
            timeout=30
        )
        if resp.status_code != 200:
            return jsonify({'success': True, 'status': 'generating', 'video_url': None, 'error': None})

        data = resp.json()
        status = data.get('status', '')

        if status == 'done':
            video_url = data.get('video', {}).get('url', '')
            if video_url:
                # Download video locally
                fname = f'{job_id}.mp4'
                local_path = os.path.join(STUDIO_DIR, fname)
                vr = http_requests.get(video_url, timeout=60)
                with open(local_path, 'wb') as f:
                    f.write(vr.content)
                job['status'] = 'done'
                job['video_url'] = f'/data/studio/{fname}'
            else:
                job['status'] = 'failed'
                job['error'] = 'No video URL returned'
            _save_studio_jobs(_studio_jobs)
        elif status in ('failed', 'error'):
            job['status'] = 'failed'
            job['error'] = data.get('error', 'Generation failed')
            _save_studio_jobs(_studio_jobs)

        return jsonify({'success': True, 'status': job['status'], 'video_url': job.get('video_url'), 'error': job.get('error')})

    except Exception as e:
        log.error(f'Studio poll error: {e}')
        return jsonify({'success': True, 'status': 'generating', 'video_url': None, 'error': None})

@app.route('/data/studio/<path:filename>')
@login_required
def serve_studio_file(filename):
    return send_from_directory(STUDIO_DIR, filename)

# ===== Video cleanup =====
def _cleanup_old_videos(max_age_days=5):
    """Delete local video files older than max_age_days."""
    for subdir in ('videos', 'studio'):
        _cleanup_dir(os.path.join(DATA_DIR, subdir), max_age_days)

def _cleanup_dir(videos_dir, max_age_days):
    if not os.path.isdir(videos_dir):
        return
    cutoff = time.time() - (max_age_days * 86400)
    deleted = 0
    for root, dirs, files in os.walk(videos_dir, topdown=False):
        for f in files:
            fp = os.path.join(root, f)
            try:
                if os.path.getmtime(fp) < cutoff:
                    os.remove(fp)
                    deleted += 1
            except Exception:
                pass
        # Remove empty directories
        try:
            if not os.listdir(root) and root != videos_dir:
                os.rmdir(root)
        except Exception:
            pass
    if deleted:
        log.info(f'Cleaned up {deleted} video files older than {max_age_days} days')

# =============================================================================
# ===== ETSY JEWELRY SCANNER (tab) ============================================
# =============================================================================
# Self-contained module: SQLite store, sync Etsy client (requests), scanner with
# self-expanding keyword + shop discovery, background thread, HTTP routes.
# All paths/routes prefixed with /api/etsy. DB lives at data/etsy.db.
# =============================================================================
import sqlite3
import csv
import io
from collections import Counter

ETSY_DB_PATH = os.path.join(DATA_DIR, 'etsy.db')
ETSY_SETTINGS_PATH = os.path.join(DATA_DIR, 'etsy_settings.json')
ETSY_API_BASE = 'https://openapi.etsy.com/v3/application'
ETSY_TOKEN_URL = 'https://api.etsy.com/v3/public/oauth/token'
ETSY_OAUTH_AUTHORIZE_URL = 'https://www.etsy.com/oauth/connect'
ETSY_OAUTH_TOKENS_PATH = os.path.join(DATA_DIR, 'etsy_oauth_tokens.json')
ETSY_OAUTH_PENDING_PATH = os.path.join(DATA_DIR, 'etsy_oauth_pending.json')
# Minimal scope — we only need an authenticated identity; listing/shop search
# endpoints are public-readable as long as a valid bearer token is present.
ETSY_OAUTH_SCOPE = 'email_r'
_etsy_db_lock = threading.Lock()
_etsy_scanner_thread = None
_etsy_scanner_state = {
    'running': False,         # True while a sweep is mid-flight
    'last_sweep_at': 0,
    'next_sweep_at': 0,
    'started_at': 0,           # when the bg thread started
    'stop_requested': False,   # set by /api/etsy/stop-scan to abort current sweep
    'paused': False,           # if True, scanner_loop skips sweeps entirely
}

ETSY_DEFAULT_SETTINGS = {
    'apiKey': '',
    'sharedSecret': '',           # Etsy v3 may require this alongside x-api-key
    'minShopSales': 0,            # No shop-sales cutoff: capture vendor diversity
    'maxShopAgeMonths': 240,      # Effectively off; UI filters by review threshold
    'minListingReviews': 500,     # Single primary criterion: 500+ reviews = winner
    'minReviewVelocity': 0.0,     # Off; reviews count is the strict gate
    'minListingFavorites': 0,     # Off; reviews count is the strict gate
    'scanIntervalSeconds': 10800,   # 3h
    'dailyRequestBudget': 4500,   # Etsy limit is 5000 QPD, leave 10% headroom
    'requestsPerSecond': 4.5,     # Etsy limit is 5 QPS, leave headroom
    'jewelryTaxonomyId': 1,         # Etsy taxonomy: Jewelry root
    'seedKeywords': [
        'personalized necklace', 'name necklace', 'birthstone ring', 'stacking ring',
        'minimalist necklace', 'initial necklace', 'gold hoop earrings', 'pearl earrings',
        'tennis bracelet', 'charm bracelet', 'engagement ring', 'wedding band',
        'couples ring', 'promise ring', 'signet ring', 'huggie earrings',
        'drop earrings', 'layered necklace', 'anklet', 'zodiac necklace',
        'moon necklace', 'evil eye necklace', 'cross necklace', 'pendant necklace',
        'cuff bracelet',
    ],
    'maxKeywordsPerSweep': 60,
    'maxPagesPerKeyword': 2,
}


def _etsy_load_settings():
    try:
        if os.path.exists(ETSY_SETTINGS_PATH):
            with open(ETSY_SETTINGS_PATH, 'r') as f:
                data = json.load(f)
            merged = dict(ETSY_DEFAULT_SETTINGS)
            merged.update({k: v for k, v in data.items() if v is not None})
            return merged
    except Exception:
        log.exception('Failed to load etsy_settings.json; using defaults')
    return dict(ETSY_DEFAULT_SETTINGS)


def _etsy_save_settings(settings):
    # Coerce types so frontend strings don't break the loop later.
    typed = dict(ETSY_DEFAULT_SETTINGS)
    typed.update(settings or {})
    int_fields = ['minShopSales', 'maxShopAgeMonths', 'minListingReviews',
                  'minListingFavorites', 'scanIntervalSeconds', 'dailyRequestBudget',
                  'jewelryTaxonomyId', 'maxKeywordsPerSweep', 'maxPagesPerKeyword']
    float_fields = ['minReviewVelocity', 'requestsPerSecond']
    for k in int_fields:
        try: typed[k] = int(typed.get(k) or 0)
        except: typed[k] = ETSY_DEFAULT_SETTINGS[k]
    for k in float_fields:
        try: typed[k] = float(typed.get(k) or 0.0)
        except: typed[k] = ETSY_DEFAULT_SETTINGS[k]
    typed['apiKey'] = (typed.get('apiKey') or '').strip()
    typed['sharedSecret'] = (typed.get('sharedSecret') or '').strip()
    if isinstance(typed.get('seedKeywords'), str):
        typed['seedKeywords'] = [s.strip() for s in typed['seedKeywords'].splitlines() if s.strip()]
    typed['seedKeywords'] = [str(s).strip() for s in (typed.get('seedKeywords') or []) if str(s).strip()]
    tmp = ETSY_SETTINGS_PATH + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(typed, f, indent=2)
    os.replace(tmp, ETSY_SETTINGS_PATH)
    return typed


# ---------- Etsy DB layer ----------
_ETSY_SCHEMA = """
CREATE TABLE IF NOT EXISTS etsy_shops (
    shop_id INTEGER PRIMARY KEY,
    shop_name TEXT,
    url TEXT,
    icon_url TEXT,
    country TEXT,
    transaction_sold_count INTEGER,
    review_count INTEGER,
    review_average REAL,
    listing_active_count INTEGER,
    opened_ts INTEGER,
    age_months REAL,
    sales_per_month REAL,
    first_seen INTEGER DEFAULT (strftime('%s','now')),
    last_updated INTEGER DEFAULT (strftime('%s','now')),
    is_winner INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS etsy_listings (
    listing_id INTEGER PRIMARY KEY,
    shop_id INTEGER,
    title TEXT,
    description TEXT,
    url TEXT,
    price REAL,
    currency TEXT,
    num_favorers INTEGER,
    views INTEGER,
    quantity INTEGER,
    tags TEXT,
    materials TEXT,
    image_url TEXT,
    image_urls TEXT,
    taxonomy_id INTEGER,
    listing_reviews INTEGER DEFAULT 0,
    review_velocity REAL DEFAULT 0,
    score REAL DEFAULT 0,
    is_winner INTEGER DEFAULT 0,
    keyword TEXT,
    first_seen INTEGER DEFAULT (strftime('%s','now')),
    last_updated INTEGER DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_etsy_listings_score ON etsy_listings(score DESC);
CREATE INDEX IF NOT EXISTS idx_etsy_listings_winner ON etsy_listings(is_winner);
CREATE INDEX IF NOT EXISTS idx_etsy_listings_shop ON etsy_listings(shop_id);

CREATE TABLE IF NOT EXISTS etsy_scan_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at INTEGER,
    finished_at INTEGER,
    keyword TEXT,
    source TEXT,             -- 'seed' | 'discovered' | 'shop' | 'manual'
    listings_seen INTEGER DEFAULT 0,
    shops_seen INTEGER DEFAULT 0,
    winners_found INTEGER DEFAULT 0,
    api_calls INTEGER DEFAULT 0,
    status TEXT,
    error TEXT
);
CREATE TABLE IF NOT EXISTS etsy_api_usage (
    day TEXT PRIMARY KEY,
    calls INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS etsy_discovered_keywords (
    keyword TEXT PRIMARY KEY,
    source TEXT,             -- 'tag' | 'title-bigram' | 'seed' | 'manual'
    occurrences INTEGER DEFAULT 1,
    winners_yielded INTEGER DEFAULT 0,
    last_scanned INTEGER,
    first_seen INTEGER DEFAULT (strftime('%s','now')),
    enabled INTEGER DEFAULT 1
);
"""

def _etsy_conn():
    conn = sqlite3.connect(ETSY_DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA busy_timeout=10000')
    return conn

def _etsy_init_db():
    with _etsy_db_lock:
        conn = _etsy_conn()
        try:
            conn.executescript(_ETSY_SCHEMA)
            # Performance indexes (safe to run repeatedly)
            conn.executescript("""
                CREATE INDEX IF NOT EXISTS idx_listings_winner ON etsy_listings(is_winner, score DESC);
                CREATE INDEX IF NOT EXISTS idx_listings_winner_reviews ON etsy_listings(is_winner, listing_reviews DESC);
                CREATE INDEX IF NOT EXISTS idx_listings_shop ON etsy_listings(shop_id);
                CREATE INDEX IF NOT EXISTS idx_listings_winner_shop ON etsy_listings(shop_id, is_winner);
                CREATE INDEX IF NOT EXISTS idx_shops_winner ON etsy_shops(is_winner);
                CREATE INDEX IF NOT EXISTS idx_kw_source ON etsy_discovered_keywords(source, last_scanned);
                CREATE INDEX IF NOT EXISTS idx_kw_enabled ON etsy_discovered_keywords(enabled, source);
            """)
            conn.commit()
        finally:
            conn.close()

class _EtsyCursor:
    # WAL mode lets readers proceed while a writer is active. We no longer
    # hold a Python-level lock for the lifetime of the connection (which was
    # causing /api/etsy/stats and /api/etsy/winners to hang for minutes
    # while the scanner thread was busy doing HTTP work).
    def __enter__(self):
        self.conn = _etsy_conn()
        return self.conn
    def __exit__(self, *a):
        try:
            self.conn.commit()
        except Exception:
            pass
        try:
            self.conn.close()
        except Exception:
            pass

def _etsy_today_usage():
    with _EtsyCursor() as c:
        r = c.execute("SELECT calls FROM etsy_api_usage WHERE day=date('now')").fetchone()
        return r['calls'] if r else 0

def _etsy_bump_usage(n=1):
    with _EtsyCursor() as c:
        c.execute("""INSERT INTO etsy_api_usage (day, calls) VALUES (date('now'), :n)
                     ON CONFLICT(day) DO UPDATE SET calls = calls + :n""", {'n': n})

# --- Priority (user-initiated) budget: separate counter so background
# scanners cannot starve manual actions like 'Import this Etsy shop'.
def _etsy_today_priority_usage():
    with _EtsyCursor() as c:
        try:
            c.execute("CREATE TABLE IF NOT EXISTS etsy_api_priority_usage (day TEXT PRIMARY KEY, calls INTEGER NOT NULL DEFAULT 0)")
        except Exception:
            pass
        r = c.execute("SELECT calls FROM etsy_api_priority_usage WHERE day=date('now')").fetchone()
        return r['calls'] if r else 0

def _etsy_bump_priority_usage(n=1):
    with _EtsyCursor() as c:
        try:
            c.execute("CREATE TABLE IF NOT EXISTS etsy_api_priority_usage (day TEXT PRIMARY KEY, calls INTEGER NOT NULL DEFAULT 0)")
        except Exception:
            pass
        c.execute("""INSERT INTO etsy_api_priority_usage (day, calls) VALUES (date('now'), :n)
                     ON CONFLICT(day) DO UPDATE SET calls = calls + :n""", {'n': n})

def _etsy_upsert_shop(s):
    with _EtsyCursor() as c:
        c.execute("""
            INSERT INTO etsy_shops (shop_id, shop_name, url, icon_url, country,
                transaction_sold_count, review_count, review_average, listing_active_count,
                opened_ts, age_months, sales_per_month, is_winner, last_updated)
            VALUES (:shop_id,:shop_name,:url,:icon_url,:country,
                :transaction_sold_count,:review_count,:review_average,:listing_active_count,
                :opened_ts,:age_months,:sales_per_month,:is_winner,strftime('%s','now'))
            ON CONFLICT(shop_id) DO UPDATE SET
                shop_name=excluded.shop_name,
                transaction_sold_count=excluded.transaction_sold_count,
                review_count=excluded.review_count,
                review_average=excluded.review_average,
                listing_active_count=excluded.listing_active_count,
                age_months=excluded.age_months,
                sales_per_month=excluded.sales_per_month,
                is_winner=excluded.is_winner,
                last_updated=strftime('%s','now')
        """, s)

def _etsy_upsert_listing(l):
    with _EtsyCursor() as c:
        c.execute("""
            INSERT INTO etsy_listings (listing_id, shop_id, title, description, url, price, currency,
                num_favorers, views, quantity, tags, materials, image_url, image_urls,
                taxonomy_id, listing_reviews, review_velocity, score, is_winner, keyword, last_updated)
            VALUES (:listing_id,:shop_id,:title,:description,:url,:price,:currency,
                :num_favorers,:views,:quantity,:tags,:materials,:image_url,:image_urls,
                :taxonomy_id,:listing_reviews,:review_velocity,:score,:is_winner,:keyword,
                strftime('%s','now'))
            ON CONFLICT(listing_id) DO UPDATE SET
                title=excluded.title,
                price=excluded.price,
                num_favorers=excluded.num_favorers,
                views=excluded.views,
                quantity=excluded.quantity,
                listing_reviews=excluded.listing_reviews,
                review_velocity=excluded.review_velocity,
                score=excluded.score,
                is_winner=excluded.is_winner,
                last_updated=strftime('%s','now')
        """, l)

def _etsy_record_run(**kw):
    with _EtsyCursor() as c:
        cols = ','.join(kw.keys())
        ph = ','.join(f':{k}' for k in kw.keys())
        cur = c.execute(f"INSERT INTO etsy_scan_runs ({cols}) VALUES ({ph})", kw)
        return cur.lastrowid

def _etsy_finish_run(run_id, **kw):
    if not kw: return
    with _EtsyCursor() as c:
        sets = ','.join(f"{k}=:{k}" for k in kw.keys())
        kw['id'] = run_id
        c.execute(f"UPDATE etsy_scan_runs SET {sets} WHERE id=:id", kw)
    # Always invalidate caches after a sweep finishes so UI sees fresh data
    try:
        _etsy_invalidate_caches()
    except Exception:
        pass

def _etsy_add_keyword(keyword, source='discovered', inc_occurrences=True):
    keyword = (keyword or '').strip().lower()
    if not keyword or len(keyword) < 3 or len(keyword) > 60:
        return
    with _EtsyCursor() as c:
        c.execute("""
            INSERT INTO etsy_discovered_keywords (keyword, source, occurrences)
            VALUES (?,?,1)
            ON CONFLICT(keyword) DO UPDATE SET occurrences = occurrences + ?
        """, (keyword, source, 1 if inc_occurrences else 0))

def _etsy_pick_keywords(max_n):
    """Return a prioritized list of keywords to scan this sweep:
    seed keywords first, then top discovered ones (most occurrences, fewer recent scans)."""
    settings = _etsy_load_settings()
    seeds = settings.get('seedKeywords') or []
    # ensure seeds exist in the discovered table for tracking
    for kw in seeds:
        _etsy_add_keyword(kw, source='seed', inc_occurrences=False)
    with _EtsyCursor() as c:
        rows = c.execute("""
            SELECT keyword FROM etsy_discovered_keywords
            WHERE enabled = 1
            ORDER BY
              CASE WHEN source='seed' THEN 0 ELSE 1 END,
              CASE WHEN last_scanned IS NULL THEN 0 ELSE last_scanned END ASC,
              winners_yielded DESC,
              occurrences DESC
            LIMIT ?
        """, (max_n,)).fetchall()
    return [r['keyword'] for r in rows]

def _etsy_mark_keyword_scanned(keyword, winners_added=0):
    with _EtsyCursor() as c:
        c.execute("""UPDATE etsy_discovered_keywords
                     SET last_scanned = strftime('%s','now'),
                         winners_yielded = winners_yielded + ?
                     WHERE keyword = ?""", (int(winners_added), keyword))

def _etsy_winner_shop_ids(limit=200):
    with _EtsyCursor() as c:
        rows = c.execute("""SELECT shop_id FROM etsy_shops WHERE is_winner=1
                            ORDER BY last_updated DESC LIMIT ?""", (limit,)).fetchall()
    return [r['shop_id'] for r in rows]

# Simple in-process cache so polling the tab stays cheap
_etsy_stats_cache = {'data': None, 'ts': 0}
_etsy_winners_cache = {}  # key -> {'data': [...], 'ts': ...}
_ETSY_STATS_TTL = 15  # seconds
_ETSY_WINNERS_TTL = 20

def _etsy_stats():
    now = time.time()
    if _etsy_stats_cache['data'] and (now - _etsy_stats_cache['ts'] < _ETSY_STATS_TTL):
        return dict(_etsy_stats_cache['data'])
    with _EtsyCursor() as c:
        out = {
            'total_listings': c.execute('SELECT COUNT(*) n FROM etsy_listings').fetchone()['n'],
            'total_shops': c.execute('SELECT COUNT(*) n FROM etsy_shops').fetchone()['n'],
            'winners': c.execute('SELECT COUNT(*) n FROM etsy_listings WHERE is_winner=1').fetchone()['n'],
            'winner_shops': c.execute('SELECT COUNT(*) n FROM etsy_shops WHERE is_winner=1').fetchone()['n'],
            'keywords_known': c.execute('SELECT COUNT(*) n FROM etsy_discovered_keywords').fetchone()['n'],
            'keywords_discovered': c.execute("SELECT COUNT(*) n FROM etsy_discovered_keywords WHERE source!='seed'").fetchone()['n'],
            'api_today': _etsy_today_usage(),
        }
        r = c.execute('SELECT * FROM etsy_scan_runs ORDER BY id DESC LIMIT 1').fetchone()
        out['last_run'] = dict(r) if r else None
        # Top vendors snapshot (shops with the most winners) - useful & cheap
        top = c.execute("""
            SELECT s.shop_id, s.shop_name, s.url AS shop_url,
                   COUNT(l.listing_id) AS winners_count,
                   s.transaction_sold_count AS shop_sales
            FROM etsy_listings l
            JOIN etsy_shops s ON l.shop_id = s.shop_id
            WHERE l.is_winner = 1
            GROUP BY s.shop_id
            ORDER BY winners_count DESC
            LIMIT 10
        """).fetchall()
        out['top_vendors'] = [dict(r) for r in top]
    _etsy_stats_cache['data'] = out
    _etsy_stats_cache['ts'] = now
    return dict(out)

def _etsy_invalidate_caches():
    _etsy_stats_cache['data'] = None
    _etsy_stats_cache['ts'] = 0
    _etsy_winners_cache.clear()

def _etsy_winners(limit=200, offset=0, sort='score', search='', min_reviews=0, shop_id=None):
    allowed = {'score','review_velocity','num_favorers','listing_reviews','price','last_updated'}
    if sort not in allowed: sort = 'score'
    cache_key = f"{limit}|{offset}|{sort}|{search}|{min_reviews}|{shop_id}"
    now = time.time()
    cached = _etsy_winners_cache.get(cache_key)
    if cached and (now - cached['ts'] < _ETSY_WINNERS_TTL):
        return list(cached['data'])
    with _EtsyCursor() as c:
        # First compute per-shop winner counts + top listing per shop (by reviews) for bestseller flag
        shop_stats = {}
        for r in c.execute("""
            SELECT shop_id, COUNT(*) AS n, MAX(listing_reviews) AS top_reviews
            FROM etsy_listings WHERE is_winner = 1 GROUP BY shop_id
        """).fetchall():
            shop_stats[r['shop_id']] = {'n': r['n'], 'top_reviews': r['top_reviews']}
        q = """SELECT l.*, s.shop_name, s.url AS shop_url, s.transaction_sold_count,
                      s.review_count AS shop_reviews, s.age_months, s.sales_per_month, s.country,
                      s.icon_url AS shop_icon
               FROM etsy_listings l LEFT JOIN etsy_shops s ON l.shop_id = s.shop_id
               WHERE l.is_winner = 1"""
        params = []
        if min_reviews and int(min_reviews) > 0:
            q += " AND l.listing_reviews >= ?"
            params.append(int(min_reviews))
        if shop_id:
            q += " AND l.shop_id = ?"
            params.append(int(shop_id))
        if search:
            q += " AND (l.title LIKE ? OR l.tags LIKE ? OR s.shop_name LIKE ?)"
            pat = f"%{search}%"
            params.extend([pat, pat, pat])
        q += f" ORDER BY l.{sort} DESC LIMIT ? OFFSET ?"
        params.extend([int(limit), int(offset)])
        rows = [dict(r) for r in c.execute(q, params).fetchall()]
        # Annotate each row with shop_winner_count and is_shop_bestseller
        for r in rows:
            sid = r.get('shop_id')
            st = shop_stats.get(sid, {'n': 0, 'top_reviews': 0})
            r['shop_winner_count'] = st['n']
            r['is_shop_bestseller'] = bool(r.get('listing_reviews') and st['top_reviews'] and r['listing_reviews'] == st['top_reviews'])
    _etsy_winners_cache[cache_key] = {'data': rows, 'ts': now}
    return list(rows)

def _etsy_winner_shops(limit=100):
    with _EtsyCursor() as c:
        rows = c.execute("""
            SELECT s.*, COUNT(l.listing_id) AS winning_listings, MAX(l.score) AS top_score
            FROM etsy_shops s LEFT JOIN etsy_listings l
              ON l.shop_id = s.shop_id AND l.is_winner = 1
            WHERE s.is_winner = 1
            GROUP BY s.shop_id
            ORDER BY top_score DESC LIMIT ?
        """, (int(limit),)).fetchall()
        return [dict(r) for r in rows]

def _etsy_recent_runs(limit=30):
    with _EtsyCursor() as c:
        rows = c.execute('SELECT * FROM etsy_scan_runs ORDER BY id DESC LIMIT ?', (int(limit),)).fetchall()
    return [dict(r) for r in rows]

def _etsy_get_listing(listing_id):
    with _EtsyCursor() as c:
        r = c.execute("""
            SELECT l.*, s.shop_name, s.url AS shop_url, s.transaction_sold_count,
                   s.review_count AS shop_reviews, s.review_average AS shop_review_avg,
                   s.age_months, s.sales_per_month, s.country, s.icon_url AS shop_icon
            FROM etsy_listings l LEFT JOIN etsy_shops s ON l.shop_id = s.shop_id
            WHERE l.listing_id = ?
        """, (int(listing_id),)).fetchone()
        return dict(r) if r else None

def _etsy_recent_discovered_keywords(limit=50):
    with _EtsyCursor() as c:
        rows = c.execute("""SELECT keyword, source, occurrences, winners_yielded, last_scanned, first_seen
                            FROM etsy_discovered_keywords
                            ORDER BY first_seen DESC LIMIT ?""", (int(limit),)).fetchall()
    return [dict(r) for r in rows]


# ---------- Etsy sync HTTP client ----------
_etsy_last_request_ts = [0.0]
# Auth circuit-breaker: if Etsy returns 401/403 repeatedly, stop hammering.
# Cleared when settings are saved (key/secret may have changed) or when an
# operator hits scan-now (to retry after Etsy approves the key).
_etsy_auth_block = {'until': 0.0, 'message': '', 'status': 0}

def _etsy_clear_auth_block():
    _etsy_auth_block['until'] = 0.0
    _etsy_auth_block['message'] = ''
    _etsy_auth_block['status'] = 0

# ---------- Etsy OAuth 2.0 (PKCE) ----------
# Etsy v3 data endpoints require an OAuth 2.0 bearer token. We store the
# access + refresh tokens on disk and auto-refresh when the access token
# is within 60s of expiry. The keystring (apiKey) is sent as x-api-key on
# every request alongside the bearer token — Etsy requires both.
_etsy_token_lock = threading.Lock()

def _etsy_load_tokens():
    try:
        if os.path.exists(ETSY_OAUTH_TOKENS_PATH):
            with open(ETSY_OAUTH_TOKENS_PATH, 'r') as f:
                return json.load(f) or {}
    except Exception as e:
        log.warning('etsy: failed to read oauth tokens: %s', e)
    return {}

def _etsy_save_tokens(tokens):
    tmp = ETSY_OAUTH_TOKENS_PATH + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(tokens, f)
    os.replace(tmp, ETSY_OAUTH_TOKENS_PATH)

def _etsy_clear_tokens():
    try:
        if os.path.exists(ETSY_OAUTH_TOKENS_PATH):
            os.remove(ETSY_OAUTH_TOKENS_PATH)
    except Exception:
        pass

def _etsy_pkce_pair():
    # RFC 7636: code_verifier is 43-128 chars, code_challenge is base64url(sha256(verifier))
    import base64, hashlib, secrets
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).decode('ascii').rstrip('=')
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode('ascii')).digest()).decode('ascii').rstrip('=')
    return verifier, challenge

def _etsy_save_pending(state, verifier, redirect_uri):
    tmp = ETSY_OAUTH_PENDING_PATH + '.tmp'
    with open(tmp, 'w') as f:
        json.dump({'state': state, 'code_verifier': verifier, 'redirect_uri': redirect_uri, 'created_at': time.time()}, f)
    os.replace(tmp, ETSY_OAUTH_PENDING_PATH)

def _etsy_load_pending():
    try:
        if os.path.exists(ETSY_OAUTH_PENDING_PATH):
            with open(ETSY_OAUTH_PENDING_PATH, 'r') as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}

def _etsy_clear_pending():
    try:
        if os.path.exists(ETSY_OAUTH_PENDING_PATH):
            os.remove(ETSY_OAUTH_PENDING_PATH)
    except Exception:
        pass

def _etsy_exchange_code(client_id, code, code_verifier, redirect_uri):
    """Exchange auth code for access + refresh tokens."""
    body = {
        'grant_type': 'authorization_code',
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'code': code,
        'code_verifier': code_verifier,
    }
    r = http_requests.post(ETSY_TOKEN_URL, data=body, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f'token exchange failed {r.status_code}: {r.text[:300]}')
    payload = r.json()
    payload['obtained_at'] = time.time()
    payload['expires_at'] = time.time() + int(payload.get('expires_in') or 3600)
    _etsy_save_tokens(payload)
    return payload

def _etsy_refresh_token():
    """Refresh the access token using the saved refresh token."""
    settings = _etsy_load_settings()
    client_id = (settings.get('apiKey') or '').strip()
    if not client_id:
        raise RuntimeError('apiKey not set')
    tokens = _etsy_load_tokens()
    refresh = (tokens.get('refresh_token') or '').strip()
    if not refresh:
        raise RuntimeError('no refresh_token saved — reconnect Etsy')
    body = {
        'grant_type': 'refresh_token',
        'client_id': client_id,
        'refresh_token': refresh,
    }
    r = http_requests.post(ETSY_TOKEN_URL, data=body, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f'token refresh failed {r.status_code}: {r.text[:300]}')
    payload = r.json()
    # Etsy returns a new refresh_token; persist both.
    payload['obtained_at'] = time.time()
    payload['expires_at'] = time.time() + int(payload.get('expires_in') or 3600)
    if 'refresh_token' not in payload and refresh:
        payload['refresh_token'] = refresh
    _etsy_save_tokens(payload)
    return payload

def _etsy_get_access_token():
    """Return a valid access token, refreshing if needed. None if not connected."""
    with _etsy_token_lock:
        tokens = _etsy_load_tokens()
        if not tokens.get('access_token'):
            return None
        # 60s safety margin
        if float(tokens.get('expires_at') or 0) > time.time() + 60:
            return tokens.get('access_token')
        try:
            refreshed = _etsy_refresh_token()
            return refreshed.get('access_token')
        except Exception as e:
            log.error('etsy: token refresh failed: %s', e)
            return None

def _etsy_is_connected():
    return bool(_etsy_load_tokens().get('access_token'))

def _etsy_request(path, params=None, retries=3, priority=False):
    """Call the Etsy Open API.

    priority=True bypasses the shared daily budget (used by background
    scanners) and instead enforces a small separate cap so user-initiated
    actions like 'Import this Etsy shop' always work even when the
    background scanner has spent the day's budget.
    """
    settings = _etsy_load_settings()
    api_key = (settings.get('apiKey') or '').strip()
    if not api_key:
        raise RuntimeError('Etsy API key not set')
    # Honour auth circuit-breaker — do NOT call Etsy while we know the
    # credentials are bad. Avoids burning daily quota on guaranteed-failures.
    if _etsy_auth_block['until'] > time.time():
        return None
    if priority:
        # Separate per-day cap for user-initiated calls (default 1000).
        prio_cap = int(settings.get('dailyPriorityBudget') or 1000)
        if _etsy_today_priority_usage() >= prio_cap:
            log.warning('Etsy priority budget reached (%d); pausing', prio_cap)
            raise RuntimeError(f'Etsy daily priority budget reached ({prio_cap} calls). Try again tomorrow or raise dailyPriorityBudget in settings.')
    else:
        if _etsy_today_usage() >= int(settings.get('dailyRequestBudget') or 8000):
            log.warning('Etsy daily budget reached; pausing')
            return None
    # Rate-limit (synchronous): enforce min interval between calls
    rps = float(settings.get('requestsPerSecond') or 5.0)
    min_interval = 1.0 / max(0.1, rps)
    elapsed = time.time() - _etsy_last_request_ts[0]
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
    url = f'{ETSY_API_BASE}{path}'
    # Etsy v3 wants the keystring (and optionally the shared secret) in x-api-key.
    # When a shared secret is configured, Etsy requires the format
    #   x-api-key: <keystring>:<shared_secret>
    # (separate x-shared-secret header is rejected with 403).
    shared_secret = (settings.get('sharedSecret') or '').strip()
    api_key_header = f'{api_key}:{shared_secret}' if shared_secret else api_key
    headers = {'x-api-key': api_key_header, 'User-Agent': 'VideoPin-EtsyScanner/1.0'}
    # Etsy v3 requires a Bearer access token from OAuth on most data endpoints.
    access_token = _etsy_get_access_token()
    if access_token:
        headers['Authorization'] = f'Bearer {access_token}'
    else:
        # Not connected — trip the breaker and bail. Don't burn quota.
        _etsy_auth_block['until'] = time.time() + 3600
        _etsy_auth_block['status'] = 0
        _etsy_auth_block['message'] = 'Not connected to Etsy. Click "Connect to Etsy" in Settings to authorize.'
        return None
    for attempt in range(retries):
        try:
            _etsy_last_request_ts[0] = time.time()
            r = http_requests.get(url, params=params, headers=headers, timeout=30)
            if priority:
                _etsy_bump_priority_usage(1)
            else:
                _etsy_bump_usage(1)
            if r.status_code == 200:
                _etsy_clear_auth_block()
                return r.json()
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            if r.status_code in (401, 403):
                # Auth problem — trip the breaker for 30 minutes so we don't
                # burn through the daily budget while waiting for Etsy approval
                # or a corrected secret. Operator can clear by saving settings
                # or hitting Scan Now.
                _etsy_auth_block['until'] = time.time() + 1800
                _etsy_auth_block['status'] = r.status_code
                try:
                    _etsy_auth_block['message'] = (r.json() or {}).get('error', r.text[:200])
                except Exception:
                    _etsy_auth_block['message'] = r.text[:200]
                log.error('Etsy %s -> %s (auth-block 30min): %s', path, r.status_code, _etsy_auth_block['message'])
                return None
            log.error('Etsy %s -> %s: %s', path, r.status_code, r.text[:200])
            return None
        except http_requests.exceptions.RequestException as e:
            log.warning('Etsy network error %s on %s, retrying', e, path)
            time.sleep(2 ** attempt)
    return None

def _etsy_find_listings(keyword, taxonomy_id=None, limit=100, offset=0):
    # NOTE: Etsy v3 /listings/active silently ignores `includes`. Shop & images must
    # be fetched separately (and lazily) — see _etsy_get_shop_cached / _etsy_get_listing_images.
    params = {
        'keywords': keyword, 'limit': limit, 'offset': offset,
        'sort_on': 'score', 'sort_order': 'descending',
    }
    if taxonomy_id:
        params['taxonomy_id'] = taxonomy_id
    return _etsy_request('/listings/active', params)

def _etsy_get_shop(shop_id):
    return _etsy_request(f'/shops/{shop_id}')

# Process-wide shop cache: shop stats change slowly so a 24h TTL is safe.
# Eliminates the most common API duplication (same shop returned by many keyword searches).
_ETSY_SHOP_CACHE = {}
_ETSY_SHOP_CACHE_TTL_SEC = 24 * 60 * 60

def _etsy_get_shop_cached(shop_id):
    """Fetch shop data with in-process TTL cache. Returns raw shop dict (or None)."""
    now = time.time()
    entry = _ETSY_SHOP_CACHE.get(shop_id)
    if entry and (now - entry[0]) < _ETSY_SHOP_CACHE_TTL_SEC:
        return entry[1]
    raw = _etsy_get_shop(shop_id)
    if raw:
        _ETSY_SHOP_CACHE[shop_id] = (now, raw)
    return raw

def _etsy_get_listing_images(listing_id):
    """Fetch images for a single listing. Called lazily only for winner candidates."""
    return _etsy_request(f'/listings/{listing_id}/images', {})

def _etsy_get_shop_listings(shop_id, limit=50, offset=0):
    # `includes=Images` is also ignored here; images fetched lazily.
    return _etsy_request(f'/shops/{shop_id}/listings/active',
                         {'limit': limit, 'offset': offset})


# ---------- Scanner: scoring + normalization ----------
def _etsy_months_since(ts):
    if not ts: return 9999.0
    return max(0.0, (time.time() - int(ts)) / (60 * 60 * 24 * 30.44))

def _etsy_score(listing_reviews, review_velocity, num_favorers,
                shop_sales_per_month, shop_age_months):
    recency = max(0.1, 24.0 / max(1.0, shop_age_months)) if shop_age_months < 36 else 0.5
    base = (listing_reviews * 1.0 + review_velocity * 25.0 +
            num_favorers * 0.05 + shop_sales_per_month * 0.5)
    return round(base * recency, 2)

def _etsy_is_winner(listing_reviews, review_velocity, num_favorers,
                   shop_sales, shop_age_months, s):
    return (
        shop_sales >= s['minShopSales']
        and shop_age_months <= s['maxShopAgeMonths']
        and listing_reviews >= s['minListingReviews']
        and review_velocity >= s['minReviewVelocity']
        and num_favorers >= s['minListingFavorites']
    )

def _etsy_norm_shop(shop):
    opened = shop.get('create_date') or shop.get('created_tsz')
    age = _etsy_months_since(opened)
    sold = int(shop.get('transaction_sold_count') or 0)
    spm = sold / max(1.0, age) if age else 0.0
    return {
        'shop_id': shop['shop_id'],
        'shop_name': shop.get('shop_name'),
        'url': shop.get('url'),
        'icon_url': shop.get('icon_url_fullxfull'),
        'country': shop.get('country_iso') or shop.get('origin_country_iso'),
        'transaction_sold_count': sold,
        'review_count': int(shop.get('review_count') or 0),
        'review_average': float(shop.get('review_average') or 0),
        'listing_active_count': int(shop.get('listing_active_count') or 0),
        'opened_ts': opened,
        'age_months': round(age, 1),
        'sales_per_month': round(spm, 2),
        'is_winner': 0,
    }

def _etsy_norm_listing(raw, shop_id, keyword):
    price = raw.get('price') or {}
    if isinstance(price, dict):
        amount = price.get('amount', 0)
        divisor = price.get('divisor', 100) or 100
        price_val = amount / divisor if amount else 0.0
        currency = price.get('currency_code', '')
    else:
        price_val = float(price or 0)
        currency = ''
    images = raw.get('images') or []
    image_urls = [img.get('url_fullxfull') for img in images if img.get('url_fullxfull')]
    return {
        'listing_id': raw['listing_id'],
        'shop_id': shop_id,
        'title': raw.get('title', ''),
        'description': (raw.get('description') or '')[:2000],
        'url': raw.get('url', ''),
        'price': round(price_val, 2),
        'currency': currency,
        'num_favorers': int(raw.get('num_favorers') or 0),
        'views': int(raw.get('views') or 0),
        'quantity': int(raw.get('quantity') or 0),
        'tags': json.dumps(raw.get('tags') or []),
        'materials': json.dumps(raw.get('materials') or []),
        'image_url': image_urls[0] if image_urls else '',
        'image_urls': json.dumps(image_urls),
        'taxonomy_id': raw.get('taxonomy_id'),
        'listing_reviews': 0,
        'review_velocity': 0,
        'score': 0,
        'is_winner': 0,
        'keyword': keyword,
    }

_ETSY_STOPWORDS = {
    'the','a','an','and','or','for','with','to','of','in','on','at','by','from','her',
    'his','your','my','our','their','this','that','these','those','is','are','be',
    'gift','gifts','set','sets','custom','personalized','handmade','vintage','unique',
    'beautiful','perfect','best','new','sale','women','men','girls','boys','kids',
}

def _etsy_extract_keywords_from_listing(raw):
    """Extract candidate keywords from a winning listing's tags + title bigrams."""
    out = []
    for tag in (raw.get('tags') or [])[:8]:
        t = (tag or '').strip().lower()
        if 2 <= len(t.split()) <= 4 and len(t) >= 4:
            out.append((t, 'tag'))
    title = (raw.get('title') or '').lower()
    words = re.findall(r"[a-z][a-z'\-]{2,}", title)
    words = [w for w in words if w not in _ETSY_STOPWORDS]
    for i in range(len(words) - 1):
        bg = f"{words[i]} {words[i+1]}"
        if len(bg) >= 6 and not any(w in _ETSY_STOPWORDS for w in bg.split()):
            out.append((bg, 'title-bigram'))
    return out

def _etsy_review_metrics_from_shop(shop_norm):
    """Per-listing review counts aren't available on Etsy v3 (the endpoint 404s),
    so we use the shop-level review_count as the per-listing review proxy.
    This actually matches the 'bestseller for that vendor' mental model better:
    a winning vendor is identified by their total review_count, not per-listing."""
    total = int(shop_norm.get('review_count') or 0)
    age = max(1.0, min(float(shop_norm.get('age_months') or 1.0), 60.0))
    velocity = round(total / age, 2) if total else 0.0
    return total, velocity

def _etsy_scan_one(keyword, source, settings):
    """Scan one keyword. Returns (winners_count, candidate_keywords_added).

    New architecture (post v3 includes-broken finding):
      1. Pull listings page (1 API call per 100 listings).
      2. Group by shop_id; fetch each unique shop ONCE per sweep via TTL cache.
      3. Apply shop-level gates (review_count is the per-listing review proxy).
      4. Only fetch images for winning listings (lazy, saves ~99% of image calls).
    """
    run_id = _etsy_record_run(
        started_at=int(time.time()),
        keyword=keyword, source=source, status='running')
    listings_seen = shops_seen = winners_found = 0
    api_before = _etsy_today_usage()
    err = None
    new_keywords = []
    try:
        # Per-sweep shop cache (normalized form); _ETSY_SHOP_CACHE handles raw API caching.
        sweep_shops = {}
        for page in range(int(settings.get('maxPagesPerKeyword') or 2)):
            data = _etsy_find_listings(
                keyword,
                taxonomy_id=int(settings.get('jewelryTaxonomyId') or 0) or None,
                limit=100, offset=page * 100,
            )
            if not data: break
            results = data.get('results') or []
            if not results: break
            min_reviews = int(settings.get('minListingReviews') or 0)
            min_favs = int(settings.get('minListingFavorites') or 0)
            for raw in results:
                listings_seen += 1
                sid = raw.get('shop_id')
                if not sid:
                    continue
                # Cheap pre-filter before fetching shop: if user demands favorites and
                # this listing has none, skip the shop lookup entirely.
                if min_favs > 0 and int(raw.get('num_favorers') or 0) < min_favs:
                    continue
                # Fetch (or re-use) the shop record
                if sid not in sweep_shops:
                    shop_raw = _etsy_get_shop_cached(sid)
                    if not shop_raw:
                        continue
                    sweep_shops[sid] = _etsy_norm_shop(shop_raw)
                    shops_seen += 1
                shop = sweep_shops[sid]
                # Shop-level gates first (cheap, drops most candidates)
                if shop['transaction_sold_count'] < settings.get('minShopSales', 0): continue
                if shop['age_months'] > settings.get('maxShopAgeMonths', 9999): continue
                if shop['review_count'] < min_reviews: continue
                listing = _etsy_norm_listing(raw, sid, keyword)
                if listing['num_favorers'] < min_favs: continue
                # Reviews & velocity come from shop (per-listing reviews endpoint is gone)
                rcount, rvel = _etsy_review_metrics_from_shop(shop)
                listing['listing_reviews'] = rcount
                listing['review_velocity'] = rvel
                listing['score'] = _etsy_score(rcount, rvel, listing['num_favorers'],
                                               shop['sales_per_month'], shop['age_months'])
                winner = _etsy_is_winner(rcount, rvel, listing['num_favorers'],
                                         shop['transaction_sold_count'], shop['age_months'], settings)
                listing['is_winner'] = 1 if winner else 0
                if winner:
                    shop['is_winner'] = 1
                    winners_found += 1
                    # Lazy: fetch images only for actual winners
                    if not listing.get('image_url'):
                        try:
                            img_data = _etsy_get_listing_images(listing['listing_id'])
                            if img_data and img_data.get('results'):
                                urls = [im.get('url_fullxfull') for im in img_data['results']
                                        if im.get('url_fullxfull')]
                                if urls:
                                    listing['image_url'] = urls[0]
                                    listing['image_urls'] = json.dumps(urls)
                        except Exception as ie:
                            log.warning('Image fetch failed for listing %s: %s',
                                        listing['listing_id'], ie)
                    # Self-expanding discovery: learn from winners
                    for kw, src in _etsy_extract_keywords_from_listing(raw):
                        _etsy_add_keyword(kw, source=src)
                        new_keywords.append(kw)
                _etsy_upsert_shop(shop)
                _etsy_upsert_listing(listing)
            for s in sweep_shops.values():
                _etsy_upsert_shop(s)
        status = 'ok'
    except Exception as e:
        log.exception('Etsy scan failed for %s', keyword)
        err = str(e); status = 'error'
    finally:
        api_calls = _etsy_today_usage() - api_before
        _etsy_finish_run(run_id,
            finished_at=int(time.time()),
            listings_seen=listings_seen, shops_seen=shops_seen,
            winners_found=winners_found, api_calls=api_calls,
            status=status, error=err)
    _etsy_mark_keyword_scanned(keyword, winners_added=winners_found)
    return winners_found, new_keywords

def _etsy_scan_shop_listings(shop_id, settings):
    """Re-scan a known winning shop's listings to catch new winners (shop-level discovery)."""
    run_id = _etsy_record_run(
        started_at=int(time.time()),
        keyword=f'shop:{shop_id}', source='shop', status='running')
    listings_seen = winners_found = 0
    api_before = _etsy_today_usage()
    err = None
    try:
        shop_raw = _etsy_get_shop_cached(shop_id)
        if not shop_raw: return 0
        shop = _etsy_norm_shop(shop_raw)
        data = _etsy_get_shop_listings(shop_id, limit=50)
        if not data: return 0
        min_favs = int(settings.get('minListingFavorites') or 0)
        for raw in (data.get('results') or []):
            listings_seen += 1
            listing = _etsy_norm_listing(raw, shop_id, f'shop:{shop_id}')
            if listing['num_favorers'] < min_favs: continue
            rcount, rvel = _etsy_review_metrics_from_shop(shop)
            listing['listing_reviews'] = rcount
            listing['review_velocity'] = rvel
            listing['score'] = _etsy_score(rcount, rvel, listing['num_favorers'],
                                           shop['sales_per_month'], shop['age_months'])
            winner = _etsy_is_winner(rcount, rvel, listing['num_favorers'],
                                     shop['transaction_sold_count'], shop['age_months'], settings)
            listing['is_winner'] = 1 if winner else 0
            if winner:
                shop['is_winner'] = 1
                winners_found += 1
                if not listing.get('image_url'):
                    try:
                        img_data = _etsy_get_listing_images(listing['listing_id'])
                        if img_data and img_data.get('results'):
                            urls = [im.get('url_fullxfull') for im in img_data['results']
                                    if im.get('url_fullxfull')]
                            if urls:
                                listing['image_url'] = urls[0]
                                listing['image_urls'] = json.dumps(urls)
                    except Exception as ie:
                        log.warning('Image fetch failed for listing %s: %s',
                                    listing['listing_id'], ie)
                for kw, src in _etsy_extract_keywords_from_listing(raw):
                    _etsy_add_keyword(kw, source=src)
            _etsy_upsert_listing(listing)
        _etsy_upsert_shop(shop)
        status = 'ok'
    except Exception as e:
        log.exception('Etsy shop rescan failed: %s', shop_id)
        err = str(e); status = 'error'
    finally:
        api_calls = _etsy_today_usage() - api_before
        _etsy_finish_run(run_id,
            finished_at=int(time.time()),
            listings_seen=listings_seen, shops_seen=1,
            winners_found=winners_found, api_calls=api_calls,
            status=status, error=err)
    return winners_found

def _etsy_full_sweep():
    """One full sweep: pick keywords (seed + discovered), then revisit top winning shops."""
    if _etsy_scanner_state['running']:
        log.info('Etsy sweep already in progress, skipping')
        return
    _etsy_scanner_state['running'] = True
    started = time.time()
    try:
        settings = _etsy_load_settings()
        if not settings.get('apiKey'):
            log.warning('Etsy sweep skipped: no API key')
            return
        max_kw = int(settings.get('maxKeywordsPerSweep') or 60)
        budget = int(settings.get('dailyRequestBudget') or 8000)
        keywords = _etsy_pick_keywords(max_kw)
        log.info('Etsy sweep starting: %d keywords queued', len(keywords))
        for kw in keywords:
            if _etsy_scanner_state.get('stop_requested'):
                log.info('Etsy sweep aborted by stop request'); break
            if _etsy_today_usage() >= budget:
                log.warning('Etsy daily budget hit; stopping sweep'); break
            try:
                _etsy_scan_one(kw, source=('seed' if kw in (settings.get('seedKeywords') or []) else 'discovered'), settings=settings)
            except Exception:
                log.exception('Etsy keyword failed: %s', kw)
            time.sleep(1.0)
        # Shop-level discovery: revisit a few winning shops for new listings
        if not _etsy_scanner_state.get('stop_requested') and _etsy_today_usage() < budget:
            for sid in _etsy_winner_shop_ids(limit=20):
                if _etsy_scanner_state.get('stop_requested'):
                    log.info('Etsy shop rescan aborted by stop request'); break
                if _etsy_today_usage() >= budget: break
                try:
                    _etsy_scan_shop_listings(sid, settings)
                except Exception:
                    log.exception('Etsy shop rescan failed: %s', sid)
                time.sleep(1.0)
        log.info('Etsy sweep complete in %.1fs', time.time() - started)
    finally:
        _etsy_scanner_state['running'] = False
        _etsy_scanner_state['stop_requested'] = False
        _etsy_scanner_state['last_sweep_at'] = int(time.time())
        try:
            settings = _etsy_load_settings()
            _etsy_scanner_state['next_sweep_at'] = _etsy_scanner_state['last_sweep_at'] + int(settings.get('scanIntervalSeconds') or 10800)
        except Exception:
            pass

def _etsy_single_keyword_sweep(keyword, source='manual'):
    """Scan exactly one user-supplied keyword. Used by the 'Scan on Etsy' button.
    Auto-adds the keyword to the queue so it's tracked for future sweeps."""
    if _etsy_scanner_state['running']:
        log.info('Etsy single-kw sweep skipped: a sweep is already running')
        return
    _etsy_scanner_state['running'] = True
    _etsy_scanner_state['current_keyword'] = keyword
    started = time.time()
    try:
        settings = _etsy_load_settings()
        if not settings.get('apiKey'):
            log.warning('Etsy single-kw sweep skipped: no API key'); return
        kw = (keyword or '').strip()
        if not kw:
            log.warning('Etsy single-kw sweep skipped: empty keyword'); return
        # Make sure it's in the queue so future sweeps remember it
        try: _etsy_add_keyword(kw, source=source)
        except Exception: pass
        budget = int(settings.get('dailyRequestBudget') or 4500)
        if _etsy_today_usage() >= budget:
            log.warning('Etsy single-kw sweep skipped: daily budget reached'); return
        try:
            _etsy_scan_one(kw, source=source, settings=settings)
        except Exception:
            log.exception('Etsy single-kw scan failed: %s', kw)
        log.info('Etsy single-kw sweep complete (%s) in %.1fs', kw, time.time() - started)
    finally:
        _etsy_scanner_state['running'] = False
        _etsy_scanner_state['stop_requested'] = False
        _etsy_scanner_state['current_keyword'] = None

def _etsy_scanner_loop():
    log.info('Etsy scanner background loop started')
    _etsy_scanner_state['started_at'] = int(time.time())
    # Small startup delay to let Flask finish booting
    time.sleep(5)
    while True:
        try:
            if _etsy_scanner_state.get('paused'):
                log.info('Etsy scanner paused; sleeping')
            else:
                settings = _etsy_load_settings()
                if settings.get('apiKey'):
                    _etsy_full_sweep()
                else:
                    log.info('Etsy scanner idle: no API key configured')
        except Exception:
            log.exception('Etsy scanner iteration crashed')
        interval = int((_etsy_load_settings() or {}).get('scanIntervalSeconds') or 10800)
        _etsy_scanner_state['next_sweep_at'] = int(time.time()) + interval
        time.sleep(max(60, interval))

def _etsy_ensure_scanner_thread():
    # DISABLED 2026-06-14 per user: the background sweep was consuming the
    # entire 4500/day Etsy API budget, starving user-initiated shop imports
    # (Etsy Shops tab). All API quota is now reserved for manual imports.
    # Re-enable by removing the early return below.
    _etsy_init_db()  # still init DB so existing endpoints don't 500
    log.info('Etsy background scanner is DISABLED (all API quota reserved for Etsy Shops tab imports)')
    return

# ---------- HTTP routes ----------
@app.route('/api/etsy/health', methods=['GET'])
@login_required
def etsy_health():
    settings = _etsy_load_settings()
    return jsonify({
        'success': True,
        'has_key': bool(settings.get('apiKey')),
        'scanner_running': _etsy_scanner_state['running'],
        'last_sweep_at': _etsy_scanner_state.get('last_sweep_at') or 0,
        'next_sweep_at': _etsy_scanner_state.get('next_sweep_at') or 0,
        'api_today': _etsy_today_usage(),
        'daily_budget': int(settings.get('dailyRequestBudget') or 8000),
    })

@app.route('/api/etsy/stats', methods=['GET'])
@login_required
def etsy_stats_route():
    s = _etsy_stats()
    settings = _etsy_load_settings()
    last_sweep_at = _etsy_scanner_state.get('last_sweep_at') or 0
    next_sweep_at = _etsy_scanner_state.get('next_sweep_at') or 0
    # Convert epoch seconds to ISO for the frontend's etsyFmtDate helper
    def _iso(ts):
        if not ts: return None
        try: return datetime.datetime.utcfromtimestamp(int(ts)).isoformat() + 'Z'
        except: return None
    # Surface auth-block state to the UI so the user sees why nothing scans
    now_ts = time.time()
    auth_blocked = _etsy_auth_block['until'] > now_ts
    flat = {
        'success': True,
        'has_api_key': bool(settings.get('apiKey')),
        'has_shared_secret': bool(settings.get('sharedSecret')),
        'oauth_connected': _etsy_is_connected(),
        'scanner_running': _etsy_scanner_state['running'],
        'scanner_paused': bool(_etsy_scanner_state.get('paused')),
        'stop_requested': bool(_etsy_scanner_state.get('stop_requested')),
        'winners': s.get('winners', 0),
        'shops': s.get('winner_shops', 0),
        'keywords': s.get('keywords_known', 0),
        'keywords_discovered': s.get('keywords_discovered', 0),
        'total_listings': s.get('total_listings', 0),
        'api_today': s.get('api_today', 0),
        'daily_budget': int(settings.get('dailyRequestBudget') or 4500),
        'last_run': s.get('last_run'),
        'next_sweep': _iso(next_sweep_at),
        'last_sweep': _iso(last_sweep_at),
        'top_vendors': s.get('top_vendors', []),
        'auth_blocked': auth_blocked,
        'auth_error': _etsy_auth_block['message'] if auth_blocked else '',
        'auth_status': _etsy_auth_block['status'] if auth_blocked else 0,
        'auth_block_until': _iso(_etsy_auth_block['until']) if auth_blocked else None,
    }
    return jsonify(flat)

@app.route('/api/etsy/settings', methods=['GET'])
@login_required
def etsy_get_settings():
    s = _etsy_load_settings()
    # Mask the key in the response
    key = s.get('apiKey') or ''
    s_safe = dict(s)
    s_safe['apiKey'] = ''
    s_safe['hasApiKey'] = bool(key)
    s_safe['apiKeyPreview'] = (key[:4] + '...' + key[-4:]) if len(key) >= 10 else ('set' if key else '')
    secret = s.get('sharedSecret') or ''
    s_safe['sharedSecret'] = ''
    s_safe['hasSharedSecret'] = bool(secret)
    s_safe['sharedSecretPreview'] = (secret[:4] + '...' + secret[-4:]) if len(secret) >= 10 else ('set' if secret else '')
    return jsonify(s_safe)

@app.route('/api/etsy/settings', methods=['POST'])
@login_required
def etsy_post_settings():
    body = request.get_json(silent=True) or {}
    current = _etsy_load_settings()
    # Only overwrite secrets if a non-empty value was provided (allows partial save)
    new_key = (body.get('apiKey') or '').strip()
    new_secret = (body.get('sharedSecret') or '').strip() if 'sharedSecret' in body else None
    merged = dict(current)
    for k, v in body.items():
        if k == 'apiKey':
            if new_key:
                merged['apiKey'] = new_key
        elif k == 'sharedSecret':
            if new_secret:
                merged['sharedSecret'] = new_secret
        else:
            merged[k] = v
    saved = _etsy_save_settings(merged)
    # Credentials may have changed — give the scanner a fresh chance.
    _etsy_clear_auth_block()
    _etsy_ensure_scanner_thread()
    return jsonify({
        'ok': True, 'success': True,
        'savedKeys': sorted(body.keys()),
        'hasApiKey': bool(saved.get('apiKey')),
        'hasSharedSecret': bool(saved.get('sharedSecret')),
    })

@app.route('/api/etsy/winners', methods=['GET'])
@login_required
def etsy_winners_route():
    limit = int(request.args.get('limit', 200))
    # Frontend sends sort values: score, velocity, favorites, reviews, price_low, price_high
    raw_sort = (request.args.get('sort') or 'score').lower()
    sort_map = {
        'score': 'score', 'velocity': 'review_velocity', 'favorites': 'num_favorers',
        'reviews': 'listing_reviews', 'price_low': 'price', 'price_high': 'price',
        'recent': 'last_updated',
    }
    sort = sort_map.get(raw_sort, 'score')
    q = (request.args.get('q') or '').strip()
    min_reviews = int(request.args.get('min_reviews') or 0)
    shop_id = request.args.get('shop_id')
    shop_id = int(shop_id) if (shop_id and shop_id.isdigit()) else None
    rows = _etsy_winners(limit=limit, sort=sort, search=q, min_reviews=min_reviews, shop_id=shop_id)
    # Reverse for price_low (ascending)
    if raw_sort == 'price_low':
        rows = sorted(rows, key=lambda r: (r.get('price') is None, r.get('price') or 0))
    # Parse JSON-encoded fields for the frontend
    for r in rows:
        for fld in ('tags', 'materials', 'image_urls'):
            try:
                r[fld] = json.loads(r.get(fld) or '[]')
            except Exception:
                r[fld] = []
    return jsonify({'success': True, 'winners': rows, 'count': len(rows)})

@app.route('/api/etsy/shops', methods=['GET'])
@login_required
def etsy_shops_route():
    limit = int(request.args.get('limit', 100))
    shops = _etsy_winner_shops(limit=limit)
    return jsonify({'success': True, 'shops': shops})

@app.route('/api/etsy/runs', methods=['GET'])
@login_required
def etsy_runs_route():
    limit = int(request.args.get('limit', 30))
    return jsonify({'success': True, 'runs': _etsy_recent_runs(limit=limit)})

@app.route('/api/etsy/keywords', methods=['GET'])
@login_required
def etsy_keywords_route():
    limit = int(request.args.get('limit', 100))
    return jsonify({'success': True, 'keywords': _etsy_recent_discovered_keywords(limit=limit)})

@app.route('/api/etsy/listing/<int:listing_id>', methods=['GET'])
@login_required
def etsy_listing_route(listing_id):
    row = _etsy_get_listing(listing_id)
    if not row:
        return jsonify({'success': False, 'error': 'Listing not found'}), 404
    for fld in ('tags', 'materials', 'image_urls'):
        try:
            row[fld] = json.loads(row.get(fld) or '[]')
        except Exception:
            row[fld] = []
    return jsonify({'success': True, 'listing': row})

@app.route('/api/etsy/export.csv', methods=['GET'])
@login_required
def etsy_export_csv():
    rows = _etsy_winners(limit=10000)
    headers = ['score','title','price','currency','num_favorers','listing_reviews',
               'review_velocity','shop_name','transaction_sold_count','age_months',
               'sales_per_month','country','url','shop_url','image_url','keyword']
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    for r in rows:
        w.writerow([r.get(h, '') if r.get(h) is not None else '' for h in headers])
    resp = make_response(buf.getvalue())
    resp.headers['Content-Type'] = 'text/csv'
    resp.headers['Content-Disposition'] = 'attachment; filename=etsy_winners.csv'
    return resp

@app.route('/api/etsy/scan-now', methods=['POST'])
@login_required
def etsy_scan_now():
    settings = _etsy_load_settings()
    if not settings.get('apiKey'):
        return jsonify({'ok': False, 'success': False, 'error': 'Etsy API key not configured. Save it first.'}), 400
    # Manual trigger clears the auth circuit-breaker so the user can retry
    # after Etsy approves the key or after correcting the shared secret.
    _etsy_clear_auth_block()
    if _etsy_scanner_state['running']:
        return jsonify({'ok': True, 'success': True, 'already_running': True, 'status': 'already_running'})
    t = threading.Thread(target=_etsy_full_sweep, daemon=True, name='etsy-manual-sweep')
    t.start()
    return jsonify({'ok': True, 'success': True, 'status': 'started'})

@app.route('/api/etsy/scan-keyword', methods=['POST'])
@login_required
def etsy_scan_keyword():
    """Scan a single user-supplied keyword (e.g. 'silver ring') immediately.
    Independent of the queue/sweep loop."""
    payload = request.get_json(silent=True) or {}
    kw = (payload.get('keyword') or '').strip()
    if not kw:
        return jsonify({'ok': False, 'success': False, 'error': 'keyword required'}), 400
    if len(kw) > 80:
        return jsonify({'ok': False, 'success': False, 'error': 'keyword too long'}), 400
    settings = _etsy_load_settings()
    if not settings.get('apiKey'):
        return jsonify({'ok': False, 'success': False, 'error': 'Etsy API key not configured'}), 400
    _etsy_clear_auth_block()
    if _etsy_scanner_state['running']:
        return jsonify({'ok': True, 'success': True, 'already_running': True, 'status': 'already_running',
                        'current_keyword': _etsy_scanner_state.get('current_keyword')})
    t = threading.Thread(target=_etsy_single_keyword_sweep, args=(kw,), daemon=True,
                        name=f'etsy-keyword-{kw[:20]}')
    t.start()
    return jsonify({'ok': True, 'success': True, 'status': 'started', 'keyword': kw})

@app.route('/api/etsy/stop-scan', methods=['POST'])
@login_required
def etsy_stop_scan():
    """Abort the in-flight sweep (best-effort: respected between keywords/shops)."""
    was_running = bool(_etsy_scanner_state.get('running'))
    _etsy_scanner_state['stop_requested'] = True
    return jsonify({'ok': True, 'success': True, 'was_running': was_running})

@app.route('/api/etsy/pause-scanner', methods=['POST'])
@login_required
def etsy_pause_scanner():
    """Pause the recurring background scanner (current sweep still finishes)."""
    body = request.get_json(silent=True) or {}
    paused = bool(body.get('paused', True))
    _etsy_scanner_state['paused'] = paused
    if paused:
        _etsy_scanner_state['stop_requested'] = True   # also abort an in-flight sweep
    return jsonify({'ok': True, 'success': True, 'paused': paused})

@app.route('/api/etsy/oauth/start', methods=['POST'])
@login_required
def etsy_oauth_start():
    """Generate authorization URL with PKCE and persist verifier+state."""
    settings = _etsy_load_settings()
    client_id = (settings.get('apiKey') or '').strip()
    if not client_id:
        return jsonify({'ok': False, 'success': False, 'error': 'Save your Etsy keystring first.'}), 400
    # Build redirect URI from request host so it matches whatever the user
    # registered on the Etsy developer dashboard (e.g. https://video-pin.xyz/api/etsy/oauth/callback)
    body = request.get_json(silent=True) or {}
    override = (body.get('redirectUri') or '').strip()
    if override:
        redirect_uri = override
    else:
        # request.url_root may be http on the internal port; force https
        host = request.host
        redirect_uri = f'https://{host}/api/etsy/oauth/callback'
    verifier, challenge = _etsy_pkce_pair()
    import secrets
    state = secrets.token_urlsafe(24)
    _etsy_save_pending(state, verifier, redirect_uri)
    from urllib.parse import urlencode
    params = {
        'response_type': 'code',
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'scope': ETSY_OAUTH_SCOPE,
        'state': state,
        'code_challenge': challenge,
        'code_challenge_method': 'S256',
    }
    authorize_url = f'{ETSY_OAUTH_AUTHORIZE_URL}?{urlencode(params)}'
    return jsonify({
        'ok': True, 'success': True,
        'authorize_url': authorize_url,
        'redirect_uri': redirect_uri,
    })

@app.route('/api/etsy/oauth/callback', methods=['GET'])
def etsy_oauth_callback():
    """Etsy redirects here after the user approves. Exchange code for tokens."""
    code = request.args.get('code')
    state = request.args.get('state')
    err = request.args.get('error')
    if err:
        return f'<h2>Etsy returned error: {err}</h2><p>{request.args.get("error_description","")}</p><p><a href="/">Back to app</a></p>', 400
    if not code or not state:
        return '<h2>Missing code/state in callback.</h2>', 400
    pending = _etsy_load_pending()
    if pending.get('state') != state:
        return '<h2>State mismatch (CSRF protection).</h2><p>Restart the connect flow from the app.</p>', 400
    if time.time() - float(pending.get('created_at') or 0) > 900:
        _etsy_clear_pending()
        return '<h2>Authorization expired.</h2><p>Restart the connect flow.</p>', 400
    settings = _etsy_load_settings()
    client_id = (settings.get('apiKey') or '').strip()
    try:
        _etsy_exchange_code(client_id, code, pending['code_verifier'], pending['redirect_uri'])
    except Exception as e:
        log.exception('etsy oauth exchange failed')
        return f'<h2>Token exchange failed</h2><pre>{str(e)[:500]}</pre><p><a href="/">Back to app</a></p>', 500
    finally:
        _etsy_clear_pending()
    _etsy_clear_auth_block()
    _etsy_ensure_scanner_thread()
    return '''<!doctype html><html><head><title>Etsy connected</title>
<style>body{font-family:system-ui,Arial,sans-serif;background:#f8fafc;padding:40px;color:#0f172a;text-align:center}
.card{background:white;max-width:520px;margin:0 auto;padding:32px;border-radius:12px;box-shadow:0 4px 16px rgba(0,0,0,.06)}
h1{color:#16a34a}a{color:#0ea5e9}</style></head>
<body><div class="card"><h1>✅ Etsy connected</h1>
<p>Your scanner is now authorised and will start sweeping for 500+ review jewelry winners.</p>
<p><a href="/#etsy">Back to the app</a></p>
<script>setTimeout(function(){window.location="/#etsy"},2500);</script></div></body></html>'''

@app.route('/api/etsy/oauth/disconnect', methods=['POST'])
@login_required
def etsy_oauth_disconnect():
    _etsy_clear_tokens()
    _etsy_clear_pending()
    return jsonify({'ok': True, 'success': True})

@app.route('/api/etsy/oauth/status', methods=['GET'])
@login_required
def etsy_oauth_status():
    tokens = _etsy_load_tokens()
    connected = bool(tokens.get('access_token'))
    expires_at = float(tokens.get('expires_at') or 0) if connected else 0
    return jsonify({
        'success': True,
        'connected': connected,
        'expires_at': datetime.datetime.utcfromtimestamp(int(expires_at)).isoformat() + 'Z' if expires_at else None,
        'has_refresh_token': bool(tokens.get('refresh_token')),
        'suggested_redirect_uri': f'https://{request.host}/api/etsy/oauth/callback',
    })

# ===== End Etsy Jewelry Scanner section ======================================


# ===== Etsy Shops tab (per-shop import + favorites) ===========================
# Imports any Etsy shop's full catalog via Etsy Open API v3 and stores it
# forever. UI: a card grid sorted by hearts (num_favorers). See
# etsy_shops_module.py for storage + routes.
try:
    from flask import send_from_directory as _es_send_from_directory  # noqa: F401
    import etsy_shops_module as _etsy_shops_module

    def _etsy_shops_request(path, params=None):
        """Adapter that reuses our existing auth/rate-limit logic.

        Background scanner is disabled, so the full daily budget is
        available to user-initiated shop imports. Re-raises so the
        import worker can surface a real error message to the UI
        instead of silently treating an API failure as 'shop not found'.
        """
        return _etsy_request(path, params=params)

    _etsy_shops_module.register_routes(
        app=app,
        data_dir=DATA_DIR,
        etsy_request_fn=_etsy_shops_request,
        login_required=login_required,
        send_from_directory=_es_send_from_directory,
    )
    log.info('Etsy Shops tab registered (data=%s)', DATA_DIR)
except Exception as _es_e:
    log.exception('Etsy Shops tab failed to register: %s', _es_e)

# Push-to-Shopify routes for the Etsy Shops tab. Uses the same Etsy API
# adapter (priority budget) and reads stored Shopify tokens from
# data/stores.json. See etsy_to_shopify.py for mapping logic.
try:
    import etsy_to_shopify as _etsy_to_shopify
    _etsy_to_shopify.register_routes(
        app=app,
        data_dir=DATA_DIR,
        etsy_request_fn=_etsy_shops_request,
        login_required=login_required,
    )
    log.info('Etsy -> Shopify push routes registered')
except Exception as _es2_e:
    log.exception('Etsy -> Shopify push routes failed to register: %s', _es2_e)

# Single-listing Etsy importer: paste a listing URL, get one row inserted
# into etsy_listing so the existing push flow can target it. Endpoint:
#   POST /api/etsy-shops/import-listing-by-url  body: {url}
try:
    import etsy_import_single as _etsy_import_single
    _etsy_import_single.register_routes(
        app=app,
        data_dir=DATA_DIR,
        etsy_request_fn=_etsy_shops_request,
        login_required=login_required,
    )
except Exception as _eis_e:
    log.exception('Etsy single-listing importer failed to register: %s', _eis_e)

# Everful Wholesale -> Shopify push routes. Mirrors the Etsy push pipeline
# but uses plain HTTP (no ScrapingBee credits) and parses JSON-LD ProductGroup
# from everfulwholesale.com product pages. See everful_to_shopify.py.
try:
    import everful_to_shopify as _everful_to_shopify
    _everful_to_shopify.register_routes(
        app=app,
        data_dir=DATA_DIR,
        login_required=login_required,
    )
    log.info('Everful -> Shopify push routes registered')
except Exception as _ev_e:
    log.exception('Everful -> Shopify push routes failed to register: %s', _ev_e)

# Shopify bulk title rename via AI rewriter. Endpoints:
#   POST /api/shopify/bulk-rename/preview  body: {store_id}
#   POST /api/shopify/bulk-rename/apply    body: {store_id, items:[{id,new_title}]}
try:
    import shopify_bulk_rename as _shopify_bulk_rename
    _shopify_bulk_rename.register_routes(
        app=app,
        data_dir=DATA_DIR,
        login_required=login_required,
    )
    log.info('Shopify bulk-rename routes registered')
except Exception as _br_e:
    log.exception('Shopify bulk-rename routes failed to register: %s', _br_e)

# Product URL -> sales lookup (Winners tab search box).
try:
    import product_lookup as _product_lookup
    _product_lookup.register_routes(
        app=app,
        data_dir=DATA_DIR,
        login_required=login_required,
        http_requests=http_requests,
        shoplazza_api_version=SHOPLAZZA_API_VERSION,
    )
except Exception as _pl_e:
    log.exception('Product-lookup route failed to register: %s', _pl_e)
# ===== End Etsy Shops tab ======================================================


# ===== Main =====
if __name__ == '__main__':
    _cleanup_old_videos(5)
    _ensure_worker()
    _etsy_ensure_scanner_thread()
    app.run(host='0.0.0.0', port=5110, debug=False)
