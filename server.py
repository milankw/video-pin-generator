#!/usr/bin/env python3
"""Video Pin Generator — AI-powered product video creation for Pinterest.
Single-file Flask backend. Port 5110.
"""

from flask import Flask, send_file, request, jsonify, redirect, session, make_response, url_for
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
DEFAULT_PASSWORD = os.environ.get('VPG_PASSWORD', 'videopins2026!')

def _get_password_hash():
    if os.path.exists(PASSWORD_HASH_FILE):
        with open(PASSWORD_HASH_FILE, 'r') as f:
            return f.read().strip().encode('utf-8')
    hashed = bcrypt.hashpw(DEFAULT_PASSWORD.encode('utf-8'), bcrypt.gensalt(rounds=12))
    with open(PASSWORD_HASH_FILE, 'w') as f:
        f.write(hashed.decode('utf-8'))
    return hashed

PASSWORD_HASH = _get_password_hash()

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
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)

def _load_stores():
    return _load_json('stores.json', [])

def _save_stores(stores):
    _save_json('stores.json', stores)

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

    if verify_password(password):
        _clear_attempts(ip)
        session['authenticated'] = True
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
@app.route('/api/stores/<store_id>/prompts', methods=['GET'])
@login_required
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
@login_required
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
        safe.append({
            'id': s.get('id', ''),
            'name': s.get('name', ''),
            'domain': s.get('domain', ''),
            'shopifyStatus': s.get('shopifyStatus', ''),
            'storeCategory': s.get('storeCategory', ''),
            'productCount': s.get('productCount', None),
            'hasToken': bool(s.get('shopifyAccessToken', '')),
            'promptCount': len(s.get('prompts', []))
        })
    return jsonify({'success': True, 'stores': safe})

@app.route('/api/stores/sync', methods=['POST'])
@login_required
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
    for s in raw_stores:
        store_id = s.get('id', '')
        if not store_id:
            continue
        # g-ads uses 'shopifyDomain', pinterest uses 'domain'
        domain = s.get('domain', '') or s.get('shopifyDomain', '')
        token = s.get('shopifyAccessToken', '')
        if not domain or not token:
            continue  # Skip stores without Shopify connection

        if store_id in existing_by_id:
            # Update existing store — refresh token/domain but keep prompts
            existing = existing_by_id[store_id]
            existing['name'] = s.get('name', existing.get('name', ''))
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
                'domain': domain,
                'shopifyAccessToken': token,
                'shopifyStatus': s.get('shopifyStatus', ''),
                'storeCategory': s.get('storeCategory', 'default'),
                'productCount': s.get('products', s.get('productCount', None)),
            }
            existing_by_id[store_id] = new_entry
            new_count += 1

    merged = list(existing_by_id.values())
    _save_stores(merged)
    connected_count = sum(1 for s in merged if s.get('shopifyStatus') == 'connected')

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
        'connectedCount': connected_count,
        'source': source_label,
        'sourcePath': source_path,
        'geminiImported': gemini_imported
    })


# ===== Routes: Settings =====
@app.route('/api/settings', methods=['GET'])
@login_required
def get_settings():
    settings = _load_settings()
    safe = dict(settings)
    # Remove sensitive Drive tokens from response (but keep client_id/secret for display)
    safe.pop('gdrive_access_token', None)
    safe.pop('gdrive_refresh_token', None)
    safe.pop('gdrive_token_expiry', None)
    return jsonify({'success': True, 'settings': safe})

@app.route('/api/settings', methods=['POST'])
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
def get_store_apps():
    """Fetch installed Shopify apps for each store using GraphQL (parallel)."""
    stores = _load_stores()
    valid_stores = [s for s in stores if s.get('domain') and s.get('shopifyAccessToken')]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(_fetch_apps_for_store, valid_stores))
    results = [r for r in results if r is not None]
    return jsonify({'success': True, 'stores': results})


# ===== Routes: Shopify Winners =====
@app.route('/api/shopify/winners/<store_id>', methods=['GET'])
@login_required
def shopify_winners(store_id):
    """Fetch top-selling products from Shopify orders."""
    stores = _load_stores()
    store = next((s for s in stores if s['id'] == store_id), None)
    if not store:
        return jsonify({'success': False, 'error': 'Store not found'}), 404

    domain = store.get('domain', '')
    token = store.get('shopifyAccessToken', '')
    if not domain or not token:
        return jsonify({'success': False, 'error': 'Shopify not connected'}), 400

    threshold = int(request.args.get('threshold', 5))
    headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
    base_url = f'https://{domain}/admin/api/2024-01'

    try:
        product_sales = {}
        page_url = f'{base_url}/orders.json?status=any&financial_status=paid&limit=250&fields=line_items,created_at'
        pages_fetched = 0
        max_pages = 100

        while page_url and pages_fetched < max_pages:
            resp = http_requests.get(page_url, headers=headers, timeout=30)
            if resp.status_code != 200:
                break

            orders = resp.json().get('orders', [])
            if not orders:
                break

            for order in orders:
                for item in order.get('line_items', []):
                    pid = item.get('product_id')
                    if not pid:
                        continue
                    qty = item.get('quantity', 1)
                    price = float(item.get('price', '0'))
                    revenue = qty * price

                    if pid not in product_sales:
                        product_sales[pid] = {
                            'product_id': pid,
                            'title': item.get('title', 'Unknown'),
                            'quantity': 0,
                            'revenue': 0.0,
                        }
                    product_sales[pid]['quantity'] += qty
                    product_sales[pid]['revenue'] += revenue

            # Pagination via Link header
            link_header = resp.headers.get('Link', '')
            page_url = None
            if 'rel="next"' in link_header:
                for part in link_header.split(','):
                    if 'rel="next"' in part:
                        page_url = part.split('<')[1].split('>')[0]
                        break
            pages_fetched += 1
            time.sleep(0.3)

        # Filter and sort
        sorted_products = sorted(product_sales.values(), key=lambda x: x['quantity'], reverse=True)
        if threshold > 0:
            sorted_products = [p for p in sorted_products if p['quantity'] >= threshold]

        # Batch-fetch product details
        product_details = {}
        all_pids = [p['product_id'] for p in sorted_products]
        for i in range(0, len(all_pids), 250):
            batch_ids = ','.join(str(pid) for pid in all_pids[i:i+250])
            try:
                pr = http_requests.get(
                    f'{base_url}/products.json?ids={batch_ids}&limit=250&fields=id,handle,images,product_type,status',
                    headers=headers, timeout=30
                )
                if pr.status_code == 200:
                    for prod in pr.json().get('products', []):
                        pid = prod.get('id')
                        if pid:
                            imgs = prod.get('images', [])
                            product_details[pid] = {
                                'handle': prod.get('handle', ''),
                                'image': imgs[0].get('src', '') if imgs else '',
                                'product_type': prod.get('product_type', ''),
                                'status': prod.get('status', 'unknown')
                            }
                time.sleep(0.3)
            except:
                pass

        # Check which products already have video jobs (done, in-progress, or queued)
        # Search both active jobs AND archive for complete status picture
        all_jobs = _load_all_jobs()
        product_video_status = {}  # pid -> 'done' | 'in_progress' | 'queued'
        for j in all_jobs:
            if j.get('storeId') != store_id:
                continue
            pid_str = str(j.get('productId', ''))
            jstatus = j.get('status', '')
            # Priority: done > in_progress > queued
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
            detail = product_details.get(pid, {})
            handle = detail.get('handle', '')
            image_url = detail.get('image', '')
            product_type = detail.get('product_type', '')
            shopify_status = detail.get('status', 'unknown')
            video_status = product_video_status.get(str(pid), 'none')  # none = no video at all

            results.append({
                'id': str(pid),
                'name': p['title'],
                'store': store.get('name', ''),
                'storeId': store_id,
                'sales': p['quantity'],
                'revenue': round(p['revenue'], 2),
                'image': image_url,
                'handle': handle,
                'productType': product_type,
                'shopifyStatus': shopify_status,
                'hasVideo': video_status == 'done',
                'videoStatus': video_status,
            })

        return jsonify({
            'success': True,
            'products': results,
            'totalOrders': sum(p['quantity'] for p in product_sales.values()),
            'totalProducts': len(product_sales),
            'qualifiedCount': len(results),
            'thresholdUsed': threshold,
            'pagesScanned': pages_fetched,
            'store': store.get('name', '')
        })

    except http_requests.exceptions.Timeout:
        return jsonify({'success': False, 'error': 'Shopify request timed out'}), 504
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== Routes: Video Generation =====
@app.route('/api/videos/generate', methods=['POST'])
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
        rev_result[0], rev_result[1] = _shopifyql_query(domain, token, revenue_q)
    def run_fun():
        fun_result[0], fun_result[1] = _shopifyql_query(domain, token, funnel_q)

    with ThreadPoolExecutor(max_workers=2) as pool:
        pool.submit(run_rev)
        pool.submit(run_fun)
        pool.shutdown(wait=True)

    if rev_result[1]:
        return None, rev_result[1]

    # Parse revenue data — rows are dicts with column names as keys
    td = rev_result[0]
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

def _extract_utm_source(landing_site, referring_site):
    """Extract UTM source from order's landing_site URL params + referring_site."""
    landing = landing_site or ''
    referring = (referring_site or '').lower()
    # Explicit utm_source in URL
    if 'utm_source=' in landing:
        try:
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(landing).query)
            src = qs.get('utm_source', [''])[0]
            if src:
                return src.lower()
        except Exception:
            pass
    # Infer from referring_site or landing page markers
    if 'pinterest' in referring or 'pins_campaign_id' in landing:
        return 'pinterest'
    if 'google' in referring or 'gclid' in landing:
        return 'google'
    if 'facebook' in referring or 'fbclid' in landing or 'fb_' in landing:
        return 'facebook'
    if 'instagram' in referring:
        return 'instagram'
    if 'tiktok' in referring or 'ttclid' in landing:
        return 'tiktok'
    if 'taboola' in referring or 'taboola' in landing.lower():
        return 'taboola'
    if 'outbrain' in referring or 'outbrain' in landing.lower():
        return 'outbrain'
    if 'teads' in referring or 'teads' in landing.lower():
        return 'teads'
    if 'bing' in referring or 'msclkid' in landing:
        return 'bing'
    if 'yahoo' in referring:
        return 'yahoo'
    if 'snapchat' in referring or 'sclid' in landing:
        return 'snapchat'
    if referring and referring != 'null':
        # Try to extract domain as source
        try:
            from urllib.parse import urlparse
            host = urlparse(referring).hostname or ''
            host = host.replace('www.', '').replace('android-app://', '').replace('com.', '')
            if host:
                return host.split('.')[0]  # e.g. "bing" from "bing.com"
        except Exception:
            pass
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
                f'&limit=250&fields=id,created_at,current_total_price,landing_site,referring_site,financial_status')
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
            src = _extract_utm_source(o.get('landing_site'), o.get('referring_site'))
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

        # Format response
        source_list = []
        for src, data in sorted(sources.items(), key=lambda x: -x[1]['revenue']):
            daily_rev = [round(data['daily'].get(lbl, {}).get('rev', 0.0), 2) for lbl in labels]
            daily_ord = [data['daily'].get(lbl, {}).get('ord', 0) for lbl in labels]
            source_list.append({
                'source': src,
                'revenue': round(data['revenue'], 2),
                'orders': data['orders'],
                'aov': round(data['revenue'] / data['orders'], 2) if data['orders'] > 0 else 0,
                'daily_revenue': daily_rev,
                'daily_orders': daily_ord
            })

        return jsonify({
            'success': True,
            'labels': labels,
            'sources': source_list,
            'total_orders': len(orders)
        })
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

        # Detect currency from store or default
        currency = store.get('currency', 'USD')

        resp_data = {
            'success': True,
            'store': store.get('name', ''),
            'currency': currency,
            'period': {'start': start, 'end': end},
            **result
        }
        if tz:
            resp_data['tz'] = tz

        return jsonify(resp_data)
    except Exception as e:
        log.error(f'Analytics error for {store.get("name","")}: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== Main =====
if __name__ == '__main__':
    _ensure_worker()
    app.run(host='0.0.0.0', port=5110, debug=False)
