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

def _load_settings():
    defaults = {
        'xai_api_key': '',
        'xai_video_model': 'grok-imagine-video',
        'video_duration': 8,
        'video_resolution': '720p',
        'aspect_ratio': '9:16',
        'prompt_templates': {
            'clothing': 'Elegant slow-motion showcase of {product_name}, professional product photography, soft studio lighting, clean background, fashion editorial style, smooth camera movement',
            'jewelry': 'Luxurious close-up of {product_name}, sparkling reflections, soft bokeh background, jewelry commercial style, rotating view, elegant lighting',
            'home': 'Beautiful lifestyle shot of {product_name}, modern interior setting, warm natural lighting, smooth camera pan, home decor editorial',
            'beauty': 'Glamorous beauty shot of {product_name}, soft diffused lighting, close-up details, premium cosmetics commercial style',
            'default': 'Professional product showcase of {product_name}, clean studio lighting, smooth rotating view, commercial quality, elegant presentation'
        },
        'google_ai_key': '',
        'google_ai_model': 'gemini-2.0-flash',
        'gdrive_client_id': '',
        'gdrive_client_secret': '',
        'gdrive_access_token': '',
        'gdrive_refresh_token': '',
        'gdrive_token_expiry': '',
        'gdrive_root_folder_id': '',
        'gdrive_status': '',
    }
    settings = _load_json('settings.json', defaults)
    # Ensure all default keys exist
    for k, v in defaults.items():
        if k not in settings:
            settings[k] = v
    if isinstance(settings.get('prompt_templates'), dict):
        for k, v in defaults['prompt_templates'].items():
            if k not in settings['prompt_templates']:
                settings['prompt_templates'][k] = v
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
    templates = settings.get('prompt_templates', {})
    category = (store_category or 'default').lower()
    template = templates.get(category, templates.get('default', 'Professional product showcase of {product_name}'))
    return template.replace('{product_name}', product_name).replace('{store_category}', category)

def _get_default_store_prompts(store):
    """Return 4 default prompts for a store, based on its category."""
    settings = _load_settings()
    templates = settings.get('prompt_templates', {})
    category = (store.get('storeCategory', '') or 'default').lower()
    base_template = templates.get(category, templates.get('default',
        'Professional product showcase of {product_name}, clean studio lighting, smooth rotating view, commercial quality'))

    return [
        {
            'label': 'Prompt 1',
            'template': base_template
        },
        {
            'label': 'Prompt 2',
            'template': 'Lifestyle shot of {product_name}, warm natural lighting, modern setting, editorial style'
        },
        {
            'label': 'Prompt 3',
            'template': 'Close-up detail shot of {product_name}, soft bokeh background, premium commercial feel'
        },
        {
            'label': 'Prompt 4',
            'template': 'Dynamic showcase of {product_name}, smooth camera movement, professional product film'
        }
    ]

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
                local_dir = os.path.join(VIDEOS_DIR, safe_store, safe_handle)
                local_path = os.path.join(local_dir, f'video_{ts}.mp4')

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
    """Sync stores from dropship-autopilot stores.json and import Gemini config."""
    # Try multiple locations for stores.json
    store_candidates = [
        '/root/dropship-autopilot/stores.json',
        os.path.join(BASE_DIR, '..', 'dropship-autopilot', 'stores.json'),
    ]
    config_candidates = [
        '/root/dropship-autopilot/config.json',
        os.path.join(BASE_DIR, '..', 'dropship-autopilot', 'config.json'),
    ]

    # --- Sync stores ---
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
        return jsonify({'success': False, 'error': 'Could not find stores.json. Tried: ' + ', '.join(store_candidates)}), 404

    # Extract relevant fields for all stores (connected and disconnected)
    # Preserve existing store-level prompts across syncs
    existing_stores = _load_stores()
    existing_prompts = {s.get('id'): s.get('prompts') for s in existing_stores if s.get('prompts')}

    synced = []
    raw_stores = source_data if isinstance(source_data, list) else source_data.get('stores', [])
    for s in raw_stores:
        store_entry = {
            'id': s.get('id', ''),
            'name': s.get('name', ''),
            'domain': s.get('domain', ''),
            'shopifyAccessToken': s.get('shopifyAccessToken', ''),
            'shopifyStatus': s.get('shopifyStatus', ''),
            'storeCategory': s.get('storeCategory', 'default'),
            'productCount': s.get('productCount', None),
        }
        # Preserve existing prompts
        if store_entry['id'] in existing_prompts:
            store_entry['prompts'] = existing_prompts[store_entry['id']]
        synced.append(store_entry)

    _save_stores(synced)
    connected_count = sum(1 for s in synced if s.get('shopifyStatus') == 'connected')

    # --- Import Gemini config ---
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
        'count': len(synced),
        'connectedCount': connected_count,
        'source': source_path,
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
    if 'prompt_templates' in data and isinstance(data['prompt_templates'], dict):
        settings['prompt_templates'].update(data['prompt_templates'])
    if 'google_ai_key' in data and data['google_ai_key']:
        settings['google_ai_key'] = data['google_ai_key']
    if 'google_ai_model' in data:
        settings['google_ai_model'] = data['google_ai_model']

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
@app.route('/api/orders/summary', methods=['GET'])
@login_required
def orders_summary():
    """Get fulfilled/unfulfilled/partial order counts per store."""
    stores = _load_stores()
    results = []
    for s in stores:
        domain = s.get('domain', '')
        token = s.get('shopifyAccessToken', '')
        if not domain or not token:
            continue
        base_url = f'https://{domain}/admin/api/2024-01'
        headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
        try:
            # Unfulfilled (open orders)
            r1 = http_requests.get(f'{base_url}/orders/count.json?status=open&fulfillment_status=unfulfilled', headers=headers, timeout=15)
            unfulfilled = r1.json().get('count', 0) if r1.status_code == 200 else 0
            time.sleep(0.3)
            # Fulfilled/shipped
            r2 = http_requests.get(f'{base_url}/orders/count.json?status=any&fulfillment_status=shipped', headers=headers, timeout=15)
            fulfilled = r2.json().get('count', 0) if r2.status_code == 200 else 0
            time.sleep(0.3)
            # Partial
            r3 = http_requests.get(f'{base_url}/orders/count.json?status=any&fulfillment_status=partial', headers=headers, timeout=15)
            partial = r3.json().get('count', 0) if r3.status_code == 200 else 0
            time.sleep(0.3)
            results.append({
                'id': s.get('id', ''),
                'name': s.get('name', ''),
                'domain': domain,
                'unfulfilled': unfulfilled,
                'fulfilled': fulfilled,
                'partial': partial
            })
        except Exception as e:
            log.warning(f"Order summary failed for {s.get('name','')}: {e}")
            continue
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

    # Load store prompts from the first product's storeId
    store_prompts = []
    if products:
        first_store_id = products[0].get('storeId', '')
        if first_store_id:
            stores = _load_stores()
            store = next((s for s in stores if s['id'] == first_store_id), None)
            if store:
                store_prompts = store.get('prompts', [])

    new_jobs = []
    with _jobs_lock:
        jobs = _load_jobs()
        for p in products:
            product_name = p.get('productName', '')
            store_category = p.get('storeCategory', '')
            group_id = uuid.uuid4().hex  # shared across all jobs for this product

            if store_prompts:
                # Create one job per store prompt
                for idx, sp in enumerate(store_prompts):
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



# ===== Main =====
if __name__ == '__main__':
    _ensure_worker()
    app.run(host='0.0.0.0', port=5110, debug=False)
