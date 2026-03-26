#!/usr/bin/env python3
"""Video Pin Generator — AI-powered product video creation for Pinterest.
Single-file Flask backend. Port 5110.
"""

from flask import Flask, send_file, request, jsonify, redirect, session, make_response
from functools import wraps
import bcrypt
import secrets
import os
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
        'gdrive_credentials': '',
        'gdrive_root_folder_id': ''
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


# ===== Job queue + background worker =====
_jobs_lock = threading.Lock()
_worker_running = False

def _get_prompt(product_name, store_category, settings):
    templates = settings.get('prompt_templates', {})
    category = (store_category or 'default').lower()
    template = templates.get(category, templates.get('default', 'Professional product showcase of {product_name}'))
    return template.replace('{product_name}', product_name).replace('{store_category}', category)

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
    aspect_ratio = settings.get('aspect_ratio', '9:16')

    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }

    # Step 1: Submit video generation request
    payload = {
        'model': 'grok-imagine-video',
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
        max_polls = 120  # 10 minutes max (5s * 120)
        for _ in range(max_polls):
            time.sleep(5)
            poll_resp = http_requests.get(
                f'https://api.x.ai/v1/videos/{request_id}',
                headers={'Authorization': f'Bearer {api_key}'},
                timeout=30
            )

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
        job['error'] = 'Polling timed out after 10 minutes'
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

def _worker_loop():
    """Background worker that processes queued video jobs."""
    global _worker_running
    _worker_running = True
    log.info("Video worker started")

    while True:
        try:
            # Find next queued job
            with _jobs_lock:
                jobs = _load_jobs()
                queued = [j for j in jobs if j['status'] == 'queued']

            if not queued:
                time.sleep(3)
                continue

            job = queued[0]
            log.info(f"Processing job {job['id']}: {job['productName']}")

            job = _process_job(job)
            _save_jobs_safe(job)

            if job['status'] == 'done':
                log.info(f"Job {job['id']} completed: {job.get('localPath', 'N/A')}")
            else:
                log.warning(f"Job {job['id']} failed: {job.get('error', 'unknown')}")

            # Rate limit protection between jobs
            time.sleep(2)

        except Exception as e:
            log.error(f"Worker error: {e}")
            time.sleep(5)

def _ensure_worker():
    global _worker_running
    if not _worker_running:
        t = threading.Thread(target=_worker_loop, daemon=True)
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
            'hasToken': bool(s.get('shopifyAccessToken', ''))
        })
    return jsonify({'success': True, 'stores': safe})

@app.route('/api/stores/sync', methods=['POST'])
@login_required
def sync_stores():
    """Sync stores from dropship-autopilot stores.json."""
    # Try multiple locations
    candidates = [
        '/root/dropship-autopilot/stores.json',
        os.path.join(BASE_DIR, '..', 'dropship-autopilot', 'stores.json'),
    ]
    source_data = None
    source_path = None
    for path in candidates:
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
        return jsonify({'success': False, 'error': 'Could not find stores.json. Tried: ' + ', '.join(candidates)}), 404

    # Extract relevant fields for connected stores
    synced = []
    raw_stores = source_data if isinstance(source_data, list) else source_data.get('stores', [])
    for s in raw_stores:
        if s.get('shopifyStatus') != 'connected':
            continue
        synced.append({
            'id': s.get('id', ''),
            'name': s.get('name', ''),
            'domain': s.get('domain', ''),
            'shopifyAccessToken': s.get('shopifyAccessToken', ''),
            'shopifyStatus': s.get('shopifyStatus', ''),
            'storeCategory': s.get('storeCategory', 'default'),
        })

    _save_stores(synced)
    return jsonify({'success': True, 'count': len(synced), 'source': source_path})


# ===== Routes: Settings =====
@app.route('/api/settings', methods=['GET'])
@login_required
def get_settings():
    settings = _load_settings()
    # Mask API key
    safe = dict(settings)
    if safe.get('xai_api_key'):
        key = safe['xai_api_key']
        safe['xai_api_key_masked'] = key[:8] + '...' + key[-4:] if len(key) > 12 else '***'
        safe['xai_api_key_set'] = True
    else:
        safe['xai_api_key_masked'] = ''
        safe['xai_api_key_set'] = False
    del safe['xai_api_key']
    return jsonify({'success': True, 'settings': safe})

@app.route('/api/settings', methods=['POST'])
@login_required
def save_settings():
    data = request.json
    settings = _load_settings()

    if 'xai_api_key' in data and data['xai_api_key']:
        settings['xai_api_key'] = data['xai_api_key']
    if 'video_duration' in data:
        settings['video_duration'] = int(data['video_duration'])
    if 'video_resolution' in data:
        settings['video_resolution'] = data['video_resolution']
    if 'prompt_templates' in data and isinstance(data['prompt_templates'], dict):
        settings['prompt_templates'].update(data['prompt_templates'])
    if 'gdrive_root_folder_id' in data:
        settings['gdrive_root_folder_id'] = data['gdrive_root_folder_id']

    _save_settings(settings)
    return jsonify({'success': True})

@app.route('/api/settings/test-xai', methods=['POST'])
@login_required
def test_xai_connection():
    """Test xAI API key by listing models."""
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
            return jsonify({'success': True, 'models': models, 'hasVideoModel': has_video})
        else:
            return jsonify({'success': False, 'error': f'API returned {resp.status_code}: {resp.text[:200]}'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


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
                    f'{base_url}/products.json?ids={batch_ids}&limit=250&fields=id,handle,images,product_type',
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
                                'product_type': prod.get('product_type', '')
                            }
                time.sleep(0.3)
            except:
                pass

        # Check which products already have video jobs
        jobs = _load_jobs()
        products_with_videos = set()
        for j in jobs:
            if j.get('storeId') == store_id and j.get('status') == 'done':
                products_with_videos.add(str(j.get('productId', '')))

        # Build results
        results = []
        for p in sorted_products:
            pid = p['product_id']
            detail = product_details.get(pid, {})
            handle = detail.get('handle', '')
            image_url = detail.get('image', '')
            product_type = detail.get('product_type', '')
            has_video = str(pid) in products_with_videos

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
                'hasVideo': has_video,
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
    """Queue video generation for selected products."""
    data = request.json
    products = data.get('products', [])
    if not products:
        return jsonify({'success': False, 'error': 'No products provided'}), 400

    settings = _load_settings()
    if not settings.get('xai_api_key'):
        return jsonify({'success': False, 'error': 'xAI API key not configured. Go to Settings tab.'}), 400

    new_jobs = []
    with _jobs_lock:
        jobs = _load_jobs()
        for p in products:
            job_id = f'job_{uuid.uuid4().hex[:12]}'
            prompt = _get_prompt(p.get('productName', ''), p.get('storeCategory', ''), settings)
            job = {
                'id': job_id,
                'storeId': p.get('storeId', ''),
                'storeName': p.get('storeName', ''),
                'productId': str(p.get('productId', '')),
                'productName': p.get('productName', ''),
                'productHandle': p.get('handle', ''),
                'imageUrl': p.get('imageUrl', ''),
                'storeCategory': p.get('storeCategory', ''),
                'status': 'queued',
                'xaiRequestId': None,
                'videoUrl': None,
                'localPath': None,
                'driveUrl': None,
                'error': None,
                'createdAt': datetime.datetime.utcnow().isoformat() + 'Z',
                'completedAt': None,
                'prompt': prompt
            }
            jobs.append(job)
            new_jobs.append(job)
        _save_jobs(jobs)

    _ensure_worker()
    return jsonify({'success': True, 'queued': len(new_jobs), 'jobs': new_jobs})

@app.route('/api/videos/queue', methods=['GET'])
@login_required
def get_queue():
    """Get all video jobs with status."""
    jobs = _load_jobs()
    # Most recent first
    jobs.sort(key=lambda j: j.get('createdAt', ''), reverse=True)
    return jsonify({'success': True, 'jobs': jobs})

@app.route('/api/videos/<job_id>/retry', methods=['POST'])
@login_required
def retry_job(job_id):
    with _jobs_lock:
        jobs = _load_jobs()
        for j in jobs:
            if j['id'] == job_id and j['status'] == 'failed':
                j['status'] = 'queued'
                j['error'] = None
                j['xaiRequestId'] = None
                j['videoUrl'] = None
                j['localPath'] = None
                j['completedAt'] = None
                break
        _save_jobs(jobs)
    _ensure_worker()
    return jsonify({'success': True})

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

@app.route('/api/videos/<job_id>/download', methods=['GET'])
@login_required
def download_video(job_id):
    jobs = _load_jobs()
    job = next((j for j in jobs if j['id'] == job_id), None)
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


# ===== Routes: Gallery =====
@app.route('/api/gallery', methods=['GET'])
@login_required
def get_gallery():
    """Get all videos organized by store/product."""
    jobs = _load_jobs()
    done_jobs = [j for j in jobs if j['status'] == 'done' and j.get('localPath')]

    # Organize by store -> product
    gallery = {}
    for j in done_jobs:
        store_name = j.get('storeName', 'Unknown Store')
        product_name = j.get('productName', 'Unknown Product')
        handle = j.get('productHandle', 'unknown')

        if store_name not in gallery:
            gallery[store_name] = {'name': store_name, 'storeId': j.get('storeId', ''), 'products': {}}

        if handle not in gallery[store_name]['products']:
            gallery[store_name]['products'][handle] = {
                'name': product_name,
                'handle': handle,
                'videos': []
            }

        gallery[store_name]['products'][handle]['videos'].append({
            'jobId': j['id'],
            'localPath': j['localPath'],
            'videoUrl': f"/data/videos/{j['localPath']}",
            'createdAt': j.get('createdAt', ''),
            'completedAt': j.get('completedAt', ''),
            'prompt': j.get('prompt', ''),
            'driveUrl': j.get('driveUrl'),
            'imageUrl': j.get('imageUrl', '')
        })

    # Convert to list format
    result = []
    for store_name, store_data in sorted(gallery.items()):
        products = []
        for handle, prod_data in sorted(store_data['products'].items()):
            products.append(prod_data)
        result.append({
            'name': store_data['name'],
            'storeId': store_data['storeId'],
            'products': products,
            'totalVideos': sum(len(p['videos']) for p in products)
        })

    total = sum(s['totalVideos'] for s in result)
    return jsonify({'success': True, 'stores': result, 'totalVideos': total})


# ===== Routes: Google Drive (placeholder for MVP) =====
@app.route('/api/drive/upload/<job_id>', methods=['POST'])
@login_required
def upload_to_drive(job_id):
    return jsonify({'success': False, 'error': 'Google Drive integration not yet configured. Videos are stored locally.'}), 501

@app.route('/api/drive/folders', methods=['GET'])
@login_required
def drive_folders():
    return jsonify({'success': False, 'error': 'Google Drive integration not yet configured.'}), 501


# ===== Main =====
if __name__ == '__main__':
    _ensure_worker()
    app.run(host='0.0.0.0', port=5110, debug=False)
