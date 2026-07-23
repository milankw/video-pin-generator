"""Facebook Creatives tab — generate 4:5 jewelry ads with Claude + Nano Banana 2.

Flow:
  1. UI sends {store_id, preset|prompt, reference_image_urls[], extra_notes,
               variant_count}.
  2. Claude Haiku 4.5 receives the brief plus image analysis of the reference
     jewelry pieces, and returns a JSON plan of N variants — each variant has:
       angle_name  ("editorial luxe" | "UGC raw" | ...),
       image_prompt (a rich, detailed Nano Banana 2 prompt including 4:5 aspect
                     ratio, lighting, mood, composition, and how to render THE
                     actual jewelry piece from the reference image),
       primary_text (Facebook primary text, 90-120 words, hook-driven),
       headline (max 40 chars),
       description (max 30 chars),
       cta ('SHOP_NOW'|'LEARN_MORE'|...),
       hook_alternatives (list of 3 scroll-stopper first-line variants).
  3. For each variant, Nano Banana 2 (gemini-2.5-flash-image) is called with
     the image_prompt AND the reference image bytes as inline data so it
     generates a photorealistic 4:5 ad featuring the actual product, not a
     generic ring.
  4. All images + a copy.txt per variant + a run.json are zipped into
     data/fb_creatives_runs/<run_id>.zip and served via /api/fb-creatives/download/<id>.

Costs (approx):
  - 1 Claude Haiku call   ~ $0.02
  - N image calls @ Nano Banana 2  ~ $0.039 each
  - 5-variant run          ~ $0.22

Endpoints (registered via register_routes()):
  POST /api/fb-creatives/top-sellers  body: {store_id, limit}
      -> [{id, title, image, sales, revenue, url}, ...]
  POST /api/fb-creatives/generate     body: {store_id, prompt, preset,
                                             reference_images:[url|dataurl],
                                             variant_count, extra_notes}
      -> {ok, run_id, variants:[{index, angle_name, image_url, primary_text,
                                 headline, description, cta, hook_alternatives}]}
  GET  /api/fb-creatives/download/<run_id>            -> ZIP
  GET  /api/fb-creatives/image/<run_id>/<idx>.png     -> PNG (for UI preview)
"""
from __future__ import annotations

import base64
import io
import json
import logging
import mimetypes
import os
import re
import tempfile
import time
import uuid
import zipfile
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import jsonify, request, send_file

log = logging.getLogger(__name__)


# --- Preset ad angles -------------------------------------------------------
# Each preset seeds Claude with a strong point of view so the 5 variants feel
# genuinely different, not five safe near-duplicates.
PRESETS: Dict[str, Dict[str, str]] = {
    'auto_mix': {
        'label': 'Auto mix — 5 different angles',
        'brief': 'Give me 5 wildly different ad angles across these six styles: '
                 'lifestyle close-up, editorial luxe, UGC raw testimonial, '
                 'problem→solution, social proof + scarcity, and emotional gift story. '
                 'Do NOT repeat angles.',
    },
    'lifestyle': {
        'label': 'Lifestyle close-up (hand/neck worn)',
        'brief': 'Aspirational close-up of the jewelry worn on hand, neck, or ear. '
                 'Natural daylight, soft background bokeh, real skin, minimal styling. '
                 'The piece is the hero; the person is anonymous (no full face).',
    },
    'editorial': {
        'label': 'Editorial luxe (magazine cover)',
        'brief': 'High-fashion editorial. Dark or moody background, dramatic side lighting, '
                 'jewelry photographed like it belongs in Vogue. Copy is confident, minimal, '
                 'aspirational. Feels expensive.',
    },
    'ugc': {
        'label': 'UGC raw (just-bought testimonial)',
        'brief': 'Amateur phone-camera aesthetic. Slightly imperfect framing, natural window '
                 'light, honest testimonial-style copy. First-person hook like "I did NOT '
                 'expect this to look this good in real life." Feels like a friend recommending it.',
    },
    'problem_solution': {
        'label': 'Problem → solution',
        'brief': 'Hook opens with a specific pain point about jewelry (tarnishing, allergies, '
                 'losing pieces, boring gifts, cheap-looking stuff) and positions this piece as '
                 'the solution. Copy is direct, punchy, ends with a clear reason to click today.',
    },
    'social_proof': {
        'label': 'Social proof + scarcity',
        'brief': 'Lead with numbers: how many customers bought, star rating, best-seller status. '
                 'Combine with genuine scarcity if honest ("selling out weekly", "restocked twice '
                 'this month"). Image is clean, product-forward, with a subtle "best-seller" feel.',
    },
    'gift': {
        'label': 'Emotional gift story',
        'brief': 'Position the piece as the perfect gift for a specific relationship '
                 '(anniversary, birthday, "for her", self-gift after a milestone). Copy tells a '
                 'micro-story in 2 sentences. Ends with an emotional CTA, not a discount.',
    },
}


CLAUDE_SYSTEM_PROMPT = """You are a senior Facebook Ads creative director for a jewelry DTC brand.
You produce ads that STOP THE SCROLL. You know Meta's rules, Meta's algorithm, and jewelry buyers' psychology.

Rules for every ad you write:
- Format is ALWAYS 4:5 vertical single-image (feed).
- Primary text: 90-120 words. First line MUST be a scroll-stopper (question, bold claim, curiosity gap, or emotional hook).
- Headline: max 40 characters. Punchy. No brand name unless requested.
- Description: max 30 characters. Reinforces urgency or benefit.
- CTA: pick ONE from SHOP_NOW, LEARN_MORE, GET_OFFER, ORDER_NOW.
- Hook alternatives: 3 short (5-12 word) alternate first-lines with DIFFERENT emotional angles (curiosity, pain, aspiration).
- Never claim medical benefits. Never use "cures", "guaranteed", "best price".
- Use sensory, concrete language — "brushed gold catches the sunset light" beats "beautiful jewelry".
- Emojis: max 2, only if they add meaning.

For the image_prompt (which will be sent to Google Nano Banana 2):
- Start with: "4:5 vertical Facebook ad. Photorealistic."
- Then describe the exact scene: lighting, background, composition, mood.
- IMPORTANT: The reference image(s) provided show the ACTUAL jewelry piece. Describe the piece faithfully — same shape, metal color, gemstones, engraving — so the AI renders THE product, not a random ring.
- Include the phrase "the exact piece shown in the reference image" once.
- End with: "Sharp focus on the jewelry. Ad-quality composition. No text overlays."
- Never request text overlays in the image (we add copy in Meta Ads Manager).

You will be given: {N} = number of variants required, a preset angle brief, an optional user prompt, extra notes about audience/offer, and 1-{max_refs} reference product images.
Return STRICT JSON with this exact shape:
{
  "variants": [
    {
      "angle_name": "...",
      "image_prompt": "...",
      "primary_text": "...",
      "headline": "...",
      "description": "...",
      "cta": "SHOP_NOW",
      "hook_alternatives": ["...", "...", "..."]
    }
  ]
}
No commentary outside the JSON."""


# ---------------------------------------------------------------------------

def _settings_path(data_dir: str) -> str:
    return os.path.join(data_dir, 'settings.json')


def _load_settings(data_dir: str) -> Dict[str, Any]:
    try:
        with open(_settings_path(data_dir)) as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _runs_dir(data_dir: str) -> str:
    d = os.path.join(data_dir, 'fb_creatives_runs')
    os.makedirs(d, exist_ok=True)
    return d


# --- Top-sellers ------------------------------------------------------------

def _load_winner_cache(data_dir: str, store_id: str) -> Dict[str, Any]:
    path = os.path.join(data_dir, 'winner_cache', f'{store_id}.json')
    try:
        with open(path) as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _load_stores(data_dir: str) -> List[Dict[str, Any]]:
    try:
        with open(os.path.join(data_dir, 'stores.json')) as f:
            return json.load(f) or []
    except Exception:
        return []


def _fetch_shopify_images(store: Dict[str, Any], product_ids: List[str],
                          http_requests) -> Dict[str, Dict[str, Any]]:
    """Return {product_id -> {image, handle, title}} for the requested ids."""
    domain = store.get('domain') or ''
    token = store.get('shopifyAccessToken') or ''
    if not (domain and token and product_ids):
        return {}
    base = f'https://{domain}/admin/api/2024-01'
    out: Dict[str, Dict[str, Any]] = {}
    # Batch in groups of 100
    for i in range(0, len(product_ids), 100):
        batch = product_ids[i:i + 100]
        try:
            r = http_requests.get(
                f'{base}/products.json',
                params={'ids': ','.join(batch), 'limit': 100,
                        'fields': 'id,handle,title,image'},
                headers={'X-Shopify-Access-Token': token},
                timeout=15,
            )
            if r.status_code != 200:
                continue
            for p in (r.json() or {}).get('products') or []:
                pid = str(p.get('id') or '')
                img = ''
                if isinstance(p.get('image'), dict):
                    img = p['image'].get('src') or ''
                out[pid] = {
                    'image': img,
                    'handle': p.get('handle') or '',
                    'title': p.get('title') or '',
                }
        except Exception as e:  # noqa: BLE001
            log.debug('shopify img fetch failed: %s', e)
    return out


# --- Claude brief-generation -----------------------------------------------

def _download_image_bytes(url: str) -> Tuple[bytes, str]:
    """Return (bytes, mime) for an http(s) image URL or a data: URL."""
    if not url:
        return b'', ''
    if url.startswith('data:'):
        try:
            header, b64 = url.split(',', 1)
            mime = header.split(';')[0].replace('data:', '') or 'image/jpeg'
            return base64.b64decode(b64), mime
        except Exception:
            return b'', ''
    r = requests.get(url, timeout=20, stream=True)
    r.raise_for_status()
    mime = r.headers.get('Content-Type', '').split(';')[0].strip() or 'image/jpeg'
    if not mime.startswith('image/'):
        mime = 'image/jpeg'
    data = r.content
    # If very large, keep as-is; Claude accepts up to ~5MB per image.
    return data, mime


def _generate_briefs_with_claude(anthropic_key: str, prompt: str, preset_brief: str,
                                  extra_notes: str, reference_images: List[Tuple[bytes, str]],
                                  variant_count: int) -> Dict[str, Any]:
    """Call Claude Haiku 4.5 with the reference product images inline and get a
    strict-JSON plan of N variants back."""
    if not anthropic_key:
        raise RuntimeError('Anthropic API key missing in settings.json')

    max_refs = min(len(reference_images), 5)
    sys_prompt = (CLAUDE_SYSTEM_PROMPT
                  .replace('{N}', str(variant_count))
                  .replace('{max_refs}', str(max_refs)))

    user_content: List[Dict[str, Any]] = []
    # Attach reference images first so Claude anchors its plan on them.
    for i, (b, mime) in enumerate(reference_images[:5]):
        user_content.append({
            'type': 'image',
            'source': {
                'type': 'base64',
                'media_type': mime,
                'data': base64.b64encode(b).decode('ascii'),
            },
        })

    user_text_parts = [f'REFERENCE PRODUCT IMAGES: {max_refs} attached above. These are '
                       f'the jewelry pieces the ads must feature.']
    if preset_brief:
        user_text_parts.append(f'PRESET ANGLE: {preset_brief}')
    if prompt:
        user_text_parts.append(f'USER PROMPT: {prompt}')
    if extra_notes:
        user_text_parts.append(f'EXTRA NOTES (audience, offer, brand voice): {extra_notes}')
    user_text_parts.append(f'PRODUCE EXACTLY {variant_count} VARIANTS. Return only JSON.')

    user_content.append({'type': 'text', 'text': '\n\n'.join(user_text_parts)})

    body = {
        'model': 'claude-haiku-4-5',
        'max_tokens': 4000,
        'system': sys_prompt,
        'messages': [{'role': 'user', 'content': user_content}],
    }

    r = requests.post(
        'https://api.anthropic.com/v1/messages',
        headers={
            'x-api-key': anthropic_key,
            'anthropic-version': '2023-06-01',
            'content-type': 'application/json',
        },
        data=json.dumps(body),
        timeout=90,
    )
    if r.status_code != 200:
        raise RuntimeError(f'Claude error {r.status_code}: {r.text[:400]}')
    data = r.json()
    text = ''
    for block in data.get('content') or []:
        if block.get('type') == 'text':
            text += block.get('text', '')

    # Extract JSON — Claude sometimes wraps in ```json fences even when told not to.
    text = text.strip()
    m = re.search(r'\{.*\}', text, re.DOTALL)
    if not m:
        raise RuntimeError(f'Claude returned no JSON: {text[:400]}')
    try:
        parsed = json.loads(m.group(0))
    except json.JSONDecodeError as e:
        raise RuntimeError(f'Claude JSON decode failed: {e}. Raw: {text[:400]}')
    if 'variants' not in parsed or not isinstance(parsed['variants'], list):
        raise RuntimeError(f'Claude response missing variants[]: {text[:400]}')
    return parsed


# --- Nano Banana 2 image generation ----------------------------------------

def _generate_image_with_nano_banana(google_key: str, prompt: str,
                                      reference_images: List[Tuple[bytes, str]]) -> bytes:
    """Call gemini-2.5-flash-image with reference images and prompt.
    Returns raw image bytes (PNG)."""
    if not google_key:
        raise RuntimeError('Google AI Studio key missing in settings.json')

    parts: List[Dict[str, Any]] = [{'text': prompt}]
    for b, mime in reference_images[:5]:
        parts.append({
            'inline_data': {
                'mime_type': mime,
                'data': base64.b64encode(b).decode('ascii'),
            },
        })

    body = {
        'contents': [{'parts': parts}],
        'generationConfig': {
            'responseModalities': ['IMAGE'],
            'imageConfig': {'aspectRatio': '4:5'},
        },
    }
    url = ('https://generativelanguage.googleapis.com/v1beta/models/'
           'gemini-2.5-flash-image:generateContent?key=' + google_key)
    r = requests.post(url, headers={'Content-Type': 'application/json'},
                      data=json.dumps(body), timeout=90)
    if r.status_code != 200:
        raise RuntimeError(f'Nano Banana error {r.status_code}: {r.text[:400]}')
    data = r.json()
    # Extract inline image bytes.
    for cand in data.get('candidates') or []:
        for part in ((cand.get('content') or {}).get('parts') or []):
            if 'inlineData' in part or 'inline_data' in part:
                inline = part.get('inlineData') or part.get('inline_data') or {}
                b64 = inline.get('data') or ''
                if b64:
                    return base64.b64decode(b64)
    raise RuntimeError(f'Nano Banana returned no image bytes: {json.dumps(data)[:400]}')


# --- ZIP export -------------------------------------------------------------

def _copy_txt_for_variant(v: Dict[str, Any]) -> str:
    lines = [
        f'ANGLE: {v.get("angle_name", "")}',
        '',
        '── PRIMARY TEXT ──',
        v.get('primary_text', ''),
        '',
        f'── HEADLINE ── ({len(v.get("headline",""))} chars)',
        v.get('headline', ''),
        '',
        f'── DESCRIPTION ── ({len(v.get("description",""))} chars)',
        v.get('description', ''),
        '',
        f'── CTA ── {v.get("cta", "SHOP_NOW")}',
        '',
        '── HOOK ALTERNATIVES ──',
    ]
    for i, h in enumerate(v.get('hook_alternatives') or [], 1):
        lines.append(f'{i}. {h}')
    return '\n'.join(lines)


def _build_zip(run_dir: str, run_id: str, variants: List[Dict[str, Any]],
               metadata: Dict[str, Any]) -> str:
    """Zip every image + copy.txt + run.json under one archive. Returns path."""
    zip_path = os.path.join(run_dir, f'{run_id}.zip')
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for i, v in enumerate(variants, 1):
            angle_slug = re.sub(r'[^a-z0-9]+', '_',
                                (v.get('angle_name') or f'variant_{i}').lower()).strip('_')[:40]
            img_bytes = v.get('_image_bytes') or b''
            if img_bytes:
                zf.writestr(f'variant_{i:02d}_{angle_slug}.png', img_bytes)
            zf.writestr(f'variant_{i:02d}_{angle_slug}.txt', _copy_txt_for_variant(v))
        zf.writestr('run.json', json.dumps({
            'run_id': run_id,
            'metadata': metadata,
            'variants': [{k: v[k] for k in v if not k.startswith('_')} for v in variants],
        }, indent=2))
    return zip_path


# --- Flask routes -----------------------------------------------------------

def register_routes(app, data_dir, login_required, http_requests):
    runs_root = _runs_dir(data_dir)

    @app.route('/api/fb-creatives/top-sellers', methods=['POST'])
    @login_required
    def top_sellers():
        body = request.get_json(silent=True) or {}
        store_id = (body.get('store_id') or '').strip()
        limit = int(body.get('limit') or 24)
        if not store_id:
            return jsonify({'ok': False, 'error': 'store_id required'}), 400
        stores = _load_stores(data_dir)
        store = next((s for s in stores if s.get('id') == store_id), None)
        if not store:
            return jsonify({'ok': False, 'error': 'store not found'}), 404

        cache = _load_winner_cache(data_dir, store_id)
        # Sort by units sold desc.
        entries = []
        for pid, prod in cache.items():
            if not isinstance(prod, dict):
                continue
            entries.append({
                'id': str(prod.get('product_id') or pid),
                'title': prod.get('title') or '',
                'sales': int(prod.get('quantity') or 0),
                'revenue': float(prod.get('revenue') or 0.0),
                'image': prod.get('fallback_image') or '',
                'url': prod.get('product_url') or '',
            })
        entries.sort(key=lambda p: p['sales'], reverse=True)
        entries = entries[:limit]

        # Fetch images for any missing ones.
        ids_missing = [p['id'] for p in entries if not p['image'] and p['id']]
        if ids_missing:
            enrich = _fetch_shopify_images(store, ids_missing, http_requests)
            for p in entries:
                info = enrich.get(p['id'])
                if info:
                    p['image'] = info.get('image') or p['image']
                    if not p['url'] and info.get('handle'):
                        p['url'] = f'https://{store.get("domain","")}/products/{info["handle"]}'

        return jsonify({'ok': True, 'products': entries})

    @app.route('/api/fb-creatives/generate', methods=['POST'])
    @login_required
    def generate():
        body = request.get_json(silent=True) or {}
        prompt = (body.get('prompt') or '').strip()
        preset = (body.get('preset') or 'auto_mix').strip()
        extra_notes = (body.get('extra_notes') or '').strip()
        variant_count = max(1, min(int(body.get('variant_count') or 5), 8))
        reference_image_urls = body.get('reference_images') or []

        if not reference_image_urls:
            return jsonify({'ok': False, 'error':
                            'Attach at least one reference product image (pick from top sellers).'}), 400

        preset_brief = PRESETS.get(preset, {}).get('brief', '') if preset else ''
        if not (prompt or preset_brief):
            return jsonify({'ok': False, 'error':
                            'Pick a preset OR write a prompt.'}), 400

        settings = _load_settings(data_dir)
        anthropic_key = settings.get('anthropic_api_key') or ''
        google_key = settings.get('google_ai_key') or ''
        if not anthropic_key:
            return jsonify({'ok': False, 'error': 'Set anthropic_api_key in Settings.'}), 400
        if not google_key:
            return jsonify({'ok': False, 'error': 'Set google_ai_key in Settings.'}), 400

        # Download reference bytes.
        refs: List[Tuple[bytes, str]] = []
        for u in reference_image_urls[:5]:
            try:
                b, mime = _download_image_bytes(u)
                if b:
                    refs.append((b, mime))
            except Exception as e:  # noqa: BLE001
                log.warning('ref image fetch failed: %s | %s', u, e)

        if not refs:
            return jsonify({'ok': False, 'error':
                            'None of the reference images could be downloaded.'}), 400

        run_id = uuid.uuid4().hex[:12]
        started = time.time()

        # 1) Claude — plan N variants
        try:
            plan = _generate_briefs_with_claude(
                anthropic_key=anthropic_key,
                prompt=prompt,
                preset_brief=preset_brief,
                extra_notes=extra_notes,
                reference_images=refs,
                variant_count=variant_count,
            )
        except Exception as e:  # noqa: BLE001
            log.exception('Claude brief failed')
            return jsonify({'ok': False, 'error': f'Claude brief generation failed: {e}'}), 500

        variants = plan.get('variants') or []
        if not variants:
            return jsonify({'ok': False, 'error': 'Claude produced 0 variants.'}), 500
        variants = variants[:variant_count]

        # 2) Nano Banana 2 per variant. Persist each PNG under runs/<run_id>/
        run_dir = os.path.join(runs_root, run_id)
        os.makedirs(run_dir, exist_ok=True)

        successes = 0
        for i, v in enumerate(variants, 1):
            img_prompt = v.get('image_prompt') or ''
            try:
                img_bytes = _generate_image_with_nano_banana(
                    google_key=google_key,
                    prompt=img_prompt,
                    reference_images=refs,
                )
                v['_image_bytes'] = img_bytes
                with open(os.path.join(run_dir, f'{i}.png'), 'wb') as f:
                    f.write(img_bytes)
                v['image_url'] = f'/api/fb-creatives/image/{run_id}/{i}.png'
                v['index'] = i
                successes += 1
            except Exception as e:  # noqa: BLE001
                log.exception('image gen failed for variant %s', i)
                v['image_url'] = ''
                v['error'] = str(e)
                v['index'] = i

        # 3) Zip
        metadata = {
            'run_id': run_id,
            'created_at': int(started),
            'duration_sec': round(time.time() - started, 1),
            'preset': preset,
            'prompt': prompt,
            'extra_notes': extra_notes,
            'variant_count': variant_count,
            'successes': successes,
            'ref_image_count': len(refs),
        }
        _build_zip(run_dir, run_id, variants, metadata)

        # Strip binary bytes before returning
        variants_out = [{k: v[k] for k in v if not k.startswith('_')} for v in variants]

        return jsonify({
            'ok': True,
            'run_id': run_id,
            'variants': variants_out,
            'download_url': f'/api/fb-creatives/download/{run_id}',
            'metadata': metadata,
        })

    @app.route('/api/fb-creatives/image/<run_id>/<int:idx>.png', methods=['GET'])
    @login_required
    def serve_image(run_id: str, idx: int):
        if not re.match(r'^[a-f0-9]{6,32}$', run_id or ''):
            return jsonify({'ok': False, 'error': 'bad run_id'}), 400
        path = os.path.join(runs_root, run_id, f'{idx}.png')
        if not os.path.isfile(path):
            return jsonify({'ok': False, 'error': 'not found'}), 404
        return send_file(path, mimetype='image/png')

    @app.route('/api/fb-creatives/download/<run_id>', methods=['GET'])
    @login_required
    def download(run_id: str):
        if not re.match(r'^[a-f0-9]{6,32}$', run_id or ''):
            return jsonify({'ok': False, 'error': 'bad run_id'}), 400
        path = os.path.join(runs_root, run_id, f'{run_id}.zip')
        if not os.path.isfile(path):
            return jsonify({'ok': False, 'error': 'not found'}), 404
        return send_file(path, mimetype='application/zip', as_attachment=True,
                         download_name=f'fb_creatives_{run_id}.zip')

    @app.route('/api/fb-creatives/presets', methods=['GET'])
    @login_required
    def list_presets():
        return jsonify({'ok': True, 'presets': [
            {'key': k, 'label': v['label']} for k, v in PRESETS.items()
        ]})

    log.info('Facebook Creatives routes registered at /api/fb-creatives/*')
