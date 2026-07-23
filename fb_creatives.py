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
# The Goth Society tested ad angles. Each preset is written the way the brand
# actually talks to its audience — not generic marketer language.
PRESETS: Dict[str, Dict[str, str]] = {
    'auto_mix': {
        'label': 'Auto mix — rotate the 7 proven angles',
        'brief': 'Rotate through The Goth Society\'s 7 proven angles. Pick a mix that gives me '
                 'DIFFERENT emotional territories — do not do 5 versions of the same idea. '
                 'Available angles: Corporate Goth, Anti-Consumerism, Baby Bats, 7-for-$85 Bundle, '
                 'Identity Confirmation, Outcast, Gothic Memes. Pick the N most distinct.',
    },
    'corporate_goth': {
        'label': 'Corporate Goth — goth in the office / meeting',
        'brief': 'Angle: goth aesthetics inside the 9-to-5. The character wears a black blazer, '
                 'muted business attire, but keeps subtle goth signals — the ring, the choker, '
                 'the black manicure. Setting: an office, coffee shop, conference room, or '
                 'commuter train. Copy hits the tension between "having to look normal" and '
                 '"staying true to the aesthetic". Hooks like: "How I stayed goth through 3 years '
                 'in corporate." / "The one ring my boss never noticed." / "Business casual, '
                 'privately unholy." Never mention being fired or corporate abuse — keep it '
                 'empowering, not victim-y.',
    },
    'anti_consumerism': {
        'label': 'Anti-Consumerism — anti fast fashion / child labor',
        'brief': 'Angle: real goth culture rejects fast fashion, sweatshops, and disposable '
                 'trend-chasing. Position The Goth Society as jewelry that lasts, made for the '
                 'lifestyle, not the algorithm. Copy is confident and slightly confrontational, '
                 'not preachy. Reference the ethos: quality, longevity, culture-over-clout. '
                 'Example energy: "Real goth culture was never about who could buy the most — '
                 'it\'s about what you stand for. Mine\'s linked below." Never make specific '
                 'unverifiable claims about competitors (no "brand X uses child labor"). Focus '
                 'on WHAT WE DO: solid materials, no throwaway pieces, aesthetic that outlasts '
                 'trends.',
    },
    'baby_bats': {
        'label': 'Baby Bats — new to goth, welcoming starter energy',
        'brief': 'Angle: written for someone new to goth ("baby bat" is the community\'s '
                 'affectionate term for a newcomer). Copy is warm, insider, welcoming — like a '
                 'friend who has been in the scene for years showing them the ropes. Hooks like: '
                 '"7 pieces every baby bat needs before their first event." / "New to the scene? '
                 'Start here." / "Me before finding this brand vs me now." Lean into the "Baby Bat '
                 'arc complete" transformation-story energy. Never condescend — baby bats know '
                 'they are new and are proud of it.',
    },
    'bundle_deal': {
        'label': '7 for $85 Bundle — the offer, hard',
        'brief': 'Angle: promote the flagship offer — "Any 7 for $85 • Free Delivery." This is a '
                 'straight commercial hitter. Copy is short, benefit-forward, urgency without lies. '
                 'Hook opens with the math: "$12 per ring. Seven picks. One flat price." or '
                 '"Build your set — any 7 for $85." Headline hammers the offer. Image is a product '
                 'hero shot: single macro ring/earring on wet black stone or velvet, dramatic '
                 'overhead spotlight, wisps of purple smoke, jewelry glinting. Layout: TOP: massive '
                 'blackletter or Impact headline like "THIS IS WHAT $85 BUILDS" or "PICK 7. PAY '
                 '$85. NO CATCH." BOTTOM: full-width white bar with "ANY 7 FOR $85 · FREE '
                 'DELIVERY".',
    },
    'identity': {
        'label': 'Identity Confirmation — "this is who you are"',
        'brief': 'Angle: the ad reads like a permission slip / identity confirmation. Copy hits '
                 '"you already know" energy. Hooks like: "You\'re not going through a phase." / '
                 '"Confirmed goth. Move to checkout." / "You didn\'t choose this aesthetic — it '
                 'chose you." / "Some people are just wired darker." The reader should feel seen, '
                 'not sold to. Copy in first-person or direct-address ("you"). No cringe — no '
                 '"embrace your dark side" cliches. Feel like a mirror, not a pitch.',
    },
    'outcast': {
        'label': 'Outcast — not for the normies',
        'brief': 'Angle: gatekeep in a fun, community-building way. The ad is FOR people who never '
                 'fit in with the "normies" (the community\'s in-group word for basic mainstream '
                 'people). Hooks like: "Not for normies." / "If you have to ask, this brand isn\'t '
                 'for you." / "The normies stare. Let them." / "Made for the ones who never fit in '
                 'the first place." Empowering, tribal, tongue-in-cheek. Never punch down at any '
                 'group — just create insider/outsider dynamics around aesthetic taste. Copy '
                 'should reward the reader for being different.',
    },
    'gothic_memes': {
        'label': 'Gothic Memes — meme headline over macro jewelry',
        'brief': 'Angle: meme-format headline HUMOUR (not comic-panel illustration). This is the '
                 'brand\'s highest-performing lane. Layout: TOP 25-30% is a two-line all-caps '
                 'Impact headline that reads like a text a real goth sent her group chat — '
                 'specific, weird, funny. Working examples that shipped: "MY DOG GROWLED AT THE '
                 'RING. MY DOG HAS NEVER BEEN WRONG." / "WORE THE EARRINGS TO A WEDDING. BECAME '
                 'THE TOPIC OF THREE TOASTS." / "ONE RING TO RUIN FAMILY DINNERS". MIDDLE 55-65% '
                 'is a photorealistic macro shot of the exact ring/earring from the reference '
                 'image in a specific setting that matches the joke (family dinner table with '
                 'tattooed hand, silver branch with black velvet, black stone slab with cat '
                 'shadow). BOTTOM 15% is a full-width white bar with "ANY 7 FOR $85" in black '
                 'Impact. Never draw comic panels or illustrated cartoons — always '
                 'photorealistic macro jewelry with meme headline text baked on top. Humour is '
                 'wholesome-shock contrast; never mock religion, family, or individuals.',
    },
}


CLAUDE_SYSTEM_PROMPT = """You are the creative director for THE GOTH SOCIETY, a DTC goth jewelry brand selling skull rings, spider rings, chokers, wrap rings, dragon rings and layered chains. You write Facebook ads that stop the scroll for goths, alt kids, metalheads, witchy communities. You know the subculture from the inside — not as an outsider marketer.

=== BRAND CONTEXT ===
- Flagship offer: ANY 7 FOR $85 - FREE DELIVERY. Reference it when the angle asks for it.
- Founder voice: first-person, casual, slightly conspiratorial. Sign-offs like "mine's linked below 🤍" work.
- Audience: goth / alt / darkwave / metal / witchy — mostly women 18-40, some men. They ALREADY identify as goth. Do not explain what goth is to them.
- In-group words to use CORRECTLY: baby bat (newcomer to goth), normies (mainstream / basic people), the scene, the aesthetic, coven, spooky. Use sparingly, never in every ad.
- BANNED cringe phrases (never use): "embrace your dark side", "unleash your inner goth", "dark elegance", "unique statement piece", "turn heads", "gothic vibes", "express yourself", "gothic beauty". These mark you as a fake.
- Never preach. Values (anti-fast-fashion, culture-over-clout) can be HINTED, never lectured.
- Never mock religion, family, or named individuals. Meme humor is at wholesome-shock contrast ("grandma prays for me daily"), never real hate.

=== THE WINNING LAYOUT SKELETON (CRITICAL — every image must obey) ===
The Goth Society's proven meta-ad format has THREE fixed zones. Never deviate. Never invent thin text strips, side captions, or scattered layouts.

  ┌──────────────────────────────────┐
  │  HEADLINE ZONE (top 22-30%)      │  huge on-image text, 2 lines
  ├──────────────────────────────────┤
  │  HERO ZONE (middle 55-65%)       │  ONE macro jewelry piece
  │  single piece from the reference │  fills 45-65% of frame width
  │  image, dead-center, dramatic    │  editorial macro photography
  │  lighting, dark rich backdrop    │  shallow depth of field
  ├──────────────────────────────────┤
  │  OFFER BAR (bottom 12-18%)       │  full-width white bar,
  │  "ANY 7 FOR $85" black text      │  black Impact text
  └──────────────────────────────────┘

Optional variants of the skeleton (still 4:5, still three zones):
- MEME variant: top zone = 2-line narrative caption (Impact white on black or black on white). Bottom zone = "ANY 7 FOR $85" bar.
- FULL-BLEED variant: no top bar — headline is baked directly over the top of the photo in massive Impact white text with strong drop shadow, covering top 25-30%. Bottom bar still present.
- BLACKLETTER HERO variant: massive blackletter title fills top 30-35% ("THIS IS WHAT $85 BUILDS"). Bottom = single-line offer bar.

DO NOT PRODUCE: centered thin text strips, side-panel text, multi-panel collages (unless meme angle), busy backgrounds, models where the jewelry is a background prop.

=== FACEBOOK-ADS HARD RULES ===
- Format: 4:5 vertical single image, feed placement, 1080x1350.
- Primary text: 60-110 words. First line is the scroll-stopper. Insert blank lines for mobile readability.
- Headline: MAX 40 characters. Punchy. Usually the offer or the angle payoff.
- Description: MAX 30 characters. Reinforces urgency / offer / identity.
- CTA: exactly ONE of SHOP_NOW, LEARN_MORE, GET_OFFER, ORDER_NOW.
- Emojis: max 2 per ad body copy. On-brand: 🤍 🕷️ 🕯️ 🌙 ⚰️. Off-brand: ✨ 💫 🔥 💜. NEVER put emojis inside the image itself.
- Never: "cures", "guaranteed", "best price", "unbeatable", unverifiable competitor claims, medical/protective claims stated as fact.

=== ON-IMAGE HEADLINE = A NARRATIVE MOMENT, NOT A SLOGAN ===
The winning ads from this brand all use the same headline formula: a specific, weirdly funny, oddly-specific micro-story a real goth would text her group chat. Examples that shipped and worked:
  - "MY DOG GROWLED AT THE RING. MY DOG HAS NEVER BEEN WRONG."
  - "WORE THE EARRINGS TO A WEDDING. BECAME THE TOPIC OF THREE TOASTS."
  - "ONE RING TO RUIN FAMILY DINNERS"
  - "THIS IS WHAT $85 BUILDS"
  - "GOTH PAYS BILLS" (short punch variant)

Losing headlines this brand REJECTS:
  - "YOUR FIRST PIECES ARE HERE" (generic welcome)
  - "YOU ALREADY KNEW" (vague, no story)
  - "NOT FOR EVERYONE" (cliche)
  - "SEVEN PIECES, ONE CHOICE" (marketer-y, no image)

Every on-image headline you write MUST be one of:
  (a) a two-clause micro-story ("[SPECIFIC MOMENT]. [DRY PAYOFF].") — 6-14 words total,
  (b) a punchy 3-5 word statement of value ("THIS IS WHAT $85 BUILDS", "GOTH PAYS BILLS"),
  (c) a POV/meme setup ("POV: MOM SEES YOUR RING COLLECTION"),
  (d) a diary-entry line ("DIDN'T PLAN TO WEAR IT DAILY. HERE WE ARE.").

Never abstract mood-slogans. Every headline names a person, moment, place, price, or number.

=== HOOK ALTERNATIVES (3 per variant) ===
Each hook is 5-14 words. Each hook pulls a DIFFERENT lever from the others: curiosity gap, identity confirmation, contrarian statement, insider-language, price/math shock, before/after transformation, POV meme setup, gentle callout of the reader.

=== IMAGE PROMPT RULES (for Nano Banana 2 / gemini-2.5-flash-image) ===
This is the critical piece. Follow this structure exactly — weak image prompts kill winning copy.

Every image_prompt MUST contain, in this order:

1. Opening line, VERBATIM:
   "4:5 vertical Facebook ad, 1080x1350, editorial macro photography, ad-quality composition. Compose in three fixed zones: TOP HEADLINE (22-30%), CENTER HERO (55-65%), BOTTOM OFFER BAR (12-18%)."
   (For meme angle only, replace "editorial macro photography" with "editorial macro photography with meme-style caption overlay".)

2. HERO ZONE description (one sentence). The jewelry piece fills 45-65% of frame width, dead-center or slight rule-of-thirds. Dark rich backdrop (black obsidian slab, wet stone, black velvet, deep-focus alley wall, tattooed hand, black lace). Cinematic single-source lighting — overhead spotlight, moonlight, candlelight, or purple gel from one side. Shallow depth of field, macro focus on the metal texture. Working templates:
   - "Hero zone: the exact ring from the reference image resting on a wet black stone slab, single overhead spotlight, wisps of purple smoke, shallow macro focus on the metal texture."
   - "Hero zone: the exact ring from the reference image on a heavily tattooed male hand resting on a wooden dinner table, family blurred in the background, warm candlelight, shallow depth of field."
   - "Hero zone: the exact earrings from the reference image hanging from a silver-painted branch, black velvet backdrop, single warm rim light, dramatic shadows."

3. Describe the JEWELRY with specificity taken from the reference image(s) — metal color (oxidized silver / stainless / brushed black), stone color (blood-red garnet / obsidian / green cats-eye), motif (skull / spider / thorn / snake / tentacle / bat / dragon / rose / coffin / cross). This is how Nano Banana renders THE actual piece and not a generic ring.

4. Include the phrase "the exact piece shown in the reference image" once. Use "pieces" / "reference images" if multiple.

5. TOP HEADLINE ZONE — MANDATORY. Specify verbatim:
   ON-IMAGE HEADLINE TEXT (top 22-30% of frame): "[YOUR HEADLINE HERE]". Render in [FONT], all-caps, [COLOR SCHEME], edge-to-edge, filling the top zone fully. Text is the second visual anchor after the jewelry. Must be legible at Instagram-feed thumbnail size.

   FONT choice, exactly ONE:
     - "heavy condensed sans-serif (Impact / Anton), white text on solid black bar" (default headline overlays — 70% of ads)
     - "heavy condensed sans-serif (Impact / Anton), black text on solid white bar" (bright/product variants)
     - "blackletter gothic font (Old English / Cloister Black), white text with strong black outline, over the photo" (brand-heritage variants only)
     - "clean bold sans-serif with speech-bubble border" (meme/POV variants only)

   Text length: 4-10 words for 1-line headlines, 8-14 words for 2-line micro-story headlines. Never truncate.

6. BOTTOM OFFER BAR — MANDATORY on every ad except meme variants that override with a caption-only footer. Specify verbatim:
   BOTTOM OFFER BAR (bottom 12-18% of frame): full-width white bar, black heavy condensed sans-serif text (Impact/Anton), all-caps, centered: "ANY 7 FOR $85" OR "ANY 7 FOR $85 · FREE DELIVERY" OR "PICK 7. PAY $85. NO CATCH." OR "SEVEN PIECES. ONE PRICE." Choose one variant per ad.

7. Aesthetic anchors to mix into images (pick 2-3, not all): purple club lighting, black lace, oxidized silver, dim brick alleys, candlelight, moonlight, underground club, low-light editorial, film grain, wet-look skin, kohl liner, graffiti wall backdrop, silver-painted branch, dark velvet, tattooed hands, coffin motif, thorn motif.

8. Every image_prompt must end with this exact NEGATIVE line: "Avoid: AI-fantasy artifacts, plastic skin, cartoon eyes, oversaturated purple, stock-photo product-on-white, sparkles, emojis inside the image, watermarks, tiny centered text strips, side captions, multi-panel collages unless explicitly a meme, models where the jewelry is a background prop. Sharp focus on the jewelry, natural texture, editorial magazine quality, headline text large and legible at thumbnail size, bottom offer bar clearly rendered."

=== ANGLE DIVERSITY (when N > 1) ===
Each variant MUST use a different angle. Draw from the preset brief — never produce 5 near-duplicates. angle_name must match one of: "Corporate Goth", "Anti-Consumerism", "Baby Bats", "7 for $85 Bundle", "Identity Confirmation", "Outcast", "Gothic Memes" — or a specific sub-angle within the requested preset.

Also vary the layout across the batch: mix white-bar-on-black, black-bar-on-white, blackletter-hero, and full-bleed with drop-shadow. Do not ship 5 identical layouts.

=== OUTPUT (STRICT JSON, NO MARKDOWN) ===
You receive: {N} = variants required, a preset brief, an optional user prompt, extra notes, 1-{max_refs} reference product images. Return ONLY this JSON structure, no commentary, no ```json fences:
{
  "variants": [
    {
      "angle_name": "Corporate Goth",
      "image_prompt": "...",
      "primary_text": "...",
      "headline": "...",
      "description": "...",
      "cta": "SHOP_NOW",
      "hook_alternatives": ["...", "...", "..."]
    }
  ]
}"""


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


# Rotating creative-provocation lines. On every Generate, one is picked at
# random and injected into the user turn so Claude cannot lazily replay its
# previous output. Each line pushes toward a slightly different creative axis.
_PROVOCATIONS = [
    'Every on-image headline must name a specific moment (my dog / grandma / the wedding / the office kitchen / mom\'s Sunday dinner). No mood-slogans.',
    'The winning ads all have MASSIVE top-zone headline text (22-30% of frame) plus a full-width white ANY 7 FOR $85 bar at the bottom. If your image_prompt does not enforce both zones, you have failed.',
    'The jewelry must be macro and dominant — 45-65% of frame width. If a model is in shot, the ring/earring/chain must still be the visual anchor, not a background prop.',
    'One variant must be a two-clause micro-story headline. Format: "[SPECIFIC MOMENT]. [DRY PAYOFF]." Example energy: "WORE THE EARRINGS TO A WEDDING. BECAME THE TOPIC OF THREE TOASTS."',
    'One variant must have a punchy 3-5 word headline (Impact/blackletter, edge-to-edge) like "GOTH PAYS BILLS" or "THIS IS WHAT $85 BUILDS".',
    'One variant must be POV/meme format with wholesome-shock contrast (grandma, mom, coworker, dog, family dinner). This is the brand\'s highest-performing lane.',
    'Vary the layout across the batch: some white-on-black top bars, some black-on-white top bars, some blackletter full-bleed. Never ship 5 identical layouts.',
    'Ban any word you would find in a generic Etsy listing: elegant, statement, unique, chic, timeless, versatile, stunning, gorgeous. Instant kill.',
    'Assume the reader has already seen 20 goth-jewelry ads today. What breaks through? Weirdly specific > broadly cool.',
    'Steal energy from doomscroll-era text-only tweets: short, cutting, self-aware. If the headline could be a tweet with 40k likes, it works.',
    'One variant should read like a diary entry, not an ad. Lowercase or handwritten scrawl. Intimate founder voice.',
    'One variant should be confrontational — gatekeep in a fun way. "Not for normies" energy, not preachy anti-consumerism.',
]


def _parse_claude_json(text: str, stop_reason: str = '') -> Optional[Dict[str, Any]]:
    """Robust extraction of the {variants: [...]} JSON from Claude's response.

    Handles three common failure modes:
      1. ```json ... ``` code fences even when told not to use them.
      2. Leading commentary before the opening `{`.
      3. Response truncated mid-string when stop_reason == 'max_tokens' — in
         that case, we salvage every fully-closed variant object.
    """
    if not text:
        return None

    # Strip code fences if present.
    fence = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
    if fence:
        body = fence.group(1)
    else:
        # Find first `{`, take from there.
        start = text.find('{')
        if start < 0:
            return None
        body = text[start:]

    # Try a straight parse first (fast path).
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        pass

    # Try trimming trailing junk after the outer object.
    depth = 0
    end = -1
    in_str = False
    esc = False
    for i, ch in enumerate(body):
        if esc:
            esc = False
            continue
        if ch == '\\' and in_str:
            esc = True
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end > 0:
        try:
            return json.loads(body[:end])
        except json.JSONDecodeError:
            pass

    # Truncated response — salvage every complete variant object we can find.
    # This handles stop_reason=='max_tokens' and any other mid-stream cutoff.
    variants: List[Dict[str, Any]] = []
    # Locate the "variants": [ ... array and walk objects inside it.
    marr = re.search(r'"variants"\s*:\s*\[', body)
    if not marr:
        return None
    scan = body[marr.end():]
    i = 0
    while i < len(scan):
        # skip whitespace / commas
        while i < len(scan) and scan[i] in ' \t\r\n,':
            i += 1
        if i >= len(scan) or scan[i] != '{':
            break
        # Find matching close for this variant.
        depth = 0
        in_str = False
        esc = False
        start = i
        found_end = -1
        for j in range(i, len(scan)):
            c = scan[j]
            if esc:
                esc = False
                continue
            if c == '\\' and in_str:
                esc = True
                continue
            if c == '"':
                in_str = not in_str
                continue
            if in_str:
                continue
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    found_end = j + 1
                    break
        if found_end < 0:
            break  # incomplete final variant — discard it
        try:
            v = json.loads(scan[start:found_end])
            if isinstance(v, dict):
                variants.append(v)
        except json.JSONDecodeError:
            break
        i = found_end
    if variants:
        log.warning('Claude response truncated (stop_reason=%s); salvaged %d complete variants',
                    stop_reason, len(variants))
        return {'variants': variants}
    return None


def _generate_briefs_with_claude(anthropic_key: str, prompt: str, preset_brief: str,
                                  extra_notes: str, reference_images: List[Tuple[bytes, str]],
                                  variant_count: int,
                                  creative_seed: Optional[str] = None) -> Dict[str, Any]:
    """Call Claude Haiku 4.5 with the reference product images inline and get a
    strict-JSON plan of N variants back.

    `creative_seed` is a short random string used to force variation between
    otherwise-identical Generate clicks. It is combined with a rotating
    provocation line and a non-zero temperature so back-to-back runs cannot
    return the same output.
    """
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

    import random as _rnd
    provocation = _rnd.choice(_PROVOCATIONS)
    seed = creative_seed or uuid.uuid4().hex[:10]

    user_text_parts = [
        f'REFERENCE PRODUCT IMAGES: {max_refs} attached above. These are the '
        f'jewelry pieces the ads must feature.',
    ]
    if preset_brief:
        user_text_parts.append(f'PRESET ANGLE (baseline direction): {preset_brief}')
    if prompt:
        # The user\'s extra prompt is CREATIVE FUEL for this specific run, not
        # a soft suggestion. Instruct Claude to actually use it.
        user_text_parts.append(
            "USER'S CREATIVE DIRECTION FOR THIS RUN (weight this HIGH — it is "
            "the fresh idea driving this batch, not a footnote):\n" + prompt
        )
    if extra_notes:
        user_text_parts.append(f'EXTRA NOTES (audience, offer, brand voice): {extra_notes}')

    user_text_parts.extend([
        f'CREATIVE SEED FOR THIS RUN: {seed}. Use this to force variation. '
        f'Even if the preset, prompt, and reference images are identical to a '
        f'previous run, the OUTPUT MUST BE DIFFERENT — different hooks, '
        f'different image scenes, different on-image text, different angle mix. '
        f'Do not repeat prior outputs.',
        f'PROVOCATION FOR THIS RUN: {provocation}',
        f'PRODUCE EXACTLY {variant_count} VARIANTS. Each variant must be a '
        f'distinct angle. Return only JSON.',
    ])

    user_content.append({'type': 'text', 'text': '\n\n'.join(user_text_parts)})

    # Each variant with the new 3-zone image_prompt is ~500-700 tokens of
    # output. 8 variants therefore need ~5-6k tokens. Give it 8k with headroom
    # so we never truncate mid-JSON.
    dynamic_max_tokens = min(16000, max(4000, 900 * variant_count + 1000))
    body = {
        'model': 'claude-haiku-4-5',
        'max_tokens': dynamic_max_tokens,
        # Non-zero temperature is the second lever that guarantees variation.
        'temperature': 1.0,
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

    # Extract JSON — Claude sometimes wraps in ```json fences even when told
    # not to, and (rarely) truncates mid-string when max_tokens is exceeded.
    text = text.strip()
    stop_reason = data.get('stop_reason') or ''
    parsed = _parse_claude_json(text, stop_reason)
    if parsed is None:
        raise RuntimeError(
            f'Claude JSON parse failed (stop_reason={stop_reason}). Raw head: {text[:400]}'
        )
    if 'variants' not in parsed or not isinstance(parsed['variants'], list) or not parsed['variants']:
        raise RuntimeError(f'Claude response missing variants[]: {text[:400]}')
    # Attach token usage so caller can compute $ cost.
    usage = data.get('usage') or {}
    parsed['_usage'] = {
        'input_tokens': int(usage.get('input_tokens') or 0),
        'output_tokens': int(usage.get('output_tokens') or 0),
    }
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

        # 3) Cost math.
        # Claude Haiku 4.5 (Nov 2025 GA): $1 / 1M input, $5 / 1M output.
        # Nano Banana 2 (gemini-2.5-flash-image): $0.039 / image at 4:5.
        usage = plan.get('_usage') or {}
        claude_in = int(usage.get('input_tokens') or 0)
        claude_out = int(usage.get('output_tokens') or 0)
        claude_cost = round((claude_in * 1.0 + claude_out * 5.0) / 1_000_000, 4)
        nano_calls = successes  # only successful renders bill
        nano_cost = round(nano_calls * 0.039, 4)
        total_cost = round(claude_cost + nano_cost, 4)

        # 4) Zip
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
            'cost': {
                'claude': {
                    'model': 'claude-haiku-4-5',
                    'input_tokens': claude_in,
                    'output_tokens': claude_out,
                    'usd': claude_cost,
                },
                'nano_banana': {
                    'model': 'gemini-2.5-flash-image',
                    'image_calls': nano_calls,
                    'usd_per_image': 0.039,
                    'usd': nano_cost,
                },
                'total_usd': total_cost,
            },
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
