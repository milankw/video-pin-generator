# Orders Tab Spec

## Overview
Add an **Orders** tab showing fulfilled/unfulfilled/partial order counts per connected Shopify store.

## Architecture
- Single-file Flask app: `server.py` (backend) + `index.html` (full SPA frontend with inline CSS/JS)
- Tabs: `<div class="tab" data-tab="xxx">` in nav bar
- Panels: `<div class="tab-panel" id="panel-xxx">`
- Tab click handler at ~line 1274 auto-switches panels and triggers load functions

## Backend — New endpoint: `GET /api/orders/summary`

Add after other Shopify endpoints (~line 1420 area) in server.py:

1. Load all stores from `_load_stores()`
2. For each store with valid `shopifyAccessToken` and `domain`:
   - Use `base_url = f'https://{domain}/admin/api/2024-01'`
   - Use `headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}`
   - Fetch these counts via Shopify REST API `orders/count.json`:
     - Unfulfilled: `GET {base_url}/orders/count.json?status=open&fulfillment_status=unfulfilled`
     - Fulfilled: `GET {base_url}/orders/count.json?status=any&fulfillment_status=shipped`
     - Partial: `GET {base_url}/orders/count.json?status=any&fulfillment_status=partial`
   - Each returns `{"count": N}`
   - Add `time.sleep(0.3)` between API calls for rate limits
   - Wrap each store in try/except, skip stores that fail
3. Return:
```json
{
  "success": true,
  "stores": [
    {"id": "...", "name": "Store Name", "domain": "...", "unfulfilled": 42, "fulfilled": 156, "partial": 3}
  ]
}
```
Add `@login_required` decorator. Use `import requests as http_requests` which is already imported in the file.

## Frontend

### Tab button
Add after Gallery tab (line ~937):
```html
<div class="tab" data-tab="orders">Orders</div>
```

### Panel HTML
Add after `</div><!-- panel-gallery -->` closing (after line ~1177), before the main container close:
```html
<div class="tab-panel" id="panel-orders">
  <div class="card">
    <div class="card-title">Order Fulfillment</div>
    <div class="card-desc">Fulfilled and unfulfilled order counts per store.</div>
    <div id="orders-loading" class="loading-overlay"><div class="spinner"></div> Loading order data...</div>
    <div id="orders-content" style="display:none;">
      <div style="margin-bottom:12px;">
        <button class="btn btn-secondary btn-sm" onclick="loadOrdersSummary()">Refresh</button>
      </div>
      <div id="orders-table-wrap"></div>
    </div>
    <div id="orders-empty" class="empty-state" style="display:none;padding:24px;">
      <div class="empty-state-icon">&#128230;</div>
      <div class="empty-state-text">No connected stores</div>
      <div class="empty-state-sub">Connect your Shopify stores in the Stores tab first.</div>
    </div>
  </div>
</div>
```

### CSS (add inside existing `<style>` block)
```css
.orders-table { width: 100%; border-collapse: collapse; }
.orders-table th { text-align: left; padding: 10px 14px; font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--gray-500); border-bottom: 1px solid var(--gray-200); }
.orders-table td { padding: 12px 14px; border-bottom: 1px solid var(--gray-100); }
.orders-table tr:hover { background: var(--gray-50); }
.orders-count-unfulfilled { color: var(--amber); font-weight: 700; font-size: 18px; }
.orders-count-fulfilled { color: var(--green); font-weight: 600; }
.orders-count-partial { color: var(--primary); font-weight: 600; }
.orders-table .summary-row td { font-weight: 700; border-top: 2px solid var(--gray-300); background: var(--gray-50); }
```

### JavaScript

Add `loadOrdersSummary()` function:
- Show `#orders-loading`, hide content and empty
- Fetch `api('/api/orders/summary')`
- If no stores or error, show `#orders-empty`
- Otherwise build table:
  - Columns: Store | Unfulfilled | Partial | Fulfilled | Total
  - Each store is a row
  - Unfulfilled uses `.orders-count-unfulfilled` class
  - Fulfilled uses `.orders-count-fulfilled` class
  - Partial uses `.orders-count-partial` class
  - Add a summary/total row at bottom with class `summary-row` summing all stores
  - Total = unfulfilled + fulfilled + partial
- Insert into `#orders-table-wrap`, show `#orders-content`, hide loading

In tab click handler (~line 1274), add:
```js
if (tab.dataset.tab === 'orders') loadOrdersSummary();
```

### IMPORTANT
- All CSS inside existing `<style>` block
- All JS inside existing `<script>` block
- Use existing `api()` helper for fetch calls
- Use `escHtml()` for HTML escaping
- Use existing CSS variables (--gray-xxx, --primary, --green, --amber, etc.)
- Do NOT create new files
