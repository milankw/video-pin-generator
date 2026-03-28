# Orders Tab + Team Tab Spec

## Overview
Add two new tabs to the Video Pin Generator SPA:
1. **Orders** tab — shows fulfilled/unfulfilled/partial order counts per connected Shopify store
2. **Team** tab — shows employee emails with easy copy buttons

## Architecture
- Single-file Flask app: `server.py` (backend) + `index.html` (full SPA frontend with inline CSS/JS)
- Tabs defined in the nav bar as `<div class="tab" data-tab="xxx">` elements
- Panels are `<div class="tab-panel" id="panel-xxx">`
- Tab click handler at ~line 1274 auto-switches panels and triggers load functions

## Task 1: Orders Tab

### Backend — New endpoint: `GET /api/orders/summary`
Add this endpoint in `server.py`. It should:
1. Load all stores from `_load_stores()`
2. For each store with a valid `shopifyAccessToken` and `domain`:
   - Call Shopify REST API: `GET /admin/api/2024-01/orders/count.json` with different `fulfillment_status` filters
   - Specifically fetch 3 counts:
     - `fulfillment_status=unfulfilled` → unfulfilled count
     - `fulfillment_status=shipped` → fulfilled/shipped count  
     - `fulfillment_status=partial` → partially fulfilled count
   - Also get total open orders: `status=open` without fulfillment filter
   - Use existing headers pattern: `{'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}`
   - Add `time.sleep(0.3)` between API calls to respect rate limits
3. Return JSON:
```json
{
  "success": true,
  "stores": [
    {
      "id": "store-id",
      "name": "Store Name",
      "domain": "store.myshopify.com",
      "unfulfilled": 42,
      "fulfilled": 156,
      "partial": 3,
      "total": 201
    }
  ]
}
```
Add `@login_required` decorator. Place the endpoint near other Shopify endpoints (~line 1420 area).

### Frontend — Orders Tab
1. Add tab button after Gallery tab (line ~937):
```html
<div class="tab" data-tab="orders">Orders</div>
```

2. Add panel after `panel-gallery` closing div (before `</div><!-- main container -->`), i.e. after line 1177:
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
      <div class="empty-state-icon">📦</div>
      <div class="empty-state-text">No connected stores</div>
      <div class="empty-state-sub">Connect your Shopify stores in the Stores tab to see order data.</div>
    </div>
  </div>
</div>
```

3. Add JS function `loadOrdersSummary()`:
- Fetch `/api/orders/summary`
- Render a table inside `#orders-table-wrap` with columns: Store Name | Unfulfilled | Partial | Fulfilled | Total
- Unfulfilled count should be styled in amber/orange color and bold (these need attention)
- Fulfilled in green
- Partial in blue
- Total in gray
- Each row is one store
- Show a summary row at bottom with totals across all stores
- Hide loading, show content div

4. In the tab click handler (~line 1274), add:
```js
if (tab.dataset.tab === 'orders') loadOrdersSummary();
```

### CSS for orders table
Use existing table patterns. Add an `.orders-table` class. Style it similarly to existing tables in the app:
```css
.orders-table { width: 100%; border-collapse: collapse; }
.orders-table th { text-align: left; padding: 10px 14px; font-size: 12px; text-transform: uppercase; color: var(--gray-500); border-bottom: 1px solid var(--gray-200); }
.orders-table td { padding: 12px 14px; border-bottom: 1px solid var(--gray-100); }
.orders-table tr:hover { background: var(--gray-50); }
.orders-count-unfulfilled { color: var(--amber); font-weight: 700; font-size: 18px; }
.orders-count-fulfilled { color: var(--green); font-weight: 600; }
.orders-count-partial { color: var(--primary); font-weight: 600; }
.orders-summary-row td { font-weight: 700; border-top: 2px solid var(--gray-200); }
```

## Task 2: Team Tab

### No backend needed — pure frontend with localStorage

1. Add tab button after Orders:
```html
<div class="tab" data-tab="team">Team</div>
```

2. Add panel:
```html
<div class="tab-panel" id="panel-team">
  <div class="card">
    <div class="card-title">Team Members</div>
    <div class="card-desc">Employee emails for quick access. Click to copy.</div>
    <div id="team-list"></div>
    <div style="margin-top:16px; display:flex; gap:8px;">
      <input type="email" id="team-new-email" class="input" placeholder="Enter email address..." style="flex:1;">
      <input type="text" id="team-new-name" class="input" placeholder="Name (optional)" style="width:180px;">
      <button class="btn btn-primary btn-sm" onclick="addTeamMember()">Add</button>
    </div>
  </div>
</div>
```

3. JS: Store team members in localStorage key `vpg_team_members` as JSON array of `{email, name}`.

Default/initial data (pre-populate if localStorage is empty):
```json
[
  {"email": "jhassellenicolas@gmail.com", "name": ""},
  {"email": "maryjoyggonzales@gmail.com", "name": ""}
]
```

Functions needed:
- `loadTeamMembers()` — reads from localStorage, renders list
- `addTeamMember()` — adds new member from inputs, saves to localStorage, re-renders
- `removeTeamMember(index)` — removes member, saves, re-renders  
- `copyEmail(email)` — copies to clipboard, shows toast "Copied!"

Each team member row should show:
- Email (clickable to copy)
- Name (if set)
- Copy button (📋 icon)
- Remove button (× icon)

Style the team list as a clean list with rows, not a table. Each row has padding, border-bottom, and hover effect.

```css
.team-row { display: flex; align-items: center; gap: 12px; padding: 12px 14px; border-bottom: 1px solid var(--gray-100); }
.team-row:hover { background: var(--gray-50); }
.team-email { font-weight: 500; color: var(--gray-800); cursor: pointer; }
.team-email:hover { color: var(--primary); }
.team-name { color: var(--gray-500); font-size: 13px; }
.team-actions { margin-left: auto; display: flex; gap: 6px; }
```

4. In the tab click handler, add:
```js
if (tab.dataset.tab === 'team') loadTeamMembers();
```

## IMPORTANT Implementation Notes
- All CSS goes inside the existing `<style>` block (before the closing `</style>` tag) 
- All JS goes inside the existing `<script>` block
- Place new endpoints in server.py with `@login_required` decorator
- Use existing `api()` helper function for fetch calls in frontend
- Use existing `showToast()` for notifications
- Use existing CSS variables (--gray-xxx, --primary, --green, --amber, etc.)
- Do NOT create any new files — everything goes in server.py and index.html
- The `escHtml()` function is available for HTML escaping
