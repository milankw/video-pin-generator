# Queue Grouped View + Retry with Prompt Selection

## Overview
Three changes:
1. Remove Gallery stats section
2. Redesign Queue tab: group jobs by `groupId` into card-style boxes (one box per product showing all 4 prompt variants)
3. Enhanced retry: popup to pick/edit the prompt before retrying. Single "Upload to Drive" per group.

---

## 1. Remove Gallery Stats (SIMPLE)

### index.html
- Delete the `<div id="gallery-stats">` element (line ~1192)
- Remove all JS references that populate `gallery-stats` (the statsHtml block that sets Total Videos / Stores / Products)

---

## 2. Queue Tab — Grouped Card Layout

### Current behavior
Queue displays a flat table with one row per job. When 4 prompts generate, you see 4 identical-looking rows.

### New behavior
Jobs that share the same `groupId` are grouped into a **card/box**. Each card shows:
- Product image (large, once)
- Product name + store name
- Created date
- A row/grid of 4 prompt variant slots inside the card
- Each slot shows: prompt label, status badge, individual actions (preview/retry)
- Group-level actions: "Upload All to Drive" button, total cost

### Card Layout Design

```
┌──────────────────────────────────────────────────────────────────┐
│  [Product Image]  Product Name                     Store Name   │
│                   Created: 3/26/2026               Total: $1.60 │
│                                                                  │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────┐│
│  │ Prompt 1     │ │ Prompt 2     │ │ Prompt 3     │ │ Prompt 4 ││
│  │ ✅ Done      │ │ ✅ Done      │ │ ❌ Failed    │ │ ✅ Done  ││
│  │ $0.40        │ │ $0.40        │ │              │ │ $0.40    ││
│  │[Preview]     │ │[Preview]     │ │[Retry]       │ │[Preview] ││
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────┘│
│                                                                  │
│  [Upload All to Drive]   or   [In Drive ✓] (if all uploaded)    │
└──────────────────────────────────────────────────────────────────┘
```

### Implementation approach

Replace the `<table>` in the Queue tab with a `<div id="queue-groups">` container. The JS rendering code groups `_queueJobs` by `groupId` and renders each group as a card div.

**For jobs WITHOUT a groupId** (legacy/fallback single jobs), render them as individual mini-cards (same card style but with only 1 slot).

### HTML structure change
Replace the queue-table with:
```html
<div id="queue-groups"></div>
```

Keep the queue-card wrapper, cost summary, load more, etc.

### JS rendering (replaces the _queueJobs.forEach in the current table render)

```
function renderQueueGroups() {
  // Group jobs by groupId
  const groups = {};
  const ungrouped = [];
  _queueJobs.forEach(j => {
    if (j.groupId) {
      if (!groups[j.groupId]) groups[j.groupId] = [];
      groups[j.groupId].push(j);
    } else {
      ungrouped.push(j);
    }
  });
  
  // Sort groups by most recent createdAt
  // Render each group as a card
  // Render ungrouped jobs as individual cards
}
```

### CSS for group cards
Add styles for:
- `.queue-group-card` — the outer card (white bg, rounded, shadow, padding)
- `.queue-group-header` — flex row with product image, name, store, date, total cost
- `.queue-group-slots` — CSS grid (auto-fill, min 180px) for the prompt variant slots
- `.queue-slot` — individual prompt variant box (border, rounded, padding)
- `.queue-slot-status` — status badge
- `.queue-group-actions` — bottom row with Upload All button

---

## 3. Retry with Prompt Selection Popup

### Current behavior
Clicking "Retry" on a failed job immediately requeues it with the same prompt.

### New behavior
Clicking "Retry" on a failed job slot opens a small popup/modal that:
1. Shows the current prompt text in a textarea (editable)
2. Shows a dropdown of the store's saved prompts to quickly pick a different one
3. Has "Retry with this prompt" button

### Backend changes

**Modify `POST /api/videos/<job_id>/retry`:**
Accept optional JSON body `{ "prompt": "new prompt text" }`. If provided, update the job's prompt before requeuing. If not provided, keep existing prompt (backward compatible).

### Frontend

**Retry Prompt Modal:**
A small modal (reuse the overlay pattern) with:
- Dropdown: "Pick a store prompt" → loads store prompts via GET /api/stores/<storeId>/prompts
- Textarea: shows current prompt (pre-filled), user can edit
- When dropdown selection changes, replace textarea content
- "Retry" button → calls POST /api/videos/<id>/retry with { prompt: textarea.value }
- "Cancel" button

---

## 4. Upload All to Drive (Group-Level)

### New backend endpoint
**POST /api/drive/upload-group**
Body: `{ "groupId": "abc123" }`

- Finds all jobs with that groupId that have status "done" and localPath
- Uploads each to Drive (same product subfolder — `_find_or_create_numbered_product_folder` handles this naturally since they share the same product handle)
- Returns list of results per job

### Frontend
The "Upload All to Drive" button on the group card calls this endpoint. Shows progress ("Uploading 2/4..."). When done, each slot updates to show "In Drive ✓".

If some jobs in the group are still processing/failed, only upload the done ones. The button label should reflect this: "Upload 3 to Drive" if only 3 are done.

Also keep per-slot individual upload capability for flexibility.

---

## Summary of file changes

### server.py
1. Modify `POST /api/videos/<job_id>/retry` to accept optional `{ "prompt": "..." }` body
2. Add `POST /api/drive/upload-group` endpoint

### index.html  
1. Remove `<div id="gallery-stats">` and its JS population code
2. Replace queue table with grouped card layout (HTML + CSS + JS)
3. Add retry prompt modal (HTML + JS)
4. Add group upload logic (JS)
