# Store-Level Prompts + Multi-Video Generation Spec

## Overview
Move prompt templates from account-level (Settings) to store-level. Each store gets at least 4 customizable prompts. When generating videos for a product, the system creates **4 separate jobs** (one per prompt), and all 4 videos land in the **same** numbered Drive subfolder (`#001-product-handle`).

## Data Model Changes

### Store object (stores.json)
Add a `prompts` array to each store. Each store gets 4 prompt slots by default:

```json
{
  "id": "store_abc123",
  "name": "Atelier Hardanger",
  "prompts": [
    { "label": "Prompt 1", "template": "Professional product showcase of {product_name}, clean studio lighting, smooth rotating view, commercial quality" },
    { "label": "Prompt 2", "template": "Lifestyle shot of {product_name}, warm natural lighting, modern setting, editorial style" },
    { "label": "Prompt 3", "template": "Close-up detail shot of {product_name}, soft bokeh background, premium commercial feel" },
    { "label": "Prompt 4", "template": "Dynamic showcase of {product_name}, smooth camera movement, professional product film" }
  ]
}
```

- Each prompt has a `label` (user-editable name) and a `template` (the actual prompt text).
- `{product_name}` is the placeholder that gets replaced with the actual product name.
- Users can have 4+ prompts. 4 is the default minimum. Allow adding more via a button.

### Global prompt_templates (Settings)
**Keep** the existing global `prompt_templates` in Settings as fallback defaults. When a store has no `prompts` array or it's empty, fall back to the global default prompt (just 1 video like before). But the main workflow should use store-level prompts.

## Backend Changes

### 1. Store prompts API endpoints

**GET /api/stores/<store_id>/prompts**
Returns the prompts array for a store. If the store has no prompts yet, return the 4 defaults.

**PUT /api/stores/<store_id>/prompts**
Save the prompts array for a store. Body: `{ "prompts": [...] }`

### 2. POST /api/videos/generate — Multi-job creation
Currently creates 1 job per product. Change to create **N jobs per product** (one per store prompt):

```
For each selected product:
    For each prompt in store.prompts:
        Create a job with that specific prompt
        Set job.promptIndex = index (0, 1, 2, 3)
        Set job.promptLabel = prompt.label
        Set job.groupId = a shared UUID for all jobs from same product
```

- The `groupId` ties all prompt variants of the same product together.
- Each job is independent and processed separately by the worker.
- `estimatedCost` should reflect PER JOB cost (duration × $0.05), not total.
- The response should indicate total jobs queued (products × prompts).

### 3. _process_job() — No changes needed
Already uses `job['prompt']` directly. Each job already has its own prompt set during creation.

### 4. Drive upload — Same subfolder
When uploading to Drive (the manual "Upload to Drive" button), all jobs in the same `groupId` should go to the **same** numbered product subfolder. This already works because `_find_or_create_numbered_product_folder` looks up by product handle — so multiple videos for the same product handle will naturally land in the same folder. No changes needed here.

## Frontend Changes

### 1. Stores Tab — Add "Prompts" column/action
Add a "Prompts" button or expandable section for each store in the Stores table. Clicking it opens a modal or inline editor showing:

- 4+ textarea fields, one per prompt
- Each with an editable label input + template textarea
- An "Add Prompt" button to add more slots (no hard upper limit, but start with 4)
- A "Remove" button on each prompt (minimum 1 prompt required)
- A "Save Prompts" button
- Use `{product_name}` placeholder hint

### 2. Winners Tab — Show prompt count
When generating videos, the Generate button should indicate the total: "Generate Videos (3 products × 4 prompts = 12 videos)"

### 3. Queue Tab — Show prompt label
In the queue table, show which prompt variant each job is (e.g., "Prompt 1" or the custom label). Add it as a small subtitle under the product name or as its own column.

### 4. Settings Tab — Prompt Templates section
Keep the existing global prompt templates section but add a note: "These are fallback defaults. Store-specific prompts are configured in the Stores tab." Consider making this section collapsible or less prominent.

## UI Design for Store Prompts Modal

```
┌─────────────────────────────────────────────┐
│  Prompts for "Atelier Hardanger"        [×] │
│─────────────────────────────────────────────│
│  Prompt 1                           [Remove]│
│  Label: [Professional Showcase        ]     │
│  ┌─────────────────────────────────────────┐│
│  │Professional product showcase of         ││
│  │{product_name}, clean studio lighting... ││
│  └─────────────────────────────────────────┘│
│  Use {product_name} as placeholder          │
│                                             │
│  Prompt 2                           [Remove]│
│  Label: [Lifestyle Shot               ]     │
│  ┌─────────────────────────────────────────┐│
│  │Lifestyle shot of {product_name},...     ││
│  └─────────────────────────────────────────┘│
│                                             │
│  Prompt 3                           [Remove]│
│  ...                                        │
│                                             │
│  Prompt 4                           [Remove]│
│  ...                                        │
│                                             │
│  [+ Add Prompt]                             │
│                                             │
│  [Save Prompts]                             │
└─────────────────────────────────────────────┘
```

## Migration
- On first load, if a store has no `prompts` field, auto-populate with 4 default prompts (using the store's `storeCategory` to pick from global templates if possible, otherwise use `default`).
- Do NOT delete the global prompt_templates — they serve as fallback and source for defaults.

## Summary of changes needed:
1. **server.py**: Add GET/PUT `/api/stores/<store_id>/prompts` endpoints. Modify `generate_videos()` to create N jobs per product (one per prompt). Add `promptIndex`, `promptLabel`, `groupId` to job dict.
2. **index.html**: Add store prompts modal to Stores tab. Update `generateVideos()` to not pass a prompt (let backend handle it from store prompts). Update generate button label to show total count. Show prompt label in Queue tab.
3. **stores.json**: Each store gets a `prompts` array.
