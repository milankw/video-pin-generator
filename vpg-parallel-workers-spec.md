# Parallel Workers + Stuck Job Recovery

## Problems
1. Single worker thread — one job at a time, if stuck everything blocks
2. Polling can hang if xAI returns unexpected status or HTTP hangs
3. No way to skip/timeout a stuck job without restarting the server

## Solution

### 1. Thread Pool Worker (2 concurrent workers)
Replace the single `_worker_loop` with a `ThreadPoolExecutor(max_workers=2)`. This lets 2 jobs process simultaneously.

Architecture:
- A dispatcher thread picks up queued jobs and submits them to the pool
- Each pool thread runs `_process_job()` independently
- Track in-flight job IDs to avoid double-processing

### 2. Polling Timeout (3 minutes instead of 10)
Reduce max_polls from 120 (10 min) to 36 (3 min). Video generation usually completes in 1-2 minutes. If it takes more than 3 minutes, mark it as failed with "Generation timed out — retry later".

### 3. HTTP Timeout on Poll Requests
The poll GET request has a 30s timeout which is fine, but if the xAI API is completely down, the worker hangs on connection. Add connect timeout: `timeout=(10, 30)` (10s connect, 30s read).

### 4. Skip Button (Frontend)
Add a "Skip" button for jobs in `generating` or `polling` status. This calls a new endpoint that marks the job as failed with "Skipped by user", allowing the worker to move on.

## Implementation

### server.py changes

Replace the worker system:

```python
from concurrent.futures import ThreadPoolExecutor

_executor = ThreadPoolExecutor(max_workers=2)
_in_flight = set()  # job IDs currently being processed
_in_flight_lock = threading.Lock()

def _process_and_save(job):
    """Wrapper that processes a job and saves result."""
    job_id = job['id']
    try:
        result = _process_job(job)
        _save_jobs_safe(result)
        if result['status'] == 'done':
            log.info(f"Job {job_id} completed")
        else:
            log.warning(f"Job {job_id} failed: {result.get('error')}")
    except Exception as e:
        job['status'] = 'failed'
        job['error'] = str(e)
        _save_jobs_safe(job)
        log.error(f"Job {job_id} error: {e}")
    finally:
        with _in_flight_lock:
            _in_flight.discard(job_id)

def _dispatcher_loop():
    """Picks up queued jobs and submits to thread pool."""
    global _worker_running
    _worker_running = True
    log.info("Video dispatcher started (2 parallel workers)")
    
    _archive_counter = 0
    while True:
        try:
            _archive_counter += 1
            if _archive_counter >= 10:
                _archive_counter = 0
                try: _archive_old_jobs()
                except: pass
            
            with _jobs_lock:
                jobs = _load_jobs()
                queued = [j for j in jobs if j['status'] == 'queued']
            
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
```

Update `_process_job`:
- Change max_polls from 120 to 36 (3 min)
- Change poll timeout to `timeout=(10, 30)`
- Change submit timeout to `timeout=(15, 60)`
- Check if job was externally cancelled/skipped during polling

Add skip endpoint:
```python
@app.route('/api/videos/<job_id>/skip', methods=['POST'])
@login_required
def skip_job(job_id):
    with _jobs_lock:
        jobs = _load_jobs()
        for j in jobs:
            if j['id'] == job_id and j['status'] in ('generating', 'polling', 'queued'):
                j['status'] = 'failed'
                j['error'] = 'Skipped by user'
                break
        _save_jobs(jobs)
    return jsonify({'success': True})
```

### index.html changes

In the queue slot rendering, add Skip button for generating/polling status:
```javascript
if (j.status === 'generating' || j.status === 'polling') {
    actions = `<button class="btn btn-danger btn-xs" onclick="skipJob('${j.id}')">Skip</button>`;
}
```

Add skipJob function:
```javascript
async function skipJob(id) {
    await api(`/api/videos/${id}/skip`, {method:'POST'});
    toast('Job skipped', 'info');
    refreshQueue();
}
```
