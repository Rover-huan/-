# SmartAnalyst Local Run Guide (Windows)

## What Redis is

Redis is a lightweight in-memory service. In this project, Celery uses Redis as the task queue broker:
- API process: receives uploads and queues jobs.
- Worker process: pulls queued jobs and runs analysis/rendering.

Without Redis, queued background jobs cannot run.

## Step-by-step setup

### 1. Use the pre-filled `.env`

This repo already has a ready-to-run `.env` for local mode (`sqlite + local storage + redis`).
Only one field must be changed:

`OPENAI_API_KEY=PASTE_YOUR_DEEPSEEK_API_KEY_HERE`

### 2. Install dependencies with the project virtual environment

```powershell
..\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

If you already activated the virtual environment, `python -m pip install -r requirements.txt` is fine.

Do not mix system Python and `.venv` service processes. If both are running, Celery may let the wrong worker pick up queued jobs, which causes inconsistent failures.

### 3. Start Redis with Docker (recommended)

Run once:

```powershell
docker run -d --name smartanalyst-redis -p 6379:6379 redis:7-alpine
```

If container already exists and is stopped:

```powershell
docker start smartanalyst-redis
```

Check Redis is running:

```powershell
docker ps
```

You should see a container named `smartanalyst-redis`.

### 4. Start the full local stack with one command

Run in the SmartAnalyst project root:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start-local-stack.ps1
```

This script will:
- stop any old SmartAnalyst API / worker / beat processes first
- force all services to use the same `.venv` interpreter
- start API, Celery worker, and Celery beat
- write logs into `.codex-logs\`

Optional helpers:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\status-local-stack.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\stop-local-stack.ps1
```

### 5. Manual startup fallback

If you prefer to start services by hand, use the same `.venv` interpreter for all three commands:

```powershell
..\.venv\Scripts\python.exe -m uvicorn service.api:app --host 127.0.0.1 --port 8000
..\.venv\Scripts\python.exe -m celery -A service.celery_app.celery_app worker --loglevel=info --pool=solo
..\.venv\Scripts\python.exe -m celery -A service.celery_app.celery_app beat --loglevel=info
```

`--pool=solo` is recommended on Windows. The default multiprocessing pool is not reliable there.

### 6. Verify API

Open:

`http://127.0.0.1:8000/docs`

Use Swagger page to call:
1. `POST /auth/register`
2. `POST /auth/login`
3. `POST /jobs`
4. `GET /jobs/{job_id}`
5. `POST /jobs/{job_id}/selection`

## Useful Redis commands

Stop:

```powershell
docker stop smartanalyst-redis
```

Start:

```powershell
docker start smartanalyst-redis
```

Delete container:

```powershell
docker rm -f smartanalyst-redis
```
