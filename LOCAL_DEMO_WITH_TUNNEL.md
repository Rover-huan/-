# SmartAnalyst Local Demo with Cloudflare Tunnel

This guide runs a zero-cost local demo on your own computer and exposes the nginx entry through Cloudflare Tunnel. It does not use RDS, Tair, OSS, SMTP, CAPTCHA, or PDF export.

## What This Demo Adds

- `docker-compose.local-demo.yml` starts nginx, API, analysis worker, render worker, beat, and Redis.
- `.env.local-demo.example` keeps local demo defaults small and conservative.
- Named Docker volumes persist SQLite, Redis, storage, runs, and outputs data.
- Frontend API base stays `/api`, so browser requests use the same tunnel domain.
- nginx preserves Cloudflare's incoming `X-Forwarded-Proto` so FastAPI can generate HTTPS download/chart URLs.

## Files

- `docker-compose.local-demo.yml`: local demo stack.
- `.env.local-demo.example`: tracked example settings with placeholders only.
- `.env.local-demo`: optional private local override file, ignored by git.
- `deploy/nginx/default.conf.template`: shared nginx template with Cloudflare-friendly forwarded headers.

## Defaults

Important demo defaults:

```env
EMAIL_VERIFICATION_REQUIRED=false
ENABLE_PDF_EXPORT=false
ENABLE_AUTO_TOC=false
AUTO_TOC_BACKEND=none
ANALYSIS_WORKER_CONCURRENCY=1
RENDER_WORKER_CONCURRENCY=1
CHART_GENERATION_CONCURRENCY=1
MAX_UPLOAD_FILES=5
MAX_FILE_SIZE_BYTES=1048576
MAX_TOTAL_UPLOAD_BYTES=5242880
VITE_API_BASE_URL=/api
```

`ENABLE_PDF_EXPORT=false` is included as the demo policy flag. The current Docker image also does not install `docx2pdf`, so PDF artifacts are not produced in this local Linux container path.

## First-Time Setup

From the repository root:

```powershell
Copy-Item .env.local-demo.example .env.local-demo
```

Edit `.env.local-demo` locally and set only private values you need, especially:

```env
OPENAI_API_KEY=your-real-server-side-model-key
SECRET_KEY=replace-with-a-long-random-local-secret
ADMIN_OWNER_EMAIL=your-admin-email@example.com
ADMIN_OWNER_INITIAL_PASSWORD=your-local-admin-password
```

Do not commit `.env.local-demo`.

## Start

From the repository root:

```powershell
docker compose -f docker-compose.local-demo.yml --env-file .env.local-demo up --build -d
```

If you want to start without private overrides, this also works with placeholders, but report generation will fail when the model key is still a placeholder:

```powershell
docker compose -f docker-compose.local-demo.yml up --build -d
```

The default local URL is:

```text
http://localhost:8080
```

The API health URL through nginx is:

```text
http://localhost:8080/api/healthz
```

## Logs

All services:

```powershell
docker compose -f docker-compose.local-demo.yml logs -f
```

API only:

```powershell
docker compose -f docker-compose.local-demo.yml logs -f api
```

Workers:

```powershell
docker compose -f docker-compose.local-demo.yml logs -f worker-analysis worker-render
```

Service status:

```powershell
docker compose -f docker-compose.local-demo.yml ps
```

## Cloudflare Tunnel

Keep the compose stack running, then open a second terminal:

```powershell
cloudflared tunnel --url http://localhost:8080
```

If `cloudflared` is not installed on Windows, use the Docker fallback instead:

```powershell
docker run --rm -it --network smartanalyst-local-demo_default cloudflare/cloudflared:latest tunnel --no-autoupdate --url http://nginx:80
```

Cloudflare will print an HTTPS URL such as:

```text
https://xxxx.trycloudflare.com
```

For the cleanest external test, update `.env.local-demo`:

```env
PUBLIC_BASE_URL=https://xxxx.trycloudflare.com
CORS_ORIGINS=http://localhost:8080,http://127.0.0.1:8080,https://xxxx.trycloudflare.com
```

Then restart the stack:

```powershell
docker compose -f docker-compose.local-demo.yml --env-file .env.local-demo up -d
```

Open the Cloudflare URL and share that URL with classmates.

## Verify

1. Open `http://localhost:8080` or the Cloudflare HTTPS URL.
2. Register a normal user. Email verification is disabled by default.
3. Log in.
4. Upload up to five small `.csv`, `.xls`, or `.xlsx` files. Keep each file under 1 MB and the total upload under 5 MB for the demo defaults.
5. Wait for analysis to finish and candidate charts to appear.
6. Select charts and submit the render step.
7. Download available artifacts: ZIP, DOCX, IPYNB, and TXT.
8. In browser devtools, confirm chart/download URLs use the Cloudflare `https://...` origin when accessed through the tunnel.

Useful health checks:

```powershell
Invoke-WebRequest -UseBasicParsing http://localhost:8080/api/healthz
Invoke-WebRequest -UseBasicParsing https://xxxx.trycloudflare.com/api/healthz
```

## Stop

Stop containers but keep data volumes:

```powershell
docker compose -f docker-compose.local-demo.yml down
```

Stop and remove local demo data volumes:

```powershell
docker compose -f docker-compose.local-demo.yml down -v
```

## Clean Data

The local demo data lives in Docker named volumes:

- `smartanalyst_data`: SQLite database.
- `smartanalyst_storage`: uploaded files, charts, and artifacts.
- `smartanalyst_runs`: worker workspaces.
- `smartanalyst_outputs`: fallback output directory.
- `smartanalyst_redis`: Redis append-only data.

To clean only by project stack:

```powershell
docker compose -f docker-compose.local-demo.yml down -v
```

To inspect volumes:

```powershell
docker volume ls | Select-String smartanalyst
```

## Port Change

If port `8080` is busy, set another local port in `.env.local-demo`:

```env
LOCAL_DEMO_HTTP_PORT=8090
```

Restart:

```powershell
docker compose -f docker-compose.local-demo.yml --env-file .env.local-demo up -d
```

Tunnel:

```powershell
cloudflared tunnel --url http://localhost:8090
```

## Rollback

To roll back this local demo change set:

```powershell
docker compose -f docker-compose.local-demo.yml down -v
Remove-Item .\docker-compose.local-demo.yml
Remove-Item .\.env.local-demo.example
git checkout -- deploy/nginx/default.conf.template LOCAL_DEMO_WITH_TUNNEL.md
```

If `.env.local-demo` was created locally and you no longer need it:

```powershell
Remove-Item .\.env.local-demo
```

If demo containers or volumes are still present and the compose file still exists:

```powershell
docker compose -f docker-compose.local-demo.yml down -v
```
