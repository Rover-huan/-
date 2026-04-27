# SmartAnalyst 项目工作区指南

这份文档用于每天整理 VS Code 工作区、判断哪些文件要保留、哪些文件不要提交。本文只面向本地开发和本地 demo，不涉及正式服务器部署。

## 1. 项目目录说明

当前项目根目录：

```powershell
C:\Users\lenovo\Desktop\实验
```

主要目录：

- `SmartAnalyst/`：后端 API、Celery worker、分析执行链路、报告生成链路、数据库模型、测试。
- `autoreport-ai-智能报告工作台/`：前端 React + Vite 项目。
- `deploy/`：生产部署相关模板和 nginx 配置。
- `docs/`：AI 接手、变更规范、开发日志等说明文档。
- `.agents/`：本项目给 AI 助手使用的本地 skill/交接说明。
- `skills/`：历史或备用的 skill 目录，需要保留，具体用途可再确认。
- `logs/`：本地运行日志，不提交。
- `.venv/`、`venv/`：本地 Python 虚拟环境，不提交。

根目录主要文件：

- `README.md`：项目总览。
- `AGENTS.md`：AI 接手项目必须阅读的入口规则。
- `ADMIN_CONSOLE.md`：管理后台说明。
- `DEPLOYMENT_ALIYUN.md`：阿里云部署说明，正式部署参考。
- `LOCAL_DEMO_WITH_TUNNEL.md`：本地 demo + Cloudflare Tunnel 说明。
- `docker-compose.local-demo.yml`：本地 0 成本 demo 的 Docker Compose 配置。
- `.env.local-demo.example`：本地 demo 环境变量模板，可以提交。
- `.env.local-demo`：本地 demo 私密配置，不能提交。
- `init_workspace.py`：工作区初始化脚本，当前用途建议保留但可再确认。
- `每日启动测试手册.md`：历史/日常启动说明。

## 2. 必须保留的核心代码和配置

后端：

- `SmartAnalyst/main.py`
- `SmartAnalyst/src/`
- `SmartAnalyst/service/`
- `SmartAnalyst/alembic/`
- `SmartAnalyst/templates/`
- `SmartAnalyst/scripts/`
- `SmartAnalyst/tests/`
- `SmartAnalyst/requirements.txt`
- `SmartAnalyst/Dockerfile`
- `SmartAnalyst/alembic.ini`
- `SmartAnalyst/.env.example`
- `SmartAnalyst/.dockerignore`

前端：

- `autoreport-ai-智能报告工作台/src/`
- `autoreport-ai-智能报告工作台/package.json`
- `autoreport-ai-智能报告工作台/package-lock.json`
- `autoreport-ai-智能报告工作台/vite.config.ts`
- `autoreport-ai-智能报告工作台/tsconfig.json`
- `autoreport-ai-智能报告工作台/index.html`
- `autoreport-ai-智能报告工作台/Dockerfile`
- `autoreport-ai-智能报告工作台/.env.example`
- `autoreport-ai-智能报告工作台/.dockerignore`

本地 demo / 反代：

- `docker-compose.local-demo.yml`
- `.env.local-demo.example`
- `deploy/nginx/default.conf.template`

部署模板：

- `deploy/docker-compose.prod.yml`
- `deploy/.env.production.example`
- `deploy/nginx/`

测试：

- `SmartAnalyst/tests/`
- 前端 `package.json` 里的 lint/build 脚本。

## 3. 必须保留的说明类文档

建议保留：

- `README.md`
- `AGENTS.md`
- `ADMIN_CONSOLE.md`
- `DEPLOYMENT_ALIYUN.md`
- `LOCAL_DEMO_WITH_TUNNEL.md`
- `PROJECT_WORKSPACE_GUIDE.md`
- `LOCAL_DAILY_RUNBOOK.md`
- `CLOUDFLARE_TUNNEL_RUNBOOK.md`
- `每日启动测试手册.md`
- `SmartAnalyst/SERVICE_README.md`
- `autoreport-ai-智能报告工作台/README.md`
- `docs/AI_PROJECT_GUIDE.md`
- `docs/AI_CHANGE_PROTOCOL.md`
- `docs/AI_TASK_PROMPTS.md`
- `docs/DEVELOPMENT_LOG.md`
- `.agents/skills/smartanalyst-handoff/SKILL.md`

环境变量模板也属于说明类配置，可以提交：

- `.env.local-demo.example`
- `SmartAnalyst/.env.example`
- `autoreport-ai-智能报告工作台/.env.example`
- `deploy/.env.production.example`

## 4. 本地私密配置，不能提交

这些文件可以在本机存在，但不要提交到 GitHub，也不要复制到聊天里：

- `.env.local-demo`
- `SmartAnalyst/.env`
- `deploy/.env.production`
- 任何包含 API Key、`SECRET_KEY`、管理员密码、邮箱密码、数据库密码的文件

注意：

- 可以检查这些文件是否存在。
- 不要打开全文，不要截图，不要粘贴内容。
- 管理员账号密码来自 `.env.local-demo`，不要写进文档。

## 5. 本地运行产物，可以忽略

这些是运行、构建或测试时产生的文件，不应该提交：

- `SmartAnalyst/smartanalyst.db`
- `SmartAnalyst/storage/`
- `SmartAnalyst/runs/`
- `SmartAnalyst/outputs/`
- `SmartAnalyst/celerybeat-schedule.*`
- `SmartAnalyst/.pytest_cache/`
- `SmartAnalyst/__pycache__/`
- `SmartAnalyst/**/__pycache__/`
- `autoreport-ai-智能报告工作台/node_modules/`
- `autoreport-ai-智能报告工作台/dist/`
- `.venv/`
- `venv/`
- `logs/`
- `*.log`
- 生成的报告产物：`*.docx`、`*.pdf`、`*.zip`
- 临时测试文件、下载文件、浏览器保存的报告文件

本地 demo 使用 Docker volume 保存数据时，不要用 `docker compose down -v`，除非明确想清空 demo 数据。

## 6. 目前不确定用途，需要确认

这些文件/目录目前不建议删除，只是用途需要你确认：

- `init_workspace.py`：看起来像工作区初始化脚本，建议保留；如果已经不用，可以之后再判断。
- `skills/`：根目录下另有 `.agents/skills/`，两个目录可能有历史来源差异，建议先保留。
- `SmartAnalyst/data/`：可能是样例数据或历史测试数据，删除前需要确认。
- `SmartAnalyst/.codex-logs/`、`autoreport-ai-智能报告工作台/.codex-logs/`：本地 AI 工具日志，不提交；是否保留取决于你是否还要追踪历史操作。
- `每日启动测试手册.md`：已有日常手册，新文档会更偏小白操作，但旧文档可能仍有历史命令，建议先保留。

## 7. .gitignore 覆盖情况

当前 `.gitignore` 和子目录 `.gitignore` 已覆盖：

- `.env`
- `.env.*`
- `.env.example`
- `.env.local-demo.example`
- `deploy/.env.production.example`
- `node_modules/`
- `dist/`
- `.venv/`
- `venv/`
- `__pycache__/`
- `.pytest_cache/`
- `storage/`
- `runs/`
- `outputs/`
- `logs/`
- SQLite 数据库：`SmartAnalyst/smartanalyst.db`、`SmartAnalyst/*.db`、`*.sqlite`、`*.sqlite3`
- Celery beat 本地状态：`SmartAnalyst/celerybeat-schedule*`
- 常见报告产物：`*.docx`、`*.pdf`、`*.zip`

建议后续确认是否补充以下规则：

- `*.ipynb`：如果 notebook 只作为生成产物，不建议提交。
- `*.coverage`、`.coverage`、`coverage.xml`、`htmlcov/`：如果之后跑覆盖率测试。
- 根目录或下载目录里的临时报告产物：如 `report_*.txt`、`*.xlsx` 测试下载件。注意不要全局忽略 `*.txt`，因为 `requirements.txt` 需要保留。
- `.codex-logs/`：当前子项目里已被忽略，但根目录如果出现同名目录，可以考虑明确加入。

这次没有修改 `.gitignore`，以上只是建议。

## 8. Git 提交前检查清单

提交前先执行：

```powershell
cd C:\Users\lenovo\Desktop\实验
git status --short --ignored
```

检查：

- 不要提交 `.env.local-demo`。
- 不要提交 `SmartAnalyst/.env`。
- 不要提交 `storage/`、`runs/`、`outputs/`、`logs/`。
- 不要提交 `smartanalyst.db` 或其他本地数据库文件。
- 不要提交 `node_modules/`、`dist/`、`.venv/`、`venv/`。
- 不要提交生成的报告、ZIP、DOCX、PDF、IPYNB、临时测试文件。
- 可以提交 `.env.local-demo.example`、`.env.example`、`deploy/.env.production.example` 等模板文件。
- 确认没有 API Key、`SECRET_KEY`、管理员密码、邮箱密码进入代码或文档。
- 如果改了后端，至少运行相关 pytest。
- 如果改了前端，至少运行 lint/build。
- 如果只改文档，确认文档命令没有写入真实密钥。
