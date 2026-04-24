# AI 项目说明：AutoReport AI / SmartAnalyst

本文档用于帮助未来 AI 编程助手快速理解项目，不用于宣传。接手任务前先读 `AGENTS.md` 和 `docs/AI_CHANGE_PROTOCOL.md`。

## 产品目标

AutoReport AI / SmartAnalyst 是一个智能数据分析与报告生成应用。用户上传表格数据后，系统自动理解数据、规划分析问题、生成候选图表；用户选择需要进入最终报告的图表后，系统生成报告产物并提供下载。

目标不是做一个普通文件上传站，而是把“数据上传 -> 自动分析 -> 候选图表 -> 人工选择 -> 报告导出”串成一个可运行、可排队、可追踪、可运营的工作流。

## 用户完整工作流

1. 上传数据  
   用户在前端上传 `csv/xls/xlsx` 文件。后端创建 Job、保存输入文件索引，并将分析任务加入队列。

2. 分析数据  
   Celery worker 拉取分析任务，下载输入文件到 job workspace，调用分析链路读取数据、规划问题、生成图表和分析摘要。

3. 生成候选图  
   分析阶段会生成候选图表和任务元数据。当前重要设计：通常生成约 10 张候选图表；分析阶段可能需要 5-8 分钟。

4. 用户勾选候选图  
   前端展示候选图、问题和分析摘要。用户勾选后调用 `/jobs/{job_id}/selection`，后端把渲染任务加入队列。

5. 生成并下载报告  
   渲染 worker 根据用户选择的图表生成报告产物。当前重要设计：报告导出阶段通常约 30 秒。完成后用户可下载 `zip/docx/pdf/ipynb/txt` 等产物。

## 当前页面主要功能模块

前端主要代码在 `autoreport-ai-智能报告工作台/src/features/smart-analyst-app.tsx`，运行状态管理在 `src/features/jobs/useJobRuntime.ts`。

- 登录/注册视图：用户认证、会话恢复、登出。
- 上传区：选择文件、显示配额、创建新 Job。
- 左侧工作流侧栏：展示 Workflow、当前任务、活跃任务、历史任务、配额。
- 实时任务阶段视图：展示分析/渲染进度、事件流。
- 候选图选择视图：展示候选任务和图表，提交用户选择。
- 完成视图：列出报告产物并下载。
- 失败/过期视图：展示中文友好错误、最近事件和可操作建议。
- 管理后台：`src/features/admin/admin-app.tsx`，用于 Owner/Viewer 查看系统、用户、任务和调试信息。

## 后端任务流转逻辑

后端主入口是 `SmartAnalyst/service/api.py`。

核心状态和模型在 `SmartAnalyst/service/models.py`。Job 大致经历：

- `uploaded`
- `queued_analysis`
- `running_analysis`
- `awaiting_selection`
- `queued_render`
- `rendering`
- `completed`
- `failed`
- `expired`

主要 API：

- `POST /auth/register`
- `POST /auth/login`
- `GET /me`
- `POST /jobs`
- `GET /jobs`
- `GET /jobs/{job_id}`
- `GET /jobs/{job_id}/tasks`
- `GET /jobs/{job_id}/events`
- `GET /jobs/{job_id}/stream`
- `POST /jobs/{job_id}/selection`
- `GET /jobs/{job_id}/artifacts`
- `GET /jobs/{job_id}/download/{artifact_type}`
- 管理端 `/admin/*`

分析任务在 `SmartAnalyst/service/tasks.py` 的 `run_analysis_job`。渲染任务在同文件的 `run_render_job`。过期清理由 `cleanup_expired_jobs` 处理。

## Celery / worker / beat 的作用

配置位置：`SmartAnalyst/service/celery_app.py`。

- API 进程只负责接收请求、保存数据库状态、入队任务。
- Celery worker 负责执行耗时任务：分析和渲染。
- Celery beat 定时触发过期任务清理。
- 本地启动脚本：`SmartAnalyst/scripts/start-local-stack.ps1` 会同时启动 API、worker、beat。
- Windows 下 worker 使用 `--pool=solo`，不要随意改成默认多进程池。

队列路由：

- `service.run_analysis_job` -> analysis queue
- `service.run_render_job` -> render queue
- `service.cleanup_expired_jobs` -> default queue

## 数据库的作用

本地默认 SQLite：`SmartAnalyst/smartanalyst.db`。生产说明中使用 RDS PostgreSQL。

数据库保存：

- 用户、会话相关数据
- 管理员账号和角色
- Job 状态、阶段、进度、错误摘要
- 输入文件索引
- 候选图任务
- 用户选择的图表
- 报告产物索引
- Job 生命周期事件
- 配额、使用量、在线状态等运营数据

不要随意修改 `service/models.py` 或 `alembic/`。涉及数据库结构必须按大改动流程处理。

## Redis / Tair 的作用

本地使用 Docker Redis 容器 `smartanalyst-redis`。生产说明中使用 Tair/Redis。

Redis/Tair 用于：

- Celery broker
- Celery result backend
- 限流计数
- 模型调用预算计数

如果 Redis 不可用，任务无法正常排队和执行。

## OSS / 本地存储的作用

存储抽象在 `SmartAnalyst/service/storage.py`。

本地开发使用：

- `SmartAnalyst/storage/` 保存上传文件、候选图、报告产物等持久对象
- `SmartAnalyst/runs/` 保存运行时 workspace

生产部署说明中使用 OSS/S3 类存储保存：

- 用户上传文件
- 候选图
- 报告产物

不要随意删除 `storage/`、`runs/` 或改变对象 key 规则。改存储路径会影响下载、清理和任务恢复。

## 前端状态同步方式

前端 API 封装在 `autoreport-ai-智能报告工作台/src/lib/api.ts`。

状态管理在 `src/features/jobs/useJobRuntime.ts`：

- 当前 Job ID 保存到 `localStorage` 的 `smartanalyst.currentJobId`
- 普通查询使用 REST API
- Job 运行中时使用 `EventSource` 打开 `/jobs/{job_id}/stream`
- 事件增量可通过 `/jobs/{job_id}/events?after=...` 获取
- 任务列表、产物列表按 Job 状态懒加载或刷新
- 会话失效时统一走 `UnauthorizedError` 处理

后端 SSE 在 `service/api.py` 的 `stream_job`。如果 SSE 不可用，需检查浏览器、CORS、cookie、API 地址和后端健康状态。

## 权限体系：Owner / Viewer

管理后台说明见 `ADMIN_CONSOLE.md`。

- Owner：由 `ADMIN_OWNER_EMAIL` 和 `ADMIN_OWNER_INITIAL_PASSWORD` 初始化。可授予 Viewer、修改用户状态/配额、重试或取消任务等。
- Viewer：可查看 dashboard、jobs、users、health，但不能修改任务、用户或配额。

管理接口依赖 `SmartAnalyst/service/admin_auth.py`。不要随意放宽权限或绕过 `require_owner_admin`。

## 错误展示策略

必须遵守：

- 普通用户看中文友好提示和可操作建议。
- Owner/Admin 可查看技术详情、事件 payload、raw detail、traceback 等调试信息。
- 不要把英文内部报错、上游模型异常、数据库异常或 traceback 直接暴露给普通用户。
- 后端内部错误可记录到事件和日志；前端展示时要经过中文化和分级。

相关位置：

- 后端错误映射：`SmartAnalyst/service/error_mapper.py`
- 任务事件：`SmartAnalyst/service/tasks.py`
- 前端错误展示：`src/features/jobs/useJobRuntime.ts`、`smart-analyst-app.tsx`
- 管理端调试详情：`src/features/admin/admin-app.tsx`

## 已知重要设计

- 分析阶段可能需要 5-8 分钟，UI 不应暗示“几秒完成”。
- 报告导出阶段通常约 30 秒。
- 分析阶段通常生成约 10 张候选图表。
- 用户选择候选图后才进入最终报告生成。
- 排队和运行中任务受控并发，不要绕开配额和队列。
- 运行中分析/渲染任务当前不一定支持强制中断；删除/取消逻辑必须尊重后端约束。
- 模型 API Key 只在后端环境中使用，不进入前端构建。

## 关键命令

本地 Redis：

```powershell
docker start smartanalyst-redis
```

后端：

```powershell
cd SmartAnalyst
powershell -ExecutionPolicy Bypass -File .\scripts\start-local-stack.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\status-local-stack.ps1
```

前端：

```powershell
cd autoreport-ai-智能报告工作台
npm run dev
npm run lint
npm run build
```

后端测试：

```powershell
cd SmartAnalyst
..\.venv\Scripts\python.exe -m pytest
```

生产部署命令见 `DEPLOYMENT_ALIYUN.md` 和 `deploy/docker-compose.prod.yml`。如部署细节与当前环境不一致，标注“待确认”，不要猜。

