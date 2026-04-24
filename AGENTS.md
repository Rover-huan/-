# AutoReport AI / SmartAnalyst - AI 接手入口

未来任何 Codex / Claude Code / 其他 AI 编程助手进入本项目后，必须先读本文件，再读：

1. `docs/AI_PROJECT_GUIDE.md`
2. `docs/AI_CHANGE_PROTOCOL.md`

本项目是一个智能分析应用：用户上传 `csv/xls/xlsx` 数据，系统自动分析数据、生成候选图表，用户勾选图表后生成并下载报告。

## 项目位置

- 前端：`autoreport-ai-智能报告工作台/`
- 后端 API / worker / 数据模型：`SmartAnalyst/`
- 部署配置：`deploy/`
- 本地运行手册：`每日启动测试手册.md`
- 管理后台说明：`ADMIN_CONSOLE.md`
- 阿里云部署说明：`DEPLOYMENT_ALIYUN.md`

## 主要架构

- 前端：React + Vite + TypeScript，入口在 `autoreport-ai-智能报告工作台/src/`
- 后端 API：FastAPI，主路由在 `SmartAnalyst/service/api.py`
- 任务队列：Celery，配置在 `SmartAnalyst/service/celery_app.py`
- Worker 任务：`SmartAnalyst/service/tasks.py`
- 数据库：SQLAlchemy models 在 `SmartAnalyst/service/models.py`，迁移在 `SmartAnalyst/alembic/`
- 本地数据库：`SmartAnalyst/smartanalyst.db`
- 存储：`SmartAnalyst/service/storage.py`，本地目录为 `SmartAnalyst/storage/`，生产可用 OSS/S3
- Redis/Tair：Celery broker/result backend，限流和预算计数也依赖 Redis
- 分析执行链路：`SmartAnalyst/src/node1_scanner.py` 到 `node4_renderer.py`

## AI 修改代码前必须遵守

- 先定位相关文件，再修改。不要为了一个小问题全仓大范围重构。
- 不读取、不打印、不复制 `.env`、`deploy/.env.production` 或任何真实密钥。
- 不把 API Key、SECRET_KEY、数据库密码、邮箱密码写入文档、日志、测试或代码。
- 不随意修改接口契约、数据库模型、迁移、鉴权、任务队列、存储、部署脚本。
- 不删除用户已有文件、生成物、数据、数据库，除非用户明确要求。
- 不把英文内部异常直接暴露给普通用户。普通用户看中文友好提示；Owner/Admin 才能看技术详情。
- 修改前检查当前工作树或文件状态；如不是 git 仓库，也要避免覆盖用户未说明的改动。

## 小改动流程

适用：文案、样式、局部 UI、单个 API 错误提示、小范围 bug。

1. 阅读 `AGENTS.md`、`docs/AI_PROJECT_GUIDE.md`、`docs/AI_CHANGE_PROTOCOL.md`。
2. 用 `rg` 定位相关文件和调用点。
3. 只改相关文件，不重构无关模块。
4. 不改接口契约、不改数据库迁移、不改队列语义。
5. 运行最小必要验证。
6. 汇报改了哪些文件、为什么改、如何验证、剩余风险。

## 大改动流程

适用：新功能、任务流调整、数据库字段、鉴权权限、存储策略、部署、报告生成链路。

1. 先写修改计划。
2. 标明影响范围和回滚方式。
3. 明确数据库、任务队列、权限、安全、部署影响。
4. 等用户确认后再动核心代码。
5. 分步修改，分步验证，避免一次性大改。

## 禁止事项

- 禁止读取或泄露真实 `.env` 密钥。
- 禁止提交或硬编码模型 Key、邮箱密码、SECRET_KEY、数据库密码。
- 禁止无确认修改 `SmartAnalyst/alembic/`、`service/models.py`、`service/security.py`、`service/admin_auth.py`、`service/tasks.py`、`service/storage.py`、`deploy/`。
- 禁止把后端 traceback 原样展示给普通用户。
- 禁止用大范围格式化、重命名、目录搬迁来完成小需求。
- 禁止删除 `storage/`、`runs/`、`smartanalyst.db`、用户上传数据或生成报告，除非用户明确授权。

## 常用运行命令

启动 Redis：

```powershell
docker start smartanalyst-redis
```

启动后端 API + worker + beat：

```powershell
cd SmartAnalyst
powershell -ExecutionPolicy Bypass -File .\scripts\start-local-stack.ps1
```

查看后端状态：

```powershell
cd SmartAnalyst
powershell -ExecutionPolicy Bypass -File .\scripts\status-local-stack.ps1
```

停止后端：

```powershell
cd SmartAnalyst
powershell -ExecutionPolicy Bypass -File .\scripts\stop-local-stack.ps1
```

启动前端：

```powershell
cd autoreport-ai-智能报告工作台
npm run dev
```

常用地址：

- 前端：`http://127.0.0.1:3000` 或前端终端显示的 Local 地址
- API health：`http://127.0.0.1:8000/healthz`
- API docs：`http://127.0.0.1:8000/docs`
- 管理后台：`/admin`

## 常用测试命令

前端：

```powershell
cd autoreport-ai-智能报告工作台
npm run lint
npm run build
```

后端：

```powershell
cd SmartAnalyst
..\.venv\Scripts\python.exe -m pytest
```

单测示例：

```powershell
cd SmartAnalyst
..\.venv\Scripts\python.exe -m pytest tests\test_api_regressions.py
```

健康检查：

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/healthz
```

## 每次修改后的验收标准

- 相关功能能在本地复现并通过。
- 前端改动至少运行 `npm run lint`，涉及构建或样式时运行 `npm run build`。
- 后端改动至少运行相关 pytest；任务流、鉴权、存储、数据库改动要运行更完整的后端测试。
- 不包含真实密钥或敏感配置。
- 不引入普通用户可见的英文内部错误。
- 汇报中列出修改文件、验证命令、结果、风险和下一步建议。

