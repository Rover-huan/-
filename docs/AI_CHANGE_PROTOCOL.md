# AI 修改项目操作规程

本文档规定未来 AI 修改 AutoReport AI / SmartAnalyst 时的操作边界。接手任务必须先读 `AGENTS.md`，再读 `docs/AI_PROJECT_GUIDE.md`。

## 接手任务前必须做什么

1. 明确用户目标：是修 UI、修 bug、加功能、排查部署，还是写文档。
2. 快速阅读：
   - `AGENTS.md`
   - `docs/AI_PROJECT_GUIDE.md`
   - 本文档
3. 用 `rg` 定位相关代码，不要全仓无目的阅读。
4. 不读取 `.env` 或生产环境配置文件中的真实值。只允许查看 `.env.example` 或文档中的占位说明。
5. 判断改动类型：小改动或大改动。
6. 修改前说明将改哪些区域；大改动必须先给计划并等用户确认。

## 如何判断小改动和大改动

小改动通常是：

- 页面文案、中文提示、按钮状态、局部布局。
- 单个前端组件的样式或结构微调。
- 单个 API 的错误提示修复。
- 不改变数据库、不改变接口、不改变任务队列语义的 bug fix。
- 不影响部署和权限模型。

大改动通常是：

- 新功能或跨前后端流程。
- 新增/修改数据库字段、迁移、索引。
- 修改鉴权、Owner/Viewer 权限、cookie/session。
- 修改 Celery 队列、任务状态、重试策略、取消逻辑。
- 修改文件存储、OSS key、下载链接、过期清理。
- 修改 AI 分析链路、执行器沙箱、报告生成链路。
- 修改部署脚本、Docker Compose、Nginx、生产环境配置。

不确定时按大改动处理。

## 小改动原则

- 只改相关文件。
- 不重构无关模块。
- 不改接口契约。
- 不改数据库模型和迁移。
- 不改队列路由、任务状态和 worker 启动方式。
- 修改前先定位文件和调用点。
- 修改后运行最小必要测试。
- UI 小改动优先运行：

```powershell
cd autoreport-ai-智能报告工作台
npm run lint
npm run build
```

- 后端小改动优先运行相关测试：

```powershell
cd SmartAnalyst
..\.venv\Scripts\python.exe -m pytest tests\test_api_regressions.py
```

如果测试命令不可用或环境缺依赖，必须在汇报中说明。

## 大改动原则

动核心代码前先写计划，至少包含：

- 目标和用户价值。
- 影响范围：前端、后端、worker、数据库、存储、部署、权限、安全。
- 文件清单和预计修改点。
- 接口契约是否变化。
- 数据库迁移是否需要。
- 任务队列和状态流转是否变化。
- 回滚方式。
- 验收标准。
- 需要用户确认的问题。

用户确认前不要修改核心代码。可以只做只读扫描和计划。

## 不能随便改的区域

以下区域必须按大改动流程处理：

- 鉴权系统：`SmartAnalyst/service/security.py`、`service/dependencies.py`、`service/admin_auth.py`
- 任务队列：`SmartAnalyst/service/celery_app.py`、`service/tasks.py`
- 数据库模型和迁移：`SmartAnalyst/service/models.py`、`SmartAnalyst/alembic/`
- 文件存储：`SmartAnalyst/service/storage.py`
- AI 执行器沙箱和分析链路：`SmartAnalyst/src/node*.py`、`src/runner_subprocess.py`
- 报告生成链路：`SmartAnalyst/src/node4_renderer.py`、`SmartAnalyst/templates/`
- `.env` 配置和生产密钥：`SmartAnalyst/.env`、`deploy/.env.production`
- 部署脚本：`deploy/`、`SmartAnalyst/Dockerfile`、前端 `Dockerfile`
- 本地数据库、上传和产物：`SmartAnalyst/smartanalyst.db`、`storage/`、`runs/`

## 安全和隐私规则

- 不读取、不输出真实密钥。
- 不把 `.env` 内容复制到文档或回答。
- 不在前端引入模型 Key。
- 不把内部英文错误、traceback、SQL 错误直接展示给普通用户。
- 不绕过用户归属校验，Job 只能由所属用户访问；管理端也要尊重 Owner/Viewer 权限。

## 验证策略

按改动范围选择最小必要验证：

- 前端文案/布局：`npm run lint`，必要时 `npm run build`，浏览器手动刷新查看。
- 前端 API 行为：同时确认后端 health，手动走登录/上传/状态同步流程。
- 后端 API：相关 pytest + `GET /healthz`。
- 任务流：Redis、API、worker、beat 全部启动后，上传样例文件跑一次完整或关键路径。
- 数据库/迁移：迁移测试、回滚方案、旧数据兼容检查。
- 部署：检查 `deploy/docker-compose.prod.yml`、Nginx、环境变量说明；真实上线命令如不确定标注“待确认”。

## 修改完成后的汇报格式

最终汇报必须包含：

- 改了哪些文件。
- 为什么改。
- 如何验证，列出命令和结果。
- 有哪些风险或未验证点。
- 下一步建议。

推荐格式：

```text
已完成：
- 文件 A：做了什么
- 文件 B：做了什么

验证：
- npm run lint：通过
- npm run build：通过
- pytest ...：通过/未运行，原因是 ...

风险：
- ...

下一步：
- ...
```

