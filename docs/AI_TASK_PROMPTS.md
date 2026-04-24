# 可复制给 AI 的任务提示词模板

这些模板用于未来让 Codex / Claude Code / 其他 AI 编程助手接手任务。使用前可把具体需求替换到括号中。

## 小 UI 文案修改模板

```text
请先阅读 AGENTS.md、docs/AI_PROJECT_GUIDE.md、docs/AI_CHANGE_PROTOCOL.md。

我要做一个小 UI 文案修改：把（页面/按钮/提示）中的（旧文案）改成（新文案）。

要求：
- 只定位并修改相关前端文件。
- 不改接口契约，不改后端，不改数据库。
- 不做无关重构或大范围格式化。
- 修改后运行 npm run lint；如涉及构建风险，再运行 npm run build。
- 汇报修改文件、验证结果和风险。
```

## 前端页面布局修复模板

```text
请先阅读 AGENTS.md、docs/AI_PROJECT_GUIDE.md、docs/AI_CHANGE_PROTOCOL.md。

我要优化/修复前端布局：（描述具体页面、截图位置、问题现象）。

要求：
- 先用 rg 定位组件，不要全仓盲扫。
- 保持现有 React/Vite/Tailwind 风格。
- 只改相关组件和样式。
- 不改后端 API、不改状态流转、不改数据库。
- 注意移动端和桌面端不要文字溢出或互相遮挡。
- 修改后运行 npm run lint 和 npm run build。
- 最后告诉我改了哪些文件，以及我应该打开哪个 URL 验收。
```

## 后端报错修复模板

```text
请先阅读 AGENTS.md、docs/AI_PROJECT_GUIDE.md、docs/AI_CHANGE_PROTOCOL.md。

后端出现这个问题：（粘贴错误摘要，不要粘贴密钥）。

要求：
- 不读取 .env 的真实密钥。
- 先定位相关 API / service / task 文件。
- 如果只是错误提示或小 bug，只做最小修复。
- 普通用户错误提示必须中文友好，不要暴露 traceback 或英文内部异常。
- 如果涉及鉴权、数据库、任务队列、存储或报告生成链路，先写计划并等我确认。
- 修改后运行相关 pytest；如果不能运行，说明原因。
- 汇报修改文件、验证命令、风险。
```

## 新功能开发模板

```text
请先阅读 AGENTS.md、docs/AI_PROJECT_GUIDE.md、docs/AI_CHANGE_PROTOCOL.md。

我想新增功能：（描述功能）。

请先不要直接改核心代码。先输出开发计划，包含：
- 产品目标和用户流程。
- 影响范围：前端、后端、worker、数据库、存储、权限、安全、部署。
- 需要修改的文件。
- 是否需要新增 API 或改变接口契约。
- 是否需要数据库迁移。
- 是否影响 Celery 队列或任务状态。
- 回滚方式。
- 验收标准。

等我确认后再开始实现。实现时保持最小改动，不做无关重构。
```

## 部署/上线检查模板

```text
请先阅读 AGENTS.md、docs/AI_PROJECT_GUIDE.md、docs/AI_CHANGE_PROTOCOL.md，以及 DEPLOYMENT_ALIYUN.md。

请帮我做部署/上线前检查。

要求：
- 不读取或输出真实 .env / production env 密钥。
- 只检查配置项名称、文件结构、命令和风险。
- 检查前端构建、后端测试、Docker Compose、Nginx、API_ROOT_PATH、CORS、cookie secure、Redis/Tair、RDS、OSS、SMTP、Captcha。
- 不确定的命令标注“待确认”，不要假装确定。
- 输出上线检查清单、风险项、建议验证步骤。
```

## 不允许大范围重构的约束模板

```text
请先阅读 AGENTS.md、docs/AI_PROJECT_GUIDE.md、docs/AI_CHANGE_PROTOCOL.md。

本次任务禁止大范围重构。请严格遵守：
- 只改和（具体问题）直接相关的文件。
- 不改目录结构。
- 不改数据库模型和迁移。
- 不改鉴权、任务队列、存储、部署脚本。
- 不做全局格式化。
- 不删除用户数据、storage、runs、smartanalyst.db。
- 不读取或泄露 .env 里的真实密钥。
- 修改后运行最小必要测试，并说明没有验证的部分。
```

