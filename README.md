# AutoReport AI / SmartAnalyst

这是一个面向数据上传、自动分析、候选图表选择和报告导出的智能分析应用。

## AI 接手项目说明

未来任何 Codex / Claude Code / 其他 AI 编程助手接手本项目时，请先阅读：

- [AGENTS.md](AGENTS.md)
- [docs/AI_PROJECT_GUIDE.md](docs/AI_PROJECT_GUIDE.md)
- [docs/AI_CHANGE_PROTOCOL.md](docs/AI_CHANGE_PROTOCOL.md)
- [docs/AI_TASK_PROMPTS.md](docs/AI_TASK_PROMPTS.md)

这些文档定义了项目架构、运行命令、修改边界、禁止事项和验收标准。不要在未阅读这些文档的情况下直接修改核心代码。

## 主要目录

- `autoreport-ai-智能报告工作台/`：前端 React/Vite 项目
- `SmartAnalyst/`：后端 FastAPI、Celery worker、数据库模型和分析/报告链路
- `deploy/`：部署相关配置
- `每日启动测试手册.md`：本地启动和测试流程

