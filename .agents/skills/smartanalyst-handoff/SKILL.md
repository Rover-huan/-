---
name: smartanalyst-handoff
description: Helps Codex quickly onboard to the SmartAnalyst / AutoReport AI project, follow project rules, locate relevant files, and make scoped changes safely.
---

# SmartAnalyst Handoff Skill

Use this skill when taking over work on the SmartAnalyst / AutoReport AI project. Its purpose is to help Codex understand the project context, avoid broad rewrites, and make safe scoped changes.

## Required Reading Order

1. `AGENTS.md`
2. `docs/AI_PROJECT_GUIDE.md`
3. `docs/AI_CHANGE_PROTOCOL.md`
4. Read `docs/AI_TASK_PROMPTS.md` only when reusable user prompts are needed.

## Working Method

1. Understand the user's task and classify it as a small change or a large change.
2. Locate relevant files with `rg` before editing.
3. Propose the smallest reasonable change. For large changes, write a plan and wait for user confirmation before touching core code.
4. Modify only relevant files. Do not refactor unrelated modules.
5. Run the minimum necessary validation.
6. Report changed files, why they changed, how validation was done, risks, and next steps.

## Boundaries

- Do not read or disclose `.env`, production environment values, or real secrets.
- Do not write API keys, `SECRET_KEY`, database passwords, or email passwords into code or docs.
- Do not casually modify auth, task queues, database migrations, file storage, the AI execution sandbox, report generation, or deployment scripts.
- Ordinary user-facing errors must be friendly Chinese messages. Technical details belong only in logs or Owner/Admin views.

## Reference Paths

- Frontend: `autoreport-ai-智能报告工作台/`
- Backend: `SmartAnalyst/`
- Deployment: `deploy/`
- Local startup guide: `每日启动测试手册.md`

