# SmartAnalyst 阿里云首版部署说明

## 架构

- ECS：运行 Docker Compose，包含 `nginx`、`api`、`worker-analysis`、`worker-render`、`beat`。
- RDS PostgreSQL：保存用户、任务、事件、产物索引。
- Tair/Redis：Celery broker/result backend、限流、模型调用预算计数。
- OSS：保存上传文件、候选图、报告产物。

## 上线步骤

1. 在 `deploy/.env.production.example` 基础上创建 `deploy/.env.production`，填入真实 RDS、Tair/Redis、OSS、Captcha、模型 Key。SMTP 配置先保留占位，首版默认不强制使用。
2. 确认 `APP_ENV=production`、`API_ROOT_PATH=/api`、`AUTH_COOKIE_SECURE=true`、`STORAGE_BACKEND=s3`。
3. 在 ECS 安装 Docker 和 Docker Compose。
4. 执行 `docker compose -f deploy/docker-compose.prod.yml --env-file deploy/.env.production up -d --build`。
5. 验收：
   - `GET https://你的域名/api/healthz`
   - `GET https://你的域名/api/readyz`
   - 注册、登录、上传数据、生成一次报告、下载 zip。

## Beta 邮箱验证策略

- 当前 Beta / 阿里云首版默认设置 `EMAIL_VERIFICATION_REQUIRED=false`。
- 用户注册后可直接登录并使用报告生成流程，后端仍会保留邮箱验证字段和 SMTP 配置项。
- 后续正式开放注册时，建议切换为 `EMAIL_VERIFICATION_REQUIRED=true`。
- 开启前需要配置可用 SMTP，并补齐前端 `/verify-email?email=...&token=...` 页面来处理邮件链接。

## 运维要点

- 模型 API Key 只放在 `deploy/.env.production` 或云端密钥配置，不提交到仓库，不进入前端构建。
- 已经出现在示例文件或日志里的模型 Key 应在供应商控制台轮换。
- 初始 worker 并发建议：`ANALYSIS_WORKER_CONCURRENCY=2`、`RENDER_WORKER_CONCURRENCY=1`，压测后再扩。
- RDS 开自动备份，OSS 配生命周期清理过期 job 文件。
- 如果 ECS 和域名在中国大陆地域，正式公网访问前完成 ICP 备案。
