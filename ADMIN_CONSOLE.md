# SmartAnalyst Admin Console

- Admin URL: `/admin`.
- Configure `ADMIN_OWNER_EMAIL` and `ADMIN_OWNER_INITIAL_PASSWORD` before first production boot.
- The service creates or preserves that Owner administrator automatically at startup.
- Owner can grant Viewer access to an already registered user by email.
- Viewer can inspect dashboard, jobs, users, and health, but cannot mutate jobs, users, or quotas.
