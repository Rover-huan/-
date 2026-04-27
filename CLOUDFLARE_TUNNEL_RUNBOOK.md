# SmartAnalyst Cloudflare Tunnel 临时公网手册

这份文档只用于 Cloudflare Tunnel 临时公网体验，不是正式部署方案。

## 一、适用场景

适合：

- 你的电脑开着。
- Docker 本地 demo 正在运行。
- 临时让同学或朋友通过公网访问你电脑上的本地网页。
- 不想购买正式服务器。
- 只做短时间体验和演示。

注意：

- Quick Tunnel 地址可能每次变化。
- 你电脑关机、睡眠、断网或关闭 tunnel 后，公网地址就失效。
- 这不适合长期正式上线。
- 不要让别人上传隐私数据。

## 二、前置条件

1. 本地 demo 已启动：

```powershell
cd C:\Users\lenovo\Desktop\实验
docker compose -f docker-compose.local-demo.yml --env-file .env.local-demo up -d
```

2. 本地首页可以访问：

```text
http://localhost:8080
```

3. 本地健康检查正常：

```text
http://localhost:8080/api/healthz
```

4. `cloudflared` 已安装。

本机常用路径：

```powershell
$env:USERPROFILE\cloudflared\cloudflared.exe
```

## 三、启动 Cloudflare Tunnel

打开一个新的 PowerShell 终端，不要关闭本地 demo 终端。

优先使用 HTTP/2 协议：

```powershell
& "$env:USERPROFILE\cloudflared\cloudflared.exe" tunnel --protocol http2 --url http://localhost:8080
```

如果失败，再试 IPv4 + HTTP/2：

```powershell
& "$env:USERPROFILE\cloudflared\cloudflared.exe" tunnel --edge-ip-version 4 --protocol http2 --url http://localhost:8080
```

如果看到 QUIC 报错或 UDP 7844 超时，不代表本地服务坏了。很多校园网、公司网或家庭路由器会限制 QUIC/UDP，可以改用：

```powershell
--protocol http2
```

## 四、获取公网地址

启动成功后，cloudflared 日志里会出现类似：

```text
https://xxxx.trycloudflare.com
```

这就是临时公网地址。

这个地址每次可能变化。不要把旧地址写死到长期文档里。

## 五、更新 .env.local-demo

拿到新的 tunnel 地址后，需要手动更新 `.env.local-demo` 里的两个值。

示例：

```text
PUBLIC_BASE_URL=https://新的地址.trycloudflare.com
CORS_ORIGINS=http://localhost:8080,http://127.0.0.1:8080,https://新的地址.trycloudflare.com
```

注意：

- 不要把 `.env.local-demo` 全文复制到聊天里。
- 不要把 API Key、`SECRET_KEY`、管理员密码写进文档。
- 只改公网地址和 CORS 地址。

## 六、更新后重启本地 demo

```powershell
cd C:\Users\lenovo\Desktop\实验
docker compose -f docker-compose.local-demo.yml --env-file .env.local-demo up -d
```

确认服务正常：

```powershell
docker compose -f docker-compose.local-demo.yml ps
```

## 七、让别人访问

公网首页：

```text
https://新的地址.trycloudflare.com
```

公网健康检查：

```text
https://新的地址.trycloudflare.com/api/healthz
```

如果公网首页打不开，先确认本地能打开：

```text
http://localhost:8080
```

如果本地能打开但公网打不开，优先看 cloudflared 终端日志。

## 八、发给同学测试时的提醒话术

可以直接复制下面这段：

```text
这是 SmartAnalyst 的临时体验版，不是正式上线网站。

访问地址：
https://新的地址.trycloudflare.com

注意：
1. 只有我电脑开着、Docker 和 Cloudflare Tunnel 都运行时，这个地址才能访问。
2. 文件尽量小，建议只上传测试用 csv/xls/xlsx。
3. 一次只跑一个任务，人多会排队或变慢。
4. 不要上传隐私数据、公司数据、身份证、手机号等敏感信息。
5. 如果页面报错或生成失败，请把页面截图和大概操作步骤发给我。
```

## 九、关闭公网访问

关闭公网访问：

- 在 cloudflared 终端按 `Ctrl + C`
- 或直接关闭运行 cloudflared 的终端窗口

关闭后：

- `https://xxxx.trycloudflare.com` 公网地址会失效。
- 本地 Docker 服务仍然运行。

如果还要停止本地服务：

```powershell
cd C:\Users\lenovo\Desktop\实验
docker compose -f docker-compose.local-demo.yml down
```

## 十、常见问题

### 1. QUIC / UDP 7844 超时

现象可能类似：

```text
Failed to dial a quic connection
timeout: no recent network activity
```

处理：

```powershell
& "$env:USERPROFILE\cloudflared\cloudflared.exe" tunnel --protocol http2 --url http://localhost:8080
```

如果还失败：

```powershell
& "$env:USERPROFILE\cloudflared\cloudflared.exe" tunnel --edge-ip-version 4 --protocol http2 --url http://localhost:8080
```

### 2. tunnel 地址打开后 API 报错

检查 `.env.local-demo` 是否更新了：

```text
PUBLIC_BASE_URL=https://新的地址.trycloudflare.com
CORS_ORIGINS=http://localhost:8080,http://127.0.0.1:8080,https://新的地址.trycloudflare.com
```

更新后重启：

```powershell
docker compose -f docker-compose.local-demo.yml --env-file .env.local-demo up -d
```

### 3. 同学打开慢

本地体验版跑在你的电脑上，不是云服务器。分析任务会调用模型、跑 worker、生成报告。建议一次只让一个人跑任务。

### 4. 下载链接异常

先检查：

- tunnel 地址是否仍然有效。
- `.env.local-demo` 里的 `PUBLIC_BASE_URL` 是否是当前 tunnel 地址。
- nginx 是否在运行。

命令：

```powershell
docker compose -f docker-compose.local-demo.yml ps
docker compose -f docker-compose.local-demo.yml logs --tail=100 nginx
docker compose -f docker-compose.local-demo.yml logs --tail=100 api
```

## 十一、临时公网最短流程

第一个终端：

```powershell
cd C:\Users\lenovo\Desktop\实验
docker compose -f docker-compose.local-demo.yml --env-file .env.local-demo up -d
```

第二个终端：

```powershell
& "$env:USERPROFILE\cloudflared\cloudflared.exe" tunnel --protocol http2 --url http://localhost:8080
```

拿到 `https://xxxx.trycloudflare.com` 后，更新 `.env.local-demo` 的 `PUBLIC_BASE_URL` 和 `CORS_ORIGINS`，再重启：

```powershell
docker compose -f docker-compose.local-demo.yml --env-file .env.local-demo up -d
```
