# hyxware-relay

13 个 hyxware 账号的 OpenAI 兼容中转网关：客户端只认一个中转密钥，
网关按轮换把请求发到 `https://hyxware.com/v1`，每号独立并发上限 10，
429 / 5xx / 无响应时自动换号重试。

## 部署（Render，手动，和 cassidy-relay 同一套路）

- 新建 Web Service，仓库选本仓库，Region 选 Singapore，Plan 选 Free。
- Start Command（单 worker，绝不能改多 worker，否则每号并发上限翻倍）：
  `uvicorn app:app --host 0.0.0.0 --port $PORT --workers 1`
- 环境变量（只填 Render 后台，绝不写进仓库）：
  - `RELAY_KEY`：客户端用的中转密钥。
  - `HYXWARE_KEY_PM01` … `HYXWARE_KEY_PM07`、`HYXWARE_KEY_16304`、
    `HYXWARE_KEY_16305`、`HYXWARE_KEY_MILINKEN2365`、`HYXWARE_KEY_16301`、
    `HYXWARE_KEY_16302`、`HYXWARE_KEY_DWDBE`：13 个号位。
  - 可选：`CONCURRENCY_PER_KEY`（默认 10）、`UPSTREAM_URL`（默认
    `https://hyxware.com/v1`）。
- 注意：pm07 号位用的是 test2（main 从未建成）。pm01 / pm04 / pm06 /
  16304 / 16305 五个号位目前无值，留空即可；恢复登录新建 key 后补填
  对应环境变量，Render 改环境变量会自动重新部署，号位即自动启用。

## 客户端用法

Base URL：`https://hyxware-relay.onrender.com/v1`，请求头
`Authorization: Bearer <RELAY_KEY>`，其余和 OpenAI 格式一样。
Free 层闲置约 15 分钟休眠、冷启动约 1 分钟。

## 自查接口（都要带 RELAY_KEY）

- `GET /health`：已配置号位数（无需鉴权）。
- `GET /admin/keys-check`：逐号调上游 `/v1/models` 探活，只报掩码与
  http 状态，不输出完整值。
