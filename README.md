# astrbot_plugin_itdog_checker

基于 itdog.cn 的网络测速插件，支持 Ping / TCPing / HTTP / Traceroute，并支持按运营商与省份/城市筛选节点。

## 内网 Seat 模式

当 AstrBot 所在机器无法直连外网或存在证书链问题时，可使用“Seat 客户端”模式：

- AstrBot 插件作为服务端：只负责接收群聊命令、下发任务、回传报告
- 独立 Python Seat 客户端作为执行端：主动轮询拉取任务，在可联网环境中执行 itdog 请求，并把结果回传给插件

服务端默认监听：

- `http://127.0.0.1:8765`

可用环境变量：

- `ITDOG_BRIDGE_HOST`：监听地址（默认 `127.0.0.1`）
- `ITDOG_BRIDGE_PORT`：监听端口（默认 `8765`）
- `ITDOG_BRIDGE_TOKEN`：可选鉴权 token（客户端需在请求头携带 `X-ITDOG-TOKEN`）
