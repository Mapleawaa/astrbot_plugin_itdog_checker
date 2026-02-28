# AGENT.md

## 1. 项目定位

`astrbot_plugin_itdog_checker` 是 AstrBot 插件，负责把群聊命令转换为 itdog 网络探测任务，并通过本地服务端（Seat bridge）异步执行，再回传结果。

核心目标：
- 支持 `/itdog ping|tcping|http|traceroute`。
- 支持运营商与地区筛选节点。
- 支持限流、冷却、管理员豁免。
- 通过可编辑消息能力优化用户体验（平台支持时优先编辑首条消息）。

## 2. 目录架构

```text
astrbot_plugin_itdog_checker/
├─ main.py                 # 插件入口、命令解析、队列调度、限流、结果回传
├─ itdog_nodes.py          # 节点数据、运营商/地区筛选、节点名称映射
├─ itdog_utils.py          # itdog Web 交互 client（HTTP + WebSocket + anti-bot 逻辑）
├─ _conf_schema.json       # AstrBot 插件配置项 schema
├─ metadata.yaml           # 插件元信息（name/version/desc）
├─ requirements.txt        # 依赖（aiohttp, websockets）
├─ README.md               # 使用说明（含 Seat 模式说明）
└─ itdog-api-docs.md       # itdog Web API 行为记录与协议说明
```

## 3. 运行架构与数据流

1. 用户在聊天中发送 `/itdog ...` 命令。
2. `main.py` 解析子命令与参数，做限流校验。
3. 合法任务进入 `asyncio.Queue`，先回一条受理消息。
4. worker 从队列取任务，请求本地 bridge：`POST {service_url}/run`。
5. bridge 执行 itdog 请求（由 Seat 客户端或服务侧能力完成），返回 `results/errors/traceroute_logs`。
6. 插件组装报告文本，优先编辑首条消息；不支持编辑则补发新消息。

说明：
- 插件本体是“任务编排层 + 结果展示层”。
- itdog 实际协议细节封装在 `itdog_utils.py` 与对应服务侧实现中。

## 4. 核心模块职责

### 4.1 `main.py`
- 插件生命周期：`initialize/terminate`。
- 会话管理：`aiohttp.ClientSession`。
- 队列执行：`worker/process_task`。
- 命令处理：`dispatch_command` + `@filter.command("itdog")`。
- 参数解析：`parse_args`（target / isp / location / 端口）。
- 限流：`_check_and_consume_quota`，状态持久化到 `data/itdog_checker_rate_limit.json`。
- 输出渲染：`_build_report`。

### 4.2 `itdog_nodes.py`
- 内置节点表 `COMPACT_NODES`。
- 节点筛选 `filter_nodes(isp, location)`。
- 节点展示 `get_node_name()`。
- 默认节点 `DEFAULT_NODES_LIST`（北上广三网）。

### 4.3 `itdog_utils.py`
- 处理 itdog anti-bot：`guard -> guardret`。
- task token 计算：`md5(task_id + secret)[8:-8]`。
- 提供 `batch_ping/tcping/http_test/traceroute`。
- 通过 WebSocket 流式接收结果。
- SSL 策略支持 `ITDOG_SSL_NO_VERIFY` 与 CA bundle 环境变量。

## 5. 配置与环境变量

插件配置（`_conf_schema.json`）：
- `service_url`：bridge 地址（默认 `http://127.0.0.1:8765`）。
- `service_token`：bridge 鉴权 token。
- `timezone`：限流时区。
- `daily_limit`：普通用户日调用上限。
- `cooldown_seconds`：普通用户冷却秒数。
- `admins`：管理员列表（豁免限流）。
- `edit_message_if_possible`：可编辑时是否编辑首条消息。

环境变量（代码里实际读取）：
- `ITDOG_SERVICE_URL`
- `ITDOG_SERVICE_TOKEN`

说明：README 中出现 `ITDOG_BRIDGE_*` 命名，当前 `main.py` 实际读取的是 `ITDOG_SERVICE_*`，后续改动需统一命名，避免配置认知分裂。

## 6. 开发约束（针对本项目）

- 保持异步流程：命令入口不阻塞，耗时任务必须走队列。
- 新增命令时，同步补全三处：`dispatch_command`、帮助文本、报告构建分支。
- 涉及节点筛选变更时，优先改 `itdog_nodes.py`，避免在 `main.py` 写散逻辑。
- 修改限流逻辑时，兼容旧 `usage_state` 数据结构，避免直接破坏线上历史数据。
- 任何和 itdog 协议有关的调整，先比对 `itdog-api-docs.md` 再改实现。

## 7. 本地联调建议

1. 安装依赖：`pip install -r requirements.txt`。
2. 启动 AstrBot 并加载插件。
3. 确认 bridge 可访问：`service_url/run` 能正常响应。
4. 逐条验证：
   - `/itdog ping 1.1.1.1`
   - `/itdog tcping 1.1.1.1:443`
   - `/itdog http https://example.com`
   - `/itdog traceroute 1.1.1.1`
5. 验证筛选参数：`ct/cu/cm`、`--isp`、`--loc`、城市缩写。
6. 验证限流：冷却、日限额、管理员豁免。
7. 验证消息编辑降级：支持编辑的平台与不支持编辑的平台都要测。

## 8. 常见坑位

- 编码与中文文本：文档和源码含大量中文，编辑器请固定 UTF-8，避免注释/提示文本乱码。
- anti-bot 变化：`TASK_TOKEN_SECRET`、`GUARD_XOR_SUFFIX` 可能随站点策略变化，失效时先排查此处。
- 超时与异常：网络波动时优先确保错误可回传给用户，而不是吞异常。
- 配置命名不一致：`BRIDGE` 与 `SERVICE` 的变量前缀应统一，防止部署侧误配置。

## 9. 给后续 Agent 的执行规则

- 先读 `main.py` 与 `_conf_schema.json`，再动命令或配置相关代码。
- 涉及外部协议时，先看 `itdog-api-docs.md` 与 `itdog_utils.py`。
- 提交前至少跑一次 4 类命令的端到端冒烟。
- 如果改了用户可见文案，同步更新 `README.md`。
