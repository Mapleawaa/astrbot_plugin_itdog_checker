# itdog.cn API 技术文档

![WARNING]
> 过高的请求速度可能会触发反爬虫机制，导致请求失败。建议控制请求频率，避免触发反爬虫机制。

## 接口概览

| 功能 | 端点 | 方法 | WebSocket |
|------|------|------|-----------|
| Batch Ping | /batch_ping/ | POST | ✓ |
| HTTP 测速 | /http/ | POST | ✓ |

## 通用流程

```
1. POST 表单数据到端点
2. 处理反爬虫 Cookie (guard → guardret)
3. 从 HTML 响应提取 wss_url 和 task_id
4. 计算 task_token (MD5)
5. WebSocket 连接并认证
6. 接收实时测速数据
```

## 反爬虫机制

### Guard Cookie 处理

服务器返回 `guard` Cookie，需计算 `guardret`:

```python
def generate_guardret(guard: str) -> str:
    key = guard[:8]
    num = int(guard[12:]) if len(guard) > 12 else 0
    value = num * 2 + 16
    encrypted = xor_encrypt(str(value), key + "PTNo2n3Ev5")
    return base64.b64encode(encrypted.encode()).decode()
```

### Task Token 生成

```python
SECRET = "token_20230313000136kwyktxb0tgspm00yo5"
task_token = md5(task_id + SECRET).hexdigest()[8:-8]
```

## Batch Ping 接口

### 请求

```
POST https://www.itdog.cn/batch_ping/
Content-Type: application/x-www-form-urlencoded

host=1.0.0.1%0D%0A1.1.1.1    # IP列表，\r\n 分隔
node_id=1274,1226            # 节点ID，逗号分隔
cidr_filter=true             # 过滤 CIDR 特殊地址
gateway=last                 # 网关位置 (last/first)
```

### WebSocket 数据格式

**发送:**
```json
{"task_id": "xxx", "task_token": "xxx"}
```

**接收:**
```json
{
  "ip": "1.0.0.1",
  "result": "171",
  "node_id": "1274",
  "task_num": 1,
  "address": "Anycast/cloudflare.com"
}
```

**结束标识:**
```json
{"type": "finished"}
```

## HTTP 测速接口

### 请求

```
POST https://www.itdog.cn/http/
Content-Type: application/x-www-form-urlencoded

line=                        # 线路
host=https://example.com     # 测试 URL
host_s=example.com           # 域名
check_mode=fast              # fast/detail
ipv4=                        # 指定 IPv4
method=get                   # HTTP 方法
referer=                     # 自定义 Referer
ua=                          # 自定义 UA
cookies=                     # 自定义 Cookies
redirect_num=5               # 重定向次数
dns_server_type=isp          # isp/custom
dns_server=                  # 自定义 DNS
```

### WebSocket 数据格式

**接收:**
```json
{
  "type": "success",
  "name": "北京电信",
  "ip": "220.181.111.1",
  "http_code": 200,
  "all_time": "0.050",
  "dns_time": "0.005",
  "connect_time": "0.005",
  "download_time": "0.023",
  "redirect": 0,
  "redirect_time": "0.000",
  "head": "HTTP/1.1 200 OK<br>...",
  "node_id": 1132,
  "address": "中国/北京/电信"
}
```

## HTTP Headers

```python
headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'accept-language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'application/x-www-form-urlencoded',
    'origin': 'https://www.itdog.cn',
    'referer': 'https://www.itdog.cn/batch_ping/',  # 或 /http/
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ...',
    'sec-ch-ua': '"Chromium";v="131"',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
}
```

## 错误处理

HTML 响应包含错误时:
```javascript
err_tip_more("<li>错误信息</li>")
```

提取: `re.search(r'err_tip_more\("<li>(.*)</li>"\)', content)`

## 关键常量

| 常量 | 值 | 用途 |
|------|-----|------|
| TASK_TOKEN_SECRET | token_20230313000136kwyktxb0tgspm00yo5 | task_token 生成 |
| GUARD_XOR_SUFFIX | PTNo2n3Ev5 | guardret XOR 后缀 |

**注意:** 这些常量可能会更新，如遇问题需重新逆向分析。