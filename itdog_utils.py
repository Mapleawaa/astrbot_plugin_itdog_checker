import re
import json
import base64
import hashlib
import asyncio
import os
import ssl
import aiohttp
import websockets
from urllib.parse import urlparse
from typing import Callable, Dict, List, Optional, Union, Any

try:
    from .itdog_nodes import get_all_node_ids
except ImportError:
    from itdog_nodes import get_all_node_ids

# 固定常量
TASK_TOKEN_SECRET = "token_20230313000136kwyktxb0tgspm00yo5"
GUARD_XOR_SUFFIX = "PTNo2n3Ev5"

# 默认 Headers
DEFAULT_HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded',
    'origin': 'https://www.itdog.cn',
    'pragma': 'no-cache',
    'sec-ch-ua': '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
}

def _xor_encrypt(input_str: str, key: str) -> str:
    """XOR 加密函数"""
    output = ""
    key = key + GUARD_XOR_SUFFIX
    for i, char in enumerate(input_str):
        char_code = ord(char) ^ ord(key[i % len(key)])
        output += chr(char_code)
    return output

def _generate_guardret(guard: str) -> str:
    """根据 guard Cookie 生成 guardret Cookie"""
    key = guard[:8]
    num = int(guard[12:]) if len(guard) > 12 else 0
    value = num * 2 + 18 - 2  # num * 2 + 16
    encrypted = _xor_encrypt(str(value), key)
    return base64.b64encode(encrypted.encode()).decode()

def _generate_task_token(task_id: str) -> str:
    """根据 task_id 生成 task_token"""
    full = task_id + TASK_TOKEN_SECRET
    md5_hash = hashlib.md5(full.encode()).hexdigest()
    return md5_hash[8:-8]

def _extract_from_response(content: str, pattern: str) -> Optional[str]:
    """从 HTML 响应中提取数据"""
    match = re.search(pattern, content)
    return match.group(1) if match else None

class ItdogClient:
    def __init__(self):
        ssl_ctx = self._build_ssl_context()
        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
        self.session = aiohttp.ClientSession(headers=DEFAULT_HEADERS, connector=connector, trust_env=True)
        self.cookies = {}

    def _build_ssl_context(self) -> ssl.SSLContext:
        no_verify = os.environ.get("ITDOG_SSL_NO_VERIFY", "").strip().lower() in ("1", "true", "yes", "on")
        if no_verify:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            return ctx

        ca_bundle = (
            os.environ.get("ITDOG_CA_BUNDLE")
            or os.environ.get("SSL_CERT_FILE")
            or os.environ.get("REQUESTS_CA_BUNDLE")
        )
        if ca_bundle:
            try:
                return ssl.create_default_context(cafile=ca_bundle)
            except Exception:
                pass

        try:
            import certifi

            return ssl.create_default_context(cafile=certifi.where())
        except Exception:
            return ssl.create_default_context()

    async def close(self):
        await self.session.close()

    async def _handle_guard_cookie(self, url: str, data: dict):
        """处理反爬虫 Cookie"""
        async with self.session.post(url, data=data) as response:
            await response.text() # 消耗内容
            
            # 检查 cookie 中是否有 guard
            cookies = self.session.cookie_jar.filter_cookies(url)
            if 'guard' in cookies:
                guard_value = cookies['guard'].value
                guardret = _generate_guardret(guard_value)
                self.session.cookie_jar.update_cookies({'guardret': guardret}, response.url)
                return True
        return False

    async def _websocket_receive(
        self,
        wss_url: str,
        task_id: str,
        task_token: str,
        callback: Callable[[Dict], Any],
        timeout: int = 15
    ):
        """WebSocket 接收数据"""
        try:
            async with websockets.connect(wss_url, ssl=self._build_ssl_context()) as ws:
                await ws.send(json.dumps({
                    "task_id": task_id,
                    "task_token": task_token
                }))

                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=timeout)
                        data = json.loads(msg)

                        if data.get('type') == 'finished':
                            break
                        
                        if asyncio.iscoroutinefunction(callback):
                            await callback(data)
                        else:
                            callback(data)

                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({
                            "task_id": task_id,
                            "task_token": task_token
                        }))
                    except json.JSONDecodeError:
                        break
                    except websockets.exceptions.ConnectionClosed:
                        break
        except Exception as e:
            if asyncio.iscoroutinefunction(callback):
                await callback({"error": str(e), "type": "error"})
            else:
                callback({"error": str(e), "type": "error"})

    async def batch_ping(
        self,
        host: Union[str, List[str]],
        node_id: str,
        callback: Callable[[Dict], Any],
        cidr_filter: bool = True,
        gateway: str = "last",
        timeout: int = 15
    ):
        if isinstance(host, list):
            host = "\r\n".join(host)

        url = 'https://www.itdog.cn/batch_ping/'
        data = {
            'host': host,
            'node_id': node_id,
            'cidr_filter': 'true' if cidr_filter else 'false',
            'gateway': gateway
        }

        await self._handle_guard_cookie(url, data)
        
        async with self.session.post(url, data=data) as response:
            content = await response.text()

        err_match = re.search(r'err_tip_more\("<li>(.*)</li>"\)', content)
        if err_match:
            raise ValueError(err_match.group(1))

        wss_url = _extract_from_response(content, r"var wss_url='(.*)';")
        task_id = _extract_from_response(content, r"var task_id='(.*)';")

        if not wss_url or not task_id:
            if "请输入正确的IP" in content:
                raise ValueError("请输入正确的 IP 或域名")
            raise ValueError("无法从响应中提取 WebSocket 参数")

        task_token = _generate_task_token(task_id)
        await self._websocket_receive(wss_url, task_id, task_token, callback, timeout)

    async def tcping(
        self,
        host: str,
        port: int,
        node_id: str,
        callback: Callable[[Dict], Any],
        timeout: int = 15
    ):
        # 修正后的 TCPing 实现，参考 itdog-web-api
        url = 'https://www.itdog.cn/batch_tcping/'
        
        # 构造 host 参数：ip:port
        # 如果 host 已经是 ip:port 格式，则不重复添加
        if ':' not in host:
            host_with_port = f"{host}:{port}"
        else:
            host_with_port = host
            
        data = {
            'host': host_with_port, # 注意：batch_tcping 要求 host 包含端口
            'port': str(port),
            'node_id': node_id,
            'cidr_filter': 'true',
            'gateway': 'first'
        }

        await self._handle_guard_cookie(url, data)
        
        async with self.session.post(url, data=data) as response:
            content = await response.text()
            
        err_match = re.search(r'err_tip_more\("<li>(.*)</li>"\)', content)
        if err_match:
            raise ValueError(err_match.group(1))

        wss_url = _extract_from_response(content, r"var wss_url='(.*)';")
        task_id = _extract_from_response(content, r"var task_id='(.*)';")

        if not wss_url or not task_id:
             raise ValueError("无法启动 TCPing 任务")

        task_token = _generate_task_token(task_id)
        await self._websocket_receive(wss_url, task_id, task_token, callback, timeout)

    async def http_test(
        self,
        url: str,
        callback: Callable[[Dict], Any],
        check_mode: str = "fast",
        method: str = "get",
        redirect_num: int = 5,
        dns_server_type: str = "isp",
        timeout: int = 15
    ):
        parsed = urlparse(url)
        if not parsed.scheme:
            url = "http://" + url
            parsed = urlparse(url)
            
        endpoint = 'https://www.itdog.cn/http/'
        
        data = {
            'line': '',
            'host': url,
            'host_s': parsed.hostname,
            'check_mode': check_mode,
            'ipv4': '',
            'method': method,
            'referer': '',
            'ua': '',
            'cookies': '',
            'redirect_num': str(redirect_num),
            'dns_server_type': dns_server_type,
            'dns_server': '',
        }

        await self._handle_guard_cookie(endpoint, data)

        async with self.session.post(endpoint, data=data) as response:
            content = await response.text()

        err_match = re.search(r'err_tip_more\("<li>(.*)</li>"\)', content)
        if err_match:
            raise ValueError(err_match.group(1))

        wss_url = _extract_from_response(content, r"var wss_url='(.*)';")
        task_id = _extract_from_response(content, r"var task_id='(.*)';")

        if not wss_url or not task_id:
            raise ValueError("无法启动 HTTP 测试")

        task_token = _generate_task_token(task_id)
        await self._websocket_receive(wss_url, task_id, task_token, callback, timeout)

    async def traceroute(
        self,
        target: str,
        node_id: str,
        callback: Callable[[Dict], Any],
        timeout: int = 30 # Traceroute 通常比较慢
    ):
        # Traceroute 实现，参考 itdog-web-api
        # URL: https://www.itdog.cn/traceroute/baidu.com
        
        # 处理 node_id: Traceroute 通常只支持单点，如果传入多个，取第一个
        if ',' in node_id:
            node_id = node_id.split(',')[0]
            
        url = f'https://www.itdog.cn/traceroute/{target}'
        
        data = {
            'node': node_id,
            'dns_server_type': 'isp',
            'dns_server': ''
        }

        await self._handle_guard_cookie(url, data)
        
        async with self.session.post(url, data=data) as response:
            content = await response.text()
            
        err_match = re.search(r'err_tip_more\("<li>(.*)</li>"\)', content)
        if err_match:
            raise ValueError(err_match.group(1))

        wss_url = _extract_from_response(content, r"var wss_url='(.*)';")
        task_id = _extract_from_response(content, r"var task_id='(.*)';")

        if not wss_url or not task_id:
             raise ValueError("无法启动 Traceroute 任务")

        task_token = _generate_task_token(task_id)
        await self._websocket_receive(wss_url, task_id, task_token, callback, timeout)
