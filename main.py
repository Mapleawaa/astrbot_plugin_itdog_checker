from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
import asyncio
import json
import time
import argparse
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional
import aiohttp
try:
    from .itdog_nodes import get_all_node_ids, get_node_name, COMPACT_NODES, filter_nodes, DEFAULT_NODES_LIST, get_available_locations
except ImportError:
    from itdog_nodes import get_all_node_ids, get_node_name, COMPACT_NODES, filter_nodes, DEFAULT_NODES_LIST, get_available_locations

# 默认节点组合 (三网北上广深)
DEFAULT_NODES = ",".join(DEFAULT_NODES_LIST)

@register("itdog_checker", "ItdogChecker", "Itdog 网络测速插件", "1.8.0")
class ItdogPlugin(Star):
    def __init__(self, context: Context, config: Optional[Dict] = None):
        super().__init__(context)
        self.config = config or {}
        self.queue: asyncio.Queue = asyncio.Queue()
        self.worker_task: Optional[asyncio.Task] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.service_url = (self.config.get("service_url") or os.environ.get("ITDOG_SERVICE_URL") or "http://127.0.0.1:8765").rstrip("/")
        self.service_token = (self.config.get("service_token") or os.environ.get("ITDOG_SERVICE_TOKEN") or "").strip() or None
        self.edit_message_if_possible = bool(self.config.get("edit_message_if_possible", True))
        self.usage_lock = asyncio.Lock()
        self.usage_state: Dict[str, Dict] = {}
        self.usage_path = os.path.join(os.getcwd(), "data", "itdog_checker_rate_limit.json")
        self._background_tasks: set = set()
        
    async def initialize(self):
        self.session = aiohttp.ClientSession(trust_env=True)
        self.worker_task = asyncio.create_task(self.worker())
        logger.info(f"ItdogPlugin service client started, service_url={self.service_url}")
        await self._load_usage_state()

    async def terminate(self):
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
        for t in list(self._background_tasks):
            t.cancel()
        if self.session:
            await self.session.close()
            self.session = None
    
    async def _load_usage_state(self):
        async with self.usage_lock:
            try:
                os.makedirs(os.path.dirname(self.usage_path), exist_ok=True)
                if os.path.exists(self.usage_path):
                    with open(self.usage_path, "r", encoding="utf-8") as f:
                        self.usage_state = json.load(f) or {}
                else:
                    self.usage_state = {}
            except Exception:
                self.usage_state = {}

    async def _save_usage_state(self):
        async with self.usage_lock:
            try:
                os.makedirs(os.path.dirname(self.usage_path), exist_ok=True)
                tmp = self.usage_path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(self.usage_state, f, ensure_ascii=False)
                os.replace(tmp, self.usage_path)
            except Exception:
                pass

    def _get_timezone(self) -> ZoneInfo:
        tz_name = (self.config.get("timezone") or "Asia/Shanghai").strip() or "Asia/Shanghai"
        try:
            return ZoneInfo(tz_name)
        except Exception:
            return ZoneInfo("Asia/Shanghai")

    def _get_sender_candidates(self, event: AstrMessageEvent) -> List[str]:
        candidates: List[str] = []
        for attr in ("get_sender_id", "get_sender_user_id", "get_user_id"):
            fn = getattr(event, attr, None)
            if callable(fn):
                try:
                    v = fn()
                    if v is not None:
                        candidates.append(str(v))
                except Exception:
                    pass
        for attr in ("sender_id", "user_id", "sender", "user"):
            v = getattr(event, attr, None)
            if v is None:
                continue
            try:
                candidates.append(str(v))
            except Exception:
                pass
        try:
            name = event.get_sender_name()
            if name:
                candidates.append(str(name))
        except Exception:
            pass
        seen = set()
        out = []
        for c in candidates:
            c = c.strip()
            if not c or c in seen:
                continue
            seen.add(c)
            out.append(c)
        return out

    def _parse_admins(self) -> set:
        raw = str(self.config.get("admins") or "").strip()
        if not raw:
            return set()
        parts = []
        for line in raw.replace(",", "\n").replace(";", "\n").splitlines():
            line = line.strip()
            if not line:
                continue
            parts.extend([p.strip() for p in line.split() if p.strip()])
        return set(parts)

    def _is_admin(self, event: AstrMessageEvent) -> bool:
        admins = self._parse_admins()
        if not admins:
            return False
        for c in self._get_sender_candidates(event):
            if c in admins:
                return True
        return False

    def _limits(self) -> Dict[str, int]:
        daily = self.config.get("daily_limit")
        cooldown = self.config.get("cooldown_seconds")
        try:
            daily = int(daily) if daily is not None else 20
        except Exception:
            daily = 20
        try:
            cooldown = int(cooldown) if cooldown is not None else 60
        except Exception:
            cooldown = 60
        if daily < 0:
            daily = 0
        if cooldown < 0:
            cooldown = 0
        return {"daily": daily, "cooldown": cooldown}

    async def _check_and_consume_quota(self, event: AstrMessageEvent) -> Optional[str]:
        if self._is_admin(event):
            return None
        ids = self._get_sender_candidates(event)
        user_key = ids[0] if ids else "unknown"
        tz = self._get_timezone()
        today = datetime.now(tz).date().isoformat()
        now_ts = int(time.time())
        limits = self._limits()
        async with self.usage_lock:
            item = self.usage_state.get(user_key) or {}
            last_ts = int(item.get("last_ts") or 0)
            if limits["cooldown"] > 0 and now_ts - last_ts < limits["cooldown"]:
                remain = limits["cooldown"] - (now_ts - last_ts)
                return f"⏳ 冷却中，请 {remain}s 后再试。"
            date = str(item.get("date") or "")
            count = int(item.get("count") or 0)
            if date != today:
                date = today
                count = 0
            if limits["daily"] > 0 and count >= limits["daily"]:
                return f"🚫 今日已达使用上限 ({limits['daily']} 次)，请明天再试。"
            item["date"] = date
            item["count"] = count + 1
            item["last_ts"] = now_ts
            self.usage_state[user_key] = item
        await self._save_usage_state()
        return None

    def parse_args(self, args: List[str]):
        target = None
        isp = None
        location = None
        
        cities_map = {
            'sh': '上海',
            'bj': '北京',
            'sz': '深圳',
            'nj': '南京',
            'wlmq': '乌鲁木齐',
            'gz': '广州',
            'cd': '成都',
            'wh': '武汉',
            'hz': '杭州',
        }
        
        isp_map = {
            'ct': 'ct',
            'cu': 'cu',
            'cm': 'cm',
            'telecom': 'ct',
            'unicom': 'cu',
            'mobile': 'cm',
            '电信': 'ct',
            '联通': 'cu',
            '移动': 'cm',
        }

        i = 0
        while i < len(args):
            arg = args[i]

            if arg in isp_map and not isp:
                isp = isp_map[arg]
                i += 1
                continue

            if arg.startswith("-"):
                raw_arg = arg.lstrip("-")

                if raw_arg in ("only-ct", "only-cu", "only-cm"):
                    isp = raw_arg.split("-", 1)[1]
                    i += 1
                    continue

                if raw_arg in ("isp", "carrier") and i + 1 < len(args):
                    v = args[i + 1]
                    isp = isp_map.get(v, v)
                    i += 2
                    continue

                if raw_arg in ("loc", "location") and i + 1 < len(args):
                    location = args[i + 1]
                    i += 2
                    continue

                if raw_arg.startswith("isp="):
                    v = raw_arg.split("=", 1)[1]
                    isp = isp_map.get(v, v)
                    i += 1
                    continue

                if raw_arg.startswith("loc=") or raw_arg.startswith("location="):
                    location = raw_arg.split("=", 1)[1]
                    i += 1
                    continue

                if raw_arg in cities_map:
                    location = cities_map[raw_arg]
                    i += 1
                    continue

                location = raw_arg
                i += 1
                continue

            if not target:
                target = arg
                i += 1
                continue

            if not location and arg not in isp_map:
                location = arg

            i += 1

        return target, isp, location

    async def worker(self):
        while True:
            task = await self.queue.get()
            try:
                await self.process_task(task)
            except Exception as e:
                event: AstrMessageEvent = task["event"]
                try:
                    await event.send(event.plain_result(f"❌ 任务执行出错: {str(e)}"))
                except Exception:
                    pass
            finally:
                self.queue.task_done()

    async def _call_service(self, payload: Dict) -> Dict:
        if not self.session:
            raise RuntimeError("service_client_not_ready")
        headers = {}
        if self.service_token:
            headers["X-ITDOG-TOKEN"] = self.service_token
        async with self.session.post(f"{self.service_url}/run", json=payload, headers=headers, timeout=90) as resp:
            data = await resp.json()
        if not data.get("ok"):
            raise RuntimeError(data.get("error") or "service_error")
        return data

    def _get_mention_prefix(self, event: AstrMessageEvent) -> str:
        try:
            name = event.get_sender_name()
            if name:
                return f"@{name} "
        except Exception:
            pass
        return ""

    def _extract_message_id(self, send_ret) -> Optional[str]:
        if send_ret is None:
            return None
        if isinstance(send_ret, str):
            return send_ret
        for attr in ("message_id", "id"):
            v = getattr(send_ret, attr, None)
            if v:
                return str(v)
        return None

    async def _try_edit_message(self, event: AstrMessageEvent, message_id: Optional[str], text: str) -> bool:
        if not self.edit_message_if_possible:
            return False
        if not message_id:
            return False
        for method in ("edit_message", "edit", "update_message"):
            fn = getattr(event, method, None)
            if callable(fn):
                try:
                    ret = fn(message_id, text)
                    if asyncio.iscoroutine(ret):
                        await ret
                    return True
                except Exception:
                    return False
        return False

    def _track_background_task(self, coro):
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(lambda t: self._background_tasks.discard(t))
        return task

    async def _try_recall_message(self, event: AstrMessageEvent, message_id: Optional[str]) -> bool:
        if not message_id:
            return False
        for method in ("recall_message", "delete_message", "remove_message", "delete_msg"):
            fn = getattr(event, method, None)
            if callable(fn):
                try:
                    ret = fn(message_id)
                    if asyncio.iscoroutine(ret):
                        await ret
                    return True
                except Exception:
                    pass
        try:
            platform_name = ""
            get_name = getattr(event, "get_platform_name", None)
            if callable(get_name):
                platform_name = str(get_name() or "")
            if platform_name == "aiocqhttp":
                bot = getattr(event, "bot", None)
                api = getattr(bot, "api", None) if bot else None
                call_action = getattr(api, "call_action", None) if api else None
                if callable(call_action):
                    await call_action("delete_msg", message_id=message_id)
                    return True
        except Exception:
            pass
        return False

    async def _recall_message_later(self, event: AstrMessageEvent, message_id: Optional[str], seconds: int = 60):
        if not message_id:
            return
        try:
            await asyncio.sleep(max(1, int(seconds)))
            await self._try_recall_message(event, message_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    def _carrier_from_node_name(self, node_name: str) -> Optional[str]:
        if "电信" in node_name:
            return "ct"
        if "联通" in node_name:
            return "cu"
        if "移动" in node_name:
            return "cm"
        return None

    def _extract_region(self, location_text: str) -> str:
        loc = (location_text or "").strip()
        if not loc:
            return "未知地区"
        provinces = [
            "北京", "天津", "上海", "重庆", "河北", "山西", "辽宁", "吉林", "黑龙江", "江苏", "浙江",
            "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南", "广东", "海南", "四川", "贵州",
            "云南", "陕西", "甘肃", "青海", "内蒙古", "广西", "西藏", "宁夏", "新疆", "香港", "澳门", "台湾"
        ]
        for p in provinces:
            if loc.startswith(p) or p in loc:
                return p
        return loc

    def _build_region_latency_summary(self, results: List[Dict]) -> str:
        order = []
        stats: Dict[str, Dict[str, Dict[str, object]]] = {}

        for r in results:
            node_id = str(r.get("node_id", ""))
            node_name = get_node_name(node_id)
            if "节点" in node_name:
                continue
            carrier = self._carrier_from_node_name(node_name)
            if not carrier:
                continue
            location = node_name.split(" - ", 1)[0].strip() if " - " in node_name else node_name.strip()
            region = self._extract_region(location)
            if region not in stats:
                stats[region] = {
                    "cm": {"seen": 0, "vals": []},
                    "cu": {"seen": 0, "vals": []},
                    "ct": {"seen": 0, "vals": []},
                }
                order.append(region)
            stats[region][carrier]["seen"] += 1
            val = r.get("result", "")
            try:
                f = float(val)
                if f >= 0:
                    stats[region][carrier]["vals"].append(f)
            except Exception:
                pass

        if not order:
            return ""

        lines = ["地区 | 移动 | 联通 | 电信"]
        for region in order:
            row = []
            for carrier in ("cm", "cu", "ct"):
                info = stats[region][carrier]
                seen = int(info["seen"])
                vals = info["vals"]
                if seen == 0:
                    row.append("无检测点")
                elif not vals:
                    row.append("失败")
                else:
                    avg = sum(vals) / len(vals)
                    row.append(f"{avg:.0f}ms")
            lines.append(f"{region}：{row[0]} | {row[1]} | {row[2]}")
        return "\n".join(lines)

    def _normalize_latency(self, raw) -> Optional[float]:
        try:
            v = float(raw)
        except Exception:
            return None
        if v < 0:
            return None
        return v

    def _format_latency_display(self, raw) -> str:
        v = self._normalize_latency(raw)
        if v is None:
            return "不可达"
        if abs(v - int(v)) < 1e-9:
            return f"{int(v)}ms"
        return f"{v:.2f}ms"

    def _build_report(self, cmd_type: str, target: str, extra: Dict, results: List[Dict], errors: List[str], traceroute_logs: List[str]) -> str:
        uniq_errors = []
        if errors:
            seen = set()
            for e in errors:
                if not e:
                    continue
                s = str(e)
                if s in seen:
                    continue
                seen.add(s)
                uniq_errors.append(s)

        if cmd_type != "traceroute" and not results:
            if uniq_errors:
                return "❌ 测试失败，未收到任何结果。\n" + "\n".join([f"- {e}" for e in uniq_errors[:5]])
            return "❌ 测试失败，未收到任何结果。"

        report = f"📊 **{cmd_type.upper()} 测试报告**\n目标: {target}\n"
        if cmd_type == "tcping":
            report += f"端口: {extra.get('port')}\n"

        if cmd_type == "traceroute":
            report += f"节点: {get_node_name(extra.get('node', '1227'))}\n\n"
            report += "```\n"
            report += "\n".join(traceroute_logs)
            report += "\n```"
            return report

        report += f"共收到 {len(results)} 个结果。\n\n"

        if cmd_type in ["ping", "tcping"]:
            success_count = 0
            total_time = 0
            min_time = float("inf")
            max_time = 0

            for r in results:
                val = self._normalize_latency(r.get("result", ""))
                if val is not None:
                    success_count += 1
                    total_time += val
                    min_time = min(min_time, val)
                    max_time = max(max_time, val)

            avg = f"{total_time / success_count:.2f}" if success_count else "N/A"
            min_s = f"{min_time:.2f}" if min_time != float("inf") else "N/A"
            max_s = f"{max_time:.2f}" if max_time != 0 else "N/A"

            report += f"📈 统计: 成功 {success_count}/{len(results)} | 平均 {avg}ms | 最小 {min_s}ms | 最大 {max_s}ms\n"
            report += "-" * 20 + "\n"
            if bool(extra.get("group_by_region", False)):
                region_summary = self._build_region_latency_summary(results)
                if region_summary:
                    report += region_summary + "\n"
                    report += "-" * 20 + "\n"

            def sort_key(x):
                v = self._normalize_latency(x.get("result", ""))
                if v is not None:
                    return v
                return 99999

            results_sorted = list(results)
            results_sorted.sort(key=sort_key)
            limit = 20
            if len(results_sorted) > 50:
                limit = 30
            for r in results_sorted[:limit]:
                res = r.get("result", "")
                node_id = r.get("node_id")
                name_display = get_node_name(node_id)
                if "节点" in name_display and r.get("address"):
                    name_display = r.get("address")
                report += f"{name_display}: {self._format_latency_display(res)}\n"
            if len(results_sorted) > limit:
                report += f"... 以及其他 {len(results_sorted) - limit} 个节点\n"

        elif cmd_type == "http":
            success_count = 0
            total_time = 0
            for r in results:
                if str(r.get("http_code")).startswith("2") or str(r.get("http_code")).startswith("3"):
                    success_count += 1
                try:
                    total_time += float(r.get("all_time", 0))
                except Exception:
                    pass
            avg = f"{total_time / len(results):.3f}" if results else "N/A"
            report += f"📈 统计: 正常响应 {success_count}/{len(results)} | 平均耗时 {avg}s\n"
            report += "-" * 20 + "\n"
            for r in results[:20]:
                name = r.get("name", r.get("node_id"))
                code = r.get("http_code")
                time_cost = r.get("all_time")
                ip = r.get("ip")
                report += f"{name}: {code} | {time_cost}s | {ip}\n"

        if uniq_errors:
            report += "\n" + "⚠️ 错误信息:\n" + "\n".join([f"- {e}" for e in uniq_errors[:5]])

        return report

    async def process_task(self, task: Dict):
        event: AstrMessageEvent = task["event"]
        cmd_type = task["type"]
        target = task["target"]
        extra = task.get("extra", {})
        ack_message_id = task.get("ack_message_id")
        prefix = self._get_mention_prefix(event)

        try:
            resp = await self._call_service({"type": cmd_type, "target": target, "extra": extra})
        except Exception as e:
            report = prefix + self._build_report(cmd_type, target, extra, [], [str(e)], [])
            edited = await self._try_edit_message(event, ack_message_id, report)
            if not edited:
                await event.send(event.plain_result(report))
            return

        results = resp.get("results") or []
        errors = resp.get("errors") or []
        traceroute_logs = resp.get("traceroute_logs") or []
        report = prefix + self._build_report(cmd_type, target, extra, results, errors, traceroute_logs)
        edited = await self._try_edit_message(event, ack_message_id, report)
        if not edited:
            await event.send(event.plain_result(report))

    async def dispatch_command(self, event: AstrMessageEvent, subcmd: str, args: List[str]):
        subcmd = (subcmd or "").lower()

        if subcmd in ("help", "h", "?"):
            help_text = """
📡 **Itdog 网络测速插件使用帮助**

**基本命令:**
- `/itdog ping <目标>`: IPv4/IPv6 Ping 测试
- `/itdog tcping <目标>`: TCPing 测试 (默认端口 80，可指定 ip:port)
- `/itdog http <目标>`: HTTP 网站测速
- `/itdog traceroute <目标>`: 路由追踪
- `/itdog list`: 查看支持的所有地区

**参数选项 (Ping/TCPing/Traceroute):**
**1. 筛选运营商:**
- `ct/cu/cm`: 作为独立参数 (如 `ping ct 1.1.1.1`)
- `--isp ct|cu|cm`: 显式指定运营商

**2. 筛选地区 (支持中文):**
- 简写: `--sh`, `--bj`, `--sz`, `--nj`, `--gz`, `--cd`, `--wh`, `--hz`
- `--loc <地区>`: 如 `--loc 广东`
- 兼容旧写法: `--上海`, `--广东`, `--江苏`, `--四川` 等 (支持模糊匹配)

**示例:**
`/itdog ping ct 1.1.1.1 --loc 广东` (测试广东电信所有节点)
`/itdog ping 1.1.1.1 --isp ct --loc 广东` (同上，显式写法)
`/itdog ping 1.1.1.1 --上海` (测试上海所有运营商节点)
`/itdog tcping www.baidu.com:443 --loc 江苏` (测试江苏所有节点到百度443端口)
            """
            help_text += "\n\n📝 说明：`isp` 和 `--loc` 都是可选参数；不指定 `isp` 默认三网全测，不指定 `--loc` 默认全地区并按地区汇总展示。"
            send_ret = await event.send(event.plain_result(help_text))
            help_message_id = self._extract_message_id(send_ret)
            self._track_background_task(self._recall_message_later(event, help_message_id, 60))
            return None

        if subcmd in ("list", "ls"):
            locs = get_available_locations()
            formatted = []
            chunk_size = 5
            for i in range(0, len(locs), chunk_size):
                formatted.append(" ".join(locs[i:i+chunk_size]))
            msg = "🌍 **可用地区列表** (支持模糊搜索，如 --广东)\n\n" + "\n".join(formatted)
            return event.plain_result(msg)

        if subcmd == "ping":
            target, isp, location = self.parse_args(args)
            if not target:
                return event.plain_result("请输入要测试的主机或IP")

            if isp or location:
                node_ids = filter_nodes(isp, location)
                if not node_ids:
                    return event.plain_result(f"未找到符合条件的节点 (ISP: {isp}, Loc: {location})")
            else:
                node_ids = filter_nodes(None, None)

            q_size = self.queue.qsize()
            node_desc = "全网节点(三网)" if not (isp or location) else "+".join([x for x in [
                {"ct": "电信", "cu": "联通", "cm": "移动"}.get(isp) if isp else None,
                location if location else None
            ] if x])
            blocked = await self._check_and_consume_quota(event)
            if blocked:
                return event.plain_result(self._get_mention_prefix(event) + blocked)
            extra_payload = {'isp': isp, 'location': location, 'node_ids': node_ids, 'group_by_region': not bool(location)}
            ack_text = (
                f"{self._get_mention_prefix(event)}🚀 已加入队列 (排队: {q_size})\n目标: {target} (PING)\n节点: {node_desc}"
                if q_size > 0
                else f"{self._get_mention_prefix(event)}🚀 开始测试！\n目标: {target} (PING)\n节点: {node_desc}"
            )
            send_ret = await event.send(event.plain_result(ack_text))
            ack_message_id = self._extract_message_id(send_ret)
            await self.queue.put({'type': 'ping', 'target': target, 'extra': extra_payload, 'event': event, 'ack_message_id': ack_message_id})
            return None

        if subcmd == "tcping":
            target, isp, location = self.parse_args(args)
            if not target:
                return event.plain_result("请输入要测试的主机或IP")

            port = 80
            if ':' in target:
                try:
                    target, p = target.split(':')
                    port = int(p)
                except:
                    pass

            if isp or location:
                node_ids = filter_nodes(isp, location)
                if not node_ids:
                    return event.plain_result(f"未找到符合条件的节点 (ISP: {isp}, Loc: {location})")
            else:
                node_ids = filter_nodes(None, None)

            q_size = self.queue.qsize()
            node_desc = "全网节点(三网)" if not (isp or location) else "+".join([x for x in [
                {"ct": "电信", "cu": "联通", "cm": "移动"}.get(isp) if isp else None,
                location if location else None
            ] if x])
            blocked = await self._check_and_consume_quota(event)
            if blocked:
                return event.plain_result(self._get_mention_prefix(event) + blocked)
            extra_payload = {'isp': isp, 'location': location, 'node_ids': node_ids, 'port': port, 'group_by_region': not bool(location)}
            ack_text = (
                f"{self._get_mention_prefix(event)}🚀 已加入队列 (排队: {q_size})\n目标: {target}:{port} (TCPING)\n节点: {node_desc}"
                if q_size > 0
                else f"{self._get_mention_prefix(event)}🚀 开始测试！\n目标: {target}:{port} (TCPING)\n节点: {node_desc}"
            )
            send_ret = await event.send(event.plain_result(ack_text))
            ack_message_id = self._extract_message_id(send_ret)
            await self.queue.put({'type': 'tcping', 'target': target, 'extra': extra_payload, 'event': event, 'ack_message_id': ack_message_id})
            return None

        if subcmd == "http":
            target, _, _ = self.parse_args(args)
            if not target:
                return event.plain_result("请输入要测试的 URL")

            q_size = self.queue.qsize()
            blocked = await self._check_and_consume_quota(event)
            if blocked:
                return event.plain_result(self._get_mention_prefix(event) + blocked)
            extra_payload = {}
            ack_text = (
                f"{self._get_mention_prefix(event)}🚀 已加入队列 (排队: {q_size})\n目标: {target} (HTTP)"
                if q_size > 0
                else f"{self._get_mention_prefix(event)}🚀 开始测试！\n目标: {target} (HTTP)"
            )
            send_ret = await event.send(event.plain_result(ack_text))
            ack_message_id = self._extract_message_id(send_ret)
            await self.queue.put({'type': 'http', 'target': target, 'extra': extra_payload, 'event': event, 'ack_message_id': ack_message_id})
            return None

        if subcmd in ("traceroute", "trace", "tr"):
            target, isp, location = self.parse_args(args)
            if not target:
                return event.plain_result("请输入目标IP或域名")

            node_id = "1227"
            if isp or location:
                nodes_str = filter_nodes(isp, location)
                if nodes_str:
                    node_id = nodes_str.split(',')[0]
                else:
                    return event.plain_result("未找到符合条件的节点，将使用默认节点")

            q_size = self.queue.qsize()
            blocked = await self._check_and_consume_quota(event)
            if blocked:
                return event.plain_result(self._get_mention_prefix(event) + blocked)
            extra_payload = {'node': node_id}
            ack_text = (
                f"{self._get_mention_prefix(event)}🚀 已加入队列 (排队: {q_size})\n目标: {target} (TRACEROUTE)\n节点: {get_node_name(node_id)}"
                if q_size > 0
                else f"{self._get_mention_prefix(event)}🚀 开始测试 \n目标: {target} (TRACEROUTE)\n节点: {get_node_name(node_id)}"
            )
            send_ret = await event.send(event.plain_result(ack_text))
            ack_message_id = self._extract_message_id(send_ret)
            await self.queue.put({'type': 'traceroute', 'target': target, 'extra': extra_payload, 'event': event, 'ack_message_id': ack_message_id})
            return None

        return event.plain_result("子命令无效，用法: /itdog help")


    @filter.command("itdog")
    async def itdog(self, event: AstrMessageEvent):
        message_str = event.message_str
        parts = message_str.split()
        if len(parts) < 2:
            ret = await self.dispatch_command(event, "help", [])
            if ret:
                await event.send(ret)
            return

        subcmd = parts[1]
        args = parts[2:]
        ret = await self.dispatch_command(event, subcmd, args)
        if ret:
            await event.send(ret)
