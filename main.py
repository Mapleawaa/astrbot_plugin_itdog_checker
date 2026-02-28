from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
import asyncio
import time
import argparse
from typing import Dict, List, Optional
try:
    from .itdog_utils import ItdogClient
    from .itdog_nodes import get_all_node_ids, get_node_name, COMPACT_NODES, filter_nodes, DEFAULT_NODES_LIST, get_available_locations
except ImportError:
    from itdog_utils import ItdogClient
    from itdog_nodes import get_all_node_ids, get_node_name, COMPACT_NODES, filter_nodes, DEFAULT_NODES_LIST, get_available_locations

# 默认节点组合 (三网北上广深)
DEFAULT_NODES = ",".join(DEFAULT_NODES_LIST)

@register("itdog_checker", "ItdogChecker", "Itdog 网络测速插件", "1.3.0")
class ItdogPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self.queue = asyncio.Queue()
        self.worker_task = None
        self.current_task_info = None # 用于查询排队状态
        
    async def initialize(self):
        self.worker_task = asyncio.create_task(self.worker())
        logger.info("ItdogPlugin worker started.")

    async def terminate(self):
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass

    async def worker(self):
        client = ItdogClient()
        try:
            while True:
                task = await self.queue.get()
                try:
                    await self.process_task(client, task)
                except Exception as e:
                    logger.error(f"Task failed: {e}")
                    try:
                        await task['event'].send(f"❌ 任务执行出错: {str(e)}")
                    except:
                        pass
                finally:
                    self.queue.task_done()
                    # 任务间隔，防止被拉黑
                    await asyncio.sleep(3) 
        except asyncio.CancelledError:
            pass
        finally:
            await client.close()
    
    def parse_args(self, args: List[str]):
        """解析命令行参数"""
        target = None
        isp = None
        location = None
        
        # 预定义城市简称映射
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
        
        for arg in args:
            if arg.startswith("-"):
                # 处理选项
                raw_arg = arg.lstrip("-")
                
                if raw_arg == "only-ct":
                    isp = "ct"
                elif raw_arg == "only-cu":
                    isp = "cu"
                elif raw_arg == "only-cm":
                    isp = "cm"
                elif raw_arg in cities_map:
                    # 匹配简写
                    location = cities_map[raw_arg]
                else:
                    # 匹配完整中文 (例如 --广东, --上海)
                    # 只要它不是 only-xx，就假设它是地点
                    # 但为了防止误判，可以做个简单的检查，或者直接赋值
                    # 如果用户输入 --xxx，且 xxx 不在 map 中，我们就当它是中文地点名
                    location = raw_arg
            else:
                if not target:
                    target = arg
                    
        return target, isp, location

    async def process_task(self, client: ItdogClient, task: Dict):
        event: AstrMessageEvent = task['event']
        cmd_type = task['type']
        target = task['target']
        extra = task.get('extra', {})
        node_ids = extra.get('node_ids', DEFAULT_NODES)
        
        # 初始消息
        node_desc = "默认节点"
        if extra.get('isp') or extra.get('location'):
             desc_parts = []
             if extra.get('isp'):
                 desc_parts.append({"ct": "电信", "cu": "联通", "cm": "移动"}.get(extra['isp']))
             if extra.get('location'):
                 desc_parts.append(extra['location'])
             node_desc = "+".join(desc_parts)
             
        yield_msg = event.plain_result(f"🚀 开始测试 {target} ({cmd_type.upper()})\n节点: {node_desc}\n请稍候，结果将实时更新。")
        await event.send(yield_msg)
        
        results = []
        errors = []
        traceroute_logs = []
        
        # 回调函数
        async def callback(data):
            if data.get('type') == 'finished':
                return
            if data.get('type') == 'error':
                errors.append(data.get('error'))
                return
                
            # 处理数据
            if cmd_type in ['ping', 'tcping']:
                results.append(data)
            elif cmd_type == 'http':
                results.append(data)
            elif cmd_type == 'traceroute':
                # Traceroute 返回结构: {'log': '...'}
                if 'log' in data:
                    traceroute_logs.append(data['log'])

        try:
            if cmd_type == 'ping':
                await client.batch_ping(target, node_ids, callback)
            elif cmd_type == 'tcping':
                port = extra.get('port', 80)
                await client.tcping(target, port, node_ids, callback)
            elif cmd_type == 'http':
                await client.http_test(target, callback)
            elif cmd_type == 'traceroute':
                # Traceroute 只需要一个节点
                node = node_ids.split(',')[0] if ',' in node_ids else node_ids
                if not node: node = '1227'
                await client.traceroute(target, node, callback)
        except Exception as e:
            await event.send(f"⚠️ 测试过程中发生错误: {str(e)}")
            return

        # 生成报告
        if not results and not errors and not traceroute_logs:
            await event.send(f"❌ 测试完成，但未收到任何结果。")
            return

        report = f"📊 **{cmd_type.upper()} 测试报告**\n目标: {target}\n"
        if cmd_type == 'tcping':
             report += f"端口: {extra.get('port')}\n"
             
        if cmd_type == 'traceroute':
            report += f"节点: {get_node_name(extra.get('node', '1227'))}\n\n"
            report += "```\n"
            report += "\n".join(traceroute_logs)
            report += "\n```"
            await event.send(report)
            return

        report += f"共收到 {len(results)} 个结果。\n\n"
        
        # 简单统计
        if cmd_type in ['ping', 'tcping']:
            success_count = 0
            total_time = 0
            min_time = float('inf')
            max_time = 0
            
            detail_lines = []
            
            for r in results:
                res = r.get('result', '')
                addr = r.get('address', '未知')
                node_id = r.get('node_id')
                node_name = get_node_name(node_id) if node_id else addr
                
                # 尝试解析延迟
                if res.isdigit() or (res.replace('.', '', 1).isdigit()):
                    val = float(res)
                    success_count += 1
                    total_time += val
                    min_time = min(min_time, val)
                    max_time = max(max_time, val)
                    detail_lines.append(f"✅ {node_name}: {res}ms")
                else:
                    detail_lines.append(f"❌ {node_name}: {res}")
            
            avg = f"{total_time / success_count:.2f}" if success_count else "N/A"
            min_s = f"{min_time:.2f}" if min_time != float('inf') else "N/A"
            max_s = f"{max_time:.2f}" if max_time != 0 else "N/A"
            
            report += f"📈 统计: 成功 {success_count}/{len(results)} | 平均 {avg}ms | 最小 {min_s}ms | 最大 {max_s}ms\n"
            report += "-" * 20 + "\n"
            
            # 排序
            def sort_key(x):
                r = x.get('result', '9999')
                if r.isdigit() or r.replace('.', '', 1).isdigit():
                    return float(r)
                return 99999
            
            results.sort(key=sort_key)
            
            # 根据数量决定显示多少条
            limit = 20
            if len(results) > 50: limit = 30 # 如果结果很多，稍微多显示一点
            
            for r in results[:limit]: 
                res = r.get('result', '')
                node_id = r.get('node_id')
                # 优先显示节点名称，没有则显示IP归属地
                name_display = get_node_name(node_id)
                if "节点" in name_display and r.get('address'):
                    name_display = r.get('address')
                    
                report += f"{name_display}: {res}ms\n"
            
            if len(results) > limit:
                report += f"... 以及其他 {len(results) - limit} 个节点\n"

        elif cmd_type == 'http':
            success_count = 0
            total_time = 0
            
            for r in results:
                if str(r.get('http_code')).startswith('2') or str(r.get('http_code')).startswith('3'):
                    success_count += 1
                try:
                    total_time += float(r.get('all_time', 0))
                except:
                    pass
            
            avg = f"{total_time / len(results):.3f}" if results else "N/A"
            report += f"📈 统计: 正常响应 {success_count}/{len(results)} | 平均耗时 {avg}s\n"
            report += "-" * 20 + "\n"
            
            for r in results[:20]:
                name = r.get('name', r.get('node_id'))
                code = r.get('http_code')
                time_cost = r.get('all_time')
                ip = r.get('ip')
                report += f"{name}: {code} | {time_cost}s | {ip}\n"

        await event.send(report)


    @filter.command("itdog-ping")
    async def itdog_ping(self, event: AstrMessageEvent):
        """IPv4/IPv6 Ping 测试"""
        message_str = event.message_str
        parts = message_str.split()
        if not parts:
            yield event.plain_result("请输入参数")
            return
            
        args = parts[1:]
        target, isp, location = self.parse_args(args)
        
        if not target:
            yield event.plain_result("请输入要测试的主机或IP")
            return
            
        # 根据 isp 和 location 筛选节点
        if isp or location:
            node_ids = filter_nodes(isp, location)
            if not node_ids:
                 yield event.plain_result(f"未找到符合条件的节点 (ISP: {isp}, Loc: {location})")
                 return
        else:
            node_ids = DEFAULT_NODES
        
        q_size = self.queue.qsize()
        await self.queue.put({
            'type': 'ping',
            'target': target,
            'extra': {'isp': isp, 'location': location, 'node_ids': node_ids},
            'event': event
        })
        
        if q_size > 0:
            yield event.plain_result(f"已加入队列，当前排队人数: {q_size}，请稍候...")

    @filter.command("itdog-tcping")
    async def itdog_tcping(self, event: AstrMessageEvent):
        """IPv4/IPv6 TCPing 测试"""
        message_str = event.message_str
        parts = message_str.split()
        args = parts[1:]
        
        target, isp, location = self.parse_args(args)
        
        if not target:
            yield event.plain_result("请输入要测试的主机或IP")
            return
            
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
                 yield event.plain_result(f"未找到符合条件的节点 (ISP: {isp}, Loc: {location})")
                 return
        else:
            node_ids = DEFAULT_NODES

        q_size = self.queue.qsize()
        await self.queue.put({
            'type': 'tcping',
            'target': target,
            'extra': {'isp': isp, 'location': location, 'node_ids': node_ids, 'port': port},
            'event': event
        })
        
        if q_size > 0:
            yield event.plain_result(f"已加入队列，当前排队人数: {q_size}，请稍候...")

    @filter.command("itdog-http")
    async def itdog_http(self, event: AstrMessageEvent):
        """HTTP 测速"""
        message_str = event.message_str
        parts = message_str.split()
        args = parts[1:]
        
        target, _, _ = self.parse_args(args)
        
        if not target:
            yield event.plain_result("请输入要测试的 URL")
            return
            
        q_size = self.queue.qsize()
        await self.queue.put({
            'type': 'http',
            'target': target,
            'event': event
        })
        
        if q_size > 0:
            yield event.plain_result(f"已加入队列，当前排队人数: {q_size}，请稍候...")

    @filter.command("itdog-traceroute")
    async def itdog_traceroute(self, event: AstrMessageEvent):
        """路由追踪测试"""
        message_str = event.message_str
        parts = message_str.split()
        args = parts[1:]
        
        target, isp, location = self.parse_args(args)
        
        if not target:
             yield event.plain_result("请输入目标IP或域名")
             return
             
        node_id = "1227" 
        
        if isp or location:
            nodes_str = filter_nodes(isp, location)
            if nodes_str:
                node_id = nodes_str.split(',')[0]
            else:
                yield event.plain_result(f"未找到符合条件的节点，将使用默认节点")

        q_size = self.queue.qsize()
        await self.queue.put({
            'type': 'traceroute',
            'target': target,
            'extra': {'node': node_id},
            'event': event
        })
        
        if q_size > 0:
            yield event.plain_result(f"已加入队列，当前排队人数: {q_size}，请稍候...")

    @filter.command("itdog-list")
    async def itdog_list(self, event: AstrMessageEvent):
        """列出所有可用地区节点"""
        locs = get_available_locations()
        # 将列表格式化为易读的文本，例如每行显示 5 个
        formatted = []
        chunk_size = 5
        for i in range(0, len(locs), chunk_size):
            formatted.append(" ".join(locs[i:i+chunk_size]))
            
        msg = "🌍 **可用地区列表** (支持模糊搜索，如 --广东)\n\n" + "\n".join(formatted)
        yield event.plain_result(msg)

    @filter.command("itdog-help")
    async def itdog_help(self, event: AstrMessageEvent):
        """显示帮助信息"""
        help_text = """
📡 **Itdog 网络测速插件使用帮助**

**基本命令:**
- `/itdog-ping <目标>`: IPv4/IPv6 Ping 测试
- `/itdog-tcping <目标>`: TCPing 测试 (默认端口 80，可指定 ip:port)
- `/itdog-http <目标>`: HTTP 网站测速
- `/itdog-traceroute <目标>`: 路由追踪
- `/itdog-list`: 查看支持的所有地区

**参数选项 (Ping/TCPing/Traceroute):**
**1. 筛选运营商:**
- `-only-ct`: 只显示电信
- `-only-cu`: 只显示联通
- `-only-cm`: 只显示移动

**2. 筛选地区 (支持中文):**
- 简写: `--sh`, `--bj`, `--sz`, `--nj`, `--gz`, `--cd`, `--wh`, `--hz`
- 中文: `--上海`, `--广东`, `--江苏`, `--四川` 等 (支持模糊匹配)

**示例:**
`/itdog-ping 1.1.1.1 -only-ct --广东` (测试广东电信所有节点)
`/itdog-ping 1.1.1.1 --上海` (测试上海所有运营商节点)
`/itdog-tcping www.baidu.com:443 --江苏` (测试江苏所有节点到百度443端口)
        """
        yield event.plain_result(help_text)
