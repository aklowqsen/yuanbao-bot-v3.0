#!/usr/bin/env python3
"""
元宝 Bot Web 控制台 - 完整功能版 v3.0 (代理双模式 + 性能优化)

=== v3.0 历史变更 ===
1. [Bug] api_update_settings 中 group_reply_text/c2c_reply_text 未同步到 sender
2. [Bug] api_send_reply 在 async 上下文里用 time.sleep 阻塞了事件循环
3. [Bug] _maybe_enqueue_proxy 使用 get_event_loop() 在异步上下文中不安全
4. [Bug] sender.loop 字段赋值后从未使用，导致混淆
5. [优化] 移除未使用的导入 (struct, wraps)
6. [优化] COS 上传增加重试机制
7. [优化] SSE 生成器使用 asyncio 感知的睡眠
8. [优化] 连接断开时自动重连
9. [优化] 统一错误处理装饰器
10. [优化] 添加请求超时配置

=== v3.0 升级 ===
1. [版本] 全部版本号统一升至 v3.0
2. [默认值] 心跳间隔默认 1.0s（原 5.0s）
3. [默认值] 刷屏间隔默认 0.1s（原 1.0s）
4. [性能] 更激进的默认参数适配高速场景

=== v3.1 图片转发修复 ===
1. [BugFix] _forward_image 不再下载+COS上传，改为直接使用原始URL构建TIMImageElem
2. [BugFix] _extract_message_content 保留所有图片URL（多图场景），正确提取宽高
3. [新增] resolve_image_url 将资源保护URL转为COS预签名直链
4. [新增] _image_download_and_send 下载图片并发送到目标群
5. [BugFix] _maybe_handle_yuanbao_reply 增加图片回复检测（元宝返回图片时也能正确转发）
6. [BugFix] _build_image_msg 传递正确宽高信息
"""
import sys
import os
import json
import asyncio
import threading
import time
import hashlib
import hmac
import random
import string
import uuid
import tempfile
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple, Any
from collections import deque
import requests
from flask import Flask, jsonify, request, render_template, Response, stream_with_context
import websockets

# ═══════════════════════════════════════════
#  配置加载
# ═══════════════════════════════════════════
def load_config() -> dict:
    """加载配置文件，自动处理路径"""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

app_config = load_config()
APP_KEY = app_config.get('APP_KEY', '')
APP_SECRET = app_config.get('APP_SECRET', '')
API_DOMAIN = app_config.get('API_DOMAIN', '')
WS_URL = app_config.get('WS_URL', '')

# ═══════════════════════════════════════════
#  协议常量
# ═══════════════════════════════════════════
CMD_TYPE_REQUEST = 0
CMD_TYPE_RESPONSE = 1
CMD_TYPE_PUSH = 2
CMD_AUTH_BIND = "auth-bind"
CMD_PING = "ping"
MODULE_CONN_ACCESS = "conn_access"
BIZ_MODULE = "yuanbao_openclaw_proxy"
BIZ_CMD_SEND_C2C = "send_c2c_message"
BIZ_CMD_SEND_GROUP = "send_group_message"
BIZ_CMD_GET_MEMBERS = "get_group_member_list"

# 默认元宝 ID
DEFAULT_YUANBAO_ID = "szUvRH8s4ekettawNjDREmAG4W7h+Lhb8Sy9tq/otZU="
IMAGE_GROUP_CODE = app_config.get('IMAGE_GROUP_CODE', '')
YUANBAO_BOT_ID = app_config.get('YUANBAO_ID', DEFAULT_YUANBAO_ID)
YUANBAO_NICKNAME = '元宝'

# HTTP 请求超时
HTTP_TIMEOUT = 30

# ═══════════════════════════════════════════
#  简易 Protobuf 编解码器
# ═══════════════════════════════════════════
class SimpleProtobufCodec:
    """最小化 Protobuf 编解码，仅覆盖本服务需要的字段"""

    # ── 编码 ──
    @staticmethod
    def encode_varint(value: int) -> bytes:
        result = []
        while value > 127:
            result.append((value & 0x7F) | 0x80)
            value >>= 7
        result.append(value)
        return bytes(result)

    @staticmethod
    def encode_string(field_num: int, value: str) -> bytes:
        tag = (field_num << 3) | 2
        encoded = value.encode('utf-8')
        return bytes([tag]) + SimpleProtobufCodec.encode_varint(len(encoded)) + encoded

    @staticmethod
    def encode_message_field(field_num: int, encoded_msg: bytes) -> bytes:
        tag = (field_num << 3) | 2
        return bytes([tag]) + SimpleProtobufCodec.encode_varint(len(encoded_msg)) + encoded_msg

    @staticmethod
    def encode_uint32(field_num: int, value: int) -> bytes:
        tag = (field_num << 3) | 0
        return bytes([tag]) + SimpleProtobufCodec.encode_varint(value)

    @staticmethod
    def encode_head(cmd_type: int, cmd: str, seq_no: int, msg_id: str, module: str) -> bytes:
        data = b''
        data += bytes([(1 << 3) | 0]) + SimpleProtobufCodec.encode_varint(cmd_type)
        data += SimpleProtobufCodec.encode_string(2, cmd)
        data += bytes([(3 << 3) | 0]) + SimpleProtobufCodec.encode_varint(seq_no)
        data += SimpleProtobufCodec.encode_string(4, msg_id)
        data += SimpleProtobufCodec.encode_string(5, module)
        return data

    @staticmethod
    def encode_conn_msg(head: bytes, data: bytes = b'') -> bytes:
        result = SimpleProtobufCodec.encode_message_field(1, head)
        if data:
            result += SimpleProtobufCodec.encode_message_field(2, data)
        return result

    @staticmethod
    def encode_send_group_msg_req(msg_id: str, group_code: str, from_account: str,
                                   text: str, ref_msg_id: str = "") -> bytes:
        data = b''
        data += SimpleProtobufCodec.encode_string(1, msg_id)
        data += SimpleProtobufCodec.encode_string(2, group_code)
        data += SimpleProtobufCodec.encode_string(3, from_account)
        data += SimpleProtobufCodec.encode_string(5, str(random.randint(1, 999999999)))
        msg_content = SimpleProtobufCodec.encode_string(1, text)
        msg_body_elem = SimpleProtobufCodec.encode_string(1, "TIMTextElem")
        msg_body_elem += SimpleProtobufCodec.encode_message_field(2, msg_content)
        data += SimpleProtobufCodec.encode_message_field(6, msg_body_elem)
        if ref_msg_id:
            data += SimpleProtobufCodec.encode_string(7, ref_msg_id)
        return data

    @staticmethod
    def encode_send_c2c_msg_req(msg_id: str, to_account: str, from_account: str, text: str) -> bytes:
        data = b''
        data += SimpleProtobufCodec.encode_string(1, msg_id)
        data += SimpleProtobufCodec.encode_string(2, to_account)
        data += SimpleProtobufCodec.encode_string(3, from_account)
        data += bytes([(4 << 3) | 0]) + SimpleProtobufCodec.encode_varint(random.randint(1, 999999999))
        msg_content = SimpleProtobufCodec.encode_string(1, text)
        msg_body_elem = SimpleProtobufCodec.encode_string(1, "TIMTextElem")
        msg_body_elem += SimpleProtobufCodec.encode_message_field(2, msg_content)
        data += SimpleProtobufCodec.encode_message_field(5, msg_body_elem)
        return data

    @staticmethod
    def encode_get_group_member_list_req(group_code: str) -> bytes:
        return SimpleProtobufCodec.encode_string(1, group_code)

    # ── 解码 ──
    @staticmethod
    def decode_varint(data: bytes, pos: int) -> tuple:
        value = 0
        shift = 0
        while pos < len(data):
            byte = data[pos]
            pos += 1
            value |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                break
            shift += 7
        return value, pos

    @staticmethod
    def decode_conn_msg(data: bytes) -> Optional[dict]:
        result = {"head": {}, "data": b""}
        i = 0
        while i < len(data):
            if i >= len(data):
                break
            tag = data[i]
            i += 1
            field_num = tag >> 3
            wire_type = tag & 7
            if wire_type != 2:
                break
            length, i = SimpleProtobufCodec.decode_varint(data, i)
            if i + length > len(data):
                break
            field_data = data[i:i + length]
            i += length
            if field_num == 1:
                result["head"] = SimpleProtobufCodec.decode_head(field_data)
            elif field_num == 2:
                result["data"] = field_data
        return result

    @staticmethod
    def decode_head(data: bytes) -> dict:
        head = {"cmd_type": 0}
        i = 0
        while i < len(data):
            tag = data[i]
            i += 1
            field_num = tag >> 3
            wire_type = tag & 7
            if wire_type == 0:
                val, i = SimpleProtobufCodec.decode_varint(data, i)
                if field_num == 1:
                    head["cmd_type"] = val
                elif field_num == 3:
                    head["seq_no"] = val
            elif wire_type == 2:
                length, i = SimpleProtobufCodec.decode_varint(data, i)
                if i + length > len(data):
                    break
                field_data = data[i:i + length]
                i += length
                if field_num == 2:
                    head["cmd"] = field_data.decode('utf-8', errors='replace')
                elif field_num == 4:
                    head["msg_id"] = field_data.decode('utf-8', errors='replace')
                elif field_num == 5:
                    head["module"] = field_data.decode('utf-8', errors='replace')
        return head

    @staticmethod
    def decode_get_group_member_list_rsp(data: bytes) -> dict:
        result = {"code": 0, "message": "", "member_list": []}
        i = 0
        while i < len(data):
            if i >= len(data):
                break
            tag = data[i]
            i += 1
            field_num = tag >> 3
            wire_type = tag & 7
            if wire_type == 0:
                val, i = SimpleProtobufCodec.decode_varint(data, i)
                if field_num == 1:
                    result["code"] = val
            elif wire_type == 2:
                length, i = SimpleProtobufCodec.decode_varint(data, i)
                if i + length > len(data):
                    break
                field_data = data[i:i + length]
                i += length
                if field_num == 2:
                    result["message"] = field_data.decode('utf-8', errors='replace')
                elif field_num == 3:
                    member = SimpleProtobufCodec._decode_member(field_data)
                    if member:
                        result["member_list"].append(member)
        return result

    @staticmethod
    def _decode_member(data: bytes) -> Optional[dict]:
        member = {}
        i = 0
        while i < len(data):
            tag = data[i]
            i += 1
            field_num = tag >> 3
            wire_type = tag & 7
            if wire_type == 0:
                val, i = SimpleProtobufCodec.decode_varint(data, i)
                if field_num == 3:
                    member["user_type"] = val
            elif wire_type == 2:
                length, i = SimpleProtobufCodec.decode_varint(data, i)
                if i + length > len(data):
                    break
                field_data = data[i:i + length]
                i += length
                if field_num == 1:
                    member["user_id"] = field_data.decode('utf-8', errors='replace')
                elif field_num == 2:
                    member["nick_name"] = field_data.decode('utf-8', errors='replace')
        return member if member else None

    # ── 高级元素编码 ──
    @staticmethod
    def encode_tim_face_elem(sticker_id: str, package_id: str, name: str,
                              width: int = 128, height: int = 128, formats: str = "png") -> bytes:
        data_json = json.dumps({
            "sticker_id": sticker_id, "package_id": package_id,
            "width": width, "height": height, "formats": formats, "name": name,
        }, ensure_ascii=False)
        msg_content = b''
        msg_content += bytes([(9 << 3) | 0]) + SimpleProtobufCodec.encode_varint(0)
        msg_content += SimpleProtobufCodec.encode_string(4, data_json)
        elem = b''
        elem += SimpleProtobufCodec.encode_string(1, "TIMFaceElem")
        elem += SimpleProtobufCodec.encode_message_field(2, msg_content)
        return elem

    @staticmethod
    def encode_tim_image_elem(url: str, uuid: str = "", size: int = 0,
                               width: int = 0, height: int = 0, image_format: int = 255) -> bytes:
        """编码 TIMImageElem 图片消息元素

        v3.1: 与 sender.py 的 encode_tim_image_elem 保持一致的 proto 结构:
          image_info_array (field 8) 内含:
            type (field 1) = 1 (原始图片)
            size (field 2)
            width (field 3)
            height (field 4)
            url (field 5)
          msg_content:
            uuid (field 2)
            image_format (field 3) = 255
            image_info_array (field 8)
        """
        img_info = b''
        img_info += bytes([(1 << 3) | 0]) + SimpleProtobufCodec.encode_varint(1)  # type=1
        img_info += bytes([(2 << 3) | 0]) + SimpleProtobufCodec.encode_varint(size)
        img_info += bytes([(3 << 3) | 0]) + SimpleProtobufCodec.encode_varint(width)
        img_info += bytes([(4 << 3) | 0]) + SimpleProtobufCodec.encode_varint(height)
        img_info += SimpleProtobufCodec.encode_string(5, url)
        mc = b''
        if uuid:
            mc += SimpleProtobufCodec.encode_string(2, uuid)
        mc += bytes([(3 << 3) | 0]) + SimpleProtobufCodec.encode_varint(image_format)
        mc += SimpleProtobufCodec.encode_message_field(8, img_info)
        elem = b''
        elem += SimpleProtobufCodec.encode_string(1, "TIMImageElem")
        elem += SimpleProtobufCodec.encode_message_field(2, mc)
        return elem

    @staticmethod
    def encode_tim_file_elem(url: str, uuid: str = "", file_size: int = 0, file_name: str = "") -> bytes:
        mc = b''
        if uuid:
            mc += SimpleProtobufCodec.encode_string(2, uuid)
        mc += SimpleProtobufCodec.encode_string(10, url)
        if file_size:
            mc += bytes([(11 << 3) | 0]) + SimpleProtobufCodec.encode_varint(file_size)
        if file_name:
            mc += SimpleProtobufCodec.encode_string(12, file_name)
        elem = b''
        elem += SimpleProtobufCodec.encode_string(1, "TIMFileElem")
        elem += SimpleProtobufCodec.encode_message_field(2, mc)
        return elem


# ═══════════════════════════════════════════
#  增强发送器
# ═══════════════════════════════════════════
class EnhancedSpamSender:
    """核心 Bot 客户端：鉴权、心跳、收发消息、代理转发"""

    def __init__(self):
        self.token: Optional[str] = None
        self.bot_id: Optional[str] = None
        self.instance_id: Optional[str] = None
        self.ws = None
        self.connected = False
        self.seq_no = 0
        self.group_code: Optional[str] = None
        self.codec = SimpleProtobufCodec()
        self.user_db: Dict[str, str] = {}
        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.msg_cache: List[dict] = []
        self._seen_proxy_msg_ids: set = set()
        self.heartbeat_task = None
        self.receive_task = None
        self.auto_reply_enabled = False
        self.heartbeat_interval = app_config.get('HEARTBEAT_INTERVAL', 1.0)
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5

        # 自动回复配置
        self.AUTO_REPLY_RULES = app_config.get('AUTO_REPLY_RULES', [])
        self.DEFAULT_REPLY = app_config.get('DEFAULT_REPLY', '')
        self.AUTO_REPLY_GROUP_TEXT = app_config.get('AUTO_REPLY_GROUP_TEXT', '@我干啥')
        self.AUTO_REPLY_C2C_TEXT = app_config.get('AUTO_REPLY_C2C_TEXT', '我是Bot')

        # 代理模式配置
        self.forward_at_yuanbao = app_config.get('FORWARD_AT_YUANBAO', True)

        # 群聊管理
        self.groups: Dict[str, Dict] = {}
        self.current_group: Optional[str] = None

        # 代理模式状态
        self.auto_reply_text: Optional[str] = None
        self.auto_reply_at_only: bool = False
        self._proxy_queue: deque = deque()
        self._proxy_worker_task: Optional[asyncio.Task] = None
        self._proxy_worker_running: bool = False

        # 元宝图片回复等待（对应 sender.py 的 _pending_image_future）
        self._pending_image_future: Optional[asyncio.Future] = None

        # 贴纸库
        self.STICKERS = self._init_stickers()

    @staticmethod
    def _init_stickers() -> dict:
        """集中管理贴纸数据"""
        stickers = {}
        sticker_data = [
            ("六六六", "278"), ("我想开了", "262"), ("害羞", "130"), ("比心", "252"),
            ("委屈", "125"), ("亲亲", "146"), ("酷", "131"), ("睡", "145"),
            ("发呆", "152"), ("可怜", "157"), ("摊手", "200"), ("头大", "213"),
            ("吓", "256"), ("吐血", "203"), ("哼", "185"), ("嘿嘿", "220"),
            ("头秃", "218"), ("暗中观察", "221"), ("我酸了", "224"), ("打call", "246"),
            ("庆祝", "251"), ("奋斗", "151"), ("惊讶", "143"), ("疑问", "144"),
            ("仔细分析", "248"), ("撅嘴", "184"), ("泪奔", "199"), ("尊嘟假嘟", "276"),
            ("略略略", "113"), ("困", "180"), ("折磨", "181"), ("抠鼻", "182"),
            ("鼓掌", "183"), ("斜眼笑", "204"), ("辣眼睛", "216"), ("哦哟", "217"),
            ("吃瓜", "222"), ("狗头", "225"), ("敬礼", "227"), ("哦", "231"),
            ("拿到红包", "236"), ("牛吖", "239"), ("贴贴", "272"), ("爱心", "138"),
            ("晚安", "170"), ("太阳", "176"), ("柠檬", "266"), ("大冤种", "267"),
            ("吐了", "132"), ("怒", "134"), ("玫瑰", "165"), ("凋谢", "119"),
            ("点赞", "159"), ("握手", "164"), ("抱拳", "163"), ("ok", "169"),
            ("拳头", "174"), ("鞭炮", "191"), ("烟花", "258"),
        ]
        for name, sid in sticker_data:
            stickers[name] = {
                "sticker_id": sid, "package_id": "1003", "name": name,
                "width": 128, "height": 128, "formats": "png"
            }
        return stickers

    # ── 工具方法 ──
    def _generate_msg_id(self) -> str:
        return uuid.uuid4().hex

    def _get_beijing_time(self) -> str:
        return (datetime.now(timezone.utc) + timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S+08:00")

    # ── 鉴权 ──
    def sign_token(self) -> bool:
        """向服务端换取 token"""
        url = f"https://{API_DOMAIN}/api/v5/robotLogic/sign-token"
        nonce = ''.join(random.choices(string.hexdigits.lower(), k=32))
        timestamp = self._get_beijing_time()
        plain = f"{nonce}{timestamp}{APP_KEY}{APP_SECRET}"
        signature = hmac.new(APP_SECRET.encode(), plain.encode(), hashlib.sha256).hexdigest()
        self.instance_id = str(random.randint(1, 1000))
        headers = {
            "Content-Type": "application/json",
            "X-AppVersion": "1.0.11",
            "X-OperationSystem": "linux",
            "X-Instance-Id": self.instance_id,
            "X-Bot-Version": "2026.3.22"
        }
        body = {"app_key": APP_KEY, "nonce": nonce, "signature": signature, "timestamp": timestamp}
        try:
            response = requests.post(url, headers=headers, json=body, timeout=HTTP_TIMEOUT)
            result = response.json()
            if result.get("code") == 0:
                self.token = result["data"]["token"]
                self.bot_id = result["data"]["bot_id"]
                return True
            print(f"[鉴权] 失败: {result.get('message', '未知错误')}")
            return False
        except Exception as e:
            print(f"[鉴权] 异常: {e}")
            return False

    # ── 连接管理 ──
    async def connect(self) -> bool:
        """连接并启动后台任务，支持自动重连"""
        if not self.token and not self.sign_token():
            return False
        try:
            self.ws = await websockets.connect(WS_URL)
            await self.ws.send(self._build_auth_bind_msg())
            await self.ws.recv()
            self.connected = True
            self.reconnect_attempts = 0
            self.heartbeat_task = asyncio.create_task(self._heartbeat())
            self.receive_task = asyncio.create_task(self._receive_loop())
            print(f"[连接] ✅ 已连接 (heartbeat={self.heartbeat_interval}s)")
            return True
        except Exception as e:
            print(f"[连接] 失败: {e}")
            return False

    async def disconnect(self):
        """优雅断开连接"""
        self.connected = False
        for task in (self.heartbeat_task, self.receive_task, self._proxy_worker_task):
            if task and not task.done():
                task.cancel()
        self.heartbeat_task = self.receive_task = self._proxy_worker_task = None
        self._proxy_worker_running = False
        for item in self._proxy_queue:
            if not item["future"].done():
                item["future"].cancel()
        self._proxy_queue.clear()
        self._seen_proxy_msg_ids.clear()
        if self._pending_image_future and not self._pending_image_future.done():
            self._pending_image_future.cancel()
            self._pending_image_future = None
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None

    async def _auto_reconnect(self):
        """自动重连逻辑"""
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            print(f"[重连] 已达最大重试次数 {self.max_reconnect_attempts}，停止重连")
            return
        self.reconnect_attempts += 1
        delay = min(2 ** self.reconnect_attempts, 30)
        print(f"[重连] {self.reconnect_attempts}/{self.max_reconnect_attempts} 等待 {delay}s...")
        await asyncio.sleep(delay)
        if await self.connect():
            print("[重连] ✅ 重连成功")
        else:
            await self._auto_reconnect()

    # ── 鉴权消息构建 ──
    def _build_auth_bind_msg(self) -> bytes:
        auth_info = (self.codec.encode_string(1, self.bot_id) +
                     self.codec.encode_string(2, "web") +
                     self.codec.encode_string(3, self.token))
        device_info = (self.codec.encode_string(1, "2.0.1") +
                       self.codec.encode_string(2, "Linux") +
                       self.codec.encode_string(3, "2026.3.23-2") +
                       self.codec.encode_string(4, "16"))
        auth_data = (self.codec.encode_string(1, "ybBot") +
                     self.codec.encode_message_field(2, auth_info) +
                     self.codec.encode_message_field(3, device_info))
        head = HeadBuilder.request(CMD_AUTH_BIND, self.seq_no, self._generate_msg_id(), MODULE_CONN_ACCESS)
        self.seq_no += 1
        return self.codec.encode_conn_msg(head, auth_data)

    # ── 心跳 ──
    async def _heartbeat(self):
        print(f"[心跳] 启动 (间隔 {self.heartbeat_interval}s)")
        while self.connected:
            await asyncio.sleep(self.heartbeat_interval)
            if not self.connected:
                break
            try:
                head = HeadBuilder.request(CMD_PING, self.seq_no, self._generate_msg_id(), MODULE_CONN_ACCESS)
                self.seq_no += 1
                await self.ws.send(self.codec.encode_conn_msg(head))
            except Exception as e:
                print(f"[心跳] 失败: {e}")
                self.connected = False
                break

    # ── 自动回复 ──
    def get_auto_reply(self, text: str, is_group: bool = False, group_code: str = "") -> str:
        """根据规则匹配自动回复文本"""
        text = text.strip()
        if not text:
            return self.DEFAULT_REPLY or (self.AUTO_REPLY_GROUP_TEXT if is_group else self.AUTO_REPLY_C2C_TEXT)

        for rule in self.AUTO_REPLY_RULES:
            if rule.get('group_only') and not is_group:
                continue
            match_type = rule.get('match_type', '')
            pattern = rule.get('pattern', '')
            reply = rule.get('reply_text', '')

            if match_type == 'exact' and text == pattern:
                return reply
            elif match_type == 'contains' and pattern and pattern in text:
                return reply
            elif match_type == 'contains_any':
                for p in rule.get('patterns', []):
                    if p and p in text:
                        return reply
            elif match_type == 'startswith' and pattern and text.startswith(pattern):
                return reply
            elif match_type == 'endswith' and pattern and text.endswith(pattern):
                return reply
            elif match_type == 'regex' and pattern:
                try:
                    if __import__('re').search(pattern, text):
                        return reply
                except Exception:
                    pass

        return self.DEFAULT_REPLY or (self.AUTO_REPLY_GROUP_TEXT if is_group else self.AUTO_REPLY_C2C_TEXT)

    async def _process_auto_reply(self, push_json: dict):
        """处理收到的消息，判断是否触发自动回复"""
        if not self.auto_reply_enabled or not self.bot_id:
            return
        try:
            from_account = push_json.get("from_account", "")
            if from_account == self.bot_id:
                return

            text_parts = []
            is_at_me = False
            for elem in push_json.get("msg_body", []):
                etype = elem.get("msg_type", "")
                content = elem.get("msg_content", {})
                if etype == "TIMTextElem":
                    t = content.get("text", "")
                    if t:
                        text_parts.append(t)
                    if f"@{self.bot_id}" in t:
                        is_at_me = True
                elif etype == "TIMCustomElem":
                    try:
                        cd = json.loads(content.get("data", "{}"))
                        if cd.get("elem_type") == 1002:
                            t = cd.get("text", "")
                            if t:
                                text_parts.append(t)
                            if cd.get("user_id") == self.bot_id or f"@{self.bot_id}" in t:
                                is_at_me = True
                    except Exception:
                        pass

            text = " ".join(text_parts).strip()
            if is_at_me:
                for prefix in (f"@{self.bot_id}", "@"):
                    if text.startswith(prefix):
                        text = text[len(prefix):].lstrip()
                        break

            group_code = push_json.get("group_code", "")
            callback_cmd = push_json.get("callback_command", "")
            is_group = bool(group_code and "Group" in callback_cmd)

            need_reply = (is_group and is_at_me) or (not is_group and bool(from_account))

            if need_reply:
                reply = self.get_auto_reply(text, is_group, group_code)
                if reply:
                    if is_group:
                        orig = self.group_code
                        self.group_code = group_code
                        try:
                            await self.send_group_message(reply)
                        finally:
                            self.group_code = orig
                    else:
                        await self.send_c2c_message(from_account, reply)
        except Exception as e:
            print(f"[自动回复] 异常: {e}")

    # ── URL 解析 ──
    def resolve_image_url(self, resource_url: str) -> Optional[str]:
        """将元宝图片资源保护 URL 转换为 COS 预签名直链

        从 URL 中提取 resourceId，调用 /api/resource/v1/download 接口，
        获取可直接下载的 COS 预签名 URL。（与 sender.py 一致）
        """
        match = re.search(r'resourceId=([^&]+)', resource_url)
        if not match:
            # 如果已经是可以直接下载的 URL，直接返回
            if resource_url.startswith("http") and "resourceId" not in resource_url:
                return resource_url
            print(f"[图片] 无法从 URL 中提取 resourceId: {resource_url[:80]}")
            return None

        resource_id = match.group(1)
        url = f"https://{API_DOMAIN}/api/resource/v1/download?resourceId={resource_id}"
        headers = {
            "X-ID": self.bot_id or "",
            "X-Token": self.token or "",
            "X-Source": "web",
            "X-AppVersion": "1.0.11",
            "X-OperationSystem": "linux",
            "X-Instance-Id": self.instance_id or "",
            "X-Bot-Version": "2026.3.22",
        }

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            result = resp.json()
            cos_url = result.get("realUrl") or result.get("url") or ""
            if cos_url and cos_url.startswith("http"):
                return cos_url
            if result.get("code", 0) == 0:
                data = result.get("data", {})
                cos_url = data.get("url") or data.get("realUrl") or ""
                if cos_url:
                    return cos_url
            print(f"[图片] 获取图片直链失败: {result}")
            return None
        except Exception as e:
            print(f"[图片] 获取图片直链错误: {e}")
            return None

    # ── 代理模式工作循环 ──
    async def _proxy_worker_loop(self):
        """后台任务：从队列取消息 → 转发到中转群

        v3.0 双模式：
        - forward_at_yuanbao=True  → @元宝模式：转发后等元宝回复，再回原群
        - forward_at_yuanbao=False → 直转模式：转发完立即完成，不等任何人回复
        """
        type_names = {"sticker": "贴纸", "image": "图片", "file": "文件"}
        self._proxy_worker_running = True
        mode = "yuanbao" if self.forward_at_yuanbao else "direct"
        print(f"[Auto-Proxy] 🔄 worker 启动 | 模式={mode}")
        try:
            while self._proxy_queue:
                item = self._proxy_queue[0]
                media = item.get("media_info", {})
                media_type = media.get("type", "")
                sender_name = item.get('ref_sender_name', '未知用户')

                item["forwarded"] = False
                item["reply_received"] = False

                # ── Step 1: 转发到中转群 ──
                ok = await self._forward_single_item(item, media, media_type, sender_name, type_names)

                if ok:
                    item["forwarded"] = True
                    content_preview = (item.get('original_content', '') or '')[:40]
                    print(f"[Auto-Proxy] 📤 已转发到中转群 | {sender_name}: {content_preview}")

                    # ── Step 2: 仅 @元宝模式才通知元宝 + 等回复 ──
                    if self.forward_at_yuanbao:
                        if media_type in ("sticker", "image", "file"):
                            try:
                                await self.send_group_message(
                                    f"@{YUANBAO_NICKNAME} 收到一条{type_names[media_type]}消息",
                                    at_user=YUANBAO_BOT_ID, at_nickname=YUANBAO_NICKNAME,
                                    target_group=IMAGE_GROUP_CODE
                                )
                            except Exception as e:
                                print(f"[Auto-Proxy] ⚠️ @元宝通知发送失败: {e}")

                        try:
                            await asyncio.wait_for(item["future"], timeout=120.0)
                            print(f"[Auto-Proxy] ✅ 元宝回复已转发到原群")
                        except asyncio.TimeoutError:
                            print(f"[Auto-Proxy] ⏰ 等待元宝回复超时(120s)，继续下一条")
                        except asyncio.CancelledError:
                            print(f"[Auto-Proxy] ⚠️ 等待被取消")
                        except Exception as e:
                            print(f"[Auto-Proxy] ⚠️ 等待异常: {e}")
                    else:
                        if not item["future"].done():
                            item["future"].set_result(True)
                        print(f"[Auto-Proxy] ⚡ 直转完成（无需等回复）")
                else:
                    print(f"[Auto-Proxy] ❌ 转发失败 | {sender_name}")
                    if not item["future"].done():
                        item["future"].cancel()

                if self._proxy_queue and self._proxy_queue[0] is item:
                    self._proxy_queue.popleft()

        except asyncio.CancelledError:
            print("[Auto-Proxy] worker 被取消")
        except Exception as e:
            print(f"[Auto-Proxy] ❌ worker 异常退出: {e}")
            self._proxy_queue.clear()
        finally:
            self._proxy_worker_running = False
            self._proxy_worker_task = None
            print(f"[Auto-Proxy] worker 已停止")

    async def _forward_single_item(self, item, media, media_type, sender_name, type_names) -> bool:
        """转发单条消息到中转群，返回是否成功"""
        try:
            if media_type == "sticker":
                return await self._forward_sticker(item, media, sender_name)
            elif media_type == "image":
                return await self._forward_image(item, media, sender_name)
            elif media_type == "file":
                return await self._forward_file(item, media, sender_name)
            else:
                return await self._forward_text(item, sender_name)
        except Exception as e:
            print(f"[Auto-Proxy] 转发异常: {e}")
            return False

    async def _forward_sticker(self, item, media, sender_name) -> bool:
        sticker_name = media.get("sticker_name", "")
        sticker_id = media.get("sticker_id", "")
        package_id = media.get("package_id", "")
        if sticker_name in self.STICKERS:
            return await self.send_sticker(sticker_name, text=f"来自{sender_name}的消息", target_group=IMAGE_GROUP_CODE)
        elif sticker_id:
            msg = self._build_raw_sticker_msg(sticker_name or "贴纸", sticker_id, package_id, group_code=IMAGE_GROUP_CODE)
            await self.ws.send(msg)
            return True
        else:
            return await self.send_group_message(f"来自{sender_name}的消息: [贴纸]", target_group=IMAGE_GROUP_CODE)

    async def _forward_image(self, item, media, sender_name) -> bool:
        """转发图片：直接使用原始 URL 构建 TIMImageElem 发送

        v3.1 重写：参考 sender.py 的代理模式图片转发逻辑，
        直接使用收到的 image_urls 构建消息，不再下载+COS上传。
        同时传递正确的 uuid/size/width/height 信息。
        """
        urls = media.get("image_urls", [])
        if not urls:
            print(f"[Auto-Proxy] ⚠️ 图片无可用 URL，降级为文本转发")
            return await self.send_group_message(f"来自{sender_name}的消息: [图片]", target_group=IMAGE_GROUP_CODE)

        uuid_val = media.get("image_uuid", "")
        width = media.get("image_width", 0)
        height = media.get("image_height", 0)
        size = media.get("image_size", 0)

        # 构建图片列表：每条 URL 都用相同的 uuid/宽高（同一张图的不同分辨率）
        # 如果有多条 URL（不同分辨率），每条都作为一个独立的 TIMImageElem
        images = []
        for url in urls:
            # 尝试解析资源保护 URL 为 COS 直链
            resolved_url = self.resolve_image_url(url) if "resourceId" in url else url
            if resolved_url:
                images.append((resolved_url, uuid_val, size, width, height))
            else:
                # 解析失败也尝试直接用原 URL
                images.append((url, uuid_val, size, width, height))

        if not images:
            print(f"[Auto-Proxy] ⚠️ 图片 URL 解析全部失败，降级为文本转发")
            return await self.send_group_message(f"来自{sender_name}的消息: [图片]", target_group=IMAGE_GROUP_CODE)

        try:
            msg = self._build_image_msg(images, group_code=IMAGE_GROUP_CODE)
            await self.ws.send(msg)
            print(f"[Auto-Proxy] 🖼️ 图片已转发 | {sender_name} | {len(images)}张 | {size/1024:.1f}KB")
            return True
        except Exception as e:
            print(f"[Auto-Proxy] ⚠️ 图片发送失败: {e}")
            # 失败降级为文本
            return await self.send_group_message(f"来自{sender_name}的消息: [图片]", target_group=IMAGE_GROUP_CODE)

    async def _forward_file(self, item, media, sender_name) -> bool:
        """转发文件：直接使用原始 URL 构建 TIMFileElem 发送

        v3.1: 与 sender.py 一致，直接转发不需要下载上传。
        """
        url = media.get("file_url", "")
        if not url:
            print(f"[Auto-Proxy] ⚠️ 文件无可用 URL，降级为文本转发")
            return await self.send_group_message(f"来自{sender_name}的消息: [文件]", target_group=IMAGE_GROUP_CODE)

        uuid_val = media.get("file_uuid", "") or uuid.uuid4().hex
        fname = media.get("file_name", "") or "file.bin"
        fsize = media.get("file_size", 0)

        try:
            msg = self._build_file_msg(url, uuid=uuid_val, file_size=fsize,
                                       file_name=fname, group_code=IMAGE_GROUP_CODE)
            await self.ws.send(msg)
            print(f"[Auto-Proxy] 📎 文件已转发 | {sender_name} | {fname} ({fsize/1024:.1f}KB)")
            return True
        except Exception as e:
            print(f"[Auto-Proxy] ⚠️ 文件发送失败: {e}")
            return await self.send_group_message(f"来自{sender_name}的消息: [文件]", target_group=IMAGE_GROUP_CODE)

    async def _forward_text(self, item, sender_name) -> bool:
        """转发文本消息：始终 @元宝（如果开启），让元宝必然看到并回复"""
        content = item['original_content']
        if self.forward_at_yuanbao:
            return await self.send_group_message(
                f"来自{sender_name}的消息: {content}",
                at_user=YUANBAO_BOT_ID, at_nickname=YUANBAO_NICKNAME,
                target_group=IMAGE_GROUP_CODE
            )
        else:
            return await self.send_group_message(
                f"来自{sender_name}的消息: {content}",
                target_group=IMAGE_GROUP_CODE
            )

    # ── 消息接收循环 ──
    async def _receive_loop(self):
        print("[接收] 启动消息接收循环")
        try:
            while self.connected and self.ws:
                raw = await self.ws.recv()
                if not isinstance(raw, bytes):
                    continue
                conn_msg = self.codec.decode_conn_msg(raw)
                if not conn_msg:
                    continue
                head = conn_msg.get("head", {})
                cmd_type = head.get("cmd_type")
                cmd = head.get("cmd", "")
                msg_id = head.get("msg_id")

                if cmd_type == CMD_TYPE_PUSH and cmd == "inbound_message":
                    await self._handle_push_message(conn_msg)
                elif cmd_type == CMD_TYPE_RESPONSE:
                    self._handle_response(msg_id, conn_msg)
        except Exception as e:
            print(f"[接收] 异常: {e}")
        finally:
            if self.connected:
                print("[接收] 连接断开，尝试重连...")
                self.connected = False
                asyncio.create_task(self._auto_reconnect())

    async def _handle_push_message(self, conn_msg):
        """处理服务端推送的消息"""
        try:
            push_json = json.loads(conn_msg.get("data", b""))
        except Exception as e:
            print(f"[推送] JSON解析失败: {e}")
            return

        text_content, media_info = self._extract_message_content(push_json)
        group_code = push_json.get("group_code", "")
        from_account = push_json.get("from_account", "")

        cache_entry = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "sender_id": from_account,
            "sender_name": push_json.get("sender_nickname", ""),
            "group_code": group_code,
            "content": text_content,
            "msg_type": push_json.get("callback_command", ""),
            "msg_id": push_json.get("msg_id", ""),
            "media_info": media_info,
        }
        self.msg_cache.append(cache_entry)
        if len(self.msg_cache) > 1000:
            self.msg_cache = self.msg_cache[-1000:]

        self._update_group_info(group_code, text_content)
        await self._process_auto_reply(push_json)
        self._maybe_enqueue_proxy(cache_entry, push_json, text_content, media_info, from_account)

        # v3.1: 先检测元宝图片回复（优先于文本回复检测）
        self._maybe_handle_yuanbao_image_reply(push_json, text_content)
        # 再检测元宝文本回复
        self._maybe_handle_yuanbao_reply(push_json, text_content)

    def _extract_message_content(self, push_json) -> Tuple[str, dict]:
        """从推送 JSON 中提取纯文本和媒体信息

        v3.1: 图片保留所有 URL（多图场景），正确提取每张图的宽高。
        """
        text_parts = []
        media_info: dict = {}
        image_urls = []  # 收集所有图片 URL
        image_uuid = ""
        image_width = 0
        image_height = 0
        image_size = 0

        for elem in push_json.get("msg_body", []):
            etype = elem.get("msg_type", "")
            content = elem.get("msg_content", {})
            if etype == "TIMTextElem":
                text_parts.append(content.get("text", ""))
            elif etype == "TIMCustomElem":
                try:
                    cd = json.loads(content.get("data", "{}"))
                    if cd.get("elem_type") == 1002:
                        text_parts.append(cd.get("text", ""))
                except Exception:
                    pass
            elif etype == "TIMFaceElem":
                face_str = content.get("data", "")
                try:
                    face = json.loads(face_str) if isinstance(face_str, str) else json.loads(face_str.decode())
                    name = face.get("name", "")
                    text_parts.append(f"[贴纸: {name}]" if name else "[贴纸]")
                    media_info = {"type": "sticker", "sticker_id": face.get("sticker_id",""),
                                  "sticker_name": name, "package_id": face.get("package_id","")}
                except Exception:
                    text_parts.append("[贴纸]")
                    media_info = {"type": "sticker"}
            elif etype == "TIMImageElem":
                # v3.1: 保留所有分辨率的 URL（与 sender.py 一致）
                # 同时记录最大尺寸的信息用于宽高
                img_array = content.get("image_info_array", [])
                last_info = {}
                for info in img_array:
                    if isinstance(info, dict) and info.get("url"):
                        last_info = info
                        image_urls.append(info["url"])
                # 去重（同一张图的不同分辨率可能重复）
                image_urls = list(dict.fromkeys(image_urls))

                text_parts.append("[图片]" if not media_info else "")
                image_uuid = content.get("uuid", "")
                image_width = last_info.get("width", 0)
                image_height = last_info.get("height", 0)
                image_size = last_info.get("size", 0)
                media_info = {"type": "image", "image_urls": image_urls,
                              "image_uuid": image_uuid,
                              "image_width": image_width,
                              "image_height": image_height,
                              "image_size": image_size}
            elif etype == "TIMFileElem":
                text_parts.append(f"[文件: {content.get('fileName','')}]")
                media_info = {"type": "file", "file_name": content.get("fileName",""),
                              "file_url": content.get("url",""),
                              "file_uuid": content.get("uuid",""),
                              "file_size": content.get("fileSize",0)}
        return " ".join(text_parts).strip(), media_info

    def _update_group_info(self, group_code, text):
        if not group_code:
            return
        short = text[:50] + "..." if len(text) > 50 else text
        if group_code not in self.groups:
            self.groups[group_code] = {"group_code": group_code, "last_message": short,
                                        "last_message_time": datetime.now().strftime("%H:%M:%S"),
                                        "message_count": 0}
        g = self.groups[group_code]
        g["last_message"] = short
        g["last_message_time"] = datetime.now().strftime("%H:%M:%S")
        g["message_count"] = g.get("message_count", 0) + 1

    def _maybe_enqueue_proxy(self, cache_entry, push_json, text_content, media_info, from_account):
        """判断是否将消息加入代理转发队列"""
        if (self.auto_reply_text != "yb" or not cache_entry.get("group_code") or
                cache_entry["group_code"] == IMAGE_GROUP_CODE or from_account == self.bot_id or
                not IMAGE_GROUP_CODE):
            return
        if self.auto_reply_at_only:
            if not self._is_at_bot(push_json):
                return

        text = text_content.strip()
        media_type = (media_info or {}).get("type", "")
        if not text and not media_type:
            return

        raw_msg_id = cache_entry.get("msg_id", "")
        if raw_msg_id:
            if raw_msg_id in self._seen_proxy_msg_ids:
                print(f"[Auto-Proxy] ⚠️ 跳过重复消息 {raw_msg_id[:16]}...")
                return
            if len(self._seen_proxy_msg_ids) > 2000:
                self._seen_proxy_msg_ids = set(list(self._seen_proxy_msg_ids)[-1000:])
            self._seen_proxy_msg_ids.add(raw_msg_id)

        if not text and media_type:
            type_label = {"sticker": "[贴纸]", "image": "[图片]", "file": "[文件]"}
            text = type_label.get(media_type, "[消息]")

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()

        item = {
            "future": loop.create_future(),
            "target_group": cache_entry["group_code"],
            "ref_msg_id": cache_entry.get("msg_id", ""),
            "ref_sender_name": cache_entry.get("sender_name", "未知用户"),
            "original_content": text,
            "media_info": media_info,
            "forwarded": False,
            "reply_received": False,
            "enqueue_time": time.time(),
        }
        self._proxy_queue.append(item)

        worker_alive = (self._proxy_worker_task is not None and
                        not self._proxy_worker_task.done() and
                        getattr(self, '_proxy_worker_running', False))
        if not worker_alive:
            self._proxy_worker_task = asyncio.create_task(self._proxy_worker_loop())
            print(f"[Auto-Proxy] 🔄 worker 已启动 (队列长度: {len(self._proxy_queue)})")

    def _is_at_bot(self, push_json) -> bool:
        for elem in push_json.get("msg_body", []):
            if elem.get("msg_type") != "TIMCustomElem":
                continue
            try:
                cd = json.loads(elem.get("msg_content", {}).get("data", "{}"))
                if cd.get("elem_type") == 1002 and cd.get("user_id") == self.bot_id:
                    return True
            except Exception:
                pass
        return False

    def _maybe_handle_yuanbao_image_reply(self, push_json, text_content):
        """检测元宝在 IMAGE_GROUP 中回复的图片消息

        v3.1 新增：当代理模式开启时，如果元宝返回的是图片（而非文本），
        需要将图片下载后转发到原群。参考 sender.py 的 _pending_image_future 逻辑。
        """
        if not self.forward_at_yuanbao:
            return
        if not self._proxy_queue:
            return
        if push_json.get("group_code") != IMAGE_GROUP_CODE:
            return
        if push_json.get("from_account") != YUANBAO_BOT_ID:
            return

        # 从 msg_body 中提取图片 URL
        msg_body = push_json.get("msg_body", [])
        img_urls = []
        for elem in msg_body:
            if elem.get("msg_type") == "TIMImageElem":
                mc = elem.get("msg_content", {})
                img_array = mc.get("image_info_array", [])
                last_url = None
                for info in img_array:
                    if isinstance(info, dict) and info.get("url"):
                        last_url = info["url"]
                if last_url:
                    img_urls.append(last_url)

        # 同时检测 text 中的 ![image](url) 格式
        text_urls = re.findall(r'!\[image\]\(([^)]+)\)', text_content)
        all_urls = list(dict.fromkeys(img_urls + text_urls))

        if not all_urls:
            return

        # 找到当前待回复的 item
        target_item = None
        for item in self._proxy_queue:
            if item.get("forwarded") and not item.get("reply_received"):
                target_item = item
                break

        if target_item is None:
            return

        print(f"[Auto-Proxy] 🖼️ 检测到元宝图片回复，共 {len(all_urls)} 张，开始解析直链...")

        # 解析所有 URL 为 COS 直链
        resolved_urls = []
        for u in all_urls:
            resolved = self.resolve_image_url(u) if "resourceId" in u else u
            if resolved:
                resolved_urls.append(resolved)

        if not resolved_urls:
            print(f"[Auto-Proxy] ⚠️ 图片 URL 解析失败")
            return

        # 标记已回复（图片类型）
        target_item["reply_received"] = True

        # 下载并发送到原群
        asyncio.create_task(self._download_and_send_images(resolved_urls, target_item))

    async def _download_and_send_images(self, urls: list, target_item: dict):
        """下载图片并发送到原群（对应 sender.py 的 _image_download_and_send）"""
        import shutil

        temp_dir = tempfile.mkdtemp(prefix="proxy_img_")
        downloaded = []
        try:
            for i, url in enumerate(urls):
                try:
                    resp = await asyncio.to_thread(requests.get, url, timeout=60)
                    if resp.status_code == 200:
                        fname = f"yuanbao_{i}.png"
                        fpath = os.path.join(temp_dir, fname)
                        with open(fpath, 'wb') as f:
                            f.write(resp.content)
                        downloaded.append(fpath)
                        print(f"[Auto-Proxy] 📥 已下载图片 {i+1}/{len(urls)}: {len(resp.content)} 字节")
                    else:
                        print(f"[Auto-Proxy] ⚠️ 下载图片 {i+1} 失败: HTTP {resp.status_code}")
                except Exception as e:
                    print(f"[Auto-Proxy] ⚠️ 下载图片 {i+1} 失败: {e}")

            if not downloaded:
                print(f"[Auto-Proxy] ❌ 没有成功下载任何图片")
                return

            # 用 send_images_multi 发送（需要切换到原群）
            orig_group = self.group_code
            self.group_code = target_item["target_group"]
            try:
                ok = await self.send_images_multi(downloaded)
                if ok:
                    print(f"[Auto-Proxy] ✅ 元宝图片已转发到原群")
                    if not target_item["future"].done():
                        target_item["future"].set_result(True)
                else:
                    print(f"[Auto-Proxy] ❌ 图片发送到原群失败")
            finally:
                self.group_code = orig_group
        finally:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass

    def _maybe_handle_yuanbao_reply(self, push_json, text_content):
        """检测中转群里元宝的文本回复，转发回原群

        v3.0: 直转模式（forward_at_yuanbao=False）下完全忽略元宝回复。
        v3.1: 仅在非图片回复时处理（图片由 _maybe_handle_yuanbao_image_reply 处理）。
        """
        if not self.forward_at_yuanbao:
            return
        if (not self._proxy_queue or
                push_json.get("group_code") != IMAGE_GROUP_CODE or
                push_json.get("from_account") != YUANBAO_BOT_ID):
            return
        reply = text_content.strip()
        if not reply:
            return

        # 如果这条消息包含图片 URL（![image]格式），让图片处理器接管
        if re.search(r'!\[image\]\([^)]+\)', reply):
            # 图片处理器会处理，这里跳过避免重复
            return

        target_item = None
        for item in self._proxy_queue:
            if item.get("forwarded") and not item.get("reply_received"):
                target_item = item
                break

        if target_item is None:
            print(f"[Auto-Proxy] ⚠️ 收到元宝回复但无匹配待回复 item (队列{len(self._proxy_queue)}条)")
            return

        if target_item.get("reply_received"):
            return
        target_item["reply_received"] = True

        msg = self._build_reply_msg(reply, target_item.get("ref_msg_id", ""), target_group=target_item["target_group"])
        if msg:
            try:
                asyncio.create_task(self._send_and_complete(msg, target_item))
                print(f"[Auto-Proxy] 📥 收到元宝回复 → 转发到原群 | {reply[:60]}")
            except Exception as e:
                print(f"[Auto-Proxy] 回复转发失败: {e}")
                target_item["reply_received"] = False
        else:
            print(f"[Auto-Proxy] ⚠️ 构建回复消息失败")
            target_item["reply_received"] = False

    async def _send_and_complete(self, msg, current):
        """发送回复到原群并标记完成"""
        try:
            if not self.connected or not self.ws:
                print(f"[Auto-Proxy] ⚠️ 未连接，无法转发回复到原群")
                return
            await self.ws.send(msg)
            print(f"[Auto-Proxy] ✅ 已转发元宝回复到原群")
            if not current["future"].done():
                current["future"].set_result(True)
        except Exception as e:
            print(f"[Auto-Proxy] ❌ 转发回复到原群失败: {e}")

    def _handle_response(self, msg_id, conn_msg):
        """处理服务端响应"""
        if msg_id in self.pending_requests:
            future = self.pending_requests.pop(msg_id)
            try:
                cmd = conn_msg.get("head", {}).get("cmd", "")
                if cmd == BIZ_CMD_GET_MEMBERS:
                    data = conn_msg.get("data", b"")
                    future.set_result(self.codec.decode_get_group_member_list_rsp(data) if data
                                      else {"code": -1, "message": "响应数据为空"})
                elif cmd in (BIZ_CMD_SEND_GROUP, BIZ_CMD_SEND_C2C):
                    future.set_result({"code": 0, "message": "发送成功"})
                else:
                    future.set_result(conn_msg)
            except Exception as e:
                future.set_exception(e)

    # ── 图片/文件上传（用于主动发送本地文件）──────────
    def _get_upload_info(self, filename: str, file_id: str) -> Optional[dict]:
        if not self.bot_id or not self.token:
            return None
        url = f"https://{API_DOMAIN}/api/resource/genUploadInfo"
        headers = {
            "Content-Type": "application/json", "X-ID": self.bot_id, "X-Token": self.token,
            "X-Source": "web", "X-AppVersion": "2.0.1", "X-OperationSystem": "Linux", "X-Instance-Id": "99",
        }
        try:
            r = requests.post(url, headers=headers,
                             json={"fileName": filename, "fileId": file_id, "docFrom": "localDoc", "docOpenId": ""},
                             timeout=HTTP_TIMEOUT)
            result = r.json()
            return result.get("data", result) if result.get("code", 0) == 0 else None
        except Exception as e:
            print(f"[上传] 获取凭证失败: {e}")
            return None

    def _upload_to_cos(self, config: dict, data: bytes, filename: str, max_retries: int = 3) -> Optional[str]:
        """上传到腾讯云 COS（带重试机制）"""
        for attempt in range(1, max_retries + 1):
            try:
                from qcloud_cos import CosConfig, CosS3Client
                cc = CosConfig(Region=config["region"], SecretId=config["encryptTmpSecretId"],
                               SecretKey=config["encryptTmpSecretKey"], Token=config["encryptToken"])
                CosS3Client(cc).put_object(Bucket=config["bucketName"], Body=data,
                                            Key=config["location"], ContentType="application/octet-stream")
                return config.get("resourceUrl")
            except ImportError:
                return self._upload_to_cos_manual(config, data)
            except Exception as e:
                print(f"[上传] COS SDK 第{attempt}次失败: {e}")
                if attempt < max_retries:
                    time.sleep(1 * attempt)
                else:
                    return self._upload_to_cos_manual(config, data)

    def _upload_to_cos_manual(self, config: dict, data: bytes) -> Optional[str]:
        """手动构造 COS 签名上传"""
        import hmac as hm
        secret_id = config.get("encryptTmpSecretId", "")
        secret_key = config.get("encryptTmpSecretKey", "")
        token = config.get("encryptToken", "")
        kt = f"{config.get('startTime',0)};{config.get('expiredTime',0)}"
        sign_key = hm.new(secret_key.encode(), kt.encode(), hashlib.sha1).hexdigest()
        http_str = f"put\n{config['location']}\n\nhost={config['bucketName']}.cos.{config['region']}.myqcloud.com\n"
        string_to_sign = f"sha1\n{kt}\n{hashlib.sha1(http_str.encode()).hexdigest()}\n"
        sig = hm.new(sign_key.encode(), string_to_sign.encode(), hashlib.sha1).hexdigest()
        auth = f"q-sign-algorithm=sha1&q-ak={secret_id}&q-sign-time={kt}&q-key-time={kt}&q-header-list=host&q-url-param-list=&q-signature={sig}"
        if token:
            auth += f"&x-cos-security-token={token}"
        url = f"https://{config['bucketName']}.cos.{config['region']}.myqcloud.com{config['location']}"
        headers = {"Host": f"{config['bucketName']}.cos.{config['region']}.myqcloud.com",
                   "Authorization": auth, "Content-Type": "application/octet-stream"}
        if token:
            headers["x-cos-security-token"] = token
        try:
            r = requests.put(url, headers=headers, data=data, timeout=60)
            return config.get("resourceUrl", url) if r.status_code == 200 else None
        except Exception as e:
            print(f"[上传] 手动签名失败: {e}")
            return None

    # ── 消息构建辅助 ──
    def _build_raw_sticker_msg(self, name, sid, pid, group_code=None) -> bytes:
        gc = group_code or self.group_code or ""
        known = self.STICKERS.get(name, {})
        face = self.codec.encode_tim_face_elem(sid, pid, name,
                                               known.get("width",128), known.get("height",128),
                                               known.get("formats","png"))
        data = self.codec.encode_string(1, self._generate_msg_id())
        data += self.codec.encode_string(2, gc)
        data += self.codec.encode_string(3, self.bot_id or "")
        data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
        data += self.codec.encode_message_field(6, face)
        head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
        self.seq_no += 1
        return self.codec.encode_conn_msg(head, data)

    def _build_image_msg(self, images: list, group_code=None) -> bytes:
        """构建多图群消息

        images: 列表，每项为 (url, uuid, size, width, height) 元组
        v3.1: 支持多个 TIMImageElem（多图消息）
        """
        gc = group_code or self.group_code or ""
        data = self.codec.encode_string(1, self._generate_msg_id())
        data += self.codec.encode_string(2, gc)
        data += self.codec.encode_string(3, self.bot_id or "")
        data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
        for img in images:
            data += self.codec.encode_message_field(6, self.codec.encode_tim_image_elem(*img))
        head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
        self.seq_no += 1
        return self.codec.encode_conn_msg(head, data)

    def _build_file_msg(self, url: str, file_id="", file_size=0, file_name="", group_code=None) -> bytes:
        gc = group_code or self.group_code or ""
        data = self.codec.encode_string(1, self._generate_msg_id())
        data += self.codec.encode_string(2, gc)
        data += self.codec.encode_string(3, self.bot_id or "")
        data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
        data += self.codec.encode_message_field(6, self.codec.encode_tim_file_elem(url, file_id, file_size, file_name))
        head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
        self.seq_no += 1
        return self.codec.encode_conn_msg(head, data)

    def _build_reply_msg(self, text: str, ref_msg_id: str, at_user_id="", at_nickname="", target_group=None) -> Optional[bytes]:
        gc = target_group or self.group_code
        if at_user_id:
            display = at_nickname or at_user_id
            at_data = json.dumps({"elem_type": 1002, "text": f"@{display}", "user_id": at_user_id})
            at_elem = self.codec.encode_string(1, "TIMCustomElem") + \
                      self.codec.encode_message_field(2, self.codec.encode_string(4, at_data))
            text_elem = self.codec.encode_string(1, "TIMTextElem") + \
                        self.codec.encode_message_field(2, self.codec.encode_string(1, text))
            data = self.codec.encode_string(1, self._generate_msg_id())
            data += self.codec.encode_string(2, gc)
            data += self.codec.encode_string(3, self.bot_id or "")
            data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
            data += self.codec.encode_message_field(6, at_elem)
            data += self.codec.encode_message_field(6, text_elem)
            data += self.codec.encode_string(7, ref_msg_id)
        else:
            data = self.codec.encode_send_group_msg_req(self._generate_msg_id(), gc, self.bot_id or "", text, ref_msg_id)
        head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
        self.seq_no += 1
        return self.codec.encode_conn_msg(head, data)

    # ── 公共发送方法 ──
    async def send_group_message(self, text, at_user=None, at_nickname=None,
                                  ref_msg_id=None, target_group=None) -> bool:
        if not self.connected or not self.ws:
            return False
        gc = target_group or self.group_code
        try:
            if at_user:
                msg = self._build_at_message(text, at_user, at_nickname, gc, ref_msg_id)
            else:
                msg = self.codec.encode_conn_msg(
                    HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE),
                    self.codec.encode_send_group_msg_req(self._generate_msg_id(), gc, self.bot_id or "", text, ref_msg_id or "")
                )
                self.seq_no += 1
            await self.ws.send(msg)
            self._cache_sent_message(text, gc, "send_group")
            return True
        except Exception as e:
            print(f"[发送] 群消息失败: {e}")
            return False

    def _build_at_message(self, text, at_user, at_nickname, gc, ref_msg_id) -> bytes:
        display = at_nickname or at_user
        at_data = json.dumps({"elem_type": 1002, "text": f"@{display}", "user_id": at_user})
        at_elem = self.codec.encode_string(1, "TIMCustomElem") + \
                  self.codec.encode_message_field(2, self.codec.encode_string(4, at_data))
        text_elem = self.codec.encode_string(1, "TIMTextElem") + \
                    self.codec.encode_message_field(2, self.codec.encode_string(1, text))
        data = self.codec.encode_string(1, self._generate_msg_id())
        data += self.codec.encode_string(2, gc)
        data += self.codec.encode_string(3, self.bot_id or "")
        data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
        data += self.codec.encode_message_field(6, at_elem)
        data += self.codec.encode_message_field(6, text_elem)
        if ref_msg_id:
            data += self.codec.encode_string(7, ref_msg_id)
        head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
        self.seq_no += 1
        return self.codec.encode_conn_msg(head, data)

    def _cache_sent_message(self, text, gc, msg_type):
        self.msg_cache.append({
            "time": datetime.now().strftime("%H:%M:%S"),
            "sender_id": self.bot_id or "", "sender_name": "我",
            "group_code": gc or "", "content": text, "msg_type": msg_type, "msg_id": "",
        })
        if len(self.msg_cache) > 1000:
            self.msg_cache = self.msg_cache[-1000:]

    async def send_sticker(self, sticker_name, text="", at_user=None, at_nickname=None, target_group=None) -> bool:
        if not self.connected or not self.ws or sticker_name not in self.STICKERS:
            return False
        gc = target_group or self.group_code
        try:
            s = self.STICKERS[sticker_name]
            face = self.codec.encode_tim_face_elem(s["sticker_id"], s["package_id"], s["name"])
            data = self.codec.encode_string(1, self._generate_msg_id())
            data += self.codec.encode_string(2, gc)
            data += self.codec.encode_string(3, self.bot_id or "")
            data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
            if at_user:
                display = at_nickname or at_user
                at_data = json.dumps({"elem_type": 1002, "text": f"@{display}", "user_id": at_user})
                at_elem = self.codec.encode_string(1, "TIMCustomElem") + \
                          self.codec.encode_message_field(2, self.codec.encode_string(4, at_data))
                data += self.codec.encode_message_field(6, at_elem)
            data += self.codec.encode_message_field(6, face)
            if text:
                data += self.codec.encode_message_field(6,
                    self.codec.encode_string(1, "TIMTextElem") +
                    self.codec.encode_message_field(2, self.codec.encode_string(1, text)))
            head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
            self.seq_no += 1
            await self.ws.send(self.codec.encode_conn_msg(head, data))
            self._cache_sent_message(f"[贴纸:{sticker_name}] {text}", gc, "send_sticker")
            return True
        except Exception as e:
            print(f"[发送] 贴纸失败: {e}")
            return False

    async def send_c2c_message(self, to_account: str, text: str) -> bool:
        if not self.connected or not self.ws:
            return False
        try:
            data = self.codec.encode_send_c2c_msg_req(self._generate_msg_id(), to_account, self.bot_id or "", text)
            head = HeadBuilder.request(BIZ_CMD_SEND_C2C, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
            self.seq_no += 1
            await self.ws.send(self.codec.encode_conn_msg(head, data))
            self._cache_sent_message(text, "", "send_c2c")
            return True
        except Exception as e:
            print(f"[发送] 私聊失败: {e}")
            return False

    async def send_image(self, image_path: str) -> bool:
        """发送本地图片到当前群（单张）"""
        return await self.send_images_multi([image_path])

    async def send_images_multi(self, image_paths: list) -> bool:
        """发送多张图片到当前群（一次消息包含多图）

        v3.1: 与 sender.py 的 send_images_multi 逻辑一致
        """
        if not self.connected or not self.ws:
            return False

        import io
        try:
            from PIL import Image
            has_pil = True
        except ImportError:
            has_pil = False

        images = []
        for path in image_paths:
            if not os.path.exists(path):
                print(f"[图片] 文件不存在: {path}")
                continue
            try:
                with open(path, 'rb') as f:
                    data = f.read()
            except Exception as e:
                print(f"[图片] 读取失败 {path}: {e}")
                continue

            max_bytes = 20 * 1024 * 1024
            if len(data) > max_bytes:
                print(f"[图片] 过大（跳过）: {path} ({len(data)/1024/1024:.1f}MB)")
                continue

            filename = os.path.basename(path)
            file_id = uuid.uuid4().hex
            config = self._get_upload_info(filename, file_id)
            if not config:
                continue

            url = self._upload_to_cos(config, data, filename)
            if not url:
                continue

            # 解析宽高
            width, height = 0, 0
            if has_pil:
                try:
                    img = Image.open(io.BytesIO(data))
                    width, height = img.size
                except Exception:
                    pass

            images.append((url, file_id, len(data), width, height))

        if not images:
            print("[图片] 没有成功处理任何图片")
            return False

        try:
            msg = self._build_image_msg(images)
            await self.ws.send(msg)
            print(f"[图片] 已发送 {len(images)} 张图片")
            for path in image_paths[:len(images)]:
                self._cache_sent_message(f"[图片] {os.path.basename(path)}", self.group_code or "", "send_image")
            return True
        except Exception as e:
            print(f"[图片] 发送失败: {e}")
            return False

    async def send_file(self, file_path: str) -> bool:
        """发送文件消息"""
        if not self.connected or not self.ws:
            return False
        if not os.path.exists(file_path):
            print(f"[文件] 文件不存在: {file_path}")
            return False
        try:
            with open(file_path, 'rb') as f:
                data = f.read()
        except Exception as e:
            print(f"[文件] 读取失败: {e}")
            return False
        if len(data) > 20 * 1024 * 1024:
            print(f"[文件] 文件过大: {len(data)/1024/1024:.1f}MB")
            return False
        fname = os.path.basename(file_path)
        fid = uuid.uuid4().hex
        config = self._get_upload_info(fname, fid)
        if not config:
            return False
        url = self._upload_to_cos(config, data, fname)
        if not url:
            return False
        try:
            msg = self._build_file_msg(url, fid, len(data), fname)
            await self.ws.send(msg)
            self._cache_sent_message(f"[文件] {fname}", self.group_code or "", "send_file")
            return True
        except Exception as e:
            print(f"[文件] 发送失败: {e}")
            return False

    async def send_multi_at_message(self, text: str, at_users: list) -> bool:
        if not self.connected or not self.ws:
            return False
        try:
            data = self.codec.encode_string(1, self._generate_msg_id())
            data += self.codec.encode_string(2, self.group_code)
            data += self.codec.encode_string(3, self.bot_id or "")
            data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
            for uid, nick in at_users:
                display = nick or uid
                at_data = json.dumps({"elem_type": 1002, "text": f"@{display}", "user_id": uid})
                at_elem = self.codec.encode_string(1, "TIMCustomElem") + \
                          self.codec.encode_message_field(2, self.codec.encode_string(4, at_data))
                data += self.codec.encode_message_field(6, at_elem)
            data += self.codec.encode_message_field(6,
                self.codec.encode_string(1, "TIMTextElem") +
                self.codec.encode_message_field(2, self.codec.encode_string(1, text)))
            head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
            self.seq_no += 1
            await self.ws.send(self.codec.encode_conn_msg(head, data))
            self._cache_sent_message(f"[批量艾特 {len(at_users)}人] {text}", self.group_code or "", "send_group")
            return True
        except Exception as e:
            print(f"[发送] 批量艾特失败: {e}")
            return False

    # ── 群成员查询 ──
    async def get_group_members(self) -> Optional[dict]:
        if not self.connected or not self.ws:
            return None
        msg_id = self._generate_msg_id()
        biz_data = self.codec.encode_get_group_member_list_req(self.group_code or "")
        head = HeadBuilder.request(BIZ_CMD_GET_MEMBERS, self.seq_no, msg_id, BIZ_MODULE)
        self.seq_no += 1
        future = asyncio.get_event_loop().create_future()
        self.pending_requests[msg_id] = future
        try:
            await self.ws.send(self.codec.encode_conn_msg(head, biz_data))
            return await asyncio.wait_for(future, timeout=30)
        except asyncio.TimeoutError:
            self.pending_requests.pop(msg_id, None)
            return None
        except Exception as e:
            print(f"[成员] 查询失败: {e}")
            return None

    # ── 刷屏工具 ──
    async def spam_with_at(self, text, count, at_user=None, at_nickname=None, interval=0.1):
        s = f = 0
        for i in range(count):
            if await self.send_group_message(text, at_user, at_nickname):
                s += 1
            else:
                f += 1
            if i < count - 1:
                await asyncio.sleep(interval)
        return s, f

    async def spam_sticker(self, name, count, text="", interval=0.1):
        s = f = 0
        for i in range(count):
            if await self.send_sticker(name, text):
                s += 1
            else:
                f += 1
            if i < count - 1:
                await asyncio.sleep(interval)
        return s, f

    async def spam_c2c(self, to, text, count, interval=0.1):
        s = f = 0
        for i in range(count):
            if await self.send_c2c_message(to, text):
                s += 1
            else:
                f += 1
            if i < count - 1:
                await asyncio.sleep(interval)
        return s, f


# ═══════════════════════════════════════════
#  Head 构建器
# ═══════════════════════════════════════════
class HeadBuilder:
    @staticmethod
    def request(cmd: str, seq: int, msg_id: str, module: str) -> bytes:
        return SimpleProtobufCodec.encode_head(CMD_TYPE_REQUEST, cmd, seq, msg_id, module)


# ═══════════════════════════════════════════
#  Flask Web 应用
# ═══════════════════════════════════════════
app = Flask(__name__)

# ── 全局状态 ──
sender = EnhancedSpamSender()
settings = {
    'group_code': app_config.get('DEFAULT_GROUP_CODE', ''),
    'interval': 0.1,
    'heartbeat_interval': 1.0,
    'auto_reply_enabled': False,
    'group_reply_text': app_config.get('AUTO_REPLY_GROUP_TEXT', '@我干啥'),
    'c2c_reply_text': app_config.get('AUTO_REPLY_C2C_TEXT', '我是Bot'),
    'forward_mode_enabled': False,
    'forward_at_only': False,
    'forward_at_yuanbao': True,
}

_proxy_config_version = 0

# Asyncio 事件循环管理
_loop = None
_loop_thread = None


def _run_loop():
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _loop.run_forever()

def _ensure_loop():
    global _loop, _loop_thread
    if _loop is None or not _loop.is_running():
        _loop = asyncio.new_event_loop()
        _loop_thread = threading.Thread(target=_run_loop, daemon=True)
        _loop_thread.start()
        time.sleep(0.1)

def async_call(coro, timeout=30):
    _ensure_loop()
    return asyncio.run_coroutine_threadsafe(coro, _loop).result(timeout=timeout)

def async_call_no_wait(coro):
    _ensure_loop()
    asyncio.run_coroutine_threadsafe(coro, _loop)


# ── 配置持久化 ──
def save_config():
    """将运行时配置写回 config.json"""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        config = {}
    config.update({
        'AUTO_REPLY_RULES': sender.AUTO_REPLY_RULES,
        'DEFAULT_REPLY': sender.DEFAULT_REPLY,
        'AUTO_REPLY_GROUP_TEXT': sender.AUTO_REPLY_GROUP_TEXT,
        'AUTO_REPLY_C2C_TEXT': sender.AUTO_REPLY_C2C_TEXT,
        'DEFAULT_GROUP_CODE': settings['group_code'],
        'HEARTBEAT_INTERVAL': sender.heartbeat_interval,
        'FORWARD_MODE_ENABLED': settings.get('forward_mode_enabled', False),
        'FORWARD_AT_ONLY': settings.get('forward_at_only', False),
        'FORWARD_AT_YUANBAO': sender.forward_at_yuanbao,
    })
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


# ── 错误处理 ──
@app.errorhandler(404)
def not_found(e):
    return jsonify({'ok': False, 'message': '接口不存在'}), 404

@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({'ok': False, 'message': '请求方法不允许'}), 405

@app.errorhandler(Exception)
def handle_exception(e):
    code = getattr(e, 'code', None)
    if code and 400 <= code < 600:
        return jsonify({'ok': False, 'message': getattr(e, 'description', str(e))}), code
    print(f"[错误] {e}")
    import traceback
    traceback.print_exc()
    return jsonify({'ok': False, 'message': f'服务器内部错误: {e}'}), 500


# ── 健康检查 ──
@app.route('/api/health')
def api_health():
    return jsonify({'ok': True, 'connected': sender.connected, 'time': datetime.now().isoformat()})


# ── 连接管理 ──
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/connect', methods=['POST'])
def api_connect():
    try:
        sender.group_code = settings['group_code']
        if async_call(sender.connect()):
            return jsonify({'ok': True, 'message': '连接成功'})
        return jsonify({'ok': False, 'message': '连接失败，请检查配置'}), 400
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500

@app.route('/api/disconnect', methods=['POST'])
def api_disconnect():
    try:
        async_call(sender.disconnect())
        return jsonify({'ok': True, 'message': '已断开连接'})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500

@app.route('/api/status', methods=['GET'])
def api_status():
    return jsonify({
        'connected': sender.connected,
        'group_code': sender.group_code or settings['group_code'],
        'user_count': len(sender.user_db),
        'message_count': len(sender.msg_cache),
        'bot_id': sender.bot_id or '',
        'auto_reply_enabled': sender.auto_reply_enabled,
        'heartbeat_interval': sender.heartbeat_interval,
        'groups': list(sender.groups.values()),
        'forward_mode_enabled': sender.auto_reply_text == "yb",
        'forward_at_only': sender.auto_reply_at_only,
        'forward_at_yuanbao': sender.forward_at_yuanbao,
        'image_group_code': IMAGE_GROUP_CODE,
        'yuanbao_id': YUANBAO_BOT_ID,
        'forward_queue_length': len(sender._proxy_queue),
        'worker_running': sender._proxy_worker_task is not None and not sender._proxy_worker_task.done(),
    })


# ── 消息 ──
@app.route('/api/messages', methods=['GET'])
def api_messages():
    limit = request.args.get('limit', 50, type=int)
    cache = sender.msg_cache
    return jsonify({'messages': cache[-limit:] if cache else [], 'total': len(cache)})


# ── 发送图片 ──
@app.route('/api/send-image', methods=['POST'])
def api_send_image():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接'}), 400
    try:
        if request.content_type and 'multipart' in request.content_type:
            if 'file' not in request.files:
                return jsonify({'ok': False, 'message': '未选择图片文件'}), 400
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix='_' + (request.files['file'].filename or ''),
                                              dir=os.path.dirname(os.path.abspath(__file__)))
            tmp.close()
            request.files['file'].save(tmp.name)
            try:
                ok = async_call(sender.send_image(tmp.name))
            finally:
                try: os.unlink(tmp.name)
                except Exception: pass
            return (jsonify({'ok': True, 'message': '图片发送成功'}) if ok
                    else jsonify({'ok': False, 'message': '图片发送失败'}), 400)
        else:
            data = request.get_json(force=True)
            path = data.get('path', '').strip()
            if not path or not os.path.exists(path):
                return jsonify({'ok': False, 'message': '文件路径无效'}), 400
            return (jsonify({'ok': True, 'message': '图片发送成功'}) if async_call(sender.send_image(path))
                    else jsonify({'ok': False, 'message': '图片发送失败'}), 400)
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


# ── 发送文件 ──
@app.route('/api/send-file', methods=['POST'])
def api_send_file():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接'}), 400
    try:
        if request.content_type and 'multipart' in request.content_type:
            if 'file' not in request.files:
                return jsonify({'ok': False, 'message': '未选择文件'}), 400
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix='_' + (request.files['file'].filename or ''),
                                              dir=os.path.dirname(os.path.abspath(__file__)))
            tmp.close()
            request.files['file'].save(tmp.name)
            try:
                ok = async_call(sender.send_file(tmp.name))
            finally:
                try: os.unlink(tmp.name)
                except Exception: pass
            return (jsonify({'ok': True, 'message': '文件发送成功'}) if ok
                    else jsonify({'ok': False, 'message': '文件发送失败'}), 400)
        else:
            data = request.get_json(force=True)
            path = data.get('path', '').strip()
            if not path or not os.path.exists(path):
                return jsonify({'ok': False, 'message': '文件路径无效'}), 400
            return (jsonify({'ok': True, 'message': '文件发送成功'}) if async_call(sender.send_file(path))
                    else jsonify({'ok': False, 'message': '文件发送失败'}), 400)
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


# ── 发送消息 ──
@app.route('/api/send', methods=['POST'])
def api_send():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接'}), 400
    data = request.get_json(force=True)
    mode = data.get('mode', 'normal')
    text = data.get('text', '')
    target = data.get('target', '')
    count = int(data.get('count', 1))
    interval = float(data.get('interval', settings['interval']))
    ref_msg_id = data.get('ref_msg_id', '')

    def _parse_at(ts, fallback_user="", fallback_nick=""):
        uid = (fallback_user or '').strip()
        nick = (fallback_nick or '').strip()
        raw = (ts or '').strip()
        if raw:
            parts = raw.split(':', 1)
            uid = parts[0].strip()
            nick = parts[1].strip() if len(parts) > 1 else nick or uid
        return uid, nick

    try:
        if mode == 'normal':
            ok = async_call(sender.send_group_message(text, ref_msg_id=ref_msg_id))
            return jsonify({'ok': ok, 'message': '发送成功' if ok else '发送失败'})
        elif mode == 'at':
            uid, nick = _parse_at(target, data.get('at_user',''), data.get('at_nickname',''))
            if not uid: return jsonify({'ok': False, 'message': '请指定目标用户'}), 400
            ok = async_call(sender.send_group_message(text, uid, nick, ref_msg_id))
            return jsonify({'ok': ok, 'message': '发送成功' if ok else '发送失败'})
        elif mode == 'spam':
            s, f = async_call(sender.spam_with_at(text, count, interval=interval))
            return jsonify({'ok': f==0, 'message': f'刷屏完成: 成功 {s}, 失败 {f}', 'success': s, 'fail': f})
        elif mode == 'atspam':
            uid, nick = _parse_at(target, data.get('at_user',''), data.get('at_nickname',''))
            if not uid: return jsonify({'ok': False, 'message': '请指定目标用户'}), 400
            s, f = async_call(sender.spam_with_at(text, count, uid, nick, interval))
            return jsonify({'ok': f==0, 'message': f'艾特刷屏完成: 成功 {s}, 失败 {f}', 'success': s, 'fail': f})
        elif mode == 'multi-at':
            if not target: return jsonify({'ok': False, 'message': '请指定目标用户'}), 400
            at_users = []
            for uid in [u.strip() for u in target.split(',') if u.strip()]:
                if ':' in uid:
                    u, n = uid.split(':', 1)
                    at_users.append((u.strip(), n.strip()))
                else:
                    at_users.append((uid, sender.user_db.get(uid, uid)))
            ok = async_call(sender.send_multi_at_message(text, at_users))
            return jsonify({'ok': ok, 'message': '发送成功' if ok else '发送失败'})
        elif mode == 'dm':
            if not target: return jsonify({'ok': False, 'message': '请指定目标用户'}), 400
            ok = async_call(sender.send_c2c_message(target, text))
            return jsonify({'ok': ok, 'message': '发送成功' if ok else '发送失败'})
        elif mode == 'dmspam':
            if not target: return jsonify({'ok': False, 'message': '请指定目标用户'}), 400
            s, f = async_call(sender.spam_c2c(target, text, count, interval))
            return jsonify({'ok': f==0, 'message': f'私聊刷屏完成: 成功 {s}, 失败 {f}', 'success': s, 'fail': f})
        else:
            return jsonify({'ok': False, 'message': f'未知模式: {mode}'}), 400
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


# ── 发送贴纸 ──
@app.route('/api/send-sticker', methods=['POST'])
def api_send_sticker():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接'}), 400
    data = request.get_json(force=True)
    name = data.get('name', '')
    if not name:
        return jsonify({'ok': False, 'message': '请选择贴纸'}), 400
    try:
        count = int(data.get('count', 1))
        if count > 1:
            s, f = async_call(sender.spam_sticker(name, count, data.get('text',''),
                                                   float(data.get('interval', settings['interval']))))
            return jsonify({'ok': f==0, 'message': f'贴纸刷屏完成: 成功 {s}, 失败 {f}', 'success': s, 'fail': f})
        ok = async_call(sender.send_sticker(name, data.get('text',''),
                                             data.get('at_user','').strip(), data.get('at_nickname','').strip()))
        return jsonify({'ok': ok, 'message': '发送成功' if ok else '发送失败'})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


# ── 引用回复 ──
@app.route('/api/send-reply', methods=['POST'])
def api_send_reply():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接'}), 400
    data = request.get_json(force=True)
    idx = int(data.get('index', -1))
    if idx < 0 or idx >= len(sender.msg_cache):
        return jsonify({'ok': False, 'message': '无效的消息序号'}), 400
    target_msg = sender.msg_cache[idx]
    ref = target_msg.get('msg_id', '')
    text = data.get('text', '')
    at_u = data.get('at_user', '').strip()
    at_n = data.get('at_nickname', '').strip()
    count = int(data.get('count', 1))
    interval = float(data.get('interval', settings['interval']))
    try:
        if count > 1:
            s = f = 0
            for i in range(count):
                ok = async_call(sender.send_group_message(text, at_u or None, at_n or None, ref))
                s += ok; f += not ok
                if i < count - 1:
                    async_call(asyncio.sleep(interval))
            return jsonify({'ok': f==0, 'message': f'引用刷屏: {s}成功 {f}失败', 'success': s, 'fail': f})
        ok = async_call(sender.send_group_message(text, at_u or None, at_n or None, ref))
        return jsonify({'ok': ok, 'message': '回复成功' if ok else '回复失败'})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


# ── 代理模式 API ──
@app.route('/api/forward-mode/enable', methods=['POST'])
def api_forward_mode_enable():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接'}), 400
    if not IMAGE_GROUP_CODE:
        return jsonify({'ok': False, 'message': '配置中缺少 IMAGE_GROUP_CODE'}), 400
    data = request.get_json(force=True)
    at_only = data.get('at_only', False)
    fwd_at_yb = data.get('forward_at_yuanbao', True)

    sender.auto_reply_text = "yb"
    sender.auto_reply_at_only = at_only
    sender.forward_at_yuanbao = fwd_at_yb
    sender._proxy_queue.clear()
    settings.update({'forward_mode_enabled': True, 'forward_at_only': at_only,
                     'forward_at_yuanbao': fwd_at_yb})
    global _proxy_config_version
    _proxy_config_version += 1
    save_config()

    return jsonify({'ok': True, 'message': '代理模式已启用（实时转发 + 等待元宝回复）',
                    'at_only': at_only, 'forward_at_yuanbao': fwd_at_yb})

@app.route('/api/forward-mode/disable', methods=['POST'])
def api_forward_mode_disable():
    sender.auto_reply_text = None
    sender.auto_reply_at_only = False
    for item in sender._proxy_queue:
        if not item["future"].done():
            item["future"].cancel()
    sender._proxy_queue.clear()
    sender._proxy_worker_running = False
    if sender._proxy_worker_task and not sender._proxy_worker_task.done():
        sender._proxy_worker_task.cancel()
    sender._proxy_worker_task = None
    settings['forward_mode_enabled'] = False
    global _proxy_config_version
    _proxy_config_version += 1
    save_config()
    return jsonify({'ok': True, 'message': '代理模式已禁用'})

@app.route('/api/forward-mode/config', methods=['GET'])
def api_forward_mode_config():
    return jsonify({'ok': True, 'enabled': sender.auto_reply_text == "yb",
                    'at_only': sender.auto_reply_at_only,
                    'forward_at_yuanbao': sender.forward_at_yuanbao,
                    'image_group_code': IMAGE_GROUP_CODE, 'yuanbao_id': YUANBAO_BOT_ID,
                    'yuanbao_nickname': YUANBAO_NICKNAME,
                    'queue_length': len(sender._proxy_queue),
                    'worker_running': sender._proxy_worker_task is not None and not sender._proxy_worker_task.done()})

@app.route('/api/forward-mode/clear-queue', methods=['POST'])
def api_forward_mode_clear_queue():
    for item in sender._proxy_queue:
        if not item["future"].done():
            item["future"].cancel()
    sender._proxy_queue.clear()
    sender._proxy_worker_running = False
    sender._proxy_worker_task = None
    return jsonify({'ok': True, 'message': '代理队列已清空'})

@app.route('/api/forward-mode/toggle-at-yuanbao', methods=['POST'])
def api_forward_mode_toggle_at_yuanbao():
    data = request.get_json(force=True)
    enabled = data.get('enabled', True)
    sender.forward_at_yuanbao = enabled
    settings['forward_at_yuanbao'] = enabled
    global _proxy_config_version
    _proxy_config_version += 1
    save_config()
    return jsonify({'ok': True, 'message': f'@元宝已{"开启" if enabled else "关闭"}', 'forward_at_yuanbao': enabled})


# ── 自动回复 API ──
@app.route('/api/auto-reply/enable', methods=['POST'])
def api_enable_auto_reply():
    sender.auto_reply_enabled = True
    return jsonify({'ok': True, 'message': '自动回复已启用'})

@app.route('/api/auto-reply/disable', methods=['POST'])
def api_disable_auto_reply():
    sender.auto_reply_enabled = False
    return jsonify({'ok': True, 'message': '自动回复已禁用'})

@app.route('/api/auto-reply/status', methods=['GET'])
def api_get_auto_reply_status():
    return jsonify({'enabled': sender.auto_reply_enabled, 'config': {
        'auto_reply_rules': sender.AUTO_REPLY_RULES, 'default_reply': sender.DEFAULT_REPLY,
        'auto_reply_group_text': sender.AUTO_REPLY_GROUP_TEXT, 'auto_reply_c2c_text': sender.AUTO_REPLY_C2C_TEXT
    }})

@app.route('/api/auto-reply/test', methods=['POST'])
def api_test_auto_reply():
    data = request.get_json(force=True)
    text = data.get('text', '').strip()
    if not text:
        return jsonify({'ok': False, 'message': '请输入测试文本'}), 400
    reply = sender.get_auto_reply(text, data.get('is_group', True), data.get('group_code', ''))
    return jsonify({'ok': True, 'reply': reply,
                    'matched': reply not in [sender.AUTO_REPLY_GROUP_TEXT, sender.AUTO_REPLY_C2C_TEXT, sender.DEFAULT_REPLY]})

@app.route('/api/auto-reply/rules', methods=['GET'])
def api_get_auto_reply_rules():
    return jsonify({'ok': True, 'rules': sender.AUTO_REPLY_RULES, 'count': len(sender.AUTO_REPLY_RULES)})

@app.route('/api/auto-reply/rules', methods=['POST'])
def api_add_auto_reply_rule():
    data = request.get_json(force=True)
    for f in ('match_type', 'pattern', 'reply_text'):
        if f not in data or not data[f]:
            return jsonify({'ok': False, 'message': f'缺少字段: {f}'}), 400
    rule = {'match_type': data['match_type'], 'pattern': data['pattern'],
            'reply_text': data['reply_text'], 'group_only': data.get('group_only', False),
            'enabled': data.get('enabled', True), 'priority': data.get('priority', len(sender.AUTO_REPLY_RULES) + 1)}
    if 'patterns' in data:
        rule['patterns'] = data['patterns']
    sender.AUTO_REPLY_RULES.append(rule)
    save_config()
    return jsonify({'ok': True, 'message': '规则添加成功', 'rule': rule})

@app.route('/api/auto-reply/rules/<int:index>', methods=['PUT'])
def api_update_auto_reply_rule(index):
    if index < 0 or index >= len(sender.AUTO_REPLY_RULES):
        return jsonify({'ok': False, 'message': '索引无效'}), 404
    rule = sender.AUTO_REPLY_RULES[index]
    for k, v in request.get_json(force=True).items():
        if k in ('match_type', 'pattern', 'reply_text', 'patterns'):
            rule[k] = v
        elif k in ('group_only', 'enabled'):
            rule[k] = bool(v)
        elif k == 'priority':
            rule[k] = int(v)
    save_config()
    return jsonify({'ok': True, 'message': '规则更新成功', 'rule': rule})

@app.route('/api/auto-reply/rules/<int:index>', methods=['DELETE'])
def api_delete_auto_reply_rule(index):
    if index < 0 or index >= len(sender.AUTO_REPLY_RULES):
        return jsonify({'ok': False, 'message': '索引无效'}), 404
    deleted = sender.AUTO_REPLY_RULES.pop(index)
    save_config()
    return jsonify({'ok': True, 'message': '规则删除成功', 'rule': deleted})

@app.route('/api/auto-reply/rules/reorder', methods=['POST'])
def api_reorder_auto_reply_rules():
    data = request.get_json(force=True)
    new_order = data.get('order', [])
    if len(new_order) != len(sender.AUTO_REPLY_RULES):
        return jsonify({'ok': False, 'message': '排序数组长度不匹配'}), 400
    try:
        sender.AUTO_REPLY_RULES = [sender.AUTO_REPLY_RULES[i] for i in new_order]
        save_config()
        return jsonify({'ok': True, 'message': '规则顺序已更新'})
    except IndexError:
        return jsonify({'ok': False, 'message': '无效索引'}), 400


# ── 群聊管理 ──
@app.route('/api/groups', methods=['GET'])
def api_get_groups():
    return jsonify({'ok': True, 'groups': list(sender.groups.values()),
                    'count': len(sender.groups), 'current_group': sender.group_code})

@app.route('/api/groups/switch', methods=['POST'])
def api_switch_group():
    gc = request.get_json(force=True).get('group_code', '').strip()
    if not gc:
        return jsonify({'ok': False, 'message': '请输入群号'}), 400
    sender.group_code = settings['group_code'] = gc
    return jsonify({'ok': True, 'message': f'已切换到群聊 {gc}'})


# ── 心跳管理 ──
@app.route('/api/heartbeat/interval', methods=['POST'])
def api_set_heartbeat_interval():
    try:
        iv = float(request.get_json(force=True).get('interval', 1.0))
        if not 0 <= iv <= 60:
            return jsonify({'ok': False, 'message': '间隔必须在 0-60 秒之间'}), 400
        sender.heartbeat_interval = settings['heartbeat_interval'] = iv
        return jsonify({'ok': True, 'message': f'心跳间隔已设为 {iv} 秒'})
    except (ValueError, TypeError):
        return jsonify({'ok': False, 'message': '无效的间隔值'}), 400

@app.route('/api/heartbeat', methods=['GET'])
def api_heartbeat():
    return jsonify({'ok': True, 'timestamp': datetime.now().isoformat(), 'connected': sender.connected,
                    'heartbeat_interval': sender.heartbeat_interval,
                    'heartbeat_running': sender.heartbeat_task is not None and not sender.heartbeat_task.done(),
                    'receive_loop_running': sender.receive_task is not None and not sender.receive_task.done()})


# ── 成员管理 ──
@app.route('/api/members', methods=['GET'])
def api_members():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接', 'members': []}), 400
    try:
        result = async_call(sender.get_group_members())
        if result and result.get('code') == 0:
            for m in result.get('member_list', []):
                uid = m.get('user_id', '')
                if uid:
                    sender.user_db[uid] = m.get('nick_name', '')
            return jsonify({'ok': True, 'members': result['member_list'], 'count': len(result['member_list'])})
        return jsonify({'ok': False, 'message': result.get('message', '获取失败'), 'members': []}), 400
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e), 'members': []}), 500

@app.route('/api/users', methods=['GET'])
def api_users():
        return jsonify({'users': [{'user_id': k, 'nickname': v} for k, v in sender.user_db.items()],
                        'count': len(sender.user_db)})

@app.route('/api/users', methods=['POST'])
def api_add_user():
    data = request.get_json(force=True)
    uid = data.get('user_id', '').strip()
    if not uid:
        return jsonify({'ok': False, 'message': '用户ID不能为空'}), 400
    sender.user_db[uid] = data.get('nickname', '') or uid
    return jsonify({'ok': True, 'message': '用户添加成功'})

@app.route('/api/users/<user_id>', methods=['DELETE'])
def api_delete_user(user_id):
    if user_id in sender.user_db:
        del sender.user_db[user_id]
        return jsonify({'ok': True, 'message': '用户删除成功'})
    return jsonify({'ok': False, 'message': '用户不存在'}), 404


# ── 贴纸列表 ──
@app.route('/api/stickers', methods=['GET'])
def api_stickers():
    q = request.args.get('q', '').lower()
    stickers = [{'key': k, 'name': v.get('name', k), 'sticker_id': v.get('sticker_id', ''), 'package_id': v.get('package_id', '')}
                for k, v in sender.STICKERS.items()
                if q in k.lower() or q in v.get('name', '').lower()]
    return jsonify({'stickers': stickers, 'count': len(stickers)})


# ── 设置 ──
@app.route('/api/settings', methods=['GET'])
def api_get_settings():
    return jsonify(settings)

@app.route('/api/settings', methods=['POST'])
def api_update_settings():
    global _proxy_config_version
    data = request.get_json(force=True)
    _proxy_dirty = False

    if 'group_code' in data:
        settings['group_code'] = str(data['group_code'])
        sender.group_code = settings['group_code']
    if 'interval' in data:
        try: settings['interval'] = float(data['interval'])
        except (ValueError, TypeError): pass
    if 'heartbeat_interval' in data:
        try:
            settings['heartbeat_interval'] = sender.heartbeat_interval = float(data['heartbeat_interval'])
        except (ValueError, TypeError): pass
    if 'auto_reply_enabled' in data:
        settings['auto_reply_enabled'] = sender.auto_reply_enabled = bool(data['auto_reply_enabled'])

    if 'group_reply_text' in data:
        settings['group_reply_text'] = sender.AUTO_REPLY_GROUP_TEXT = str(data['group_reply_text'])
    if 'c2c_reply_text' in data:
        settings['c2c_reply_text'] = sender.AUTO_REPLY_C2C_TEXT = str(data['c2c_reply_text'])

    if 'forward_mode_enabled' in data:
        enabled = bool(data['forward_mode_enabled'])
        settings['forward_mode_enabled'] = enabled
        if enabled:
            sender.auto_reply_text = "yb"
            sender.auto_reply_at_only = data.get('forward_at_only', False)
        else:
            sender.auto_reply_text = None
            sender.auto_reply_at_only = False
            sender._proxy_queue.clear()
        _proxy_dirty = True
    for key in ('forward_at_only', 'forward_at_yuanbao'):
        if key in data:
            val = bool(data[key])
            settings[key] = val
            setattr(sender, key, val)
            _proxy_dirty = True
    if _proxy_dirty:
        _proxy_config_version += 1
    if 'auto_reply_rules' in data:
        sender.AUTO_REPLY_RULES = data['auto_reply_rules']
    if 'default_reply' in data:
        sender.DEFAULT_REPLY = data['default_reply']
    save_config()
    return jsonify({'ok': True, 'settings': settings})


# ── 代理队列详情 ──
@app.route('/api/forward-mode/queue', methods=['GET'])
def api_forward_queue_detail():
    items = [{'index': i, 'sender_name': item.get("ref_sender_name",""),
              'content': item.get("original_content","")[:60],
              'media_type': (item.get("media_info") or {}).get("type",""),
              'target_group': item.get("target_group",""),
              'done': item["future"].done() if item.get("future") else False}
             for i, item in enumerate(sender._proxy_queue)]
    return jsonify({'ok': True, 'enabled': sender.auto_reply_text == "yb",
                    'at_only': sender.auto_reply_at_only,
                    'forward_at_yuanbao': sender.forward_at_yuanbao,
                    'image_group_code': IMAGE_GROUP_CODE, 'yuanbao_id': YUANBAO_BOT_ID,
                    'queue_length': len(sender._proxy_queue),
                    'worker_running': sender._proxy_worker_task is not None and not sender._proxy_worker_task.done(),
                    'items': items})


# ── SSE 事件流 ──
@app.route('/api/events')
def api_events():
    def generate():
        last_len = len(sender.msg_cache)
        last_queue_sig = ""
        while True:
            cur_len = len(sender.msg_cache)
            if cur_len > last_len:
                for msg in sender.msg_cache[last_len:cur_len]:
                    yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"
                last_len = cur_len
            elif cur_len < last_len:
                last_len = cur_len

            queue_items = [{'sender': it.get("ref_sender_name",""),
                            'content': (it.get("original_content","") or "")[:60],
                            'media_type': (it.get("media_info") or {}).get("type",""),
                            'done': it["future"].done() if it.get("future") else False,
                            'forwarded': it.get("forwarded", False),
                            'reply_received': it.get("reply_received", False)}
                           for it in sender._proxy_queue]
            qlen = len(sender._proxy_queue)
            worker = getattr(sender, '_proxy_worker_running', False)
            sig = json.dumps({'qlen': qlen, 'worker': worker, 'ver': _proxy_config_version,
                               'at_yb': sender.forward_at_yuanbao,
                               'items': queue_items}, ensure_ascii=False)
            if sig != last_queue_sig:
                last_queue_sig = sig
                current = None
                for it in queue_items:
                    if it.get("forwarded") and not it.get("reply_received"):
                        current = it
                        break
                evt = json.dumps({'type': 'proxy_status', 'enabled': sender.auto_reply_text == "yb",
                                  'at_only': sender.auto_reply_at_only,
                                  'forward_at_yuanbao': sender.forward_at_yuanbao,
                                  'image_group_code': IMAGE_GROUP_CODE, 'yuanbao_id': YUANBAO_BOT_ID,
                                  'queue_length': qlen, 'worker_running': worker, 'current': current,
                                  'items': queue_items}, ensure_ascii=False)
                yield f"data: {evt}\n\n"

            yield ": heartbeat\n\n"
            time.sleep(0.5)

    return Response(stream_with_context(generate()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no'})


# ── 启动入口 ──
if __name__ == '__main__':
    import socket

    template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    os.makedirs(template_dir, exist_ok=True)

    def get_local_ip():
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return '127.0.0.1'

    local_ip = get_local_ip()
    print("=" * 60)
    print("  元宝 Bot Web 控制台 - v3.0 (代理双模式 + 高速默认)")
    print("=" * 60)
    print(f"  本地:  http://127.0.0.1:5000")
    print(f"  网络:  http://{local_ip}:5000")
    print("=" * 60)
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
