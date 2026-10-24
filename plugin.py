"""麦麦 × DeepSeek Harness (dsh) 桥插件（带任务守卫 + 精简转发回传）。

作用：把 DeepSeek Harness (dsh) 接入麦麦。
  - 命令 `/dsh [--timeout 秒] <任务>`：把任务外包给 dsh，结果回传当前聊天流。
  - 工具 `dsh_run`：让麦麦（Planner）在聊天中自主把重活交给 dsh（自主下单）。

执行模型（异步 + 守卫）：
  - 异步：命令立即回执，dsh 在后台跑，完成后主动把结果推回群里（绕开宿主 60s RPC 超时）。
  - 单并发：同一时刻只允许一个 dsh 任务。
  - 不设时间预算（默认）：模型估不准自己的耗时，硬限时只会砍掉快做完的活；
    改由用户手动控制——`/dsh stop` 随时停、`/dsh peek` 看进展。可选 `--timeout N` 单独设上限。
  - 进展判据（三信号）：输出 / 进程 CPU 活跃 / 子进程存在，任一有变化即续命。
  - 重复检测：输出原地复读超阈值 → 判定卡循环，终止。
  - 死锁兜底：长时间无任何输出且进程静默才判卡死（默认 10 分钟，可配）。
  - 分层终止：先温和 terminate，宽限期后 taskkill /T /F 杀整棵进程树。
  - 异常/手动停止时：把本轮新产出的文件救回发给用户，不让活白干。

回传形态（所有内容收进转发，外部不刷屏）：
  - dsh 以 --json 输出结构化事件，桥逐事件解析。
  - 外部：只发一句极简状态（完成 / 超时原因）。
  - 转发：一条合并转发卡片，节点精简——
      * 工具调用 = 一行「工具名 + 关键参数」（去掉 JSON 壳）
      * 连续文本输出合并为一段
      * 工具报错才保留一条
      * 最终答案一个节点
  - 这样群里只见「一句状态 + 一张卡片」，永不刷屏。
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import random
import re
import shlex
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Literal, Optional

from maibot_sdk import Command, Field, HookHandler, MaiBotPlugin, PluginConfigBase, Tool
from maibot_sdk.types import ErrorPolicy, HookMode, HookOrder, ToolParameterInfo, ToolParamType

try:  # 进程脉搏（可选，缺失则退化为“仅输出/子进程”判据）
    import psutil  # type: ignore
except Exception:  # noqa: BLE001
    psutil = None  # type: ignore

_CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)
_IS_WINDOWS = os.name == "nt"
_POLL_SECONDS = 2.0

# ── dsh 提问通道 / QQ 文件入站（2026-10-24） ─────────────────────────
# dsh 需要用户拍板时，在工作区写 ask-requests/<名>.json，桥把问题发到 QQ，
# 用户下一条消息就是答案，桥用 --session-id 续跑同一个 dsh 会话。
# 用户从 QQ 发来的文件（以及图片）落到工作区 uploads/，下个任务可直读。
ASK_DIR_NAME = "ask-requests"
UPLOAD_DIR_NAME = "uploads"
UPLOAD_INDEX_NAME = "index.json"
WORKSPACE_AGENTS_MARKER = "<!-- dsh-bridge workspace guide v2 -->"

# 适配器把 QQ 的 file 段转成了文本：[文件] 名字，大小: 123，链接: https://...
# 文件名里本身可能带「，」，所以只在这些精确标记处切分。
_FILE_SEG_RE = re.compile(r"\[文件\]\s*(?P<body>[^\n]{0,500})")
_FILE_LINE_RE = re.compile(
    r"\[文件\]\s*[^，\n]*(?:，大小:\s*\d+)?(?:，链接:\s*https?://\S+)?"
)
_FILE_SIZE_RE = re.compile(r"大小:\s*(\d+)")
_FILE_URL_RE = re.compile(r"链接:\s*(https?://\S+)")
_FILE_NAME_SPLIT_RE = re.compile(r"，(?:大小:|链接:)")

_LOG_NAME_MAX = 120

# 写进每个聊天流工作区的 AGENTS.md（dsh 会自动读取工作区的 AGENTS.md）
_WORKSPACE_AGENTS_MD = """<!-- dsh-bridge workspace guide v4 -->
# 你在「麦麦 × dsh 桥」里工作

这个工作区由 MaiBot 的 maibot-dsh-bridge 插件给 QQ 用户建的任务目录：你替用户在服务器上干活，最终回答会被转给用户。

## 需要用户拍板时：先调 ask 工具（重要）

需要用户决定（选方案、确认要不要删/改、缺关键信息）时，**不要自己猜**：

1. 调用工具 `mcp__ask__ask_user`（参数：`question` 必填，`options` 可选，`note` 可选）。
   它会阻塞，直到用户在 QQ 里回答，然后把答案作为工具结果返回给你；你接着做即可。
2. 如果它返回超时，按提示处理（说明卡点、先做能独立完成的部分）。
3. 如果该工具不可用（不存在 / 报错），再退回到：在工作区写 `ask-requests/<短名>.json`
   （`{"question": "...", "options": ["..."], "note": "..."}`），然后结束这一轮，桥会用同一会话续跑你。

## 交付文件：用 present

要交给用户的文件（文档、压缩包、图片等）用 `present` 声明：
`present({"files": [{"path": "报告.docx", "description": "说明"}]})`。
桥会把这些文件发回 QQ；交付物必须在你当前工作区里（或 session 可访问的普通文件）。

## 想稍后再做：写 schedule-requests

在工作区建 `schedule-requests/<短名>.json`：
`{"title": "任务名", "prompt": "到点要执行的任务", "after_seconds": 3600}`
时间规则四选一：`after_seconds` / `every_seconds` / `at`（ISO 时刻）/ `cron`（5 字段）。

## 用户从 QQ 发来的文件

用户发的文件/图片会保存在本工作区的 `uploads/`，绝对路径会写在当次任务提示的【工作区附件】里，可以直接读。

## Office 文档（Word/PPT/Excel）

用这个 Python（已装 python-docx / python-pptx / openpyxl）：
`/opt/dsh-office-venv/bin/python`
不要自己 pip 安装，也不要用别处找的 python。

## 交付节奏（别陷进无限打磨）

任务没有硬性时限，用户可以随时发 `/dsh stop` 停你。所以：

- **一旦有可用结果，先 `present` 交付**；想继续改进可以，但先交一版再迭代（present 可以多次）。
- 不要为排版、措辞、细节反复重写同一个文件超过 **2 轮**；交一版能用的，并在回答里说明还能怎么改。
- 长时间只打磨不交付，用户就只能干等——先交付永远比"再改改"更受欢迎。
"""



class PluginSectionConfig(PluginConfigBase):
    __ui_label__ = "插件"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(default=True, description="是否启用插件")
    config_version: str = Field(default="0.6.1", description="配置版本")


class DshConfig(PluginConfigBase):
    __ui_label__ = "dsh"
    __ui_icon__ = "terminal"
    __ui_order__ = 1

    command: str = Field(default="dsh", description="dsh 可执行命令（如 dsh，或 C:\\nodejs\\dsh.cmd）")
    extra_args: str = Field(default="--profile headless", description="固定附加参数；桥会自动追加 --json")
    workspace_dir: str = Field(default="", description="dsh 工作目录；留空则用 data/dsh-bridge/workspace")
    timeout_default: int = Field(
        default=0,
        description="默认时间预算（秒）；0=不限时（默认）。模型估不准自己的耗时，所以默认靠用户 /dsh stop 控制",
        ge=0,
        le=86400,
    )
    timeout_min: int = Field(default=0, description="时间预算下限（秒）；0=不设下限", ge=0, le=86400)
    timeout_max: int = Field(default=0, description="时间预算硬上限（秒）；0=无上限（留作可选保险）", ge=0, le=86400)
    heartbeat_seconds: int = Field(
        default=600,
        description="死锁兜底：没有任何输出且进程静默超过此秒数才判卡死（这是防死锁，不是时间预算）",
        ge=60,
        le=7200,
    )
    repeat_threshold: int = Field(default=25, description="重复检测：同一行连续出现达到此次数即判循环", ge=5, le=500)
    grace_seconds: int = Field(default=15, description="温和终止后的宽限等待（秒），超时才强杀进程树", ge=0, le=120)
    max_output_chars: int = Field(default=8000, description="文本回退时最大字符数", ge=100, le=50000)
    max_forward_nodes: int = Field(default=40, description="合并转发最多节点数（超出则折叠中段）", ge=3, le=200)
    node_max_chars: int = Field(default=1500, description="单个转发节点最大字符数", ge=100, le=8000)
    display_mode: str = Field(default="forward", description="回传形态：forward=合并转发 / text=纯文本")
    deliver_max_files: int = Field(default=8, description="单次任务最多打包发送的交付物文件数", ge=1, le=50)
    salvage_on_fail: bool = Field(
        default=True,
        description="任务超时/异常终止时，把本轮在工作区新产出的文件也发出来（救回半成品）",
    )
    salvage_max_mb: int = Field(default=20, description="救回文件的单文件大小上限（MB）", ge=1, le=200)
    api_key_env: str = Field(default="DEEPSEEK_API_KEY", description="存放 API Key 的环境变量名")
    api_key: str = Field(default="", description="DeepSeek API Key（明文，留空则用系统环境变量；勿分享本文件）")
    permission_mode: Literal["workspace-write", "read-only", "danger-full-access"] = Field(
        default="danger-full-access",
        description=(
            "dsh 沙箱权限模式。默认 danger-full-access：git clone / npm 等需要子进程管道的命令在 "
            "workspace-write 下会被 Windows ACL 沙箱拒绝。要更严可改为 workspace-write 或 read-only，"
            "但需接受相应命令失败。"
        ),
    )
    silent_inject_chars: int = Field(
        default=6000,
        description="静默模式下回灌给麦麦的结果上限（字符）。墨是唯一出口，默认给足",
        ge=200,
        le=20000,
    )
    context_inject_chars: int = Field(
        default=600,
        description="回灌进麦麦上下文的结果摘要最大字符数（越小越省上下文，超出部分截断）",
        ge=100,
        le=5000,
    )
    allow_self_schedule: bool = Field(
        default=True,
        description="是否允许 dsh 经 schedule-requests 自建定时任务（默认开；关则禁止，风险自负）",
    )
    # ── dsh 提问通道 ──
    ask_enabled: bool = Field(
        default=True,
        description="允许 dsh 用 ask-requests 向你提问：桥把问题发到 QQ，你回一句就是答案，然后自动接着跑",
    )
    ask_timeout: int = Field(
        default=900,
        description="等待回答的秒数；超时就放弃这轮（默认 15 分钟）",
        ge=60,
        le=7200,
    )
    ask_max_rounds: int = Field(
        default=5, description="单个任务里最多连续向用户提问几轮（防死循环）", ge=1, le=20
    )
    # ── QQ 文件入站 ──
    uploads_enabled: bool = Field(
        default=True, description="接收 QQ 发来的文件/图片，存进 dsh 工作区的 uploads/ 供任务直接读取"
    )
    upload_max_mb: int = Field(default=50, description="单个附件最大体积（MB）", ge=1, le=500)
    upload_ttl_minutes: int = Field(
        default=180, description="附件出现在任务提示里的时长（分钟），越久越容易误带旧文件", ge=1, le=10080
    )
    upload_ack: bool = Field(
        default=False,
        description="收到附件时是否回一句确认（默认关：那条文件消息本来也会交给墨，避免双份回复）",
    )
    upload_wait_instruct: bool = Field(
        default=True,
        description=(
            "收到文件后先拦住墨的回复，等用户说要干什么再开工。"
            "开着能避免「墨自己去看文件」和「dsh 也在看文件」打架；纯文件消息才会被拦，文件+文字说明不拦。"
        ),
    )


class PermissionConfig(PluginConfigBase):
    __ui_label__ = "权限"
    __ui_icon__ = "shield"
    __ui_order__ = 2

    enabled: bool = Field(default=True, description="是否启用权限名单")
    mode: Literal["whitelist", "blacklist"] = Field(
        default="whitelist",
        description="名单模式：whitelist=仅名单内可用；blacklist=名单内不可用、其余放行",
    )
    allowed_users: str = Field(default="", description="白名单用户 ID，逗号分隔；白名单模式下留空且启用则拒绝所有人（fail-closed）")
    allowed_groups: str = Field(
        default="",
        description="白名单群 ID，逗号分隔；群聊里取不到发言人 ID 时作为兜底",
    )
    blocked_users: str = Field(default="", description="黑名单用户 ID，逗号分隔；黑名单模式下命中即拒绝")
    blocked_groups: str = Field(default="", description="黑名单群 ID，逗号分隔；黑名单模式下命中即拒绝")
    enforce_tools: bool = Field(
        default=True,
        description="是否对 dsh_run / schedule_* 工具也施加名单（默认开=安全；关=工具面放开，风险自负）",
    )


class TopicConfig(PluginConfigBase):
    __ui_label__ = "话题"
    __ui_icon__ = "hash"
    __ui_order__ = 3

    enabled: bool = Field(
        default=True,
        description="话题模式：每个话题 = 一条持久 dsh 会话 + 独立工作目录。关掉则退回「一个聊天流一个目录」的旧行为",
    )
    private_only: bool = Field(default=True, description="只在私聊启用话题模式（群聊保持旧行为）")
    idle_hours: int = Field(
        default=24, description="当前话题闲置超过这么多小时，下次派活自动新开一个话题", ge=1, le=720
    )
    name_max_chars: int = Field(default=12, description="话题名长度上限（dsh 起名超出会被截断）", ge=4, le=40)
    queue_max: int = Field(default=3, description="排队上限：全局串行，超出直接拒绝", ge=0, le=20)
    archive_keep_days: int = Field(
        default=7, description="归档话题保留天数，到期自动删目录并释放编号", ge=1, le=365
    )
    archive_readonly: bool = Field(default=True, description="归档话题只读：必须先 /恢复 才能继续派活")


class ExperimentalConfig(PluginConfigBase):
    __ui_label__ = "实验功能"
    __ui_icon__ = "flask"
    __ui_order__ = 4

    agent_tools: bool = Field(
        default=False,
        description=(
            "【实验性】允许墨自主调用话题/派活工具（不用你打 /dsh 命令）。打开后还要在 QQ 里 /dsh 实验开启 + 二次确认，"
            "最长只能开启 24 小时，到期自动关闭；期间 /dsh 命令会被临时禁用"
        ),
    )
    agent_tools_hours: int = Field(
        default=24, description="实验功能单次最长开启小时数（硬上限 24，填更大也只按 24 算）", ge=1, le=24
    )
    agent_confirm_ttl_minutes: int = Field(
        default=10, description="二次确认码有效期（分钟），过期要重新 /dsh 实验开启", ge=1, le=120
    )


class DshBridgeConfig(PluginConfigBase):
    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    dsh: DshConfig = Field(default_factory=DshConfig)
    permission: PermissionConfig = Field(default_factory=PermissionConfig)
    topic: TopicConfig = Field(default_factory=TopicConfig)
    experimental: ExperimentalConfig = Field(default_factory=ExperimentalConfig)


class _RunState:
    """一次任务运行的共享状态（读线程与监控循环共用）。"""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.events: list[dict[str, Any]] = []
        self.final_text = ""
        self.last_progress = time.time()
        self.repeat_line: Optional[str] = None
        self.repeat_count = 0
        self.repeat_triggered = False
        self.session_id: str = ""

    def push_raw(self, raw_line: str, repeat_threshold: int) -> None:
        text = raw_line.rstrip("\r\n")
        if not text.strip():
            return
        stripped = text.strip()
        evt: Optional[dict[str, Any]] = None
        if stripped.startswith("{"):
            try:
                obj = json.loads(stripped)
                if isinstance(obj, dict):
                    evt = obj
            except Exception:  # noqa: BLE001
                evt = None
        with self.lock:
            self.last_progress = time.time()
            if evt is not None:
                evt["_ts"] = time.time()
                self.events.append(evt)
                if str(evt.get("type") or "") == "final":
                    self.final_text = str(evt.get("text") or "")
                probe = str(evt.get("text") or evt.get("tool") or stripped)
            else:
                self.events.append({"type": "raw", "text": text, "_ts": time.time()})
                probe = text
            probe = probe.strip()
            if probe and probe == self.repeat_line:
                self.repeat_count += 1
            else:
                self.repeat_line = probe
                self.repeat_count = 1
            if self.repeat_count >= repeat_threshold:
                self.repeat_triggered = True

    def touch(self) -> None:
        with self.lock:
            self.last_progress = time.time()

    def snapshot(self) -> tuple[float, bool]:
        with self.lock:
            return self.last_progress, self.repeat_triggered

    def events_copy(self) -> list[dict[str, Any]]:
        with self.lock:
            return list(self.events)

    def final_copy(self) -> str:
        with self.lock:
            return self.final_text


class DshBridgePlugin(MaiBotPlugin):
    config_model = DshBridgeConfig

    # 规划轮只是个小前置动作，给它一个内部上限，避免极小概率卡住整条链（与用户预算无关）
    _PLAN_SECONDS = 180

    async def on_load(self) -> None:
        self._running = False
        self._current_pid: Optional[int] = None
        self._cpu_prev: dict[int, float] = {}
        self._tasks: dict[str, dict[str, Any]] = {}
        self._history: list[dict[str, Any]] = []
        self._current_task: str = ""
        self._pending: dict[str, dict[str, Any]] = {}  # stream_id -> 等回答的问题
        self._topics: dict[int, dict[str, Any]] = {}  # 话题 id -> 话题记录
        self._topic_current: dict[str, int] = {}  # 聊天流 -> 当前话题 id
        self._queue: list[dict[str, Any]] = []  # 排队中的任务
        self._last_purge = 0.0
        self._ipc_seen: set[str] = set()  # 已处理过的 MCP 提问 id
        self._ipc_waiting = False  # 正在等 MCP 提问的回答（期间暂停卡死判定）
        self._exp: dict[str, Any] = {}  # 实验功能状态（armed / 待确认码 / 到期时间）
        self._last_chat_id = ""
        self._exp_master_prev = bool(self.config.experimental.agent_tools)
        self._stop_requested = False  # 用户发了 /dsh stop
        self._run_state: Optional[_RunState] = None  # 正在跑的任务状态（peek 用）
        self._run_started = 0.0
        self._run_label = ""
        self._run_ws = ""
        self._run_task_text = ""
        self._run_budget = 0
        self._last_run: dict[str, Any] = {}
        self._load_exp()
        self._scheduler_running = True
        self._load_tasks()
        self._load_pending()
        self._load_topics()
        asyncio.create_task(self._scheduler_loop())
        self.ctx.logger.info(
            "麦麦×dsh 桥插件已加载 (command=%s, budget=%s, display=%s)",
            self.config.dsh.command,
            "不限时" if int(self.config.dsh.timeout_default) <= 0 else f"{self.config.dsh.timeout_default}s",
            self.config.dsh.display_mode,
        )

    async def on_unload(self) -> None:
        self._scheduler_running = False
        pid = self._current_pid
        if pid:
            await asyncio.to_thread(self._kill_tree, pid)
        self.ctx.logger.info("麦麦×dsh 桥插件已卸载")

    # ── 定时任务（桥内置调度） ────────────────────────────────

    def _tasks_file(self) -> Path:
        return Path(self.ctx.paths.data_dir) / "scheduled_tasks.json"

    def _load_tasks(self) -> None:
        try:
            fp = self._tasks_file()
            if fp.is_file():
                data = json.loads(fp.read_text(encoding="utf-8"))
                for t in data if isinstance(data, list) else []:
                    if isinstance(t, dict) and t.get("id"):
                        self._tasks[str(t["id"])] = t
            self.ctx.logger.info("已载入定时任务 %d 个", len(self._tasks))
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("读取定时任务失败", exc_info=True)

    def _save_tasks(self) -> None:
        try:
            fp = self._tasks_file()
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(json.dumps(list(self._tasks.values()), ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("保存定时任务失败", exc_info=True)

    # ── 话题（topic）：一条持久 dsh 会话 + 独立工作目录 + 编号 ─────

    def _topics_file(self) -> Path:
        return Path(self.ctx.paths.data_dir) / "topics.json"

    def _workspace_root(self) -> Path:
        root = self.config.dsh.workspace_dir.strip()
        base = Path(root) if root else (Path(self.ctx.paths.data_dir) / "workspace")
        base.mkdir(parents=True, exist_ok=True)
        return base

    def _load_topics(self) -> None:
        try:
            fp = self._topics_file()
            if fp.is_file():
                data = json.loads(fp.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    for item in data.get("topics") or []:
                        if isinstance(item, dict) and item.get("id") is not None:
                            self._topics[int(item["id"])] = item
                    for chat, tid in (data.get("current") or {}).items():
                        try:
                            self._topic_current[str(chat)] = int(tid)
                        except Exception:  # noqa: BLE001
                            continue
            if self._topics:
                self.ctx.logger.info(
                    "已载入话题 %d 个，当前：%s",
                    len(self._topics),
                    self._topic_current or "-",
                )
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("读取话题失败", exc_info=True)

    def _save_topics(self) -> None:
        try:
            fp = self._topics_file()
            fp.parent.mkdir(parents=True, exist_ok=True)
            payload = {"topics": list(self._topics.values()), "current": self._topic_current}
            tmp = fp.with_suffix(".json.part")
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(tmp, fp)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("保存话题失败", exc_info=True)

    def _alloc_topic_id(self) -> int:
        """半唯一编号：取当前未被占用的最小正整数（归档未删除的仍占号）。"""
        used = set(self._topics.keys())
        idx = 1
        while idx in used:
            idx += 1
        return idx

    def _topic_dir(self, topic: dict[str, Any]) -> Path:
        d = self._workspace_root() / str(topic.get("dir") or f"topic-{topic['id']}")
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _topic_label(self, topic: Optional[dict[str, Any]]) -> str:
        if not topic:
            return ""
        name = str(topic.get("name") or "新话题")
        return f"#{topic['id']}（{name}）"

    def _topics_of_chat(self, chat_id: str) -> list[dict[str, Any]]:
        return [t for t in self._topics.values() if chat_id in (t.get("chats") or [])]

    def _current_topic(self, chat_id: str) -> Optional[dict[str, Any]]:
        tid = self._topic_current.get(chat_id)
        return self._topics.get(int(tid)) if tid else None

    def _topic_mode_for(self, chat_id: str, group_id: str = "") -> bool:
        conf = self.config.topic
        if not conf.enabled or not chat_id:
            return False
        if conf.private_only and str(group_id or "").strip():
            return False
        return True

    def _find_topic(self, chat_id: str, spec: str) -> Optional[dict[str, Any]]:
        """按 #N / N / 第N个 / 名字 查找本聊天的话题。"""
        raw = str(spec or "").strip().replace("（", "(").replace("）", ")")
        m = re.search(r"#?\s*(\d+)", raw)
        if m:
            topic = self._topics.get(int(m.group(1)))
            if topic and chat_id in (topic.get("chats") or []):
                return topic
            return None
        name = raw.strip("# ()　")
        if not name:
            return None
        for t in self._topics_of_chat(chat_id):
            if str(t.get("name") or "") == name:
                return t
        return None

    def _create_topic(self, chat_id: str, *, name: str = "新话题", dir_name: str = "") -> dict[str, Any]:
        tid = self._alloc_topic_id()
        now = time.time()
        topic: dict[str, Any] = {
            "id": tid,
            "name": str(name or "新话题")[: max(4, int(self.config.topic.name_max_chars))],
            "dir": str(dir_name or f"topic-{tid}"),
            "session_id": "",
            "created_at": now,
            "last_used_at": now,
            "archived_at": None,
            "chats": [chat_id],
        }
        self._topics[tid] = topic
        self._topic_current[chat_id] = tid
        self._save_topics()
        self.ctx.logger.info("新建话题 %s（目录 %s）", self._topic_label(topic), topic["dir"])
        return topic

    def _get_or_create_topic(self, chat_id: str) -> dict[str, Any]:
        """取当前话题；没有或闲置超时就新建。

        兼容迁移：如果工作区根目录下还存在旧的「聊天流 id」目录（旧版按聊天流建目录），
        就把它认领成一个「遗留」话题，旧的 uploads / AGENTS.md / 产出都还能用。
        """
        conf = self.config.topic
        topic = self._current_topic(chat_id)
        if topic and not topic.get("archived_at"):
            idle = time.time() - float(topic.get("last_used_at") or 0)
            if idle <= int(conf.idle_hours) * 3600:
                return topic
            self.ctx.logger.info(
                "话题 %s 闲置 %.1f 小时，自动新开", self._topic_label(topic), idle / 3600
            )
        legacy = self._workspace_root() / chat_id
        # 只有「这个目录还没被任何话题占用」时才认领，避免两个话题共用一个目录
        legacy_taken = any(str(t.get("dir") or "") == chat_id for t in self._topics.values())
        if not legacy_taken and legacy.is_dir() and any(legacy.iterdir()):
            return self._create_topic(chat_id, name=f"遗留·{chat_id[:6]}", dir_name=chat_id)
        return self._create_topic(chat_id)

    def _chat_ws(self, chat_id: str, group_id: str = "") -> str:
        """聊天对应的 dsh 工作目录：话题模式用当前话题目录，否则退回旧版按聊天流的目录。"""
        if self._topic_mode_for(chat_id, group_id):
            topic = self._current_topic(chat_id)
            if topic and not topic.get("archived_at"):
                return str(self._topic_dir(topic))
        return self._workspace(chat_id)

    def _resolve_topic(
        self, chat_id: str, group_id: str = "", topic_id: Optional[int] = None
    ) -> Optional[dict[str, Any]]:
        """任务落在哪个话题：指定 id 优先；否则当前话题/自动新建。非话题模式返回 None。"""
        if not self._topic_mode_for(chat_id, group_id):
            return None
        if topic_id:
            topic = self._topics.get(int(topic_id))
            if topic and not topic.get("archived_at"):
                self._topic_current[chat_id] = int(topic_id)
                self._save_topics()
                return topic
        return self._get_or_create_topic(chat_id)

    def _touch_topic(self, topic: Optional[dict[str, Any]]) -> None:
        if not topic:
            return
        topic["last_used_at"] = time.time()
        self._save_topics()

    def _maybe_name_topic(self, topic: Optional[dict[str, Any]], title: str) -> None:
        """首个任务结束：用规划轮给的话题名替换「新话题」/自动生成的「遗留·xxxx」。"""
        if not topic or not title:
            return
        current = str(topic.get("name") or "")
        if current and current != "新话题" and not current.startswith("遗留"):
            return
        clean = re.sub(r"\s+", " ", str(title)).strip().strip("「」\"'")
        if not clean:
            return
        topic["name"] = clean[: max(4, int(self.config.topic.name_max_chars))]
        self._save_topics()
        self.ctx.logger.info("话题 #%s 命名为「%s」", topic["id"], topic["name"])

    def _topic_archive_root(self) -> Path:
        d = self._workspace_root() / ".archive"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _archive_topic(self, topic: dict[str, Any]) -> None:
        if topic.get("archived_at"):
            return
        src = self._workspace_root() / str(topic.get("dir") or f"topic-{topic['id']}")
        stamp = time.strftime("%Y%m%d-%H%M%S")
        dest = self._topic_archive_root() / f"topic-{topic['id']}-{stamp}"
        try:
            if src.is_dir():
                shutil.move(str(src), str(dest))
                topic["dir"] = str(dest)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("归档目录失败 %s", src, exc_info=True)
        topic["archived_at"] = time.time()
        for chat, tid in list(self._topic_current.items()):
            if int(tid) == int(topic["id"]):
                self._topic_current.pop(chat, None)
        self._save_topics()

    def _restore_topic(self, topic: dict[str, Any]) -> None:
        if not topic.get("archived_at"):
            return
        cur = Path(str(topic.get("dir") or ""))
        target = self._workspace_root() / f"topic-{topic['id']}"
        try:
            if cur.is_dir() and not target.exists():
                shutil.move(str(cur), str(target))
            topic["dir"] = f"topic-{topic['id']}"
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("恢复目录失败 %s", cur, exc_info=True)
        topic["archived_at"] = None
        topic["last_used_at"] = time.time()
        for chat in topic.get("chats") or []:
            self._topic_current.setdefault(str(chat), int(topic["id"]))
        self._save_topics()

    def _purge_archived(self) -> None:
        """归档满 archive_keep_days 天：删目录 + 删记录（编号随之释放）。"""
        keep = int(self.config.topic.archive_keep_days) * 86400
        now = time.time()
        removed: list[int] = []
        for tid, topic in list(self._topics.items()):
            archived_at = float(topic.get("archived_at") or 0)
            if not archived_at or now - archived_at <= keep:
                continue
            path = Path(str(topic.get("dir") or ""))
            try:
                if path.is_dir():
                    shutil.rmtree(path, ignore_errors=True)
            except Exception:  # noqa: BLE001
                pass
            self._topics.pop(tid, None)
            removed.append(tid)
        if removed:
            self._save_topics()
            self.ctx.logger.info("清理归档话题 %s，编号已释放", removed)

    # ── 实验功能：墨自主调度（24h 临时窗口 + 二次确认） ─────────

    def _exp_file(self) -> Path:
        return Path(self.ctx.paths.data_dir) / "experimental.json"

    def _load_exp(self) -> None:
        try:
            fp = self._exp_file()
            if fp.is_file():
                data = json.loads(fp.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    self._exp = data
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("读取实验功能状态失败", exc_info=True)

    def _save_exp(self) -> None:
        try:
            fp = self._exp_file()
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(json.dumps(self._exp, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("保存实验功能状态失败", exc_info=True)

    def _agent_armed(self) -> bool:
        if not self.config.experimental.agent_tools:
            return False
        try:
            return bool(self._exp.get("armed")) and time.time() < float(self._exp.get("expires_at") or 0)
        except Exception:  # noqa: BLE001
            return False

    def _agent_remaining_text(self) -> str:
        left = max(0, int(float(self._exp.get("expires_at") or 0) - time.time()))
        hours, minutes = left // 3600, (left % 3600) // 60
        return f"{hours} 小时 {minutes} 分钟" if hours else f"{minutes} 分钟"

    def _arm_hours(self) -> int:
        return max(1, min(24, int(self.config.experimental.agent_tools_hours or 24)))

    def _start_agent_pending(self, chat_id: str) -> str:
        if not self.config.experimental.agent_tools:
            return (
                "🧪 实验功能的**总开关**还没打开：请先在插件设置里把「实验功能 → agent_tools」打开，"
                "再发 /dsh 实验开启。"
            )
        code = f"{random.randint(100000, 999999)}"
        now = time.time()
        ttl = max(1, int(self.config.experimental.agent_confirm_ttl_minutes or 10)) * 60
        self._exp = {
            "pending_code": code,
            "pending_at": now,
            "pending_expires": now + ttl,
            "requested_by": chat_id,
            "armed": False,
            "expires_at": 0,
        }
        self._save_exp()
        hours = self._arm_hours()
        return (
            "🧪 实验功能（墨自主调度）· 二次确认\n"
            f"开启后 {hours} 小时内：\n"
            "· /dsh 命令全部临时禁用\n"
            "· 墨可以直接建/切/改/删话题、派活（不用你打命令）\n"
            "· 到期自动关闭，也可 /dsh 实验关闭 提前关\n\n"
            f"确认码：{code}\n回复 /dsh 实验确认 {code}（{ttl // 60} 分钟内有效）"
        )

    async def _disarm_agent(self, reason: str, notify: bool = True) -> None:
        was = bool(self._exp.get("armed"))
        target = self._last_chat_id or str(self._exp.get("requested_by") or "")
        self._exp = {"armed": False, "expires_at": 0, "closed_reason": reason, "closed_at": time.time()}
        self._save_exp()
        if was:
            self.ctx.logger.info("实验功能已关闭：%s", reason)
            if notify and target:
                try:
                    await self.ctx.send.text(f"🧪 实验功能已关闭（{reason}）。/dsh 命令已恢复。", target)
                except Exception:  # noqa: BLE001
                    pass

    def _expire_agent_if_needed(self) -> Optional[str]:
        if not self._exp.get("armed"):
            return None
        if time.time() < float(self._exp.get("expires_at") or 0):
            return None
        self._exp = {"armed": False, "expires_at": 0, "closed_reason": "24 小时到期", "closed_at": time.time()}
        self._save_exp()
        self.ctx.logger.info("实验功能到期自动关闭")
        return "🧪 实验功能到期（最长 24 小时），已自动关闭。/dsh 命令恢复，墨也不再自主调度。"

    def _agent_tool_gate(self, kwargs: dict[str, Any]) -> Optional[dict[str, Any]]:
        """实验工具的统一准入检查；返回错误 dict，None 表示放行。"""
        if not self._agent_armed():
            return {
                "success": False,
                "error": "实验功能未开启或已到期（最长 24 小时）。请让用户在插件设置打开 agent_tools，"
                "再 /dsh 实验开启 + 二次确认。",
            }
        stream_id = str(kwargs.get("stream_id") or "")
        if not self._topic_mode_for(stream_id, str(kwargs.get("group_id") or "")):
            return {"success": False, "error": "话题实验功能只在私聊可用。"}
        if self.config.permission.enforce_tools and not self._is_allowed(kwargs):
            return {"success": False, "error": "无权限使用这个工具。"}
        if stream_id:
            self._last_chat_id = stream_id
        return None

    async def _dispatch_experimental(
        self, body: str, stream_id: str, kwargs: dict[str, Any]
    ) -> Optional[str]:
        """识别 /dsh 实验xxx 子指令；不是则返回 None。"""
        raw = body.strip()
        if re.match(r"^(?:实验|experiment)\s*(?:开启|打开|on)$", raw, re.IGNORECASE):
            return self._start_agent_pending(stream_id)
        m = re.match(r"^(?:实验|experiment)\s*确认\s*(\d{4,8})$", raw)
        if m:
            code = m.group(1)
            pending = str(self._exp.get("pending_code") or "")
            if not pending:
                return "还没有待确认的开启请求，先发 /dsh 实验开启。"
            if time.time() > float(self._exp.get("pending_expires") or 0):
                return "确认码已过期，请重新发 /dsh 实验开启。"
            if code != pending:
                return "确认码不对，请再发一次 /dsh 实验确认 <6 位数字>。"
            hours = self._arm_hours()
            self._exp = {
                "armed": True,
                "armed_at": time.time(),
                "expires_at": time.time() + hours * 3600,
                "armed_by": stream_id,
            }
            self._save_exp()
            self.ctx.logger.info("实验功能已开启 %d 小时（by %s）", hours, stream_id)
            return (
                f"🧪 实验功能已开启，{hours} 小时有效（剩余 {self._agent_remaining_text()}）。\n"
                "· /dsh 命令已临时禁用\n"
                "· 墨现在可以自主建/切/改/删话题、派活\n"
                "· 提前关闭发 /dsh 实验关闭"
            )
        if re.match(r"^(?:实验|experiment)\s*(?:关闭|off)$", raw, re.IGNORECASE):
            if not self._exp.get("armed"):
                return "实验功能当前没有开启。"
            await self._disarm_agent("用户手动关闭", notify=False)
            return "🧪 实验功能已手动关闭，/dsh 命令恢复。"
        if re.match(r"^(?:实验|experiment)\s*(?:状态|status)$", raw, re.IGNORECASE):
            if self._agent_armed():
                return f"🧪 实验功能：已开启，剩余 {self._agent_remaining_text()}（到期自动关闭）。"
            if self._exp.get("pending_code"):
                return "🧪 实验功能：有待确认的开启请求，回复 /dsh 实验确认 <码> 生效。"
            master = "开" if self.config.experimental.agent_tools else "关"
            return (
                f"🧪 实验功能：未开启（总开关：{master}）。"
                "开启：先在插件设置打开 agent_tools，再 /dsh 实验开启。"
            )
        return None

    @staticmethod
    def _compute_next(trigger: dict[str, Any], base: float) -> Optional[float]:
        """根据触发器推算下次触发时间戳（秒）。返回 None 表示不再触发。"""
        kind = str(trigger.get("kind") or "")
        if kind == "after":
            return base + float(trigger.get("seconds") or 0)
        if kind == "at":
            at = float(trigger.get("timestamp") or 0)
            return at if at > base + 0.5 else None
        if kind == "every":
            every = max(60.0, float(trigger.get("seconds") or 60))
            anchor = float(trigger.get("anchor") or base)
            if anchor > base:
                return anchor
            n = int((base - anchor) // every) + 1
            return anchor + n * every
        if kind == "cron":
            return DshBridgePlugin._cron_next(str(trigger.get("expression") or ""), base)
        return None

    @staticmethod
    def _cron_field_match(field: str, value: int) -> bool:
        """单个 cron 字段匹配（* / a / a-b / */n / a-b/n / 逗号列表）。"""
        for part in str(field).split(","):
            part = part.strip()
            if not part:
                continue
            step = 1
            base = part
            if "/" in part:
                base, _, s = part.partition("/")
                try:
                    step = int(s)
                except ValueError:
                    continue
                if step <= 0:
                    continue
            if base == "*":
                lo, hi = 0, 10 ** 9
            elif "-" in base:
                a, _, b = base.partition("-")
                try:
                    lo, hi = int(a), int(b)
                except ValueError:
                    continue
                if lo > hi:
                    lo, hi = hi, lo
            else:
                try:
                    v = int(base)
                except ValueError:
                    continue
                if step == 1:
                    if value == v:
                        return True
                    continue
                lo, hi = v, 10 ** 9
            if lo <= value <= hi and (value - lo) % step == 0:
                return True
        return False

    @classmethod
    def _cron_next(cls, expr: str, base: float) -> Optional[float]:
        """求 cron（5 字段，minute 精度）下一个匹配时点；限制搜索 366 天。"""
        import datetime as _dt
        parts = (expr or "").split()
        if len(parts) != 5:
            return None
        mi_f, ho_f, dom_f, mo_f, dow_f = parts
        start = _dt.datetime.fromtimestamp(base + 1).replace(second=0, microsecond=0) + _dt.timedelta(minutes=1)
        limit = start + _dt.timedelta(days=366)
        cur = start
        while cur < limit:
            dow = (cur.weekday() + 1) % 7  # cron: 周日=0
            if (
                cls._cron_field_match(mo_f, cur.month)
                and cls._cron_field_match(dom_f, cur.day)
                and cls._cron_field_match(dow_f, dow)
                and cls._cron_field_match(ho_f, cur.hour)
                and cls._cron_field_match(mi_f, cur.minute)
            ):
                return cur.timestamp()
            cur += _dt.timedelta(minutes=1)
        return None

    async def _scheduler_loop(self) -> None:
        """后台调度循环：每 10s 扫描到期任务、排队任务与归档清理。"""
        while self._scheduler_running:
            try:
                await self._expire_pending()
                now = time.time()
                # 实验功能到期自动关闭
                expired_text = self._expire_agent_if_needed()
                if expired_text:
                    target = self._last_chat_id or str(self._exp.get("requested_by") or "")
                    if target:
                        try:
                            await self.ctx.send.text(expired_text, target)
                        except Exception:  # noqa: BLE001
                            pass
                # 归档清理（每小时一次）
                if now - self._last_purge > 3600:
                    self._last_purge = now
                    try:
                        self._purge_archived()
                    except Exception:  # noqa: BLE001
                        self.ctx.logger.warning("清理归档话题失败", exc_info=True)
                # 排队任务：空闲时取一个执行
                if not self._running and self._queue:
                    item = self._queue.pop(0)
                    self._running = True
                    asyncio.create_task(self._run_and_report(**item))
                due = [t for t in list(self._tasks.values()) if float(t.get("next_at") or 0) and float(t["next_at"]) <= now]
                for task in due:
                    try:
                        await self._fire_task(task)
                    except Exception:  # noqa: BLE001
                        self.ctx.logger.warning("触发定时任务失败", exc_info=True)
            except Exception:  # noqa: BLE001
                self.ctx.logger.warning("调度循环异常", exc_info=True)
            await asyncio.sleep(10)

    async def _fire_task(self, task: dict[str, Any]) -> None:
        stream_id = str(task.get("stream_id") or "")
        prompt = str(task.get("prompt") or "")
        ctx = task.get("ctx") or {}
        title = str(task.get("title") or "定时任务")
        topic_id = task.get("topic_id")
        if self._reject_busy():
            # 正忙：顺延 60s 后再试，避免与手动任务打架
            task["next_at"] = time.time() + 60
            self._save_tasks()
            self.ctx.logger.info("定时任务「%s」因忙碌顺延", title)
            return
        # 到点的定时任务也算新任务：旧提问作废
        self._drop_pending(stream_id)
        try:
            label = ""
            if self._topic_mode_for(stream_id, str((ctx or {}).get("group_id") or "")):
                t = self._topics.get(int(topic_id)) if topic_id else self._current_topic(stream_id)
                label = self._topic_label(t) + " " if t else ""
            await self.ctx.send.text(f"⏰ {label}定时任务「{title}」触发：{prompt[:60]}", stream_id)
        except Exception:  # noqa: BLE001
            pass
        self._running = True
        try:
            await self._run_and_report(
                prompt, stream_id, self._clamp_timeout(task.get("timeout")), ctx
            )
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("定时任务执行失败", exc_info=True)
        trigger = task.get("trigger") or {}
        kind = str(trigger.get("kind") or "")
        if kind == "every":
            task["next_at"] = self._compute_next(trigger, time.time())
        elif kind == "cron":
            task["next_at"] = self._compute_next(trigger, time.time())
        else:
            self._tasks.pop(str(task.get("id")), None)
        self._save_tasks()

    def _parse_schedule(self, after_seconds: Any, every_seconds: Any, at: Any, cron: Any = None) -> Optional[dict[str, Any]]:
        """把可选参数解析成一个触发器。"""
        now = time.time()
        if after_seconds not in (None, ""):
            return {"kind": "after", "seconds": float(after_seconds), "next_at": now + float(after_seconds)}
        if every_seconds not in (None, ""):
            sec = max(60.0, float(every_seconds))
            return {"kind": "every", "seconds": sec, "anchor": now, "next_at": now + sec}
        if cron not in (None, ""):
            expr = str(cron).strip()
            nxt = self._cron_next(expr, now)
            if nxt is None:
                return None
            return {"kind": "cron", "expression": expr, "next_at": nxt}
        if at not in (None, ""):
            ts = self._parse_iso(str(at))
            if ts is None or ts <= now:
                return None
            return {"kind": "at", "timestamp": ts, "at_text": str(at), "next_at": ts}
        return None

    def _create_task(self, title: str, prompt: str, after: Any, every: Any, at: Any, cron: Any, stream_id: str, ctx: dict[str, Any]) -> Optional[str]:
        """建一个定时任务并持久化；返回任务 id 或 None。"""
        trigger = self._parse_schedule(after, every, at, cron)
        if not trigger:
            return None
        next_at = trigger.pop("next_at")
        task_id = "st_" + time.strftime("%Y%m%d%H%M%S") + "_" + str(int(time.time() * 1000) % 1000)
        self._tasks[task_id] = {
            "id": task_id,
            "title": title,
            "prompt": prompt,
            "trigger": trigger,
            "next_at": next_at,
            "timeout": self._clamp_timeout(None),
            "stream_id": stream_id,
            "ctx": ctx or {},
            "topic_id": self._topic_current.get(stream_id),
            "created_at": time.time(),
        }
        self._save_tasks()
        return task_id

    def _schedule_requests_dir(self, stream_id: str, group_id: str = "") -> Path:
        return Path(self._chat_ws(stream_id, group_id)) / "schedule-requests"

    async def _process_schedule_requests(self, stream_id: str, ctx: dict[str, Any]) -> None:
        """扫描 dsh 写下的 schedule-requests/*.json，建任务后移到 .done（幂等）。"""
        if not self.config.dsh.allow_self_schedule:
            return
        req_dir = self._schedule_requests_dir(stream_id, str((ctx or {}).get("group_id") or ""))
        if not req_dir.is_dir():
            return
        created: list[str] = []
        for fp in sorted(req_dir.glob("*.json")):
            try:
                req = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                continue
            if isinstance(req, dict):
                title = str(req.get("title") or "").strip()
                prompt = str(req.get("prompt") or "").strip()
                if title and prompt:
                    tid = self._create_task(
                        title,
                        prompt,
                        req.get("after_seconds"),
                        req.get("every_seconds"),
                        req.get("at"),
                        req.get("cron"),
                        stream_id,
                        ctx or {},
                    )
                    if tid:
                        created.append(f"「{title}」(id={tid})")
            # 无论是否成功都移入 .done，避免重复处理
            try:
                done_dir = req_dir / ".done"
                done_dir.mkdir(parents=True, exist_ok=True)
                fp.replace(done_dir / fp.name)
            except Exception:  # noqa: BLE001
                pass
        if created:
            self.ctx.logger.info("dsh 新建定时任务 %d 个: %s", len(created), "; ".join(created))
            try:
                await self.ctx.send.text("⏰ dsh 新建定时任务：" + "、".join(created), stream_id)
            except Exception:  # noqa: BLE001
                pass

    # ── dsh 提问通道：ask-requests → QQ → 回答 → 续跑同一会话 ──────

    def _pending_file(self) -> Path:
        return Path(self.ctx.paths.data_dir) / "pending_asks.json"

    def _load_pending(self) -> None:
        """载入未过期的等待回答（重启后仍能接住用户的下一条消息）。"""
        try:
            fp = self._pending_file()
            if not fp.is_file():
                return
            data = json.loads(fp.read_text(encoding="utf-8"))
            now = time.time()
            for item in data if isinstance(data, list) else []:
                if not isinstance(item, dict):
                    continue
                sid = str(item.get("stream_id") or "")
                if sid and float(item.get("deadline") or 0) > now:
                    self._pending[sid] = item
            if self._pending:
                self.ctx.logger.info("已载入等待回答的 dsh 提问 %d 个", len(self._pending))
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("读取待回答问题失败", exc_info=True)

    def _save_pending(self) -> None:
        try:
            fp = self._pending_file()
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(
                json.dumps(list(self._pending.values()), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("保存待回答问题失败", exc_info=True)

    def _drop_pending(self, stream_id: str) -> bool:
        """丢掉某聊天流里等待中的提问；返回是否真的丢掉了一个。"""
        if self._pending.pop(stream_id, None) is None:
            return False
        self._save_pending()
        return True

    def _ask_dir(self, stream_id: str) -> Path:
        return Path(self._chat_ws(stream_id)) / ASK_DIR_NAME

    def _take_ask_request(self, stream_id: str) -> Optional[dict[str, Any]]:
        """取一条 dsh 写下的提问（消费后移入 .done，幂等）。"""
        req_dir = self._ask_dir(stream_id)
        if not req_dir.is_dir():
            return None
        done_dir = req_dir / ".done"
        taken: Optional[dict[str, Any]] = None
        for fp in sorted(req_dir.glob("*.json")):
            data: Any = None
            try:
                data = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                data = None
            try:
                done_dir.mkdir(parents=True, exist_ok=True)
                fp.replace(done_dir / fp.name)
            except Exception:  # noqa: BLE001
                pass
            if taken is None and isinstance(data, dict):
                question = str(data.get("question") or data.get("prompt") or "").strip()
                if question:
                    options = [str(o).strip() for o in (data.get("options") or []) if str(o).strip()]
                    taken = {
                        "question": question[:2000],
                        "options": options[:8],
                        "note": str(data.get("note") or "").strip()[:600],
                    }
        return taken

    async def _start_ask(
        self,
        ask: dict[str, Any],
        state: "_RunState",
        stream_id: str,
        ctx: Optional[dict[str, Any]],
        full: bool,
        timeout: int,
        depth: int,
    ) -> None:
        """把 dsh 的问题发到聊天，并登记"等回答"。"""
        conf = self.config.dsh
        minutes = max(1, int(int(conf.ask_timeout) // 60))
        lines = ["❓ dsh 需要你确认：", str(ask.get("question") or "")]
        if ask.get("note"):
            lines.append(f"（{ask['note']}）")
        if ask.get("options"):
            lines.append("参考选项：")
            for idx, opt in enumerate(ask["options"], 1):
                lines.append(f"  {idx}. {opt}")
        lines.append("")
        lines.append(f"直接回一句话就是答案，{minutes} 分钟内有效（以 / 开头的命令不算答案）。")
        await self.ctx.send.text("\n".join(lines), stream_id)
        self._pending[stream_id] = {
            "stream_id": stream_id,
            "mode": "file",
            "session_id": state.session_id,
            "question": str(ask.get("question") or "")[:600],
            "created_at": time.time(),
            "deadline": time.time() + int(conf.ask_timeout),
            "timeout": int(timeout),
            "full": bool(full),
            "depth": int(depth),
            "ctx": ctx or {},
            "topic_id": self._topic_current.get(stream_id),
        }
        self._save_pending()
        self.ctx.logger.info(
            "dsh 提问已发出（stream=%s, session=%s, 等待 %ss）",
            stream_id,
            state.session_id,
            conf.ask_timeout,
        )

    @staticmethod
    def _format_ask_text(
        question: str, options: list[Any], note: str, label: str, deadline: float
    ) -> str:
        minutes = max(1, int((deadline - time.time()) // 60))
        lines = [f"❓ {label}dsh 需要你确认：", str(question)]
        if note:
            lines.append(f"（{note}）")
        if options:
            lines.append("参考选项：")
            for idx, opt in enumerate(options, 1):
                lines.append(f"  {idx}. {opt}")
        lines.append("")
        lines.append(f"直接回一句话就是答案，{minutes} 分钟内有效（以 / 开头的命令不算答案）。")
        return "\n".join(lines)

    async def _watch_ipc_questions(self, ws: str, stream_id: str, ctx: dict[str, Any]) -> None:
        """任务运行中：把 dsh 通过 MCP ask 工具写下的问题发到 QQ，并登记等待回答。"""
        qdir = Path(ws) / "ipc" / "questions"
        adir = Path(ws) / "ipc" / "answers"
        try:
            while True:
                await asyncio.sleep(1.0)
                if not qdir.is_dir():
                    continue
                for fp in sorted(qdir.glob("*.json")):
                    qid = fp.stem
                    if qid in self._ipc_seen:
                        continue
                    try:
                        data = json.loads(fp.read_text(encoding="utf-8"))
                    except Exception:  # noqa: BLE001
                        continue
                    self._ipc_seen.add(qid)
                    question = str(data.get("question") or "").strip()
                    if not question:
                        continue
                    try:
                        timeout = int(data.get("timeout") or self.config.dsh.ask_timeout)
                    except Exception:  # noqa: BLE001
                        timeout = int(self.config.dsh.ask_timeout)
                    deadline = time.time() + timeout
                    adir.mkdir(parents=True, exist_ok=True)
                    self._pending[stream_id] = {
                        "stream_id": stream_id,
                        "mode": "ipc",
                        "qid": qid,
                        "question": question[:600],
                        "answer_path": str(adir / f"{qid}.json"),
                        "question_path": str(fp),
                        "created_at": time.time(),
                        "deadline": deadline,
                        "ctx": ctx or {},
                        "topic_id": self._topic_current.get(stream_id),
                    }
                    self._save_pending()
                    self._ipc_waiting = True
                    topic = self._topics.get(int(self._topic_current.get(stream_id) or 0))
                    text = self._format_ask_text(
                        question, list(data.get("options") or []), str(data.get("note") or ""),
                        f"{self._topic_label(topic)} " if topic else "", deadline,
                    )
                    try:
                        await self.ctx.send.text(text, stream_id)
                    except Exception:  # noqa: BLE001
                        pass
                    self.ctx.logger.info("MCP 提问已转发到 QQ（qid=%s）", qid)
                # 检查等待中的问题是否已被工具自己收走（超时/取消）
                pend = self._pending.get(stream_id)
                if pend and pend.get("mode") == "ipc":
                    qpath = Path(str(pend.get("question_path") or ""))
                    if qpath.is_file():
                        continue
                    self._pending.pop(stream_id, None)
                    self._save_pending()
                    self._ipc_waiting = False
                    try:
                        await self.ctx.send.text("⏳ dsh 的提问没等到回答（工具超时），它已经自己往下走了。", stream_id)
                    except Exception:  # noqa: BLE001
                        pass
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("IPC 提问监听异常", exc_info=True)

    def _write_ipc_answer(self, pending: dict[str, Any], answer: str, *, timeout: bool = False) -> bool:
        path = str(pending.get("answer_path") or "")
        if not path:
            return False
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "qid": pending.get("qid"),
                "answer": answer,
                "timeout": bool(timeout),
                "answered_at": time.time(),
            }
            tmp = path + ".part"
            Path(tmp).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, path)
            return True
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("写 MCP 答案失败", exc_info=True)
            return False

    async def _expire_pending(self) -> None:
        """超时没人回答 → 放弃这轮并告知。"""
        if not self._pending:
            return
        now = time.time()
        expired = [k for k, v in list(self._pending.items()) if now > float(v.get("deadline") or 0)]
        if not expired:
            return
        for sid in expired:
            pending = self._pending.pop(sid, None) or {}
            if pending.get("mode") == "ipc":
                self._write_ipc_answer(pending, "", timeout=True)
                self._ipc_waiting = False
            try:
                await self.ctx.send.text(
                    "⏳ 刚才 dsh 的问题超时没等到回答，这轮先放弃了。要接着做就重新发一条指令。",
                    sid,
                )
            except Exception:  # noqa: BLE001
                pass
        self._save_pending()

    async def _resume_with_answer(self, pending: dict[str, Any], answer: str, stream_id: str) -> None:
        """用户回答了 dsh 的问题：带 --session-id 续跑同一个会话。"""
        self._running = True
        try:
            prompt = (
                f"用户回答：{answer}\n\n"
                "（这是对你上一个问题的回答。接着上一次的工作继续做完，不要重复已完成的部分；"
                "如果还需要确认，可以继续在工作区写 ask-requests/*.json。）"
            )
            await self._run_and_report(
                prompt,
                stream_id,
                self._clamp_timeout(pending.get("timeout")),
                pending.get("ctx") or {},
                bool(pending.get("full")),
                session_id=str(pending.get("session_id") or ""),
                resume=True,
                depth=int(pending.get("depth") or 0) + 1,
                topic_id=pending.get("topic_id"),
            )
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("续跑 dsh 失败", exc_info=True)
        finally:
            self._running = False

    # ── 入站钩子：接住回答 + 收 QQ 发来的文件 ─────────────────────

    @HookHandler(
        "chat.receive.after_process",
        name="dsh_bridge_inbox",
        description="接住用户对 dsh 提问的回答；并把 QQ 发来的文件和图片存进 dsh 工作区",
        mode=HookMode.BLOCKING,
        order=HookOrder.LATE,
        timeout_ms=6000,
        error_policy=ErrorPolicy.SKIP,
    )
    async def handle_receive(self, message: dict | None = None, **kwargs: Any):
        try:
            return await self._handle_receive(message or {}, **kwargs)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("dsh 桥收件钩子异常", exc_info=True)
            return None

    async def _handle_receive(self, message: dict[str, Any], **kwargs: Any) -> Any:
        del kwargs
        stream_id = self._msg_stream_id(message)
        if not stream_id:
            return None
        text = self._msg_plain_text(message)
        allowed = self._is_allowed(
            {"user_id": self._msg_user_id(message), "group_id": self._msg_group_id(message)}
        )

        pending = self._pending.get(stream_id)
        if pending:
            if self._is_attachment_message(message, text):
                # 等回答期间发文件：文件收下，但别把文件当成答案
                if self.config.dsh.uploads_enabled and allowed:
                    try:
                        asyncio.create_task(
                            self._ingest_attachments(message, stream_id, self._msg_group_id(message))
                        )
                    except Exception:  # noqa: BLE001
                        pass
                try:
                    await self.ctx.send.text(
                        "📎 文件已收下（dsh 下个任务能直接用）。不过 dsh 还等你回答：\n"
                        + str(pending.get("question") or "")[:300],
                        stream_id,
                    )
                except Exception:  # noqa: BLE001
                    pass
                return None
            if (
                pending.get("mode") == "ipc"
                and text
                and not text.lstrip().startswith("/")
                and allowed
            ):
                # MCP 提问：把答案写进 ipc/answers/，dsh 同一进程接着跑（无需重启）
                self._pending.pop(stream_id, None)
                self._save_pending()
                ok = self._write_ipc_answer(pending, text.strip())
                self._ipc_waiting = False
                try:
                    await self.ctx.send.text(
                        "✅ 收到，已交给 dsh，它接着说。" if ok else "⚠️ 答案写回失败，dsh 可能等到超时。",
                        stream_id,
                    )
                except Exception:  # noqa: BLE001
                    pass
                return {"action": "abort"}
            if not text or text.lstrip().startswith("/"):
                pass  # 命令照常走，不当答案
            elif not allowed:
                pass
            elif self._reject_busy():
                try:
                    await self.ctx.send.text("dsh 现在正忙，等这轮跑完再把答案发一遍就行。", stream_id)
                except Exception:  # noqa: BLE001
                    pass
                return None
            else:
                self._pending.pop(stream_id, None)
                self._save_pending()
                try:
                    await self.ctx.send.text("✅ 收到，继续交给 dsh…", stream_id)
                except Exception:  # noqa: BLE001
                    pass
                asyncio.create_task(self._resume_with_answer(pending, text.strip(), stream_id))
                return {"action": "abort"}

        # 附件：只收名单内用户发来的文件/图片（避免别人往工作区塞东西）
        if self.config.dsh.uploads_enabled and allowed:
            if text and _FILE_SEG_RE.search(text):
                leftover = self._strip_file_segments(text)
                if self.config.dsh.upload_wait_instruct and not leftover:
                    # 纯文件消息：收下但不叫醒墨 —— 等用户说要干什么，避免墨和 dsh 抢着看文件
                    try:
                        asyncio.create_task(
                            self._ingest_attachments(message, stream_id, self._msg_group_id(message))
                        )
                    except Exception:  # noqa: BLE001
                        pass
                    names = self._file_names(text)
                    try:
                        await self.ctx.send.text(
                            "📎 收到文件：" + "、".join(names) +
                            "。\n已放进 dsh 工作区，等你说要做什么（比如「把这个转成 PDF」），我再让 dsh 开工。",
                            stream_id,
                        )
                    except Exception:  # noqa: BLE001
                        pass
                    try:
                        await self.ctx.maisaka.context.append(
                            stream_id,
                            segments=[{
                                "type": "text",
                                "content": f"（用户发来文件：{'、'.join(names)}，已存入 dsh 工作区；等用户说明要做什么，不要主动去读该文件。）",
                            }],
                            source_kind="dsh-bridge",
                        )
                    except Exception:  # noqa: BLE001
                        pass
                    return {"action": "abort"}
            try:
                asyncio.create_task(self._ingest_attachments(message, stream_id))
            except Exception:  # noqa: BLE001
                pass
        return None

    @staticmethod
    def _strip_file_segments(text: str) -> str:
        """去掉消息里的「[文件] 名字，大小: N」片段，返回剩余文字。

        用来判断用户是不是在文件消息里同时说了要干什么（那样就不该拦墨）。
        """
        leftovers: list[str] = []
        for line in (text or "").splitlines():
            stripped = line.strip()
            if not stripped.startswith("[文件]"):
                if stripped:
                    leftovers.append(stripped)
                continue
            m = _FILE_LINE_RE.match(stripped)
            tail = stripped[m.end():].strip() if m else stripped[len("[文件]"):].strip()
            if tail:
                leftovers.append(tail)
        return "\n".join(leftovers).strip()

    @staticmethod
    def _file_names(text: str) -> list[str]:
        names: list[str] = []
        for m in _FILE_SEG_RE.finditer(text or ""):
            body = (m.group("body") or "").strip()
            if not body:
                continue
            name = _FILE_NAME_SPLIT_RE.split(body, maxsplit=1)[0].strip()
            if name:
                names.append(name)
        return names or ["文件"]

    @staticmethod
    def _msg_stream_id(message: dict[str, Any]) -> str:
        return str(message.get("session_id") or "").strip()

    @staticmethod
    def _msg_plain_text(message: dict[str, Any]) -> str:
        text = str(message.get("processed_plain_text") or "").strip()
        if text:
            return text
        parts: list[str] = []
        for comp in message.get("raw_message") or []:
            if isinstance(comp, dict) and comp.get("type") == "text":
                parts.append(str(comp.get("data") or ""))
        return "\n".join(p for p in parts if p).strip()

    @staticmethod
    def _msg_user_id(message: dict[str, Any]) -> str:
        info = message.get("message_info") or {}
        user = info.get("user_info") or {}
        return str(user.get("user_id") or "").strip()

    @staticmethod
    def _msg_group_id(message: dict[str, Any]) -> str:
        info = message.get("message_info") or {}
        group = info.get("group_info") or {}
        return str(group.get("group_id") or "").strip()

    @staticmethod
    def _is_attachment_message(message: dict[str, Any], text: str) -> bool:
        """这条消息是不是"发了个文件/图片"（而不是在回答问题）。"""
        if _FILE_SEG_RE.search(text or ""):
            return True
        for comp in message.get("raw_message") or []:
            if not isinstance(comp, dict):
                continue
            if str(comp.get("type") or "") in ("image", "emoji") and comp.get("binary_data_base64"):
                return True
        return False

    # ── QQ 文件 → 工作区 uploads/ ────────────────────────────────

    def _upload_dir(self, ws: str) -> Path:
        d = Path(ws) / UPLOAD_DIR_NAME
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _uploads_index_file(self, ws: str) -> Path:
        return self._upload_dir(ws) / UPLOAD_INDEX_NAME

    @staticmethod
    def _safe_filename(raw: str) -> str:
        name = str(raw or "").strip().replace("\\", "/").split("/")[-1]
        name = re.sub(r"[^\w.\-()（） ]", "_", name).strip(" ._")
        return name[:_LOG_NAME_MAX]

    def _save_upload_bytes(self, data: bytes, name: str, ws: str, url: str = "") -> dict[str, Any]:
        up_dir = self._upload_dir(ws)
        safe = self._safe_filename(name) or "file.bin"
        stamp = time.strftime("%Y%m%d-%H%M%S")
        target = up_dir / f"{stamp}-{safe}"
        idx = 1
        while target.exists():
            target = up_dir / f"{stamp}-{idx}-{safe}"
            idx += 1
        try:
            target.write_bytes(data)
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.warning("保存附件失败 %s: %s", safe, exc)
            return {"name": safe, "path": "", "size": 0, "url": url, "ts": time.time(), "ok": False}
        self.ctx.logger.info("已保存 QQ 附件 %s -> %s（%d 字节）", safe, target, len(data))
        return {"name": safe, "path": str(target), "size": len(data), "url": url, "ts": time.time(), "ok": True}

    async def _download_upload(self, url: str, name: str, ws: str) -> dict[str, Any]:
        limit = max(1, int(self.config.dsh.upload_max_mb)) * 1024 * 1024
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) MaiBot-dsh-bridge",
                "Accept": "*/*",
            },
        )

        def _fetch() -> bytes:
            with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310 - 管理员受信来源（QQ 文件直链）
                return resp.read(limit + 1)

        try:
            data = await asyncio.to_thread(_fetch)
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.warning("下载 QQ 附件失败 %s: %s", name, exc)
            return {"name": name, "path": "", "size": 0, "url": url, "ts": time.time(), "ok": False}
        if not data:
            return {"name": name, "path": "", "size": 0, "url": url, "ts": time.time(), "ok": False}
        if len(data) > limit:
            self.ctx.logger.warning("QQ 附件超过上限被丢弃: %s（%d 字节）", name, len(data))
            return {"name": name, "path": "", "size": len(data), "url": url, "ts": time.time(), "ok": False}
        return self._save_upload_bytes(data, name, ws, url=url)

    async def _has_napcat_api(self, api_name: str) -> bool:
        """Host 的 api.get 返回 {"success": True, "api": 元信息|None}，不是裸 None。"""
        try:
            res = await self.ctx.api.get(api_name)
        except Exception:  # noqa: BLE001
            return False
        if isinstance(res, dict):
            return bool(res.get("api")) or bool(res.get("name"))
        return bool(res)

    @staticmethod
    def _unwrap_api(res: Any) -> Any:
        """兼容两种宿主返回形态：{"success":True,"result":X} 或直接 X。"""
        if isinstance(res, dict) and "result" in res and "success" in res:
            return res.get("result")
        return res

    @staticmethod
    def _napcat_payload(res: Any) -> dict[str, Any]:
        """把 NapCat 动作返回值收敛成 data 字典。

        实测（MaiBot 1.3.15）：插件里 ctx.api.call 已经把 OneBot 响应里的 data 拆出来了 ——
        拿到的是 {"message": [...], ...} / {"file": "/path", ...}，不是
        {"status":"ok","data":{...}}。但不同宿主/适配器版本可能没拆，所以两种都认。
        """
        payload = DshBridgePlugin._unwrap_api(res)
        if not isinstance(payload, dict):
            return {}
        if isinstance(payload.get("data"), dict) and ("status" in payload or "retcode" in payload):
            return payload["data"]
        return payload

    async def _napcat_file_record(
        self, message: dict[str, Any], name: str, ws: str
    ) -> Optional[dict[str, Any]]:
        """文件段没给下载直链时（NapCat 私聊文件常见）走适配器 API 把文件取回来。

        链路：message.get_msg → 原始 file 段里的 file_id → file.get_file
              → NapCat 返回服务器上的本地临时路径 → 读出来存进 uploads/。

        实测（2026-10-24）：私聊发文件时适配器给的文本只有「[文件] 名字，大小: N」，
        没有链接（旧版 NapCat 才带 url），所以这条回退是必须的。
        """
        if not await self._has_napcat_api("adapter.napcat.message.get_msg"):
            self.ctx.logger.warning("NapCat 文件回退不可用：拿不到 adapter.napcat.message.get_msg")
            return None
        mid = str(message.get("message_id") or "").strip()
        if not mid:
            self.ctx.logger.warning("NapCat 文件回退失败：消息里没有 message_id")
            return None
        try:
            detail = self._unwrap_api(
                await self.ctx.api.call("adapter.napcat.message.get_msg", message_id=mid)
            )
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.warning("取文件详情失败（get_msg）: %s", exc)
            return None
        seg: dict[str, Any] = {}
        data = self._napcat_payload(detail)
        for item in data.get("message") or []:
            if isinstance(item, dict) and str(item.get("type")) == "file":
                seg = item.get("data") or {}
                break
        fid = str(seg.get("file_id") or "").strip()
        if not fid:
            self.ctx.logger.warning(
                "get_msg 没给 file 段/file_id（message_id=%s）: %s", mid, str(detail)[:400]
            )
            return None
        self.ctx.logger.info("NapCat 文件回退：message_id=%s file_id=%s", mid, fid[:32])
        fname = self._safe_filename(str(seg.get("file") or name or "file.bin")) or "file.bin"
        try:
            got = self._unwrap_api(
                await self.ctx.api.call(
                    "adapter.napcat.file.get_file",
                    params={"file_id": fid, "file_name": fname, "file": fname},
                )
            )
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.warning("取文件失败（get_file）: %s", exc)
            return None
        gdata = self._napcat_payload(got)
        path = str(gdata.get("file") or gdata.get("url") or "").strip()
        if not path:
            self.ctx.logger.warning("get_file 没给本地路径（file_id=%s）: %s", fid[:32], str(got)[:300])
            return None
        try:
            raw = Path(path).read_bytes()
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.warning("读取 NapCat 临时文件失败 %s: %s", path, exc)
            return None
        if not raw:
            return None
        limit = max(1, int(self.config.dsh.upload_max_mb)) * 1024 * 1024
        if len(raw) > limit:
            self.ctx.logger.warning("QQ 附件超过上限被丢弃: %s（%d 字节）", fname, len(raw))
            return None
        self.ctx.logger.info("经 NapCat get_file 取回文件 %s（%d 字节，file_id=%s）", fname, len(raw), fid[:24])
        return self._save_upload_bytes(raw, fname, ws, url=f"napcat:{fid}")

    async def _ingest_attachments(
        self, message: dict[str, Any], chat_id: str, group_id: str = ""
    ) -> None:
        """把消息里的 QQ 文件与图片落盘（文件优先用直链，其次走 NapCat get_file）。"""
        ws = self._chat_ws(chat_id, group_id)
        stream_id = chat_id
        saved: list[dict[str, Any]] = []
        failed: list[str] = []
        text = self._msg_plain_text(message)
        for m in _FILE_SEG_RE.finditer(text):
            body = (m.group("body") or "").strip()
            if not body:
                continue
            name = self._safe_filename(_FILE_NAME_SPLIT_RE.split(body, maxsplit=1)[0])
            size_m = _FILE_SIZE_RE.search(body)
            url_m = _FILE_URL_RE.search(body)
            url = url_m.group(1).strip() if url_m else ""
            expected = int(size_m.group(1)) if size_m else 0
            if url:
                rec = await self._download_upload(url, name, ws)
                if rec.get("path"):
                    saved.append(rec)
                else:
                    failed.append(name or "(未命名)")
                continue
            rec = await self._napcat_file_record(message, name, ws)
            if rec and rec.get("path"):
                saved.append(rec)
            else:
                self.ctx.logger.info(
                    "文件 %s（%s）没拿到直链、NapCat 回退也失败，跳过",
                    name or "(未命名)",
                    self._size_text(expected),
                )
                failed.append(name or "(未命名)")
        for comp in message.get("raw_message") or []:
            if not isinstance(comp, dict):
                continue
            ctype = str(comp.get("type") or "")
            if ctype not in ("image", "emoji"):
                continue
            raw_b64 = comp.get("binary_data_base64")
            if not raw_b64:
                continue
            try:
                data = base64.b64decode(str(raw_b64), validate=False)
            except Exception:  # noqa: BLE001
                continue
            ext = ".gif" if ctype == "emoji" else ".png"
            rec = self._save_upload_bytes(data, f"{ctype}-{int(time.time() * 1000)}{ext}", ws)
            if rec.get("path"):
                saved.append(rec)
        if failed:
            try:
                await self.ctx.send.text(
                    "⚠️ 你发的文件没取到内容：" + "、".join(failed[:3]) + "（可能已过期或太大）。重发一次就行。",
                    stream_id,
                )
            except Exception:  # noqa: BLE001
                pass
        if not saved:
            return
        self._record_uploads(ws, saved)
        if self.config.dsh.upload_ack:
            try:
                names = "、".join(f"{u['name']}（{self._size_text(u.get('size') or 0)}）" for u in saved)
                await self.ctx.send.text(f"📎 收到：{names}\n已放进 dsh 工作区，下个任务可以直接用。", stream_id)
            except Exception:  # noqa: BLE001
                pass

    def _record_uploads(self, ws: str, items: list[dict[str, Any]]) -> None:
        fp = self._uploads_index_file(ws)
        data: list[dict[str, Any]] = []
        try:
            if fp.is_file():
                loaded = json.loads(fp.read_text(encoding="utf-8"))
                if isinstance(loaded, list):
                    data = [x for x in loaded if isinstance(x, dict)]
        except Exception:  # noqa: BLE001
            data = []
        data.extend(items)
        cutoff = time.time() - 7 * 86400
        data = [x for x in data if float(x.get("ts") or 0) >= cutoff][-300:]
        try:
            fp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("保存附件索引失败", exc_info=True)

    def _recent_uploads(self, ws: str, limit: int = 8) -> list[dict[str, Any]]:
        """最近发给 dsh 的附件（按 upload_ttl_minutes 过期）。"""
        fp = self._uploads_index_file(ws)
        if not fp.is_file():
            return []
        try:
            loaded = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return []
        if not isinstance(loaded, list):
            return []
        ttl = max(60, int(self.config.dsh.upload_ttl_minutes) * 60)
        now = time.time()
        out = [
            x
            for x in loaded
            if isinstance(x, dict)
            and x.get("path")
            and os.path.isfile(str(x["path"]))
            and now - float(x.get("ts") or 0) <= ttl
        ]
        out.sort(key=lambda x: float(x.get("ts") or 0), reverse=True)
        return out[:limit]

    @staticmethod
    def _size_text(size: Any) -> str:
        try:
            value = float(size)
        except Exception:  # noqa: BLE001
            return "?"
        for unit in ("B", "KB", "MB", "GB"):
            if value < 1024 or unit == "GB":
                if unit == "B":
                    return f"{int(value)} B"
                return f"{value:.1f} {unit}"
            value /= 1024
        return f"{int(value)} B"

    @staticmethod
    def _age_text(ts: Any) -> str:
        try:
            age = max(0.0, time.time() - float(ts))
        except Exception:  # noqa: BLE001
            return ""
        if age < 60:
            return "刚刚"
        if age < 3600:
            return f"{int(age // 60)} 分钟前"
        if age < 86400:
            return f"{int(age // 3600)} 小时前"
        return f"{int(age // 86400)} 天前"

    # ── 任务提示增强：告诉 dsh 有哪些附件 ────────────────────────

    def _ensure_ws_agents(self, ws: str) -> None:
        """确保工作区里有 AGENTS.md（桥约定：ask 工具 / present / schedule-requests / uploads）。"""
        fp = Path(ws) / "AGENTS.md"
        try:
            if fp.is_file():
                head = fp.read_text(encoding="utf-8", errors="ignore")[:400].lstrip()
                if head.startswith("<!-- dsh-bridge workspace guide"):
                    # 桥自己写的：直接升级到最新版本
                    fp.write_text(_WORKSPACE_AGENTS_MD, encoding="utf-8")
                    return
                # 用户自己的 AGENTS.md：追加桥约定，不覆盖
                with fp.open("a", encoding="utf-8") as fh:
                    fh.write("\n\n" + _WORKSPACE_AGENTS_MD)
                return
            fp.write_text(_WORKSPACE_AGENTS_MD, encoding="utf-8")
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("写工作区 AGENTS.md 失败", exc_info=True)

    def _augment_task(self, task: str, ws: str) -> str:
        """在任务文本前面补上【工作区附件】清单，并确保 AGENTS.md 就位。"""
        try:
            self._ensure_ws_agents(ws)
        except Exception:  # noqa: BLE001
            pass
        try:
            atts = self._recent_uploads(ws)
        except Exception:  # noqa: BLE001
            atts = []
        if not atts:
            return task
        lines = ["【工作区附件】用户刚从 QQ 发来的文件已保存，可直接读取："]
        for a in atts:
            lines.append(
                f"- {a['path']}  （{self._size_text(a.get('size') or 0)}，{self._age_text(a.get('ts') or 0)}）"
            )
        return "\n".join(lines) + "\n\n----\n\n" + task

    @staticmethod
    def _parse_iso(text: str) -> Optional[float]:
        """解析 ISO 时间（支持 +08:00 时区）为时间戳。"""
        try:
            s = text.strip().replace("Z", "+00:00")
            m = re.match(r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?\s*([+-]\d{2}:?\d{2})?$", s)
            if not m:
                return None
            y, mo, d, h, mi = (int(m.group(i)) for i in range(1, 6))
            sec = int(m.group(6) or 0)
            import datetime as _dt
            tzs = m.group(7)
            tz = _dt.timezone.utc
            if tzs:
                sign = 1 if tzs[0] == "+" else -1
                tzs2 = tzs[1:].replace(":", "")
                tz = _dt.timezone(sign * _dt.timedelta(hours=int(tzs2[:2]), minutes=int(tzs2[2:4])))
            return _dt.datetime(y, mo, d, h, mi, sec, tzinfo=tz).timestamp()
        except Exception:  # noqa: BLE001
            return None

    async def on_config_update(self, scope: str, config_data: dict, version: str) -> None:
        del config_data, version
        try:
            now_master = bool(self.config.experimental.agent_tools)
            prev = self._exp_master_prev
            self._exp_master_prev = now_master
            if now_master and not prev:
                target = self._last_chat_id or next(iter(self._topic_current), "")
                if target:
                    text = self._start_agent_pending(target)
                    await self.ctx.send.text(text, target)
                    self.ctx.logger.info("实验总开关已打开，已向 %s 发出二次确认请求", target)
                else:
                    self.ctx.logger.warning("实验总开关已打开，但没有可用私聊会话；请在 QQ 里发 /dsh 实验开启")
            elif prev and not now_master:
                await self._disarm_agent("设置里的总开关被关闭")
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("处理实验功能配置变更失败", exc_info=True)
        del scope

    # ── 内部辅助 ──────────────────────────────────────────────

    @staticmethod
    def _extract_user_id(kwargs: dict[str, Any]) -> str:
        user_id = (
            kwargs.get("sender_id")
            or kwargs.get("user_id")
            or kwargs.get("operator")
            or ""
        )
        if isinstance(user_id, dict):
            user_id = user_id.get("user_id") or user_id.get("id") or ""
        return str(user_id).strip()

    @staticmethod
    def _only_digits(value: Any) -> str:
        return re.sub(r"\D", "", str(value or ""))

    def _is_allowed(self, kwargs: dict[str, Any]) -> bool:
        perm = self.config.permission
        if not perm.enabled:
            return True
        user_key = self._only_digits(self._extract_user_id(kwargs))
        group_key = self._only_digits(kwargs.get("group_id"))

        if perm.mode == "blacklist":
            blocked_users = {self._only_digits(u) for u in perm.blocked_users.split(",") if u.strip()}
            blocked_groups = {self._only_digits(g) for g in perm.blocked_groups.split(",") if g.strip()}
            if user_key and user_key in blocked_users:
                return False
            if group_key and group_key in blocked_groups:
                return False
            return True

        allowed_users = {self._only_digits(u) for u in perm.allowed_users.split(",") if u.strip()}
        allowed_groups = {self._only_digits(g) for g in perm.allowed_groups.split(",") if g.strip()}
        if user_key and user_key in allowed_users:
            return True
        # 群聊里 chat_stream 未必带发言人 ID，允许在指定群内放行
        return bool(group_key and group_key in allowed_groups)

    def _clamp_timeout(self, value: Optional[int]) -> int:
        """时间预算：0 = 不限时（默认）。只有显式给了值才设上限，且受 timeout_min/max 约束。"""
        conf = self.config.dsh
        try:
            val = int(value) if value is not None else int(conf.timeout_default)
        except Exception:  # noqa: BLE001
            val = 0
        if val <= 0:
            return 0
        lo, hi = int(conf.timeout_min), int(conf.timeout_max)
        if lo > 0:
            val = max(lo, val)
        if hi > 0:
            val = min(hi, val)
        return val

    @staticmethod
    def _budget_text(timeout: int) -> str:
        """给用户看的时间策略文案。"""
        if int(timeout) <= 0:
            return "不限时，/dsh stop 可停"
        return f"上限 {int(timeout)}s，/dsh stop 可停"

    def _workspace(self, stream_id: str = "") -> str:
        root = self.config.dsh.workspace_dir.strip()
        base = Path(root) if root else (Path(self.ctx.paths.data_dir) / "workspace")
        if stream_id:
            safe = re.sub(r"[^A-Za-z0-9_-]", "_", stream_id).strip("_") or "default"
            base = base / safe
        base.mkdir(parents=True, exist_ok=True)
        return str(base)

    def _proc_active(self, pid: int) -> bool:
        if psutil is None:
            return True
        try:
            root = psutil.Process(pid)
            procs = [root, *root.children(recursive=True)]
            total = 0.0
            for q in procs:
                try:
                    times = q.cpu_times()
                    total += float(times.user) + float(times.system)
                except Exception:  # noqa: BLE001
                    continue
        except Exception:  # noqa: BLE001
            return False
        prev = self._cpu_prev.get(pid)
        self._cpu_prev[pid] = total
        if prev is None:
            return True
        return total > prev + 0.01

    @staticmethod
    def _kill_tree(pid: int) -> None:
        """终止整棵进程树（Windows: taskkill /T；类 Unix: 先杀进程组再兜底）。"""
        try:
            if _IS_WINDOWS:
                subprocess.run(
                    ["taskkill", "/T", "/F", "/PID", str(pid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    creationflags=_CREATE_NO_WINDOW,
                )
            else:
                try:
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
                except Exception:  # noqa: BLE001
                    os.kill(pid, signal.SIGKILL)
        except Exception:  # noqa: BLE001
            pass

    def _terminate(self, proc: "subprocess.Popen[bytes]") -> None:
        try:
            proc.terminate()
        except Exception:  # noqa: BLE001
            pass
        deadline = time.time() + self.config.dsh.grace_seconds
        while proc.poll() is None and time.time() < deadline:
            time.sleep(0.5)
        if proc.poll() is None:
            self._kill_tree(proc.pid)

    def _spawn(
        self, task: str, ws: str, env: dict[str, str], session_id: str = ""
    ) -> "subprocess.Popen[bytes]":
        conf = self.config.dsh
        extra = conf.extra_args.split()
        tail = ["--json"]
        if session_id:
            # 续跑同一个 dsh 会话：用户回答完 dsh 的问题后接着跑，上下文不丢
            tail += ["--session-id", session_id]
        tail += ["-"]
        if _IS_WINDOWS:
            # Windows 下经 cmd.exe /c 启动（command / extra_args 属管理员可信配置）
            args = ["cmd.exe", "/c", conf.command, *extra, *tail]
            popen_kwargs: dict[str, Any] = {"creationflags": _CREATE_NO_WINDOW}
        else:
            # 类 Unix：用 shell 解析 command 与参数，子进程独立进程组以便整体终止
            cmdline = " ".join([shlex.quote(conf.command), *[shlex.quote(x) for x in extra], *[shlex.quote(x) for x in tail]])
            args = ["/bin/sh", "-c", cmdline]
            popen_kwargs = {"start_new_session": True}
        proc = subprocess.Popen(
            args,
            cwd=ws,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            **popen_kwargs,
        )
        try:
            if proc.stdin:
                proc.stdin.write(task.encode("utf-8"))
                proc.stdin.close()
        except Exception:  # noqa: BLE001
            pass
        return proc

    async def _execute(
        self, task: str, ws: str, timeout: int, session_id: str = ""
    ) -> tuple[Optional[str], _RunState]:
        conf = self.config.dsh
        env = dict(os.environ)
        api_key = conf.api_key.strip() or os.environ.get(conf.api_key_env, "").strip()
        if api_key:
            env[conf.api_key_env] = api_key
        env["DSH_PERMISSION_MODE"] = conf.permission_mode

        state = _RunState()
        self._run_state = state
        self._run_started = time.time()
        try:
            proc = await asyncio.to_thread(self._spawn, task, ws, env, session_id)
        except Exception as exc:  # noqa: BLE001
            state.events.append({"type": "raw", "text": f"❌ 启动 dsh 失败：{exc}"})
            self._run_state = None
            return "spawn-error", state

        self._current_pid = proc.pid
        self._cpu_prev.pop(proc.pid, None)

        def _reader(stream: Any) -> None:
            if stream is None:
                return
            try:
                for raw in iter(stream.readline, b""):
                    state.push_raw(raw.decode("utf-8", "replace"), conf.repeat_threshold)
            except Exception:  # noqa: BLE001
                pass

        t_out = threading.Thread(target=_reader, args=(proc.stdout,), daemon=True)
        t_err = threading.Thread(target=_reader, args=(proc.stderr,), daemon=True)
        t_out.start()
        t_err.start()

        start = time.time()
        reason: Optional[str] = None
        while proc.poll() is None:
            await asyncio.sleep(_POLL_SECONDS)
            if self._proc_active(proc.pid):
                state.touch()
            now = time.time()
            last_progress, repeat = state.snapshot()
            if self._stop_requested:
                # 用户发 /dsh stop：这里是首要停止入口（比预算优先）
                reason = "stop"
                break
            if timeout > 0 and now - start > timeout:
                # 只有显式设了预算（timeout>0）才按时间终止；默认不限时
                reason = "timeout"
                break
            if self._ipc_waiting:
                # 等用户回答 MCP 提问期间没有输出，不能判为"无进展"
                state.touch()
                continue
            if now - last_progress > conf.heartbeat_seconds:
                reason = "stalled"
                break
            if repeat:
                reason = "loop"
                break

        if reason is not None:
            await asyncio.to_thread(self._terminate, proc)
        else:
            await asyncio.to_thread(proc.wait)

        t_out.join(timeout=5)
        t_err.join(timeout=5)
        # 记下本次会话 id：用户回答 dsh 的提问后要用 --session-id 接着跑
        for evt in state.events_copy():
            if str(evt.get("type") or "") == "session" and evt.get("sessionId"):
                state.session_id = str(evt["sessionId"])
                break
        self._current_pid = None
        self._cpu_prev.pop(proc.pid, None)
        self._run_state = None
        return reason, state

    _REASON_MSG = {
        "failed": "❌ dsh 任务执行失败（详见转发）。",
        "timeout": "⏱ dsh 任务达到时间上限，已终止。",
        "stalled": "🛑 dsh 长时间无任何输出（死锁兜底），已终止。",
        "loop": "🔁 dsh 输出疑似陷入循环（重复度过高），已终止。",
        "stop": "🛑 按你的要求停下了。",
    }

    @staticmethod
    def _tool_summary(inp: Any) -> str:
        """从工具 input 里挑一行人类可读的关键参数，去掉 JSON 壳。"""
        if inp in (None, ""):
            return ""
        if not isinstance(inp, dict):
            return str(inp).strip()[:120]
        for key in ("command", "file_path", "path", "file", "name", "query", "url", "pattern", "skill"):
            val = inp.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()[:120]
        for k, v in inp.items():
            if isinstance(v, str) and v.strip():
                return f"{k}={v.strip()[:100]}"
        return ""

    # ── 回传：合并转发 ────────────────────────────────────────

    @staticmethod
    def _strip_tags(res: str) -> str:
        """去掉 dsh 结果里的 XML 壳（<path>…</path> / <content>…</content>）。"""
        cleaned = re.sub(r"</?[A-Za-z][^>]*>", "", res)
        return cleaned.strip()

    def _build_forward_nodes(self, state: _RunState) -> list[dict[str, Any]]:
        conf = self.config.dsh
        events = state.events_copy()
        nodes: list[dict[str, Any]] = []
        text_buf: list[str] = []
        seen_texts: set[str] = set()
        pending: dict[str, tuple[str, float]] = {}  # callId -> (tool, start_ts)

        def flush_text() -> None:
            if not text_buf:
                return
            joined = "\n".join(text_buf).strip()
            text_buf.clear()
            if not joined:
                return
            seen_texts.add(joined)
            if len(joined) > conf.node_max_chars:
                joined = joined[: conf.node_max_chars] + "…"
            nodes.append(
                {"user_id": "0", "nickname": "📝 输出", "segments": [{"type": "text", "content": joined}]}
            )

        for evt in events:
            t = str(evt.get("type") or "")
            if t in ("session", "status", "thinking"):
                continue
            if t == "tool_call":
                flush_text()
                tool = str(evt.get("tool") or evt.get("name") or "?")
                cid = str(evt.get("callId") or "")
                if cid:
                    pending[cid] = (tool, float(evt.get("_ts") or time.time()))
                summary = self._tool_summary(evt.get("input"))
                nodes.append(
                    {
                        "user_id": "0",
                        "nickname": f"🔧 {tool}",
                        "segments": [{"type": "text", "content": summary or "(调用)"}],
                    }
                )
            elif t == "tool_result":
                flush_text()
                cid = str(evt.get("callId") or "")
                tool = str(evt.get("tool") or "")
                status = str(evt.get("status") or "")
                dur = ""
                if cid in pending:
                    _tool_name, ts0 = pending.pop(cid)
                    dt = max(0.0, float(evt.get("_ts") or time.time()) - ts0)
                    dur = f" {dt:.1f}s"
                res = self._strip_tags(str(evt.get("result") or evt.get("text") or "")).strip()
                if len(res) > 300:
                    res = res[:300] + "…"
                ok = (not status) or status == "completed"
                icon = "↩️" if ok else "⚠️"
                label = tool or ("结果" if ok else "失败")
                content = res or ("完成" if ok else status or "失败")
                nodes.append(
                    {
                        "user_id": "0",
                        "nickname": f"{icon} {label}{dur}",
                        "segments": [{"type": "text", "content": content}],
                    }
                )
            elif t in ("text", "raw"):
                text_buf.append(str(evt.get("text") or ""))
            elif t == "final":
                flush_text()
                ft = str(evt.get("text") or "").strip()
                if not ft:
                    continue
                # 末尾重复：最终答案若与前面已输出的整段相同则不重复展示
                if ft in seen_texts:
                    continue
                if len(ft) > conf.node_max_chars:
                    ft = ft[: conf.node_max_chars] + "…"
                nodes.append(
                    {"user_id": "0", "nickname": "✅ 最终", "segments": [{"type": "text", "content": ft}]}
                )
        flush_text()

        limit = conf.max_forward_nodes
        if len(nodes) > limit:
            head = max(1, limit - 8)
            tail = limit - head - 1
            folded = len(nodes) - head - tail
            nodes = nodes[:head] + [
                {"user_id": "0", "nickname": "…", "segments": [{"type": "text", "content": f"（中间略去 {folded} 条）"}]}
            ] + nodes[-tail:]
        return nodes

    @staticmethod
    def _extract_ctx(kwargs: dict[str, Any]) -> dict[str, str]:
        def _g(k: str) -> str:
            v = kwargs.get(k)
            return str(v).strip() if v is not None else ""
        return {"group_id": _g("group_id"), "user_id": _g("user_id"), "platform": _g("platform")}

    def _write_result_file(self, state: _RunState) -> Optional[str]:
        """把最终输出写成 .md 文件，落在插件 data_dir。"""
        final_text = state.final_copy().strip()
        if not final_text:
            return None
        base = Path(self.ctx.paths.data_dir) / "results"
        try:
            base.mkdir(parents=True, exist_ok=True)
            fp = base / f"dsh-output-{time.strftime('%Y%m%d-%H%M%S')}.md"
            fp.write_text(final_text, encoding="utf-8")
            return str(fp)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("写结果文件失败", exc_info=True)
            return None

    async def _send_result_file(self, path: str, ctx: dict[str, str]) -> bool:
        """经 NapCat 把结果文件发到流（群/私聊）。**NapCat 专属**；其他适配器自动跳过。"""
        group_id = (ctx or {}).get("group_id", "")
        user_id = (ctx or {}).get("user_id", "")
        name = Path(path).name
        if group_id:
            api = "adapter.napcat.file.upload_group_file"
            params = {"group_id": group_id, "file": path, "name": name}
        elif user_id:
            api = "adapter.napcat.file.upload_private_file"
            params = {"user_id": user_id, "file": path, "name": name}
        else:
            return False
        try:
            got = await self.ctx.api.get(api)
            meta = got.get("api") if isinstance(got, dict) else None
            if not meta:
                return False  # 非 NapCat 适配器：没有此接口，回退卡片
            result = self._unwrap_api(await self.ctx.api.call(api, params=params))
            if isinstance(result, dict):
                status = str(result.get("status") or "").lower()
                retcode = result.get("retcode")
                if status in ("failed", "error") or (isinstance(retcode, int) and retcode != 0):
                    self.ctx.logger.warning("发送结果文件失败：%s", result)
                    return False
            return True
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.warning("发送结果文件失败：%s", exc)
            return False

    async def _notify_bot(self, state: _RunState, stream_id: str, full: bool = False) -> None:
        """把结果回灌进麦麦的上下文，让它知道任务已返回。"""
        final_text = state.final_copy().strip()
        if not final_text:
            return
        conf = self.config.dsh
        # 静默模式下墨是唯一出口，给足内容，避免它以为"被截断"而反复追问 dsh
        limit = int(conf.context_inject_chars) if full else int(getattr(conf, "silent_inject_chars", 6000))
        if len(final_text) > limit:
            body = final_text[:limit].rstrip() + "…（内容过长，已截断）"
        else:
            body = final_text
        try:
            await self.ctx.maisaka.context.append(
                stream_id,
                segments=[{"type": "text", "content": f"【dsh 任务完成】\n{body}"}],
                source_kind="dsh-bridge",
            )
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.warning("回灌结果给麦麦失败：%s", exc)

    async def _report(
        self,
        state: _RunState,
        reason: Optional[str],
        stream_id: str,
        ctx: Optional[dict[str, Any]] = None,
        full: bool = False,
        notify: bool = True,
        ws: Optional[str] = None,
    ) -> int:
        conf = self.config.dsh
        header = self._REASON_MSG.get(reason or "", "")
        final_text = state.final_copy().strip()
        # 默认静默：不发回执、不发转发，结果交给墨自己组织回复。
        # 只有任务里带 (all) 时才发完整过程；异常（失败/超时）仍提示一句，避免用户干等。
        use_forward = full and conf.display_mode.strip().lower() != "text"
        nodes = self._build_forward_nodes(state) if use_forward else []

        if full:
            if header:
                receipt = header
            elif nodes:
                receipt = "✅ dsh 任务完成（过程见转发）"
            else:
                receipt = final_text[: conf.max_output_chars] if final_text else "(dsh 无输出)"
            text_to_send = f"dsh：{receipt}"
        elif header:
            text_to_send = f"dsh：{header}"
        else:
            text_to_send = ""

        if text_to_send:
            try:
                await self.ctx.send.text(text_to_send, stream_id)
            except Exception:  # noqa: BLE001
                self.ctx.logger.warning("dsh 回执发送失败", exc_info=True)

        # 只在本地留一份文本转录（便于排查），不再发到聊天里。
        # 聊天里只发 dsh 自己通过 present 声明的交付文件（见下方 _send_deliverables）。
        try:
            self._write_result_file(state)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("写结果转录失败", exc_info=True)

        # 提问等待中先不回灌：让墨别抢着回答 dsh 自己的问题，答案要用户给
        if notify:
            try:
                await self._notify_bot(state, stream_id, full=full)
            except Exception:  # noqa: BLE001
                self.ctx.logger.warning("回灌结果失败", exc_info=True)

        # 交付物：present 声明的文件 → 打包/发送（NapCat 专属，非 NapCat 自动跳过）
        sent_deliv = 0
        try:
            sent_deliv = await self._send_deliverables(state, stream_id, ctx or {}, ws=ws)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("交付物处理失败", exc_info=True)

        if not nodes:
            return sent_deliv
        try:
            await self.ctx.send.forward(nodes, stream_id)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("dsh 转发发送失败", exc_info=True)
            # 兜底：转发失败时把最终答案补发出来（仅此一次，避免重复刷屏）
            if final_text:
                try:
                    await self.ctx.send.text(final_text[: conf.max_output_chars], stream_id)
                except Exception:  # noqa: BLE001
                    pass
        return sent_deliv

    def _collect_deliverables(self, state: _RunState, ws: str) -> list[str]:
        """从 dsh 事件流里收集 present 声明的交付文件（绝对路径，仅保留存在的）。"""
        paths: list[str] = []
        seen: set[str] = set()
        for evt in state.events_copy():
            if str(evt.get("type") or "") != "tool_call":
                continue
            tool = str(evt.get("tool") or evt.get("name") or "")
            if tool not in ("present", "present_deliverables"):
                continue
            inp = evt.get("input")
            if not isinstance(inp, dict):
                continue
            files = inp.get("files")
            if not isinstance(files, list):
                continue
            for f in files:
                p = f.get("path") if isinstance(f, dict) else (f if isinstance(f, str) else None)
                if not isinstance(p, str) or not p.strip():
                    continue
                cand = Path(p)
                if not cand.is_absolute():
                    cand = Path(ws) / p
                try:
                    cand_resolved = cand.resolve()
                    rp = str(cand_resolved)
                except Exception:  # noqa: BLE001
                    continue
                # 必须落在本流工作区内（防 ../ 或宿主任意路径被外发）
                try:
                    if not cand_resolved.is_relative_to(Path(ws).resolve()):
                        continue
                except Exception:  # noqa: BLE001
                    continue
                if "schedule-requests" in rp:
                    continue  # 定时任务请求文件不算交付物
                if rp in seen or not cand.is_file():
                    continue
                seen.add(rp)
                paths.append(rp)
        return paths

    async def _send_deliverables(
        self, state: _RunState, stream_id: str, ctx: dict[str, str], ws: Optional[str] = None
    ) -> int:
        """把 present 声明的交付物打包发送到流（单文件直发，多文件打 zip）。返回成功发送数。"""
        ws = ws or self._workspace(stream_id)
        files = self._collect_deliverables(state, ws)
        if not files:
            return 0
        limit = int(self.config.dsh.deliver_max_files)
        files = files[:limit]
        tmp_dir: Optional[str] = None
        sent = 0
        try:
            if len(files) == 1:
                send_list = files
            else:
                tmp_dir = tempfile.mkdtemp(prefix="dsh-deliver-")
                zip_path = os.path.join(tmp_dir, f"deliver-{time.strftime('%Y%m%d-%H%M%S')}.zip")
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
                    for f in files:
                        z.write(f, arcname=Path(f).name)
                send_list = [zip_path]
            sent = 0
            for f in send_list:
                if await self._send_result_file(f, ctx):
                    sent += 1
            if sent:
                names = "、".join(Path(f).name for f in files)
                await self.ctx.send.text(f"📦 dsh 产出文件（{len(files)} 个）：{names}", stream_id)
        finally:
            if tmp_dir:
                shutil.rmtree(tmp_dir, ignore_errors=True)
        return sent

    # 救回（超时/异常终止）时扫工作区：跳过的目录与可外发的扩展名
    _SALVAGE_SKIP_DIRS = {
        "uploads",
        "scripts",
        "build",
        "ipc",
        "ask-requests",
        "schedule-requests",
        "node_modules",
        "__pycache__",
        ".git",
        ".venv",
        "results",
    }
    _SALVAGE_EXTS = {
        ".docx",
        ".doc",
        ".xlsx",
        ".xls",
        ".pptx",
        ".ppt",
        ".pdf",
        ".csv",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".zip",
        ".md",
        ".txt",
        ".html",
    }

    def _salvage_artifacts(self, ws: str, since: float) -> list[str]:
        """扫描本轮（mtime >= since）在工作区顶层与常规子目录里新产出的文件，用于超时/异常救回。"""
        base = Path(ws)
        found: list[tuple[float, str]] = []
        try:
            for p in base.rglob("*"):
                try:
                    rel = p.relative_to(base)
                except Exception:  # noqa: BLE001
                    continue
                parts = rel.parts
                if len(parts) > 4:
                    continue
                if any(
                    part in self._SALVAGE_SKIP_DIRS or part.startswith(".dsh")
                    for part in parts[:-1]
                ):
                    continue
                if rel.name == "AGENTS.md" or rel.name.startswith("."):
                    continue
                if p.suffix.lower() not in self._SALVAGE_EXTS or not p.is_file():
                    continue
                try:
                    st = p.stat()
                except Exception:  # noqa: BLE001
                    continue
                if st.st_mtime < since - 2:
                    continue
                if st.st_size > int(self.config.dsh.salvage_max_mb) * 1024 * 1024:
                    continue
                found.append((st.st_mtime, str(p.resolve())))
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("扫描救回文件失败", exc_info=True)
        found.sort()
        limit = int(self.config.dsh.deliver_max_files)
        return [path for _, path in found[-limit:]]

    async def _send_salvage(
        self, paths: list[str], stream_id: str, ctx: dict[str, str], reason: Optional[str]
    ) -> int:
        """把超时/异常终止时救回的文件发到流。"""
        if not paths:
            return 0
        why = {
            "timeout": "任务到时间上限了，被终止",
            "stalled": "进程长时间没输出，被判死锁终止",
            "loop": "输出陷入循环，被终止",
            "error": "执行异常中断",
            "stop": "你让它停下了",
        }.get(reason or "", "任务异常终止")
        names = "、".join(Path(p).name for p in paths)
        sent_paths: list[str] = []
        for p in paths:
            try:
                if await self._send_result_file(p, ctx):
                    sent_paths.append(p)
            except Exception:  # noqa: BLE001
                self.ctx.logger.warning("发送救回文件 %s 失败", p, exc_info=True)
        try:
            if sent_paths:
                await self.ctx.send.text(
                    f"📎 {why}，但工作区里已产出这些文件，先发给你："
                    + "、".join(Path(p).name for p in sent_paths),
                    stream_id,
                )
            else:
                await self.ctx.send.text(
                    f"📎 {why}；工作区里已产出：{names}"
                    "（我这边没能把文件发出来，要看直接跟我说「把 <文件名> 发我」）",
                    stream_id,
                )
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("发送救回提示失败", exc_info=True)
        return len(sent_paths)

    def _scan_plugin_dirs(self, ws: str) -> set[str]:
        """扫描工作目录，返回含 _manifest.json 的插件目录集合。"""
        ws = Path(ws)
        found: set[str] = set()
        try:
            for d in ws.iterdir():
                if d.is_dir() and (d / "_manifest.json").is_file():
                    found.add(str(d.resolve()))
        except Exception:  # noqa: BLE001
            pass
        return found

    async def _submit_new_plugins(self, before: set[str], ws: str, stream_id: str) -> None:
        """将本次任务新增的插件目录提交给安装审批器。"""
        after = self._scan_plugin_dirs(ws)
        for path in sorted(after - before):
            try:
                res = await self.ctx.api.call(
                    "ji-or-ji.maibot-plugin-installer.submit",
                    plugin_dir=path,
                    stream_id=stream_id,
                )
                self.ctx.logger.info("已提交待审插件 %s -> %s", path, res)
            except Exception as exc:  # noqa: BLE001
                self.ctx.logger.warning("提交待审插件 %s 失败：%s", path, exc)

    def _parse_plan(self, text: str) -> tuple[str, Optional[int], str]:
        """从规划轮输出里提取计划、dsh 自定的时长、话题名。"""
        plan = ""
        seconds: Optional[int] = None
        title = ""
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            try:
                obj = json.loads(m.group(0))
                if isinstance(obj, dict):
                    plan = str(obj.get("plan") or "").strip()
                    title = str(obj.get("title") or "").strip()
                    raw = obj.get("max_seconds") or obj.get("seconds") or obj.get("timeout_seconds")
                    if isinstance(raw, (int, float)):
                        seconds = int(raw)
            except Exception:  # noqa: BLE001
                pass
        if not plan:
            plan = text[:500].strip()
        return plan, seconds, title

    async def _plan_task(self, task: str, ws: str) -> tuple[str, str]:
        """第一轮：让 dsh 解析意图、给出操作计划，并顺带起一个话题名（不要求它估时长——它估不准）。"""
        prompt = (
            "你是任务规划器，只做规划、不执行。先分析下面这个任务要做什么，"
            "给出简洁的操作计划，并给这次工作起一个 12 字以内的话题名。"
            "只输出一个 JSON 对象，"
            '格式：{"plan": "操作计划", "title": "话题名"}，不要输出其它内容。'
            f"\n\n任务：{task}"
        )
        # 规划轮只是个小前置动作：给它一个内部小上限，避免极小概率卡住整条链
        plan_timeout = self._PLAN_SECONDS
        try:
            _reason, state = await self._execute(prompt, ws, plan_timeout)
            plan, _secs, title = self._parse_plan(state.final_copy().strip())
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("规划轮失败，跳过规划", exc_info=True)
            plan, title = "", ""
        self.ctx.logger.info("dsh 规划轮：话题名=%s，计划=%.120s", title or "-", plan)
        return plan, title

    async def _run_and_report(
        self,
        task: str,
        stream_id: str,
        timeout: int,
        ctx: Optional[dict[str, Any]] = None,
        full: bool = False,
        session_id: str = "",
        resume: bool = False,
        depth: int = 0,
        topic_id: Optional[int] = None,
    ) -> None:
        self._current_task = task
        ctx_map = ctx or {}
        group_id = str(ctx_map.get("group_id") or "")
        # 话题模式下：一个话题 = 一个目录 + 一条持久会话
        topic = self._resolve_topic(stream_id, group_id, topic_id)
        ws = str(self._topic_dir(topic)) if topic else self._workspace(stream_id)
        if not session_id and topic:
            session_id = str(topic.get("session_id") or "")
        self._ipc_seen.clear()
        before = self._scan_plugin_dirs(ws)
        prompt = self._augment_task(task, ws)
        title = ""
        timeout = self._clamp_timeout(timeout)
        self._stop_requested = False
        if resume:
            # 续跑不重新规划：用户已经回答，直接接着上次的会话往下做
            plan = ""
        else:
            # 第一轮：只问「怎么做 + 起个名」；不问时长（模型估不准，改由用户 /dsh stop 控制）
            plan, title = await self._plan_task(task, ws)
            if self._stop_requested:
                # 规划阶段就被叫停：不进入正式执行
                self._running = False
                self._last_run = {
                    "task": task,
                    "reason": "stop",
                    "elapsed": time.time() - self._run_started,
                    "label": self._topic_label(topic) if topic else "",
                }
                await self.ctx.send.text("🛑 已按你的要求停下（还停在规划阶段，没开始干活）。", stream_id)
                return
        # 运行期快照（供 /dsh peek 读取）
        self._run_label = self._topic_label(topic) if topic else ""
        self._run_ws = ws
        self._run_task_text = task
        self._run_budget = timeout
        self._run_started = time.time()
        watcher = asyncio.create_task(self._watch_ipc_questions(ws, stream_id, ctx_map))
        run_started = self._run_started
        try:
            reason, state = await self._execute(prompt, ws, timeout, session_id=session_id)
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.error("dsh 任务执行异常: %s", exc, exc_info=True)
            state = _RunState()
            state.events.append({"type": "raw", "text": f"❌ 执行异常：{exc}"})
            reason = "error"
        finally:
            watcher.cancel()
            try:
                await watcher
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._ipc_waiting = False
            pend = self._pending.get(stream_id)
            if pend and pend.get("mode") == "ipc":
                self._pending.pop(stream_id, None)
                self._save_pending()
            self._running = False
            self._run_state = None
        elapsed = time.time() - run_started
        self._last_run = {"task": task, "reason": reason, "elapsed": elapsed, "label": self._run_label}
        # 话题记账：记会话 id、刷新使用时间、首任务用规划轮的标题命名
        if topic:
            if state.session_id:
                topic["session_id"] = state.session_id
            self._touch_topic(topic)
            self._maybe_name_topic(topic, title)
            self.ctx.logger.info(
                "话题 %s 任务结束（会话 %s）", self._topic_label(topic), (state.session_id or "-")[:28]
            )
        # 判定真实结果：文本里出现凭据/鉴权类错误时，不能算成功
        if reason in (None, "") and self._looks_failed(state.final_copy()):
            reason = "failed"
        # 会话被 Web GUI 占着（写句柄冲突）时给一句人话，否则用户只看到"执行失败"
        if "active write handle" in state.final_copy().lower():
            self.ctx.logger.warning("dsh 会话被 Web GUI 占用，无法续跑")
            try:
                await self.ctx.send.text(
                    "🔒 这个话题的 dsh 会话正被 Web GUI 打开着，这边没法续跑；"
                    "先在 Web GUI 里关掉这个话题的会话，再重新派活。",
                    stream_id,
                )
            except Exception:  # noqa: BLE001
                pass
        self._record_history(task, reason, state)
        self._current_task = ""
        # dsh 如果写了 ask-requests/*.json，就发问题、等回答、稍后续跑
        ask = None
        if self.config.dsh.ask_enabled and not reason:
            try:
                ask = self._take_ask_request(stream_id)
            except Exception:  # noqa: BLE001
                self.ctx.logger.warning("读取 ask-requests 失败", exc_info=True)
                ask = None
        waiting = bool(ask and state.session_id and depth < self.config.dsh.ask_max_rounds)
        sent_deliv = 0
        try:
            sent_deliv = await self._report(state, reason, stream_id, ctx, full=full, notify=not waiting, ws=ws)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("dsh 结果回传失败", exc_info=True)
        # 超时/异常终止且 dsh 没来得及 present：把本轮新产出的文件救回来发出去，别让用户白等
        if (
            reason in ("timeout", "stalled", "loop", "error", "stop")
            and not sent_deliv
            and self.config.dsh.salvage_on_fail
        ):
            try:
                rescued = self._salvage_artifacts(ws, run_started)
                if rescued:
                    n = await self._send_salvage(rescued, stream_id, ctx_map, reason)
                    self.ctx.logger.info("救回 %d 个文件（原因 %s）", n, reason)
            except Exception:  # noqa: BLE001
                self.ctx.logger.warning("救回产出文件失败", exc_info=True)
        if waiting:
            try:
                await self._start_ask(ask, state, stream_id, ctx, full, timeout, depth)
            except Exception:  # noqa: BLE001
                self.ctx.logger.warning("发送 dsh 提问失败", exc_info=True)
        elif ask:
            try:
                await self.ctx.send.text(
                    "❓ dsh 想再问你一次，但这次不能续跑（"
                    + ("已达到提问轮数上限" if state.session_id else "没拿到会话 id")
                    + "），这轮就到这里。",
                    stream_id,
                )
            except Exception:  # noqa: BLE001
                pass
        elif resume and not reason:
            # 续跑是用户亲手回答后触发的，他正等着结果：这里直接把 dsh 的最终回答发出来，
            # 不再只回灌给墨（否则聊天里可能一直没动静）。
            final_text = state.final_copy().strip()
            if final_text:
                try:
                    await self.ctx.send.text("✅ dsh 继续跑完了：\n" + final_text[:2000], stream_id)
                except Exception:  # noqa: BLE001
                    pass
        try:
            await self._submit_new_plugins(before, ws, stream_id)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("提交新插件失败", exc_info=True)
        # dsh 若在 schedule-requests/ 写下了定时任务请求，建任务（幂等）
        try:
            await self._process_schedule_requests(stream_id, ctx or self._extract_ctx({}))
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("处理定时任务请求失败", exc_info=True)

    @staticmethod
    def _looks_failed(text: str) -> bool:
        """从 dsh 输出里识别"看起来成功、实际失败"的情况（主要是缺少凭据）。"""
        low = (text or "").lower()
        # 只认 dsh 自己抛出的明确错误码；普通报告里提到 "api key" 不算失败
        markers = (
            "missing_credential",
            "missing credential",
            "invalid_api_key",
            "authentication failed",
            "active write handle",
        )
        return any(m in low for m in markers)

    def _record_history(self, task: str, reason: Optional[str], state: "_RunState") -> None:
        final = state.final_copy().strip()
        ok = reason not in ("failed", "timeout", "stalled", "loop", "spawn-error", "stop")
        self._history.append(
            {
                "ts": time.strftime("%m-%d %H:%M:%S"),
                "task": task[:120],
                "ok": ok,
                "reason": reason or ("ok" if ok else "failed"),
                "summary": final[:300],
            }
        )
        del self._history[:-10]

    def _credential_state(self) -> str:
        conf = self.config.dsh
        if str(conf.api_key or "").strip():
            return "已配置（插件配置 api_key）"
        env_name = str(conf.api_key_env or "DEEPSEEK_API_KEY").strip() or "DEEPSEEK_API_KEY"
        if str(os.environ.get(env_name, "")).strip():
            return f"已配置（环境变量 {env_name}）"
        # dsh 自己有凭据库（~/.dsh/.credentials.yaml），与麦麦进程的环境变量无关
        try:
            cred = Path.home() / ".dsh" / ".credentials.yaml"
            if cred.is_file() and env_name in cred.read_text(encoding="utf-8", errors="ignore"):
                return f"已配置（dsh 凭据库 {cred}）"
        except Exception:  # noqa: BLE001
            pass
        return f"未配置（dsh 需先登录，或提供 {env_name} 环境变量 / 插件 api_key）"

    _ALL_RE = re.compile(r"[（(]\s*all\s*[)）]", re.IGNORECASE)

    @classmethod
    def _wants_full(cls, task: str) -> bool:
        """任务里带 (all) / （all） 时，才发回执与完整转发。"""
        return bool(cls._ALL_RE.search(task or ""))

    @classmethod
    def _strip_all(cls, task: str) -> str:
        return cls._ALL_RE.sub("", task or "").strip()

    def _reject_busy(self) -> bool:
        return self._running

    # ── 命令：/dsh（任务 + 全部话题指令都收在 /dsh 下） ─────────

    _DSH_HELP = (
        "🛠 /dsh 用法（话题指令都在 /dsh 下）\n"
        "  /dsh <任务>                     在当前话题派活（没有话题会自动建；默认不限时）\n"
        "  /dsh #3 <任务>                  指定话题派活\n"
        "  /dsh peek | 状态 | 进展          看它现在在干什么\n"
        "  /dsh stop [全部]                 停止当前任务（全部=连排队的一起清掉）\n"
        "  /dsh new | 新建 | est            新建话题\n"
        "  /dsh set <名> | 修改话题 <名>     改当前话题名\n"
        "  /dsh del [#3] | 删除话题 [#3]     归档（7 天后自动删除，可 restore）\n"
        "  /dsh restore #3 | 恢复 #3         恢复归档话题\n"
        "  /dsh list | 话题 [#3]            列出 / 切换话题\n"
        "  /dsh help                       显示这段帮助\n"
        "\n任务末尾加 (all) 可看完整合并转发过程；以 -- 开头的正文强制当任务（如 /dsh -- new 功能调研）\n"
        "想给单次任务设时间上限：/dsh --timeout 600 <任务>（默认不设上限）"
    )

    _STOP_RE = re.compile(r"^(?:stop|停止|停下|别做了)(?:\s+(?P<arg>.+))?$", re.IGNORECASE)
    _PEEK_RE = re.compile(
        r"^(?:peek|status|进展|进度|状态|在干嘛|在干什么|现在在干什么|看看进展)$", re.IGNORECASE
    )

    def _dispatch_control(self, body: str, stream_id: str) -> Optional[str]:
        """始终可用的控制指令（实验模式窗口内也能用）：停止 / 看进展。"""
        raw = body.strip()
        m = self._STOP_RE.match(raw)
        if m:
            return self._cmd_stop(stream_id, (m.group("arg") or "").strip())
        if self._PEEK_RE.match(raw):
            return self._cmd_peek(stream_id)
        return None

    def _cmd_stop(self, stream_id: str, arg: str = "") -> str:
        """让正在跑的任务停下；arg 为「全部/all/队列」时连排队任务一起清掉。"""
        clear_queue = arg.strip().lower() in ("全部", "all", "队列", "queue", "所有")
        cleared = 0
        if clear_queue and self._queue:
            cleared = len(self._queue)
            self._queue.clear()
        if not self._running:
            if cleared:
                return f"💤 现在没有在跑的任务；已清掉 {cleared} 个排队任务。"
            if self._queue:
                return (
                    f"💤 现在没有在跑的任务，但有 {len(self._queue)} 个排队任务；"
                    "要一起清掉发 /dsh stop 全部"
                )
            return "💤 现在没有在跑的任务。"
        self._stop_requested = True
        pid = self._current_pid
        self.ctx.logger.info("收到 /dsh stop（pid=%s, clear_queue=%s）", pid, clear_queue)
        tail = f"；排队中的 {cleared} 个也一起清掉了" if cleared else ""
        return f"🛑 收到，正在停止当前任务…（通常几秒内结束，产出文件会尽量发给你）{tail}"

    def _cmd_peek(self, stream_id: str) -> str:
        """看 dsh 现在在干什么（运行中给实时快照；空闲给最近一次结果）。"""
        queue_note = f"（另有 {len(self._queue)} 个排队）" if self._queue else ""
        state = self._run_state
        if not self._running or state is None:
            lines = [f"😴 现在没在跑任务{queue_note}。"]
            last = self._last_run or {}
            if last:
                reason = last.get("reason")
                flag = "成功" if not reason else f"结束（{reason}）"
                lines.append(
                    f"最近一次：{str(last.get('task') or '')[:50]} · {flag} · 用时 {self._dur_text(last.get('elapsed') or 0)}"
                )
            return "\n".join(lines)

        elapsed = time.time() - self._run_started
        events = state.events_copy()
        turn = 0
        step = 0
        tool = ""
        tool_arg = ""
        recent: list[str] = []
        last_ts = self._run_started
        for evt in events:
            kind = str(evt.get("type") or "")
            ts = float(evt.get("_ts") or 0)
            if ts:
                last_ts = ts
            if kind == "status" and str(evt.get("phase") or "") == "step_start":
                step = int(evt.get("step") or step or 0)
                turn = int(evt.get("turn") or turn or 0)
            elif kind == "tool_call":
                tool = str(evt.get("tool") or evt.get("name") or "")
                tool_arg = self._tool_summary(evt.get("input"))
            elif kind in ("text", "raw", "final", "result"):
                text = self._strip_tags(str(evt.get("text") or "")).strip()
                if text:
                    recent.append(text.replace("\n", " ")[:80])
        if recent:
            recent = recent[-3:]

        label = f"{self._run_label} " if self._run_label else ""
        lines = [f"👀 {label}正在干活：已 {self._dur_text(elapsed)}"]
        where = f"第 {turn} 轮第 {step} 步" if step else "刚起步"
        lines.append(f"进度：{where} · 最后动静 {self._dur_text(max(0.0, time.time() - last_ts))}前")
        if self._ipc_waiting:
            lines.append("状态：⏳ 在等你回答它的提问（回一句话就是答案）")
        elif tool:
            lines.append(f"现在在跑：{tool}" + (f" · {tool_arg}" if tool_arg else ""))
        if recent:
            lines.append("最近动静：")
            lines.extend(f"  · {r}" for r in recent)
        lines.append(f"预算：{self._budget_text(self._run_budget)}")
        return "\n".join(lines)

    @staticmethod
    def _dur_text(seconds: Any) -> str:
        try:
            sec = max(0.0, float(seconds))
        except Exception:  # noqa: BLE001
            return "-"
        if sec < 60:
            return f"{int(sec)} 秒"
        if sec < 3600:
            return f"{int(sec // 60)} 分 {int(sec % 60)} 秒"
        return f"{int(sec // 3600)} 小时 {int((sec % 3600) // 60)} 分"

    @Command(
        "dsh",
        description="把任务外包给 DeepSeek Harness (dsh)；话题管理也收在 /dsh 下（new/set/del/restore/list/help）",
        pattern=r"^/dsh(?:\s+(?P<body>.+))?$",
    )
    async def handle_dsh(self, stream_id: str = "", **kwargs: Any):
        if not self.config.plugin.enabled:
            return False, "插件未启用", True
        if not self._is_allowed(kwargs):
            await self.ctx.send.text("你没有权限使用 /dsh", stream_id)
            return False, "无权限", True

        if stream_id:
            self._last_chat_id = stream_id
        body = ""
        matched = kwargs.get("matched_groups")
        if isinstance(matched, dict):
            body = str(matched.get("body") or "").strip()
        if not body:
            body = self._cmd_body(kwargs)

        # 控制指令（stop / peek）：始终可用，实验模式窗口内也不禁用
        ctrl_text = self._dispatch_control(body, stream_id)
        if ctrl_text is not None:
            await self.ctx.send.text(ctrl_text, stream_id)
            return True, "控制指令", True

        # 实验功能优先：开启/确认/关闭/状态
        exp_text = await self._dispatch_experimental(body, stream_id, kwargs)
        if exp_text is not None:
            await self.ctx.send.text(exp_text, stream_id)
            return True, "实验功能", True

        # 实验窗口内：/dsh 命令临时禁用（只留实验关闭/状态，上面已处理）
        if self._agent_armed():
            await self.ctx.send.text(
                f"🧪 实验模式已开启（剩余 {self._agent_remaining_text()}），/dsh 命令暂时禁用"
                "（/dsh stop、/dsh peek 仍可用）。\n"
                "直接跟墨说要做什么即可（它会自己建/切话题、派活）；要关闭发 /dsh 实验关闭。",
                stream_id,
            )
            return True, "实验模式", True

        # 帮助
        if not body or body.strip().lower() in ("help", "帮助", "?"):
            await self.ctx.send.text(self._DSH_HELP, stream_id)
            return True, "帮助", True

        # `-- ` 前缀：强制当任务（避免任务正文碰巧以 new/del 开头被误判）
        force_task = body.startswith("-- ")
        if force_task:
            body = body[3:].strip()

        # 话题子指令
        if not force_task:
            text = self._dispatch_topic_command(body, stream_id, kwargs)
            if text is not None:
                await self.ctx.send.text(text, stream_id)
                return True, "话题指令", True

        # 任务：解析 --timeout / #N
        timeout: Optional[int] = None
        cmd_full = self._wants_full(body)
        if cmd_full:
            body = self._strip_all(body)
        mt = re.match(r"^--timeout\s+(\d+)\s+(.+)$", body, re.DOTALL)
        if mt:
            timeout = int(mt.group(1))
            body = mt.group(2).strip()

        topic_id: Optional[int] = None
        mt_topic = re.match(r"^#\s*(\d+)\s+(.+)$", body, re.DOTALL)
        if mt_topic:
            topic_id = int(mt_topic.group(1))
            body = mt_topic.group(2).strip()

        task_ctx = self._extract_ctx(kwargs)
        group_id = str(task_ctx.get("group_id") or "")
        topic: Optional[dict[str, Any]] = None
        if self._topic_mode_for(stream_id, group_id):
            if topic_id:
                topic = self._topics.get(topic_id)
                if not topic or stream_id not in (topic.get("chats") or []):
                    await self.ctx.send.text(f"没有 #{topic_id} 这个话题（/dsh list 看现有话题）", stream_id)
                    return False, "无此话题", True
                if topic.get("archived_at"):
                    await self.ctx.send.text(f"#{topic_id} 已归档，先 /dsh restore #{topic_id} 才能继续", stream_id)
                    return False, "已归档", True
                self._topic_current[stream_id] = topic_id
                self._save_topics()
            else:
                topic = self._get_or_create_topic(stream_id)

        to = self._clamp_timeout(timeout)
        label = f"{self._topic_label(topic)} " if topic else ""

        if self._reject_busy():
            item = {
                "task": body,
                "stream_id": stream_id,
                "timeout": to,
                "ctx": task_ctx,
                "full": cmd_full,
                "topic_id": topic["id"] if topic else None,
            }
            if len(self._queue) < int(self.config.topic.queue_max):
                self._queue.append(item)
                await self.ctx.send.text(
                    f"🚦 {label}dsh 正在忙，任务已排队（第 {len(self._queue)} 位）", stream_id
                )
                return True, "已排队", True
            await self.ctx.send.text("已有 dsh 任务在执行，队列也满了，请稍后再试。", stream_id)
            return False, "忙碌", True

        if self._drop_pending(stream_id):
            await self.ctx.send.text("（之前 dsh 的问题已作废，按这条新指令走）", stream_id)

        self._running = True
        await self.ctx.send.text(f"🛠 {label}已受理（{self._budget_text(to)}）：{body[:80]}", stream_id)
        asyncio.create_task(
            self._run_and_report(
                body,
                stream_id,
                to,
                task_ctx,
                full=cmd_full,
                topic_id=topic["id"] if topic else None,
            )
        )
        return True, "已提交", True

    # ── 话题子指令（由 /dsh 分发；全部返回要发的文案） ─────────

    @staticmethod
    def _cmd_body(kwargs: dict[str, Any]) -> str:
        matched = kwargs.get("matched_groups")
        if isinstance(matched, dict):
            return str(matched.get("body") or "").strip()
        return ""

    def _topic_cmd_guard(self, stream_id: str, kwargs: dict[str, Any]) -> Optional[str]:
        """话题指令的通用前置检查；返回错误文案，None 表示通过。"""
        group_id = str(self._extract_ctx(kwargs).get("group_id") or "")
        if not self.config.topic.enabled:
            return "话题功能已在插件配置里关闭（topic.enabled=false）"
        if not self._topic_mode_for(stream_id, group_id):
            return "话题功能只在私聊启用"
        return None

    def _dispatch_topic_command(
        self, body: str, stream_id: str, kwargs: dict[str, Any]
    ) -> Optional[str]:
        """识别 /dsh 的话题子指令；不是子指令返回 None（当任务处理）。"""
        raw = body.strip()
        low = raw.lower()
        if low in ("new", "est") or raw in ("新建", "创建会话"):
            return self._cmd_new_topic(stream_id, kwargs)
        m = re.match(r"^(?:set|修改话题)\s+(.+)$", raw, re.DOTALL)
        if m:
            return self._cmd_rename_topic(stream_id, kwargs, m.group(1).strip())
        m = re.match(r"^(?:del(?:ete)?|删除话题)(?:\s+(.+))?$", raw, re.DOTALL)
        if m:
            return self._cmd_archive_topic(stream_id, kwargs, (m.group(1) or "").strip())
        m = re.match(r"^(?:restore|恢复)\s+(.+)$", raw, re.DOTALL)
        if m:
            return self._cmd_restore_topic(stream_id, kwargs, m.group(1).strip())
        m = re.match(r"^(?:list|topics?|话题)(?:\s+(.+))?$", raw, re.DOTALL)
        if m:
            return self._cmd_list_topics(stream_id, kwargs, (m.group(1) or "").strip())
        return None

    def _cmd_new_topic(self, stream_id: str, kwargs: dict[str, Any]) -> str:
        err = self._topic_cmd_guard(stream_id, kwargs)
        if err:
            return err
        topic = self._create_topic(stream_id)
        return (
            f"🗒 已创建 {self._topic_label(topic)}；之后的任务会落在这个话题"
            f"（可用 /dsh set <新名字> 改名）"
        )

    def _cmd_rename_topic(self, stream_id: str, kwargs: dict[str, Any], name: str) -> str:
        err = self._topic_cmd_guard(stream_id, kwargs)
        if err:
            return err
        if not name:
            return "用法：/dsh set <新名字> 或 /dsh 修改话题 <新名字>"
        topic = self._current_topic(stream_id)
        if not topic or topic.get("archived_at"):
            return "当前没有可用话题；先发 /dsh <任务> 自动建一个，或 /dsh new"
        clean = re.sub(r"\s+", " ", name).strip().strip("「」\"'")
        topic["name"] = clean[: max(4, int(self.config.topic.name_max_chars))]
        self._save_topics()
        return f"✏️ {self._topic_label(topic)} 已改名"

    def _cmd_archive_topic(self, stream_id: str, kwargs: dict[str, Any], spec: str) -> str:
        err = self._topic_cmd_guard(stream_id, kwargs)
        if err:
            return err
        topic = self._find_topic(stream_id, spec) if spec else self._current_topic(stream_id)
        if not topic:
            return "没找到这个话题（/dsh list 看现有话题）"
        if topic.get("archived_at"):
            return f"{self._topic_label(topic)} 已经归档了（/dsh restore #{topic['id']} 可恢复）"
        if self._running and int(topic.get("id") or 0) == int(self._topic_current.get(stream_id) or 0):
            return "dsh 正在这个话里干活，等它结束再归档。"
        self._archive_topic(topic)
        keep = int(self.config.topic.archive_keep_days)
        return (
            f"🗄 {self._topic_label(topic)} 已归档，{keep} 天后自动删除；"
            f"期间可用 /dsh restore #{topic['id']}"
        )

    def _cmd_restore_topic(self, stream_id: str, kwargs: dict[str, Any], spec: str) -> str:
        err = self._topic_cmd_guard(stream_id, kwargs)
        if err:
            return err
        if not spec:
            return "用法：/dsh restore #3 或 /dsh 恢复 #3"
        topic = self._find_topic(stream_id, spec)
        if not topic:
            return "没找到这个话题（/dsh list 看现有话题）"
        if not topic.get("archived_at"):
            return f"{self._topic_label(topic)} 本来就在用，不需要恢复"
        self._restore_topic(topic)
        return f"♻️ {self._topic_label(topic)} 已恢复，之后的任务会继续用它。"

    def _cmd_list_topics(self, stream_id: str, kwargs: dict[str, Any], spec: str) -> str:
        err = self._topic_cmd_guard(stream_id, kwargs)
        if err:
            return err
        if spec:
            topic = self._find_topic(stream_id, spec)
            if not topic:
                return "没找到这个话题（/dsh list 看现有话题）"
            if topic.get("archived_at"):
                return f"📦 {self._topic_label(topic)} 已归档，先 /dsh restore #{topic['id']} 才能继续用。"
            self._topic_current[stream_id] = int(topic["id"])
            self._save_topics()
            files = 0
            up = self._workspace_root() / str(topic.get("dir") or "") / UPLOAD_DIR_NAME
            if up.is_dir():
                files = len([x for x in up.iterdir() if x.is_file() and x.name != UPLOAD_INDEX_NAME])
            return (
                f"📌 已切到 {self._topic_label(topic)}\n"
                f"会话：{str(topic.get('session_id') or '未建立（下个任务建）')[:24]}\n"
                f"目录：{topic.get('dir')}\n"
                f"附件：{files} 个 · 最后使用 {self._age_text(topic.get('last_used_at') or 0)}"
            )
        items = sorted(self._topics_of_chat(stream_id), key=lambda t: int(t.get("id") or 0))
        current = self._topic_current.get(stream_id)
        lines = [f"📚 话题列表（当前 {self._topic_label(self._topics.get(int(current))) if current else '无'}）"]
        for t in items:
            if t.get("archived_at"):
                left = int(self.config.topic.archive_keep_days * 86400 - (time.time() - float(t["archived_at"])))
                lines.append(f"#{t['id']}（{t['name']}）· 已归档，{max(0, left // 86400)} 天后删除")
                continue
            mark = " ← 当前" if current and int(t["id"]) == int(current) else ""
            lines.append(
                f"#{t['id']}（{t['name']}）· 最后使用 {self._age_text(t.get('last_used_at') or 0)}{mark}"
            )
        if len(lines) == 1:
            lines.append("还没有话题：/dsh <任务> 会自动建一个，或 /dsh new")
        return "\n".join(lines)

    # ── 工具：dsh_run（供 Planner 自主下单） ──────────────────

    @Tool(
        "dsh_run",
        description=(
            "把较重任务外包给 dsh 在服务器执行（写插件 / 跑脚本 / 深度调研）。"
            "默认不限时（用户可用 /dsh stop 随时停）；只有确实需要设上限时才填 timeout_seconds。"
            "仅当任务明显超出即时聊天能力时使用。"
            "默认结果只回给你（由你组织回复，不发聊天记录）；若用户明确要「看全过程」，在 task 末尾加 (all)。"
        ),
        visibility="visible",
        parameters=[
            ToolParameterInfo(
                name="task",
                param_type=ToolParamType.STRING,
                description="要 dsh 完成的任务描述",
                required=True,
            ),
            ToolParameterInfo(
                name="timeout_seconds",
                param_type=ToolParamType.INTEGER,
                description="可选：本次任务的时间上限（秒）；省略=不限时",
                required=False,
            ),
            ToolParameterInfo(
                name="topic",
                param_type=ToolParamType.STRING,
                description="可选：指定话题（#3 或话题名）；省略=当前话题",
                required=False,
            ),
        ],
    )
    async def _tool_dsh_run(self, **kwargs: Any) -> dict[str, Any]:
        raw_task = str(kwargs.get("task") or "").strip()
        full = self._wants_full(raw_task)
        task = self._strip_all(raw_task) if full else raw_task
        stream_id = str(kwargs.get("stream_id") or "").strip()
        raw_timeout = kwargs.get("timeout_seconds")

        if not self.config.plugin.enabled:
            return {"success": False, "error": "插件未启用"}
        if self.config.permission.enforce_tools and not self._is_allowed(kwargs):
            return {"success": False, "error": "无权限使用 dsh_run"}
        if not task:
            return {"success": False, "error": "缺少 task"}
        if self._reject_busy():
            return {"success": False, "error": "已有 dsh 任务在执行"}

        # 新任务开始：之前那个没回答的提问作废
        self._drop_pending(stream_id)

        task_ctx = self._extract_ctx(kwargs)
        group_id = str(task_ctx.get("group_id") or "")
        topic: Optional[dict[str, Any]] = None
        if self._topic_mode_for(stream_id, group_id):
            topic_spec = str(kwargs.get("topic") or "").strip()
            if topic_spec:
                topic = self._find_topic(stream_id, topic_spec)
                if not topic:
                    return {"success": False, "error": f"没找到话题 {topic_spec}（可用 dsh_topic_list 看现有话题）"}
                if topic.get("archived_at"):
                    return {"success": False, "error": f"{self._topic_label(topic)} 已归档，先恢复（dsh_topic_restore）"}
                self._topic_current[stream_id] = int(topic["id"])
                self._save_topics()
            else:
                topic = self._get_or_create_topic(stream_id)
        label = f"{self._topic_label(topic)} " if topic else ""

        try:
            to = self._clamp_timeout(int(raw_timeout)) if raw_timeout not in (None, "") else 0
        except Exception:  # noqa: BLE001
            to = 0
        self._running = True
        await self.ctx.send.text(f"🛠 {label}已受理（{self._budget_text(to)}）：{task[:80]}", stream_id)
        asyncio.create_task(
            self._run_and_report(
                task,
                stream_id,
                to,
                task_ctx,
                full=full,
                topic_id=topic["id"] if topic else None,
            )
        )
        return {
            "success": True,
            "accepted": True,
            "timeout_seconds": to,
            "topic": self._topic_label(topic) if topic else None,
        }

    # ── 工具：实验功能（墨自主调度，仅在 24h 窗口内可用） ─────────

    @Tool(
        "dsh_topic_list",
        description="【实验】列出当前私聊的 dsh 话题（编号/名字/最后使用/当前）。需要实验功能已开启。",
        visibility="visible",
        parameters=[],
    )
    async def _tool_topic_list(self, **kwargs: Any) -> dict[str, Any]:
        gate = self._agent_tool_gate(kwargs)
        if gate:
            return gate
        stream_id = str(kwargs.get("stream_id") or "")
        return {
            "success": True,
            "content": self._cmd_list_topics(stream_id, kwargs, ""),
            "topic": self._topic_label(self._current_topic(stream_id)),
        }

    @Tool(
        "dsh_topic_new",
        description="【实验】新建一个 dsh 话题并切过去（用户说“开个新话题/换个方向”时用）。",
        visibility="visible",
        parameters=[
            ToolParameterInfo(name="name", param_type=ToolParamType.STRING, description="话题名（≤12 字，可省略）", required=False)
        ],
    )
    async def _tool_topic_new(self, name: str = "", **kwargs: Any) -> dict[str, Any]:
        gate = self._agent_tool_gate(kwargs)
        if gate:
            return gate
        stream_id = str(kwargs.get("stream_id") or "")
        topic = self._create_topic(stream_id, name=(str(name or "").strip() or "新话题"))
        return {"success": True, "content": f"已新建 {self._topic_label(topic)}", "topic": self._topic_label(topic)}

    @Tool(
        "dsh_topic_rename",
        description="【实验】给当前话题改名。",
        visibility="visible",
        parameters=[
            ToolParameterInfo(name="name", param_type=ToolParamType.STRING, description="新话题名（≤12 字）", required=True)
        ],
    )
    async def _tool_topic_rename(self, name: str = "", **kwargs: Any) -> dict[str, Any]:
        gate = self._agent_tool_gate(kwargs)
        if gate:
            return gate
        stream_id = str(kwargs.get("stream_id") or "")
        text = self._cmd_rename_topic(stream_id, kwargs, str(name or "").strip())
        return {"success": text.startswith("✏️"), "content": text}

    @Tool(
        "dsh_topic_archive",
        description=(
            "【实验】归档一个 dsh 话题（7 天后自动删除，期间可 restore）。**破坏性操作**："
            "必须先向用户确认，用户明确同意后再传 confirm=true 调用。topic 可填 #3 或话题名，省略=当前话题。"
        ),
        visibility="visible",
        parameters=[
            ToolParameterInfo(name="topic", param_type=ToolParamType.STRING, description="话题（#3 / 名字，可省略）", required=False),
            ToolParameterInfo(name="confirm", param_type=ToolParamType.BOOLEAN, description="用户已确认归档时传 true", required=False),
        ],
    )
    async def _tool_topic_archive(self, topic: str = "", confirm: bool = False, **kwargs: Any) -> dict[str, Any]:
        gate = self._agent_tool_gate(kwargs)
        if gate:
            return gate
        if not confirm:
            return {
                "success": False,
                "error": "归档是破坏性操作：先问用户「要归档 #N 吗」，用户明确同意后再带 confirm=true 调用。",
            }
        stream_id = str(kwargs.get("stream_id") or "")
        text = self._cmd_archive_topic(stream_id, kwargs, str(topic or "").strip())
        return {"success": text.startswith("🗄"), "content": text}

    @Tool(
        "dsh_topic_restore",
        description="【实验】恢复一个已归档的话题（7 天内）。topic 填 #3 或名字。",
        visibility="visible",
        parameters=[
            ToolParameterInfo(name="topic", param_type=ToolParamType.STRING, description="话题（#3 / 名字）", required=True)
        ],
    )
    async def _tool_topic_restore(self, topic: str = "", **kwargs: Any) -> dict[str, Any]:
        gate = self._agent_tool_gate(kwargs)
        if gate:
            return gate
        stream_id = str(kwargs.get("stream_id") or "")
        text = self._cmd_restore_topic(stream_id, kwargs, str(topic or "").strip())
        return {"success": text.startswith("♻️"), "content": text}

    # ── 工具：定时任务（供 Planner 建/看/删） ────────────────

    @Tool(
        "dsh_status",
        description=(
            "查看 dsh 桥的真实状态：是否正在执行任务、最近几次任务的成败与摘要、定时任务数量、凭据是否配置。"
            "回答「dsh 在干嘛」「刚才那个任务成功了吗」「dsh 能用吗」之前必须先查这个，不要凭印象编。"
        ),
        visibility="visible",
        parameters=[],
    )
    async def _tool_dsh_status(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        lines = []
        if self._running:
            lines.append(f"状态：正在执行任务（{self._current_task[:60]}）")
            lines.append(self._cmd_peek(""))
        else:
            lines.append("状态：空闲")
        lines.append(f"凭据：{self._credential_state()}")
        lines.append(f"定时任务：{len(self._tasks)} 个")
        if self._history:
            lines.append("最近任务：")
            for item in reversed(self._history[-3:]):
                flag = "成功" if item.get("ok") else f"失败({item.get('reason')})"
                lines.append(f"  - {item.get('ts')} [{flag}] {item.get('task','')[:50]}")
                summary = str(item.get("summary") or "").strip().replace("\n", " ")[:120]
                if summary:
                    lines.append(f"    结果：{summary}")
        else:
            lines.append("最近任务：暂无记录")
        return {"success": True, "content": "\n".join(lines)}

    @Tool(
        "schedule_task",
        description=(
            "新建一个定时任务：到点后把 prompt 交给 dsh 执行、结果发回当前聊天。"
            "after_seconds / every_seconds / at 三者只填一个。"
        ),
        visibility="visible",
        parameters=[
            ToolParameterInfo(name="title", param_type=ToolParamType.STRING, description="任务短标题", required=True),
            ToolParameterInfo(name="prompt", param_type=ToolParamType.STRING, description="到点交给 dsh 的任务描述", required=True),
            ToolParameterInfo(name="after_seconds", param_type=ToolParamType.INTEGER, description="N 秒后执行一次", required=False),
            ToolParameterInfo(name="every_seconds", param_type=ToolParamType.INTEGER, description="每 N 秒重复（≥60）", required=False),
            ToolParameterInfo(name="at", param_type=ToolParamType.STRING, description="指定时刻（ISO，带时区），如 2026-10-03T09:00:00+08:00", required=False),
            ToolParameterInfo(name="cron", param_type=ToolParamType.STRING, description="5 字段 cron（分 时 日 月 周），如 0 9 * * 1-5", required=False),
        ],
    )
    async def _tool_schedule_task(self, **kwargs: Any) -> dict[str, Any]:
        if not self.config.plugin.enabled:
            return {"success": False, "error": "插件未启用"}
        if self.config.permission.enforce_tools and not self._is_allowed(kwargs):
            return {"success": False, "error": "无权限使用 schedule_task"}
        title = str(kwargs.get("title") or "").strip()
        prompt = str(kwargs.get("prompt") or "").strip()
        if not title or not prompt:
            return {"success": False, "error": "title 与 prompt 不能为空"}
        stream_id = str(kwargs.get("stream_id") or "").strip()
        task_id = self._create_task(
            title,
            prompt,
            kwargs.get("after_seconds"),
            kwargs.get("every_seconds"),
            kwargs.get("at"),
            kwargs.get("cron"),
            stream_id,
            self._extract_ctx(kwargs),
        )
        if not task_id:
            return {"success": False, "error": "请提供 after_seconds / every_seconds / at / cron 之一（cron 需 5 字段且能找到匹配时点）"}
        task = self._tasks[task_id]
        when = time.strftime("%m-%d %H:%M:%S", time.localtime(task["next_at"]))
        self.ctx.logger.info("已建定时任务 %s「%s」下一次 %s", task_id, title, when)
        try:
            await self.ctx.send.text(f"⏰ 已设定定时任务「{title}」：下次 {when}", stream_id)
        except Exception:  # noqa: BLE001
            pass
        return {"success": True, "id": task_id, "next_at": task["next_at"]}

    @Tool("schedule_list", description="列出当前所有定时任务。", visibility="visible", parameters=[])
    async def _tool_schedule_list(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        items = []
        for t in self._tasks.values():
            nx = t.get("next_at")
            items.append(
                {
                    "id": t.get("id"),
                    "title": t.get("title"),
                    "kind": (t.get("trigger") or {}).get("kind"),
                    "next_at": time.strftime("%m-%d %H:%M:%S", time.localtime(nx)) if nx else None,
                    "prompt": str(t.get("prompt") or "")[:80],
                }
            )
        return {"success": True, "count": len(items), "tasks": items}

    @Tool(
        "schedule_update",
        description="按 id 修改一个定时任务：可改 title / prompt / 时间规则（after_seconds / every_seconds / at / cron 之一）。",
        visibility="visible",
        parameters=[
            ToolParameterInfo(name="id", param_type=ToolParamType.STRING, description="任务 id", required=True),
            ToolParameterInfo(name="title", param_type=ToolParamType.STRING, description="新标题（可选）", required=False),
            ToolParameterInfo(name="prompt", param_type=ToolParamType.STRING, description="新任务描述（可选）", required=False),
            ToolParameterInfo(name="after_seconds", param_type=ToolParamType.INTEGER, description="改为 N 秒后一次（可选）", required=False),
            ToolParameterInfo(name="every_seconds", param_type=ToolParamType.INTEGER, description="改为每 N 秒重复（可选）", required=False),
            ToolParameterInfo(name="at", param_type=ToolParamType.STRING, description="改为指定 ISO 时刻（可选）", required=False),
            ToolParameterInfo(name="cron", param_type=ToolParamType.STRING, description="改为 5 字段 cron（可选）", required=False),
        ],
    )
    async def _tool_schedule_update(self, **kwargs: Any) -> dict[str, Any]:
        if self.config.permission.enforce_tools and not self._is_allowed(kwargs):
            return {"success": False, "error": "无权限使用 schedule_update"}
        tid = str(kwargs.get("id") or "").strip()
        task = self._tasks.get(tid)
        if not task:
            return {"success": False, "error": "未找到该任务"}
        title = str(kwargs.get("title") or "").strip()
        prompt = str(kwargs.get("prompt") or "").strip()
        if title:
            task["title"] = title
        if prompt:
            task["prompt"] = prompt
        if any(kwargs.get(k) not in (None, "") for k in ("after_seconds", "every_seconds", "at", "cron")):
            trigger = self._parse_schedule(kwargs.get("after_seconds"), kwargs.get("every_seconds"), kwargs.get("at"), kwargs.get("cron"))
            if not trigger:
                return {"success": False, "error": "时间规则无效"}
            next_at = trigger.pop("next_at")
            task["trigger"] = trigger
            task["next_at"] = next_at
        self._save_tasks()
        nx = task.get("next_at")
        when = time.strftime("%m-%d %H:%M:%S", time.localtime(nx)) if nx else None
        return {"success": True, "id": tid, "next_at": when}

    @Tool(
        "schedule_cancel",
        description="按 id 取消一个定时任务。",
        visibility="visible",
        parameters=[ToolParameterInfo(name="id", param_type=ToolParamType.STRING, description="任务 id", required=True)],
    )
    async def _tool_schedule_cancel(self, **kwargs: Any) -> dict[str, Any]:
        if self.config.permission.enforce_tools and not self._is_allowed(kwargs):
            return {"success": False, "error": "无权限使用 schedule_cancel"}
        tid = str(kwargs.get("id") or "").strip()
        if tid in self._tasks:
            self._tasks.pop(tid, None)
            self._save_tasks()
            return {"success": True, "removed": tid}
        return {"success": False, "error": "未找到该任务"}


def create_plugin() -> DshBridgePlugin:
    return DshBridgePlugin()
