#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""dsh ↔ 麦麦 的提问 MCP 服务器（stdio 传输，纯标准库）。

dsh 通过 @deepseek-ai/dsh-mcp-client 以 stdio 方式启动本进程；
工具 mcp__ask__ask_user 被调用时：
  1. 往 <话题目录>/ipc/questions/q-<qid>.json 写问题
  2. 轮询 <话题目录>/ipc/answers/q-<qid>.json
  3. 读到答案后把文本返回给模型；超时返回“用户未回答”

话题目录：环境变量 DSH_TOPIC_DIR，缺省用进程 cwd（dsh 的工作目录就是话题目录）。

日志：<话题目录>/ipc/mcp.log
"""
from __future__ import annotations

import json
import os
import random
import sys
import time
from pathlib import Path

# 可选：同目录的 dsh_files_mcp.py 提供文件柜工具（files_*）。
# 为什么合并进同一个 server：dsh 的 @deepseek-ai/dsh-mcp-client 对同一个 package 的
# 第二个实例（不同 id、不同 serverName）不生效——单独起 files server 时工具不会被注册，
# 所以把文件工具挂到本 server（serverName=ask）下，工具名形如 mcp__ask__files_list。
_FILES_MOD = None
try:
    _here = str(Path(__file__).resolve().parent)
    if _here not in sys.path:
        sys.path.insert(0, _here)
    import dsh_files_mcp as _FILES_MOD  # type: ignore  # noqa: E402
except Exception:  # noqa: BLE001
    _FILES_MOD = None

_FILES_TOOL_NAMES = [str(t.get("name")) for t in getattr(_FILES_MOD, "TOOL_DEFS", []) or []] if _FILES_MOD else []

SERVER_NAME = "dsh-ask-mcp"
SERVER_VERSION = "0.1.0"
PROTOCOL_VERSION = "2025-06-18"
DEFAULT_TIMEOUT = int(os.environ.get("DSH_ASK_TIMEOUT", "900"))
POLL_SECONDS = 0.5


def topic_dir() -> Path:
    raw = os.environ.get("DSH_TOPIC_DIR", "").strip()
    base = Path(raw) if raw else Path.cwd()
    return base


def ipc_dir() -> Path:
    d = topic_dir() / "ipc"
    (d / "questions").mkdir(parents=True, exist_ok=True)
    (d / "answers").mkdir(parents=True, exist_ok=True)
    return d


def log(msg: str) -> None:
    try:
        d = ipc_dir()
        with (d / "mcp.log").open("a", encoding="utf-8") as fh:
            fh.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")
    except Exception:
        pass


def _write_json_atomic(path: Path, data: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".part")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


TOOL_DEF = {
    "name": "ask_user",
    "description": (
        "向 QQ 用户提问并等待回答（会阻塞直到用户回答或超时）。"
        "需要用户拍板、补充关键信息、或确认要不要继续时使用。"
        "问题要简短、自包含；有明确选项时放进 options。"
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "question": {"type": "string", "description": "要问用户的问题（一两句）"},
            "options": {
                "type": "array",
                "items": {"type": "string"},
                "description": "可选选项（用户可以回编号或原文）",
            },
            "note": {"type": "string", "description": "补充说明（可省略）"},
            "timeout_seconds": {
                "type": "integer",
                "description": "等待秒数，缺省用桥配置（默认 900）",
                "minimum": 10,
            },
        },
        "required": ["question"],
    },
}


def ask_user(arguments: dict) -> tuple[str, bool]:
    question = str(arguments.get("question") or "").strip()
    if not question:
        return "问题为空，无法提问。", True
    options = arguments.get("options") or []
    if not isinstance(options, list):
        options = [str(options)]
    options = [str(o).strip() for o in options if str(o).strip()][:8]
    note = str(arguments.get("note") or "").strip()
    try:
        timeout = int(arguments.get("timeout_seconds") or DEFAULT_TIMEOUT)
    except Exception:
        timeout = DEFAULT_TIMEOUT
    timeout = max(10, min(timeout, 7200))

    qid = f"q-{int(time.time())}-{random.randint(1000, 9999)}"
    d = ipc_dir()
    qpath = d / "questions" / f"{qid}.json"
    apath = d / "answers" / f"{qid}.json"
    _write_json_atomic(
        qpath,
        {
            "qid": qid,
            "question": question,
            "options": options,
            "note": note,
            "timeout": timeout,
            "created_at": time.time(),
        },
    )
    log(f"ask qid={qid} written, waiting {timeout}s: {question[:80]}")

    deadline = time.time() + timeout
    while time.time() < deadline:
        if apath.is_file():
            try:
                payload = json.loads(apath.read_text(encoding="utf-8"))
            except Exception:
                time.sleep(POLL_SECONDS)
                continue
            answer = str(payload.get("answer") or "").strip()
            timed_out = bool(payload.get("timeout"))
            try:
                apath.unlink()
            except Exception:
                pass
            try:
                qpath.unlink()
            except Exception:
                pass
            if timed_out:
                log(f"ask qid={qid} 用户超时未回答")
                return f"用户未在 {timeout} 秒内回答（超时）。请自行决定下一步，或在最终回复里说明你在等什么。", False
            log(f"ask qid={qid} 收到答案: {answer[:80]}")
            return f"用户回答：{answer}", False
        time.sleep(POLL_SECONDS)

    # 超时：留一个问题文件让桥知道（桥会清理），并把状态写回给模型
    try:
        qpath.unlink()
    except Exception:
        pass
    log(f"ask qid={qid} 超时（{timeout}s）")
    return (
        f"用户未在 {timeout} 秒内回答（超时）。不要反复重复同一个问题；"
        "可以说明卡在哪里、把可自行完成的部分做完，或在最终回复里说明你在等用户确认。",
        False,
    )


def respond(msg_id, result=None, error=None) -> None:
    payload = {"jsonrpc": "2.0", "id": msg_id}
    if error is not None:
        payload["error"] = error
    else:
        payload["result"] = result
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def handle(msg: dict) -> None:
    method = str(msg.get("method") or "")
    msg_id = msg.get("id")
    params = msg.get("params") or {}
    if msg_id is None and method.startswith("notifications/"):
        if method == "notifications/initialized":
            log("initialized")
        return
    if method == "initialize":
        client_proto = str(params.get("protocolVersion") or PROTOCOL_VERSION)
        proto = client_proto if client_proto else PROTOCOL_VERSION
        respond(
            msg_id,
            {
                "protocolVersion": proto,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                "instructions": "用 ask_user 向 QQ 用户提问（阻塞到回答或超时）；用 files_* 管理用户发来的文件（按 QQ 号隔离）。",
            },
        )
        log(f"initialize proto={proto}")
        return
    if method == "ping":
        respond(msg_id, {})
        return
    if method == "tools/list":
        tools = [TOOL_DEF] + list(getattr(_FILES_MOD, "TOOL_DEFS", []) or []) if _FILES_MOD else [TOOL_DEF]
        respond(msg_id, {"tools": tools})
        return
    if method == "tools/call":
        name = str(params.get("name") or "")
        args = params.get("arguments") or {}
        if not isinstance(args, dict):
            args = {}
        if name in _FILES_TOOL_NAMES:
            try:
                text, is_error = _FILES_MOD.call_tool(name, args)  # type: ignore[union-attr]
            except Exception as exc:  # noqa: BLE001
                log(f"{name} 异常: {exc!r}")
                respond(msg_id, {"content": [{"type": "text", "text": f"文件操作失败: {exc}"}], "isError": True})
                return
            respond(msg_id, {"content": [{"type": "text", "text": text}], "isError": is_error})
            return
        if name != "ask_user":
            respond(msg_id, {"content": [{"type": "text", "text": f"未知工具: {name}"}], "isError": True})
            return
        try:
            text, is_error = ask_user(args)
        except Exception as exc:  # noqa: BLE001
            log(f"ask_user 异常: {exc!r}")
            respond(msg_id, {"content": [{"type": "text", "text": f"提问失败: {exc}"}], "isError": True})
            return
        respond(msg_id, {"content": [{"type": "text", "text": text}], "isError": is_error})
        return
    if method in ("resources/list", "prompts/list"):
        respond(msg_id, {"resources": [], "prompts": []})
        return
    respond(msg_id, error={"code": -32601, "message": f"method not found: {method}"})


def main() -> None:
    log(f"启动 {SERVER_NAME} v{SERVER_VERSION}，topic={topic_dir()}")
    for raw in sys.stdin.buffer:
        line = raw.decode("utf-8", "replace").strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except Exception as exc:  # noqa: BLE001
            log(f"非法 JSON: {exc!r} line={line[:200]}")
            continue
        if isinstance(msg, list):
            for item in msg:
                if isinstance(item, dict):
                    handle(item)
            continue
        if isinstance(msg, dict):
            try:
                handle(msg)
            except Exception as exc:  # noqa: BLE001
                log(f"处理异常: {exc!r}")
                if msg.get("id") is not None:
                    respond(msg["id"], error={"code": -32603, "message": str(exc)})


if __name__ == "__main__":
    main()
