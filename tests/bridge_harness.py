# -*- coding: utf-8 -*-
"""桥插件 P1（话题层）集成测试：假 Host + 真 dsh。

用法（服务器上）：
  <MAIBOT_HOME>/.venv/bin/python /tmp/bridge_harness.py
"""
import asyncio
import json
import os
import shutil
import sys
import threading
import time
import http.server
import socketserver
import functools
from pathlib import Path

PLUGIN_DIR = os.environ.get("BRIDGE_PLUGIN_DIR", "<MAIBOT_HOME>/plugins/maibot-dsh-bridge")
sys.path.insert(0, PLUGIN_DIR)
import plugin as bmod  # noqa: E402

WS_ROOT = "/tmp/bridge-harness-ws"
DATA_DIR = "/tmp/bridge-harness-data"
STREAM = "test-stream-1"
UID = "1000000001"


class FakeSend:
    def __init__(self):
        self.sent = []

    async def text(self, text, stream_id=None, **kw):
        self.sent.append((stream_id, text))
        print(f"\n[发到 QQ] ({stream_id}) {text}")
        return True

    async def forward(self, nodes, stream_id=None, **kw):
        print(f"\n[合并转发] {len(nodes)} 个节点")
        return True

    def clear(self):
        self.sent.clear()

    def joined(self):
        return "\n".join(t for _, t in self.sent)


class FakeApi:
    NAPCAT = {
        "adapter.napcat.message.get_msg",
        "adapter.napcat.file.get_file",
        "adapter.napcat.file.upload_private_file",
        "adapter.napcat.file.upload_group_file",
    }

    def __init__(self, napcat_file: str = ""):
        self.napcat_file = napcat_file

    async def get(self, name):
        if name in self.NAPCAT:
            # 真宿主返回 {"success": true, "api": {...}}；这里两个形状都给，桥两种都认
            return {"success": True, "api": {"name": name}, "name": name, "public": True, "version": "1"}
        return {"success": True, "api": None}

    async def call(self, name, **kw):
        print(f"[api.call] {name} {json.dumps(kw, ensure_ascii=False)[:100]}")
        if name.endswith("message.get_msg"):
            return {"message": [{"type": "file", "data": {"file": "课表.yml", "file_id": "FID-TEST"}}]}
        if name.endswith("file.get_file"):
            return {"file": self.napcat_file, "url": self.napcat_file, "file_name": "课表.yml"}
        return {"status": "ok"}


class FakeMaisaka:
    class _Ctx:
        async def append(self, stream_id, segments=None, source_kind="", **kw):
            body = str((segments or [{}])[0].get("content") or "")
            print(f"[回灌墨上下文] {body[:120]}".replace("\n", " "))

    def __init__(self):
        self.context = FakeMaisaka._Ctx()


class FakePaths:
    data_dir = DATA_DIR


class FakeCtx:
    def __init__(self, napcat_file: str = ""):
        import logging

        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
        self.send = FakeSend()
        self.api = FakeApi(napcat_file)
        self.maisaka = FakeMaisaka()
        self.paths = FakePaths()
        self.logger = logging.getLogger("fake-host")


def build_plugin(napcat_file: str = ""):
    p = bmod.DshBridgePlugin()
    cfg = p.build_default_config()
    cfg["dsh"]["workspace_dir"] = WS_ROOT
    cfg["dsh"]["extra_args"] = os.environ.get("BRIDGE_DSH_ARGS", "--profile headless")
    cfg["dsh"]["ask_enabled"] = True
    cfg["dsh"]["ask_timeout"] = 600
    cfg["dsh"]["uploads_enabled"] = True
    cfg["permission"]["enabled"] = True
    cfg["permission"]["mode"] = "whitelist"
    cfg["permission"]["allowed_users"] = UID
    cfg["topic"]["enabled"] = True
    cfg["topic"]["private_only"] = True
    cfg["topic"]["idle_hours"] = 24
    cfg["topic"]["queue_max"] = 2
    p.set_plugin_config(cfg)
    p._set_context(FakeCtx(napcat_file))
    return p


def http_file_server(path: Path, port: int = 0):
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *a, **k):
            pass

    class Reusable(socketserver.TCPServer):
        allow_reuse_address = True

    handler_cls = functools.partial(Quiet, directory=str(path.parent))
    srv = Reusable(("127.0.0.1", port), handler_cls)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def msg(text: str, stream: str = STREAM, uid: str = UID):
    return {
        "message_id": "m-test",
        "session_id": stream,
        "processed_plain_text": text,
        "raw_message": [{"type": "text", "data": text}],
        "message_info": {"user_info": {"user_id": uid, "user_nickname": "测试用户"}, "group_info": None},
    }


async def wait_idle(p, timeout=300):
    t0 = time.time()
    await asyncio.sleep(0.4)
    while p._running and time.time() - t0 < timeout:
        await asyncio.sleep(1)
    return not p._running


async def run_cmd(p, text: str, body: str = ""):
    """模拟一条 /dsh 命令（含子指令），返回 (ok, resp, intercept)。"""
    p.ctx.send.clear()
    kwargs = {"text": text, "stream_id": STREAM, "user_id": UID, "group_id": ""}
    if body:
        kwargs["matched_groups"] = {"body": body}
    res = await p.handle_dsh(**kwargs)
    print(f"\n[命令] {text} -> {res}")
    print(f"[回复] {p.ctx.send.joined()[:300]}")
    assert res[2] is True, "命令必须以拦截等级 1 返回，否则墨会接话茬"
    await wait_idle(p)
    return res


async def main():
    for d in (WS_ROOT, DATA_DIR):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)

    p = build_plugin()
    await p.on_load()

    print("\n===== 0) 附件入站（先落旧版按流目录） =====")
    src = Path("/tmp/bridge-harness-files")
    src.mkdir(exist_ok=True)
    sample = src / "hello.py"
    sample.write_text("print('hello from qq file')\n", encoding="utf-8")
    srv = http_file_server(sample)
    port = srv.server_address[1]
    res = await p.handle_receive(
        msg(f"[文件] hello.py，大小: {sample.stat().st_size}，链接: http://127.0.0.1:{port}/hello.py")
    )
    await asyncio.sleep(1.5)
    assert res == {"action": "abort"}, "纯文件应拦墨"
    legacy = Path(WS_ROOT) / STREAM
    assert (legacy / "uploads").is_dir() and any((legacy / "uploads").iterdir()), "附件没落到旧目录"
    print("旧流目录附件 OK")

    print("\n===== 0b) /dsh 无参数 = 帮助（且拦截） =====")
    await run_cmd(p, "/dsh")
    assert "/dsh new" in p.ctx.send.joined(), "帮助里没有子指令说明"
    await run_cmd(p, "/dsh 帮助", "帮助")

    print("\n===== 1) /dsh 首任务：建话题 + 命名 + 会话 =====")
    await run_cmd(p, "/dsh 读一下工作区的 hello.py 说它是什么语言", "读一下工作区的 hello.py 说它是什么语言")
    assert "#1" in p.ctx.send.joined(), "受理消息里没有话题号"
    ids = sorted(p._topics.keys())
    print("话题 id:", ids)
    assert 1 in p._topics, "没有建出 #1"
    t1 = p._topics[1]
    print("话题1:", json.dumps(t1, ensure_ascii=False))
    assert t1["name"] != "新话题", "规划轮标题没有写进话题名"
    assert t1["session_id"], "没有记录 dsh 会话 id"
    assert p._topic_current[STREAM] == 1
    assert (Path(WS_ROOT) / t1["dir"]).is_dir()

    print("\n===== 2) /话题 列表 & 切换 =====")
    await run_cmd(p, "/dsh list", "list")
    assert "#1" in p.ctx.send.joined()
    await run_cmd(p, "/dsh 话题 #1", "话题 #1")

    print("\n===== 3) /修改话题 改名 =====")
    await run_cmd(p, "/dsh set 读 hello 文件", "set 读 hello 文件")
    assert p._topics[1]["name"] == "读 hello 文件"

    print("\n===== 4) /新建 + 任务落到 #2 =====")
    await run_cmd(p, "/dsh new", "new")
    assert p._topic_current[STREAM] == 2
    await run_cmd(p, "/dsh 只回复两个字：好的", "只回复两个字：好的")
    t2 = p._topics[2]
    print("话题2:", json.dumps({k: t2[k] for k in ("id", "name", "dir", "session_id")}, ensure_ascii=False))
    assert t2["session_id"], "#2 没有会话 id"
    assert t1["session_id"] != t2["session_id"], "两个话题共用了会话"

    print("\n===== 5) /dsh #1 指定话题 =====")
    await run_cmd(p, "/dsh #1 只回复：回到一号话题", "#1 只回复：回到一号话题")
    assert p._topic_current[STREAM] == 1, "#1 没有被设为当前"

    print("\n===== 6) 归档 / 拒绝派活 / 恢复 =====")
    await run_cmd(p, "/dsh del #2", "del #2")
    assert p._topics[2]["archived_at"], "#2 没归档"
    await run_cmd(p, "/dsh #2 还能用吗", "#2 还能用吗")
    assert "已归档" in p.ctx.send.joined(), "归档话题没拒绝派活"
    await run_cmd(p, "/dsh list", "list")
    assert "已归档" in p.ctx.send.joined()
    await run_cmd(p, "/dsh restore #2", "restore #2")
    assert not p._topics[2]["archived_at"], "#2 没恢复"

    print("\n===== 7) 闲置 24h 自动新开 =====")
    p._topics[1]["last_used_at"] = time.time() - 25 * 3600
    p._save_topics()
    await run_cmd(p, "/dsh 说一句：新的一天", "说一句：新的一天")
    assert 3 in p._topics, "闲置后没有自动新开话题"
    assert p._topic_current[STREAM] == 3

    print("\n===== 8) 归档到期清理 + 编号复用 =====")
    await run_cmd(p, "/dsh del #3", "del #3")
    p._topics[3]["archived_at"] = time.time() - 8 * 86400
    p._save_topics()
    p._purge_archived()
    assert 3 not in p._topics, "到期归档没被清理"
    await run_cmd(p, "/dsh new", "new")
    assert p._topic_current[STREAM] == 3, "编号没有复用"
    print("清理后新话题编号:", p._topic_current[STREAM])

    print("\n===== 9) dsh 提问 → 回答（自动适配文件/MCP 两种通道） =====")
    task = (
        "需要用户拍板时按工作区 AGENTS.md 的约定来。现在就问用户「选 A 还是 B？」（options 给 A/B），"
        "拿到回答后只回复一句：收到：<答案>"
    )
    p.ctx.send.clear()
    p._history.clear()
    await p.handle_dsh(stream_id=STREAM, user_id=UID, group_id="", matched_groups={"body": task}, text="/dsh " + task)
    t0 = time.time()
    while time.time() - t0 < 120 and STREAM not in p._pending:
        await asyncio.sleep(0.5)
    pend = p._pending.get(STREAM)
    assert pend, f"提问没有登记（send={p.ctx.send.joined()[:200]}）"
    print("提问模式:", pend.get("mode"), "|", pend.get("question"))
    res = await p.handle_receive(msg("选 B"))
    assert res == {"action": "abort"}, "回答没被拦截"
    assert STREAM not in p._pending
    await wait_idle(p, 300)
    hist = p._history[-1] if p._history else {}
    print("结果摘要:", str(hist.get("summary") or "")[:200])
    assert "B" in str(hist.get("summary") or ""), "结果里没有答案"

    print("\n===== 10) 实验功能：总开关关着时拒绝 =====")
    p.config.experimental.agent_tools = False
    p.ctx.send.clear()
    r = await p.handle_dsh(stream_id=STREAM, user_id=UID, group_id="", matched_groups={"body": "实验开启"}, text="/dsh 实验开启")
    assert r[2] is True
    assert "总开关" in p.ctx.send.joined(), p.ctx.send.joined()

    print("\n===== 11) 打开总开关（模拟设置勾上）→ 自动发二次确认码 =====")
    p._last_chat_id = STREAM
    p.ctx.send.clear()
    p.config.experimental.agent_tools = True
    await p.on_config_update("self", {}, "x")
    code = str(p._exp.get("pending_code") or "")
    assert code, "总开关打开后没生成确认码"
    assert code in p.ctx.send.joined(), "确认码没发到 QQ"
    assert not p._agent_armed()
    print("确认码:", code)

    print("\n===== 12) 未确认前工具不可用、/dsh 仍正常 =====")
    g = await p._tool_topic_list(stream_id=STREAM, user_id=UID, group_id="")
    assert g["success"] is False, "没确认就能用工具了"
    p.ctx.send.clear()
    await p.handle_dsh(stream_id=STREAM, user_id=UID, group_id="", matched_groups={"body": "实验状态"}, text="/dsh 实验状态")
    assert "待确认" in p.ctx.send.joined()

    print("\n===== 13) 二次确认 → 开启 24h，/dsh 被临时禁用 =====")
    p.ctx.send.clear()
    r = await p.handle_dsh(stream_id=STREAM, user_id=UID, group_id="", matched_groups={"body": f"实验确认 {code}"}, text="/dsh 实验确认")
    assert p._agent_armed(), "确认后没开启"
    left = (float(p._exp["expires_at"]) - time.time()) / 3600
    print(f"剩余 {left:.2f} 小时")
    assert 23.5 < left <= 24.0, "有效期不是 24 小时"
    p.ctx.send.clear()
    await p.handle_dsh(stream_id=STREAM, user_id=UID, group_id="", matched_groups={"body": "帮我干点活"}, text="/dsh 帮我干点活")
    assert "实验模式已开启" in p.ctx.send.joined(), "实验窗口内 /dsh 没被禁用"
    assert not p._running, "实验窗口内居然派活了"

    print("\n===== 14) 墨可自主调用的工具（列表/新建/改名/归档需 confirm/恢复） =====")
    t = await p._tool_topic_list(stream_id=STREAM, user_id=UID, group_id="")
    assert t["success"] and "#" in t["content"], t
    nt = await p._tool_topic_new(stream_id=STREAM, user_id=UID, group_id="", name="实验话题")
    assert nt["success"], nt
    rn = await p._tool_topic_rename(stream_id=STREAM, user_id=UID, group_id="", name="实验话题改")
    assert rn["success"], rn
    bad = await p._tool_topic_archive(stream_id=STREAM, user_id=UID, group_id="", topic=nt["topic"], confirm=False)
    assert bad["success"] is False and "confirm" in bad["error"], bad
    okk = await p._tool_topic_archive(stream_id=STREAM, user_id=UID, group_id="", topic=nt["topic"], confirm=True)
    assert okk["success"], okk
    rs = await p._tool_topic_restore(stream_id=STREAM, user_id=UID, group_id="", topic=nt["topic"])
    assert rs["success"], rs
    print("工具链 OK:", nt["topic"])

    print("\n===== 15) 到期自动关闭 + 工具重新不可用 + /dsh 恢复 =====")
    p.ctx.send.clear()
    p._exp["expires_at"] = time.time() - 1
    p._save_exp()
    expired = p._expire_agent_if_needed()
    assert expired and not p._agent_armed(), "到期没自动关闭"
    g2 = await p._tool_topic_list(stream_id=STREAM, user_id=UID, group_id="")
    assert g2["success"] is False, "到期后工具还能用"
    p.ctx.send.clear()
    await p.handle_dsh(stream_id=STREAM, user_id=UID, group_id="", matched_groups={"body": "list"}, text="/dsh list")
    assert "话题列表" in p.ctx.send.joined(), "/dsh 没恢复"

    print("\n===== 16) P2：MCP 提问 → QQ → 回答 → 同一进程继续 =====")
    task = '请调用工具 mcp__ask__ask_user 问用户「选 A 还是 B？」（options 给 A、B），然后按回答只回复：收到：<答案>'
    p.ctx.send.clear()
    p._history.clear()
    await p.handle_dsh(stream_id=STREAM, user_id=UID, group_id="", matched_groups={"body": task}, text="/dsh " + task)
    t0 = time.time()
    while time.time() - t0 < 120:
        pend = p._pending.get(STREAM) or {}
        if pend.get("mode") == "ipc":
            break
        await asyncio.sleep(0.5)
    pend = p._pending.get(STREAM) or {}
    assert pend.get("mode") == "ipc", f"没有登记 IPC 提问: {p._pending}"
    print("待回答:", pend.get("question"))
    assert "❓" in p.ctx.send.joined(), "提问没发到 QQ"
    res = await p.handle_receive(msg("选 B"))
    assert res == {"action": "abort"}, "回答没被拦截"
    assert STREAM not in p._pending, "pending 没清"
    await wait_idle(p, 300)
    hist = p._history[-1] if p._history else {}
    print("任务结果摘要:", str(hist.get("summary") or "")[:200])
    assert "B" in str(hist.get("summary") or ""), "续跑结果里没有答案"

    print("\n===== 17) MCP 提问超时（工具自身 timeout）→ 自动清理 =====")
    task2 = '调用工具 mcp__ask__ask_user 问用户「要 A 吗？」，timeout_seconds 设为 8，然后按工具返回只回复一句'
    p.ctx.send.clear()
    p._history.clear()
    await p.handle_dsh(stream_id=STREAM, user_id=UID, group_id="", matched_groups={"body": task2}, text="/dsh " + task2)
    t0 = time.time()
    while time.time() - t0 < 120:
        pend = p._pending.get(STREAM) or {}
        if pend.get("mode") == "ipc":
            break
        await asyncio.sleep(0.5)
    assert (p._pending.get(STREAM) or {}).get("mode") == "ipc", "超时用例没登记提问"
    await wait_idle(p, 300)
    hist = p._history[-1] if p._history else {}
    print("超时结果摘要:", str(hist.get("summary") or "")[:200])
    assert not p._ipc_waiting, "超时后还卡在等待态"
    assert STREAM not in p._pending, "超时后 pending 没清"

    print("\n===== 18) 超时/异常救回：工作区新产出文件也能发出来 =====")
    ws18 = Path(WS_ROOT) / "salvage-test"
    ws18.mkdir(parents=True, exist_ok=True)
    since = time.time()
    old_f = ws18 / "old.txt"
    old_f.write_text("old", encoding="utf-8")
    os.utime(old_f, (since - 3600, since - 3600))
    report = ws18 / "报告.docx"
    report.write_bytes(b"PK\x03\x04fake-docx")
    (ws18 / "uploads").mkdir(exist_ok=True)
    (ws18 / "uploads" / "input.docx").write_bytes(b"x")
    (ws18 / "scripts").mkdir(exist_ok=True)
    (ws18 / "scripts" / "build.py").write_text("print(1)", encoding="utf-8")
    (ws18 / ".dsh-office.abc").mkdir(exist_ok=True)
    (ws18 / ".dsh-office.abc" / "tmp.docx").write_bytes(b"x")
    (ws18 / "AGENTS.md").write_text("guide", encoding="utf-8")
    (ws18 / "answer.json").write_text("{}", encoding="utf-8")
    files = p._salvage_artifacts(str(ws18), since - 2)
    names = sorted(Path(f).name for f in files)
    print("救回候选:", names)
    assert names == ["报告.docx"], names
    p.ctx.send.clear()
    n = await p._send_salvage(files, STREAM, {"user_id": UID}, "timeout")
    print("救回发送数:", n)
    assert n == 1, n
    joined = p.ctx.send.joined()
    assert "报告.docx" in joined, joined
    print("救回提示:", joined[:160])

    print("\n===== 19) /dsh peek：空闲时给最近一次 =====")
    await run_cmd(p, "/dsh peek", "peek")
    assert "没在跑活儿" in p.ctx.send.joined(), p.ctx.send.joined()

    print("\n===== 20) /dsh peek 运行中 + /dsh stop 停止（不限时也能停） =====")
    long_task = "先执行 bash 命令：sleep 45，然后再回复一句：睡完了"
    p.ctx.send.clear()
    p._history.clear()
    res0 = await p.handle_dsh(
        stream_id=STREAM, user_id=UID, group_id="", matched_groups={"body": long_task}, text="/dsh " + long_task
    )
    assert res0[2] is True
    assert "不限时" in p.ctx.send.joined(), p.ctx.send.joined()
    assert p._running, "长任务没启动"
    await asyncio.sleep(8)
    p.ctx.send.clear()
    r = await p.handle_dsh(stream_id=STREAM, user_id=UID, group_id="", matched_groups={"body": "peek"}, text="/dsh peek")
    assert r[2] is True, r
    peek_text = p.ctx.send.joined()
    print("[peek 输出]", peek_text[:400].replace("\n", " / "))
    assert "正在干活" in peek_text, peek_text
    p.ctx.send.clear()
    r2 = await p.handle_dsh(stream_id=STREAM, user_id=UID, group_id="", matched_groups={"body": "stop"}, text="/dsh stop")
    assert r2[2] is True, r2
    print("[stop 回复]", p.ctx.send.joined()[:200])
    assert "正在停止" in p.ctx.send.joined(), p.ctx.send.joined()
    stopped = await wait_idle(p, 120)
    assert stopped, "发 /dsh stop 之后任务没停下来"
    hist = p._history[-1] if p._history else {}
    print("停止后的记录:", hist.get("reason"), "|", str(hist.get("task"))[:40])
    joined_stop = p.ctx.send.joined()
    assert "停下了" in joined_stop, joined_stop
    # 可能在规划阶段就被停（那时还没进正式执行）：两种都算通过
    assert hist.get("reason") == "stop" or "规划阶段" in joined_stop, hist

    print("\n===== 21) dsh 报错即退出（会话被占用）→ 判失败 + 自动换新会话 + 用户看到原因 =====")
    fake = Path("/tmp/fake-dsh-lock")
    fake.write_text(
        "#!/bin/sh\n"
        "echo '{\"type\":\"error\",\"message\":\"session \\\"session-dead\\\" is already owned by an active write handle\"}'\n"
        "echo 'dsh: session \"session-dead\" is already owned by an active write handle' >&2\n"
        "exit 1\n",
        encoding="utf-8",
    )
    fake.chmod(0o755)
    orig_cmd, orig_extra = p.config.dsh.command, p.config.dsh.extra_args
    p.config.dsh.command = str(fake)
    p.config.dsh.extra_args = ""
    try:
        p._history.clear()
        p.ctx.send.clear()
        # 话题里带着一个（被占用的）会话 id → 应触发「换新会话重试」提示
        await p._run_and_report("测试：随便干点活", STREAM, 0, {"user_id": UID})
        joined = p.ctx.send.joined()
        print("[输出]", joined[:300].replace("\n", " / "))
        assert "🔒" in joined, f"没有会话占用提示：{joined}"
        assert "干活失败" in joined, f"没有失败提示：{joined}"
        hist = p._history[-1] if p._history else {}
        print("记录:", hist.get("reason"), "|", str(hist.get("summary"))[:80])
        assert hist.get("reason") == "failed", hist
        assert "active write handle" in str(hist.get("summary") or ""), hist
        # 再跑一次（此时话题已没有会话 id）→ 不再重试，但仍要报错而不是静默
        p._history.clear()
        p.ctx.send.clear()
        await p._run_and_report("测试：再干一次", STREAM, 0, {"user_id": UID})
        joined2 = p.ctx.send.joined()
        print("[第二次输出]", joined2[:200].replace("\n", " / "))
        assert "干活失败" in joined2, f"静默失败：{joined2}"
    finally:
        p.config.dsh.command = orig_cmd
        p.config.dsh.extra_args = orig_extra

    print("\n===== 22) DSH_HOME 注入 + 会话不存在时自动换新会话 =====")
    fake_home = Path("/tmp/fake-dsh-home")
    script = "#!/bin/sh\n"
    script += 'echo \'{"type":"session","sessionId":"session-fake-home"}\'\n'
    script += 'echo "{\\"type\\":\\"final\\",\\"text\\":\\"DSH_HOME=$DSH_HOME\\"}"\n'
    script += "exit 0\n"
    fake_home.write_text(script, encoding="utf-8")
    fake_home.chmod(0o755)
    fake_missing = Path("/tmp/fake-dsh-missing")
    fake_missing.write_text(
        "#!/bin/sh\n"
        'echo \'{"type":"error","message":"session \\"session-gone\\" not found"}\'\n'
        "exit 1\n",
        encoding="utf-8",
    )
    fake_missing.chmod(0o755)
    orig_cmd, orig_extra = p.config.dsh.command, p.config.dsh.extra_args
    try:
        # 22a：dsh_home 配了 → 子进程环境里应带上 DSH_HOME
        p.config.dsh.command = str(fake_home)
        p.config.dsh.extra_args = ""
        p.config.dsh.dsh_home = "/tmp/fake-dsh-home-dir"
        p._history.clear()
        p.ctx.send.clear()
        await p._run_and_report("测试：看看 DSH_HOME", STREAM, 0, {"user_id": UID})
        hist = p._history[-1] if p._history else {}
        summary = str(hist.get("summary") or "")
        print("结果摘要:", summary[:100])
        assert "DSH_HOME=/tmp/fake-dsh-home-dir" in summary, f"DSH_HOME 没注入：{summary}"
        # 22b：会话不存在 → 自动换新会话并提示
        p.config.dsh.command = str(fake_missing)
        p.config.dsh.dsh_home = ""
        cur = p._topics.get(p._topic_current.get(STREAM) or 0)
        if cur is not None:
            cur["session_id"] = "session-gone"
            p._save_topics()
        p._history.clear()
        p.ctx.send.clear()
        await p._run_and_report("测试：旧会话没了", STREAM, 0, {"user_id": UID})
        joined = p.ctx.send.joined()
        print("[输出]", joined[:260].replace("\n", " / "))
        assert "🔄" in joined, f"没有会话不存在提示：{joined}"
        assert "干活失败" in joined, f"没有失败提示：{joined}"
        hist = p._history[-1] if p._history else {}
        assert hist.get("reason") == "failed", hist
    finally:
        p.config.dsh.command = orig_cmd
        p.config.dsh.extra_args = orig_extra
        p.config.dsh.dsh_home = ""

    print("\n===== 23) 任务成功 → 结果直接发到聊天（不再只回灌给墨） =====")
    fake_ok = Path("/tmp/fake-dsh-ok")
    fake_ok.write_text(
        "#!/bin/sh\n"
        'echo \'{"type":"session","sessionId":"session-ok-1"}\'\n'
        'echo \'{"type":"final","text":"这是 dsh 的最终结果"}\'\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_ok.chmod(0o755)
    orig_cmd, orig_extra = p.config.dsh.command, p.config.dsh.extra_args
    p.config.dsh.command = str(fake_ok)
    p.config.dsh.extra_args = ""
    try:
        p._history.clear()
        p.ctx.send.clear()
        await p._run_and_report("测试：给个结果", STREAM, 0, {"user_id": UID})
        joined = p.ctx.send.joined()
        print("[输出]", joined[:220].replace("\n", " / "))
        assert "✅ 大肥鱼完成" in joined, f"结果没直接发出来：{joined}"
        assert "这是 dsh 的最终结果" in joined, joined
        # 空输出也要有回执，不能静默
        fake_empty = Path("/tmp/fake-dsh-empty")
        fake_empty.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        fake_empty.chmod(0o755)
        p.config.dsh.command = str(fake_empty)
        p._history.clear()
        p.ctx.send.clear()
        await p._run_and_report("测试：啥也不说", STREAM, 0, {"user_id": UID})
        joined2 = p.ctx.send.joined()
        print("[空输出]", joined2[:180].replace("\n", " / "))
        assert "没写字" in joined2, f"空输出静默了：{joined2}"
    finally:
        p.config.dsh.command = orig_cmd
        p.config.dsh.extra_args = orig_extra

    print("\n===== 24) 陈旧提问清理 + 文件发送失败要可见 =====")
    # 24a：上一轮遗留的提问文件，新一轮开始时应被清掉
    cur_topic = p._topics.get(p._topic_current.get(STREAM) or 0)
    ws24 = str(p._topic_dir(cur_topic)) if cur_topic else p._workspace(STREAM)
    qdir = Path(ws24) / "ipc" / "questions"
    qdir.mkdir(parents=True, exist_ok=True)
    stale = qdir / "q-stale.json"
    stale.write_text('{"question":"上一轮遗留的问题","options":[]}', encoding="utf-8")
    os.utime(stale, (time.time() - 600, time.time() - 600))
    p._clear_ipc_dirs(ws24)
    assert not stale.exists(), "陈旧提问没被清掉"
    print("陈旧提问已清理 OK")
    # 24b：api.get 两种返回形态都要认
    assert p._api_meta({"success": True, "api": {"name": "x"}}) == {"name": "x"}
    assert p._api_meta({"name": "x", "description": "y"}) == {"name": "x", "description": "y"}
    assert p._api_meta(None) is None
    p.ctx.send.clear()
    # 24c：适配器没有文件上传接口时，要给用户一句「没发出去」
    fake_file = Path(ws24) / "报告.docx"
    fake_file.write_bytes(b"PK\x03\x04fake")
    st24 = bmod._RunState()
    st24.events.append(
        {"type": "tool_call", "tool": "present", "input": {"files": [{"path": "报告.docx"}]}}
    )
    saved = p.ctx.api.NAPCAT
    p.ctx.api.NAPCAT = set()  # 模拟非 NapCat 适配器
    try:
        sent = await p._send_deliverables(st24, STREAM, {"user_id": UID}, ws=ws24)
        joined = p.ctx.send.joined()
        print("[无接口输出]", joined[:200].replace("\n", " / "))
        assert sent == 0, sent
        assert "没发出去" in joined, f"文件发失败却静默：{joined}"
    finally:
        p.ctx.api.NAPCAT = saved

    print("\n===== 全部通过 =====")
    srv.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
