#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""dsh_files_mcp.py 的单元测试（纯标准库 unittest，Python 3.8+）。

运行：
    cd bridge2/p0
    python -m unittest -v test_dsh_files_mcp

覆盖：
    * list / read / info / mkdir / move / delete 正常路径
    * 普通用户访问别人目录被拒（没有权限看别人的文件夹）
    * ``../`` 目录穿越、绝对路径被拒
    * 符号链接逃逸被拒（Windows 无符号链接权限时自动 skip）
    * 超级管理员跨目录 list/read/mkdir/files_users
    * 二进制文件 files_read 被拒
    * 超过读取上限被拒
    * 非空目录 recursive=true 才允许删除
    * DSH_FILES_OWNER_QQ 缺失时全部工具拒绝
    * 起真实子进程走 stdio JSON-RPC（initialize / tools/list / tools/call / ping）
"""
from __future__ import annotations

import json
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import dsh_files_mcp as m  # noqa: E402


OWNER = "10001"
OTHER = "10002"
SUPER = "2953679189"
SERVER_SCRIPT = os.path.join(HERE, "dsh_files_mcp.py")


# --------------------------------------------------------------------------
# 公共工具
# --------------------------------------------------------------------------
def _read_json(text):
    return json.loads(text)


class _Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dshfiles-test-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.owner_dir = os.path.join(self.tmp, OWNER)
        self.other_dir = os.path.join(self.tmp, OTHER)
        os.makedirs(self.owner_dir, exist_ok=True)
        os.makedirs(self.other_dir, exist_ok=True)
        self.cfg = m.FilesConfig(
            root=self.tmp,
            owner_qq=OWNER,
            superusers=[SUPER],
            max_read_bytes=64 * 1024,
        )
        self.super_cfg = m.FilesConfig(
            root=self.tmp,
            owner_qq=SUPER,
            superusers=[SUPER],
            max_read_bytes=64 * 1024,
        )

    # ---- 断言辅助 ----
    def ok(self, name, config=None, **arguments):
        text, is_error = m.call_tool(name, arguments, config or self.cfg)
        self.assertFalse(is_error, "期望成功但失败：%s" % text)
        payload = _read_json(text)
        self.assertTrue(payload.get("ok"), payload)
        return payload

    def err(self, name, config=None, **arguments):
        text, is_error = m.call_tool(name, arguments, config or self.cfg)
        self.assertTrue(is_error, "期望失败但成功：%s" % text)
        payload = _read_json(text)
        self.assertFalse(payload.get("ok"), payload)
        return payload["error"]

    # ---- 文件辅助 ----
    def write(self, rel, data, directory=None):
        base = directory or self.owner_dir
        path = os.path.join(base, rel)
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        mode = "wb" if isinstance(data, bytes) else "w"
        if isinstance(data, bytes):
            with open(path, "wb") as fh:
                fh.write(data)
        else:
            # newline="" 避免 Windows 换行转换，保证读回内容与写入一致
            with open(path, "w", encoding="utf-8", newline="") as fh:
                fh.write(data)
        return path

    def path(self, rel, directory=None):
        return os.path.join(directory or self.owner_dir, rel)


# --------------------------------------------------------------------------
# 正常路径
# --------------------------------------------------------------------------
class NormalPathTest(_Base):
    def test_list_sorted_by_mtime_desc(self):
        old = self.write("old.txt", "old")
        new = self.write("new.txt", "new")
        os.utime(old, (1_600_000_000, 1_600_000_000))
        os.utime(new, (1_700_000_000, 1_700_000_000))
        payload = self.ok("files_list")
        self.assertEqual(payload["user"], OWNER)
        self.assertEqual(payload["path"], ".")
        self.assertEqual(payload["count"], 2)
        names = [item["name"] for item in payload["entries"]]
        self.assertEqual(names, ["new.txt", "old.txt"])
        first = payload["entries"][0]
        for key in ("name", "path", "type", "is_dir", "size", "mtime"):
            self.assertIn(key, first)
        self.assertEqual(first["size"], 3)
        self.assertEqual(first["type"], "file")
        self.assertRegex(first["mtime"], r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")

    def test_list_subdir_and_limit(self):
        self.write("资料/a.txt", "a")
        self.write("资料/b.txt", "b")
        payload = self.ok("files_list", path="资料", limit=1)
        self.assertEqual(payload["path"], "资料")
        self.assertEqual(payload["count"], 2)
        self.assertEqual(payload["returned"], 1)
        self.assertTrue(payload["truncated"])

    def test_read_text(self):
        self.write("你好.txt", "你好，世界\n第二行")
        payload = self.ok("files_read", path="你好.txt")
        self.assertEqual(payload["text"], "你好，世界\n第二行")
        self.assertEqual(payload["encoding"], "utf-8")
        self.assertFalse(payload["truncated"])
        self.assertGreater(payload["size"], 0)

    def test_read_max_chars_truncates(self):
        self.write("long.txt", "A" * 50)
        payload = self.ok("files_read", path="long.txt", max_chars=10)
        self.assertEqual(payload["returned_chars"], 10)
        self.assertEqual(payload["total_chars"], 50)
        self.assertTrue(payload["truncated"])

    def test_read_gbk_text(self):
        self.write("gbk.txt", "中文测试".encode("gb18030"))
        payload = self.ok("files_read", path="gbk.txt")
        self.assertEqual(payload["text"], "中文测试")
        self.assertIn(payload["encoding"], ("gb18030", "gbk"))

    def test_info_file_preview(self):
        self.write("note.txt", "x" * 500)
        payload = self.ok("files_info", path="note.txt")
        self.assertEqual(payload["type"], "file")
        self.assertTrue(payload["is_text"])
        self.assertEqual(payload["size"], 500)
        self.assertEqual(len(payload["preview"]), m.PREVIEW_CHARS)
        self.assertTrue(payload["preview_truncated"])

    def test_info_dir(self):
        self.write("d/a.txt", "a")
        self.write("d/b.txt", "b")
        payload = self.ok("files_info", path="d")
        self.assertEqual(payload["type"], "dir")
        self.assertTrue(payload["is_dir"])
        self.assertFalse(payload["is_text"])
        self.assertEqual(payload["entry_count"], 2)

    def test_mkdir_move_delete(self):
        created = self.ok("files_mkdir", path="资料/2024")
        self.assertTrue(created["created"])
        self.assertTrue(os.path.isdir(self.path("资料/2024")))
        again = self.ok("files_mkdir", path="资料/2024")
        self.assertFalse(again["created"])

        self.write("a.txt", "hello")
        moved = self.ok("files_move", src="a.txt", dst="资料/2024/b.txt")
        self.assertTrue(moved["moved"])
        self.assertFalse(os.path.exists(self.path("a.txt")))
        self.assertEqual(self.ok("files_read", path="资料/2024/b.txt")["text"], "hello")

        deleted = self.ok("files_delete", path="资料/2024/b.txt")
        self.assertEqual(deleted["type"], "file")
        self.assertFalse(os.path.exists(self.path("资料/2024/b.txt")))

        self.ok("files_delete", path="资料/2024")
        self.assertFalse(os.path.exists(self.path("资料/2024")))

    def test_mkdir_existing_file_conflict(self):
        self.write("x", "1")
        error = self.err("files_mkdir", path="x")
        self.assertIn("同名文件", error)

    def test_move_rejects_existing_destination(self):
        self.write("a.txt", "a")
        self.write("b.txt", "b")
        error = self.err("files_move", src="a.txt", dst="b.txt")
        self.assertIn("目标已存在", error)

    def test_move_dir_into_itself(self):
        self.ok("files_mkdir", path="d")
        error = self.err("files_move", src="d", dst="d/sub")
        self.assertIn("不能把目录移动到", error)

    def test_move_requires_existing_parent(self):
        self.write("a.txt", "a")
        error = self.err("files_move", src="a.txt", dst="no-such-dir/a.txt")
        self.assertIn("目标父目录不存在", error)

    def test_list_file_is_not_dir(self):
        self.write("a.txt", "a")
        error = self.err("files_list", path="a.txt")
        self.assertIn("不是目录", error)

    def test_read_directory_rejected(self):
        self.ok("files_mkdir", path="d")
        error = self.err("files_read", path="d")
        self.assertIn("目录", error)

    def test_read_missing_file(self):
        error = self.err("files_read", path="nope.txt")
        self.assertIn("不存在", error)

    def test_delete_user_root_rejected(self):
        error = self.err("files_delete", path=".")
        self.assertIn("根目录", error)


# --------------------------------------------------------------------------
# 权限隔离
# --------------------------------------------------------------------------
class PermissionTest(_Base):
    def setUp(self):
        super().setUp()
        self.write("secret.txt", "别人的秘密", directory=self.other_dir)

    def test_normal_user_cannot_pass_other_user(self):
        for name, arguments in (
            ("files_list", {"user": OTHER}),
            ("files_read", {"user": OTHER, "path": "secret.txt"}),
            ("files_info", {"user": OTHER, "path": "secret.txt"}),
            ("files_mkdir", {"user": OTHER, "path": "hack"}),
            ("files_move", {"user": OTHER, "src": "secret.txt", "dst": "x.txt"}),
            ("files_delete", {"user": OTHER, "path": "secret.txt"}),
        ):
            error = self.err(name, **arguments)
            self.assertIn("没有权限看别人的文件夹", error, "%s: %s" % (name, error))
        # 确认别家文件没被改
        with open(self.path("secret.txt", self.other_dir), "r", encoding="utf-8") as fh:
            self.assertEqual(fh.read(), "别人的秘密")

    def test_normal_user_files_users_forbidden(self):
        error = self.err("files_users")
        self.assertIn("超级管理员", error)

    def test_superuser_can_cross_user_dirs(self):
        payload = self.ok("files_list", config=self.super_cfg, user=OTHER)
        self.assertEqual(payload["user"], OTHER)
        self.assertEqual([i["name"] for i in payload["entries"]], ["secret.txt"])
        read = self.ok("files_read", config=self.super_cfg, user=OTHER, path="secret.txt")
        self.assertEqual(read["text"], "别人的秘密")
        info = self.ok("files_info", config=self.super_cfg, user=OTHER, path="secret.txt")
        self.assertEqual(info["type"], "file")
        made = self.ok("files_mkdir", config=self.super_cfg, user=OTHER, path="新建")
        self.assertTrue(made["created"])
        self.assertTrue(os.path.isdir(self.path("新建", self.other_dir)))

    def test_superuser_own_dir_without_user(self):
        super_dir = os.path.join(self.tmp, SUPER)
        os.makedirs(super_dir, exist_ok=True)
        self.write("mine.txt", "我的", directory=super_dir)
        payload = self.ok("files_list", config=self.super_cfg)
        self.assertEqual(payload["user"], SUPER)
        self.assertEqual([i["name"] for i in payload["entries"]], ["mine.txt"])

    def test_superuser_files_users(self):
        self.write("a.txt", "12345")
        payload = self.ok("files_users", config=self.super_cfg)
        self.assertEqual(payload["owner"], SUPER)
        by_name = {item["user"]: item for item in payload["users"]}
        self.assertIn(OWNER, by_name)
        self.assertIn(OTHER, by_name)
        self.assertEqual(by_name[OWNER]["file_count"], 1)
        self.assertEqual(by_name[OWNER]["total_bytes"], 5)
        self.assertEqual(by_name[OTHER]["file_count"], 1)
        # 本次任务所属 QQ 是 SUPER，因此 OWNER 不是“自己”
        self.assertFalse(by_name[OWNER]["is_owner"])
        self.assertFalse(by_name[OTHER]["is_superuser"])

    def test_superuser_still_cannot_use_bad_user(self):
        error = self.err("files_list", config=self.super_cfg, user="../../etc")
        self.assertIn("合法的 QQ 号", error)

    def test_missing_owner_rejects_all_tools(self):
        cfg = m.FilesConfig(root=self.tmp, owner_qq="", superusers=[SUPER])
        cases = (
            ("files_list", {}),
            ("files_read", {"path": "a.txt"}),
            ("files_info", {"path": "a.txt"}),
            ("files_mkdir", {"path": "d"}),
            ("files_move", {"src": "a.txt", "dst": "b.txt"}),
            ("files_delete", {"path": "a.txt"}),
            ("files_users", {}),
        )
        for name, arguments in cases:
            error = self.err(name, config=cfg, **arguments)
            self.assertIn("DSH_FILES_OWNER_QQ", error, "%s: %s" % (name, error))


# --------------------------------------------------------------------------
# 路径穿越 / 符号链接
# --------------------------------------------------------------------------
class PathSecurityTest(_Base):
    def setUp(self):
        super().setUp()
        self.write("secret.txt", "别人的秘密", directory=self.other_dir)
        self.write("ok.txt", "自己的")

    def test_dotdot_traversal_rejected(self):
        cases = (
            ("files_list", {"path": ".."}),
            ("files_list", {"path": "../" + OTHER}),
            ("files_read", {"path": "../" + OTHER + "/secret.txt"}),
            ("files_info", {"path": "../../etc/passwd"}),
            ("files_mkdir", {"path": "../evil"}),
            ("files_move", {"src": "../" + OTHER + "/secret.txt", "dst": "x"}),
            ("files_delete", {"path": "../" + OTHER + "/secret.txt"}),
        )
        for name, arguments in cases:
            error = self.err(name, **arguments)
            self.assertIn("..", error, "%s: %s" % (name, error))
        self.assertTrue(os.path.exists(self.path("secret.txt", self.other_dir)))

    def test_absolute_path_rejected(self):
        absolute = self.path("ok.txt")
        error = self.err("files_read", path=absolute)
        self.assertIn("绝对路径", error)
        error = self.err("files_list", path="/etc")
        self.assertIn("绝对路径", error)
        if os.name == "nt":
            error = self.err("files_read", path="C:\\Windows\\win.ini")
            self.assertIn("绝对路径", error)

    def test_symlink_escape_rejected(self):
        outside = tempfile.mkdtemp(prefix="dshfiles-outside-")
        self.addCleanup(shutil.rmtree, outside, True)
        with open(os.path.join(outside, "leak.txt"), "w", encoding="utf-8") as fh:
            fh.write("根外内容")
        link = os.path.join(self.owner_dir, "link_out")
        try:
            os.symlink(outside, link, target_is_directory=True)
        except (OSError, NotImplementedError, AttributeError):
            self.skipTest("当前环境不允许创建符号链接")
        error = self.err("files_read", path="link_out/leak.txt")
        self.assertIn("越界", error)
        error = self.err("files_list", path="link_out")
        self.assertIn("越界", error)
        error = self.err("files_delete", path="link_out/leak.txt")
        self.assertIn("越界", error)

    def test_symlink_inside_own_dir_delete_only_link(self):
        target = self.write("real.txt", "real")
        link = os.path.join(self.owner_dir, "link.txt")
        try:
            os.symlink(target, link)
        except (OSError, NotImplementedError, AttributeError):
            self.skipTest("当前环境不允许创建符号链接")
        payload = self.ok("files_delete", path="link.txt")
        self.assertEqual(payload["type"], "symlink")
        self.assertFalse(os.path.islink(link))
        self.assertTrue(os.path.exists(target))


# --------------------------------------------------------------------------
# 二进制 / 大小限制 / recursive
# --------------------------------------------------------------------------
class ContentSafetyTest(_Base):
    def test_binary_read_rejected(self):
        self.write("bin.dat", b"\x00\x01\x02\xff\xfe binary")
        error = self.err("files_read", path="bin.dat")
        self.assertIn("不是文本文件", error)
        info = self.ok("files_info", path="bin.dat")
        self.assertFalse(info["is_text"])
        self.assertIsNone(info["preview"])

    def test_size_limit_rejected(self):
        cfg = m.FilesConfig(
            root=self.tmp, owner_qq=OWNER, superusers=[SUPER], max_read_bytes=10
        )
        self.write("big.txt", "x" * 100)
        error = self.err("files_read", config=cfg, path="big.txt")
        self.assertIn("文件过大", error)
        info = self.ok("files_info", config=cfg, path="big.txt")
        self.assertEqual(info["size"], 100)

    def test_delete_nonempty_requires_recursive(self):
        self.write("d/a.txt", "a")
        self.write("d/sub/b.txt", "b")
        error = self.err("files_delete", path="d")
        self.assertIn("recursive", error)
        self.assertTrue(os.path.isdir(self.path("d")))
        payload = self.ok("files_delete", path="d", recursive=True)
        self.assertEqual(payload["type"], "dir")
        self.assertTrue(payload["recursive"])
        self.assertFalse(os.path.exists(self.path("d")))

    def test_delete_empty_dir_without_recursive(self):
        self.ok("files_mkdir", path="empty")
        payload = self.ok("files_delete", path="empty")
        self.assertEqual(payload["type"], "dir")
        self.assertFalse(os.path.exists(self.path("empty")))


# --------------------------------------------------------------------------
# 工具定义 / 日志 / 分发
# --------------------------------------------------------------------------
class MetadataTest(_Base):
    def test_tool_definitions(self):
        names = [tool["name"] for tool in m.TOOL_DEFS]
        self.assertEqual(len(names), 7)
        self.assertEqual(len(set(names)), 7)
        expected = {
            "files_list",
            "files_read",
            "files_info",
            "files_mkdir",
            "files_move",
            "files_delete",
            "files_users",
        }
        self.assertEqual(set(names), expected)
        self.assertEqual(m.TOOL_NAMES, names)
        for tool in m.TOOL_DEFS:
            self.assertTrue(tool["name"].startswith("files_"))
            self.assertTrue(tool["description"])
            schema = tool["inputSchema"]
            self.assertEqual(schema["type"], "object")
            self.assertIn("properties", schema)
        required = {
            tool["name"]: set(tool["inputSchema"].get("required") or [])
            for tool in m.TOOL_DEFS
        }
        self.assertEqual(required["files_read"], {"path"})
        self.assertEqual(required["files_mkdir"], {"path"})
        self.assertEqual(required["files_delete"], {"path"})
        self.assertEqual(required["files_move"], {"src", "dst"})
        self.assertEqual(required["files_info"], {"path"})
        self.assertEqual(required["files_list"], set())
        self.assertEqual(required["files_users"], set())

    def test_unknown_tool(self):
        error = self.err("files_nope")
        self.assertIn("未知工具", error)

    def test_log_written(self):
        self.write("a.txt", "a")
        self.ok("files_list")
        self.ok("files_read", path="a.txt")
        log_path = os.path.join(self.tmp, m.LOG_NAME)
        self.assertTrue(os.path.isfile(log_path))
        with open(log_path, "r", encoding="utf-8") as fh:
            content = fh.read()
        self.assertIn("tool=files_list", content)
        self.assertIn("tool=files_read", content)
        self.assertIn("ok", content)

    def test_log_failure_does_not_break_tool(self):
        # 根目录不存在时写日志失败，但工具本身仍应正常工作（用根外的配置不行，
        # 这里直接把 root 指到不可写路径会连带路径校验失败，故只验证 log() 不抛）
        m.log(m.FilesConfig(root=os.path.join(self.tmp, "nope"), owner_qq=OWNER), "x", {}, "y")


# --------------------------------------------------------------------------
# 真实 stdio 子进程
# --------------------------------------------------------------------------
class _StdioServer:
    def __init__(self, env, cwd=HERE):
        self.proc = subprocess.Popen(
            [sys.executable, SERVER_SCRIPT],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=cwd,
            env=env,
        )
        self._queue = queue.Queue()
        self._next_id = 0
        self._thread = threading.Thread(target=self._pump, daemon=True)
        self._thread.start()

    def _pump(self):
        try:
            for raw in self.proc.stdout:
                self._queue.put(raw.decode("utf-8", "replace"))
        except Exception:  # noqa: BLE001
            pass
        finally:
            self._queue.put(None)

    def _write(self, message):
        line = json.dumps(message, ensure_ascii=False) + "\n"
        self.proc.stdin.write(line.encode("utf-8"))
        self.proc.stdin.flush()

    def notify(self, method, params=None):
        message = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        self._write(message)

    def request(self, method, params=None, timeout=15):
        self._next_id += 1
        message = {"jsonrpc": "2.0", "id": self._next_id, "method": method}
        if params is not None:
            message["params"] = params
        self._write(message)
        deadline = time.time() + timeout
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise AssertionError("等待 %s 响应超时" % method)
            try:
                line = self._queue.get(timeout=remaining)
            except queue.Empty:
                raise AssertionError("等待 %s 响应超时" % method)
            if line is None:
                raise AssertionError("服务器 stdout 已关闭：%s" % self._stderr_tail())
            data = json.loads(line)
            if data.get("id") == self._next_id:
                return data

    def call_tool(self, name, arguments=None, timeout=15):
        return self.request(
            "tools/call",
            {"name": name, "arguments": arguments or {}},
            timeout=timeout,
        )

    def _stderr_tail(self):
        try:
            data = self.proc.stderr.read()
            return data.decode("utf-8", "replace")[-2000:]
        except Exception:  # noqa: BLE001
            return ""

    def close(self):
        try:
            self.proc.stdin.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)
        for stream in (self.proc.stdout, self.proc.stderr):
            try:
                if stream is not None:
                    stream.close()
            except Exception:  # noqa: BLE001
                pass


class StdioServerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dshfiles-stdio-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        os.makedirs(os.path.join(self.tmp, OWNER), exist_ok=True)
        self.servers = []

    def tearDown(self):
        for server in self.servers:
            server.close()

    def _start(self, with_owner=True):
        env = dict(os.environ)
        env.update(
            {
                "DSH_FILES_ROOT": self.tmp,
                "DSH_FILES_SUPERUSERS": SUPER,
                "DSH_FILES_MAX_READ_BYTES": "65536",
                "PYTHONIOENCODING": "utf-8",
            }
        )
        if with_owner:
            env["DSH_FILES_OWNER_QQ"] = OWNER
        else:
            env.pop("DSH_FILES_OWNER_QQ", None)
        server = _StdioServer(env)
        self.servers.append(server)
        return server

    @staticmethod
    def _tool_text(response):
        return response["result"]["content"][0]["text"]

    def test_initialize_tools_list_and_call(self):
        server = self._start()
        init = server.request(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "unittest", "version": "1.0"},
            },
        )
        self.assertEqual(init["result"]["protocolVersion"], "2025-06-18")
        self.assertEqual(init["result"]["serverInfo"]["name"], "dsh-files-mcp")
        self.assertIn("tools", init["result"]["capabilities"])
        server.notify("notifications/initialized")

        ping = server.request("ping")
        self.assertEqual(ping["result"], {})

        listed = server.request("tools/list")
        names = [tool["name"] for tool in listed["result"]["tools"]]
        self.assertEqual(len(names), 7)
        self.assertIn("files_read", names)

        made = server.call_tool("files_mkdir", {"path": "stdio目录"})
        self.assertFalse(made["result"]["isError"], made)
        made_payload = json.loads(self._tool_text(made))
        self.assertTrue(made_payload["created"])
        self.assertTrue(os.path.isdir(os.path.join(self.tmp, OWNER, "stdio目录")))

        missing = server.call_tool("files_read", {"path": "不存在.txt"})
        self.assertTrue(missing["result"]["isError"])
        self.assertIn("不存在", self._tool_text(missing))

        echo = server.call_tool(
            "files_mkdir", {"path": "no-such", "user": OTHER}
        )
        self.assertTrue(echo["result"]["isError"])
        self.assertIn("没有权限看别人的文件夹", self._tool_text(echo))

    def test_missing_owner_rejects_operations(self):
        server = self._start(with_owner=False)
        server.request("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}})
        server.notify("notifications/initialized")
        response = server.call_tool("files_list")
        self.assertTrue(response["result"]["isError"])
        self.assertIn("DSH_FILES_OWNER_QQ", self._tool_text(response))

    def test_unknown_method_returns_error(self):
        server = self._start()
        response = server.request("no/such/method")
        self.assertEqual(response["error"]["code"], -32601)


if __name__ == "__main__":
    unittest.main(verbosity=2)
