#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""dsh 文件管理 MCP 服务器（stdio 传输，纯标准库，Python 3.8+）。

dsh 通过 @deepseek-ai/dsh-mcp-client 以 stdio 方式启动本进程。QQ 用户发给
dsh 的文件由桥按 QQ 号分目录存放：

    <DSH_FILES_ROOT>/<QQ>/...

本服务器让 dsh 能列目录、读文本、看详情、建目录、移动、删除，并按 QQ 号隔离：

    * 普通用户（本次任务所属 QQ = DSH_FILES_OWNER_QQ）只能看/改自己的目录；
    * 超级管理员（DSH_FILES_SUPERUSERS，默认 2953679189）可以跨目录看别人。

环境变量：
    DSH_FILES_ROOT            文件根目录，默认 /root/DSH-work/mai/files
    DSH_FILES_OWNER_QQ        本次 dsh 任务所属 QQ（桥注入）；缺失时拒绝所有操作
    DSH_FILES_SUPERUSERS      逗号分隔超管 QQ，默认 2953679189
    DSH_FILES_MAX_READ_BYTES  单次读取上限，默认 2MB（2*1024*1024）

工具（全部前缀 files_）：
    files_list / files_read / files_info / files_mkdir / files_move /
    files_delete / files_users

安全模型：
    * 用户目录 = realpath(<ROOT>/<QQ>)，必须仍在 realpath(ROOT) 内；
    * 请求路径只接受用户目录内相对路径，拒绝绝对路径与含 ``..`` 的路径；
    * 一律 realpath 解析后再用 is_relative_to 语义做包含校验，符号链接
      解析到允许根之外会被拒绝（不跟随逃逸链接）；
    * 删除前对路径 realpath 二次校验；
    * files_read 同时受文件大小上限与 max_chars 限制。

日志：``<DSH_FILES_ROOT>/.mcp.log``（追加写，写失败不影响主流程）。
"""
from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import stat
import sys
import time

SERVER_NAME = "dsh-files-mcp"
SERVER_VERSION = "0.1.0"
PROTOCOL_VERSION = "2025-06-18"

DEFAULT_ROOT = "/root/DSH-work/mai/files"
DEFAULT_SUPERUSERS = "2953679189"
DEFAULT_MAX_READ_BYTES = 2 * 1024 * 1024
DEFAULT_LIST_LIMIT = 200
MAX_LIST_LIMIT = 2000
DEFAULT_MAX_CHARS = 8000
MAX_READ_CHARS = 200000
PREVIEW_CHARS = 200
LOG_NAME = ".mcp.log"

# QQ 号只允许数字，顺带挡掉用 user 参数做目录穿越/绝对路径的花样
USER_QQ_RE = re.compile(r"^[0-9]{1,20}$")


class ToolError(Exception):
    """工具层可预期的错误：消息会原样返回给模型（isError=true）。"""


# --------------------------------------------------------------------------
# 通用小工具
# --------------------------------------------------------------------------
def _norm(path):
    """realpath + expanduser，用于所有路径解析。"""
    return os.path.realpath(os.path.expanduser(str(path)))


def _is_relative_to(path, base):
    """``path`` 是否在 ``base`` 内（含相等）。

    等价于 Python 3.9+ 的 ``Path.is_relative_to``；这里用 commonpath 实现
    以兼容 Python 3.8，并在 Windows 上做 normcase（大小写不敏感）。
    """
    p = os.path.normcase(os.path.abspath(path))
    b = os.path.normcase(os.path.abspath(base))
    if p == b:
        return True
    try:
        return os.path.commonpath([b, p]) == b
    except ValueError:  # 不同盘符（Windows）等
        return False


def _to_int(value, default):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "y", "on"):
        return True
    if text in ("0", "false", "no", "n", "off", ""):
        return False
    return default


def _fmt_time(ts):
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(ts)))
    except (TypeError, ValueError, OSError):
        return ""


def _decode_text(data):
    """把字节解码成文本；判定为二进制时抛 ToolError。

    返回 (text, encoding)。识别 UTF-8 / UTF-8 BOM / UTF-16 BOM / GB18030。
    """
    if not data:
        return "", "utf-8"
    if data.startswith(b"\xef\xbb\xbf"):
        try:
            return data.decode("utf-8-sig"), "utf-8-sig"
        except UnicodeDecodeError:
            raise ToolError("文件编码异常（UTF-8 BOM 但内容不是合法 UTF-8），请改用 files_info")
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        try:
            return data.decode("utf-16"), "utf-16"
        except UnicodeDecodeError:
            raise ToolError("文件编码异常（UTF-16 BOM 但内容无法解码），请改用 files_info")
    if b"\x00" in data:
        raise ToolError(
            "这不是文本文件（检测到二进制内容，可能是图片/压缩包/Office 文档等），"
            "请改用 files_info 查看大小与类型"
        )
    for enc in ("utf-8", "gb18030"):
        try:
            return data.decode(enc), enc
        except UnicodeDecodeError:
            continue
    raise ToolError(
        "这不是文本文件（无法按 UTF-8/GB18030 解码），请改用 files_info 查看大小与类型"
    )


def _sample_text(path, limit=4096):
    """读文件开头一小段并尽力解码，用于 files_info。

    返回 (text, encoding)；不是文本或读取失败返回 (None, None)。
    """
    try:
        with open(path, "rb") as fh:
            head = fh.read(limit)
    except OSError:
        return None, None
    if not head:
        return "", "utf-8"
    try:
        text, enc = _decode_text(head)
        return text, enc
    except ToolError:
        pass
    # 采样可能在多字节字符中间截断：去掉尾部最多 4 字节再试（仅 UTF-8 场景）
    if not head.startswith((b"\xef\xbb\xbf", b"\xff\xfe", b"\xfe\xff")) and b"\x00" not in head:
        for cut in range(1, 5):
            try:
                return head[:-cut].decode("utf-8"), "utf-8"
            except UnicodeDecodeError:
                continue
    return None, None


def _result_brief(payload):
    parts = []
    for key in (
        "user",
        "path",
        "count",
        "returned",
        "size",
        "total_chars",
        "encoding",
        "created",
        "moved",
        "deleted",
        "recursive",
    ):
        if key in payload:
            parts.append("%s=%s" % (key, payload[key]))
    return " ".join(parts)[:400]


def log(config, tool, arguments, result):
    """追加写 ``<ROOT>/.mcp.log``；任何异常都吞掉，绝不影响主流程。"""
    try:
        root = config.root if config is not None else DEFAULT_ROOT
        try:
            args_text = json.dumps(arguments or {}, ensure_ascii=False, default=str)
        except Exception:  # noqa: BLE001
            args_text = str(arguments)
        line = "[%s] tool=%s args=%s result=%s\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"),
            tool,
            args_text[:400],
            str(result)[:400],
        )
        with open(os.path.join(root, LOG_NAME), "a", encoding="utf-8") as fh:
            fh.write(line)
    except Exception:  # noqa: BLE001
        pass


# --------------------------------------------------------------------------
# 配置
# --------------------------------------------------------------------------
class FilesConfig:
    """一次 dsh 任务的上下文：根目录、所属 QQ、超管列表、读取上限。"""

    def __init__(self, root=DEFAULT_ROOT, owner_qq="", superusers=(), max_read_bytes=DEFAULT_MAX_READ_BYTES):
        root_text = str(root or "").strip() or DEFAULT_ROOT
        self.root = _norm(root_text)
        self.owner_qq = str(owner_qq or "").strip()
        supers = []
        if isinstance(superusers, str):
            supers = superusers.split(",")
        else:
            supers = list(superusers or ())
        merged = {str(x).strip() for x in supers if str(x).strip()}
        self.superusers = tuple(sorted(merged))
        try:
            limit = int(max_read_bytes)
        except (TypeError, ValueError):
            limit = DEFAULT_MAX_READ_BYTES
        if limit <= 0:
            limit = DEFAULT_MAX_READ_BYTES
        self.max_read_bytes = limit

    @staticmethod
    def _read_context_file():
        """读桥写的「本任务上下文」文件。

        dsh 的 mcp-client 在 spawn 子进程前会**剥掉 DSH_ 开头的环境变量**（只留 PATH 等），
        所以「本次任务属于哪个 QQ」这种每轮变化的信息没法靠 env 继承，改由桥每轮写进
        工作区的 ``ipc/files-context.json``：``{"owner_qq": "...", "root": "...", "superusers": [...]}``。
        """
        context_path = os.environ.get("DSH_FILES_CONTEXT", "").strip()
        candidates = [pathlib.Path(context_path)] if context_path else []
        candidates.append(pathlib.Path.cwd() / "ipc" / "files-context.json")
        for fp in candidates:
            try:
                if fp.is_file():
                    data = json.loads(fp.read_text(encoding="utf-8"))
                    if isinstance(data, dict):
                        return data
            except Exception:  # noqa: BLE001
                continue
        return {}

    @classmethod
    def from_env(cls, environ=None):
        env = os.environ if environ is None else environ
        ctx = cls._read_context_file()

        def get(name, default=""):
            value = env.get(name)
            return default if value is None else str(value)

        root = get("DSH_FILES_ROOT").strip() or str(ctx.get("root") or "").strip() or DEFAULT_ROOT
        owner = get("DSH_FILES_OWNER_QQ").strip() or str(ctx.get("owner_qq") or "").strip()
        supers_text = (
            get("DSH_FILES_SUPERUSERS").strip()
            or ",".join(str(x) for x in (ctx.get("superusers") or []))
            or DEFAULT_SUPERUSERS
        )
        max_read = _to_int(get("DSH_FILES_MAX_READ_BYTES").strip(), DEFAULT_MAX_READ_BYTES)
        return cls(
            root=root,
            owner_qq=owner,
            superusers=[x.strip() for x in supers_text.split(",") if x.strip()],
            max_read_bytes=max_read,
        )

    def require_owner(self):
        if not self.owner_qq:
            raise ToolError(
                "拿不到本次任务所属 QQ（既没有 DSH_FILES_OWNER_QQ 环境变量，也没有工作区 "
                "ipc/files-context.json），文件操作已全部拒绝（请让桥写入该上下文文件）"
            )

    def is_superuser(self, qq=None):
        who = self.owner_qq if qq is None else str(qq).strip()
        return bool(who) and who in self.superusers


# --------------------------------------------------------------------------
# 业务实现
# --------------------------------------------------------------------------
class FilesService:
    """一个 FilesConfig 下的文件操作实现（与 MCP 传输层解耦，便于单测）。"""

    def __init__(self, config):
        if not isinstance(config, FilesConfig):
            raise TypeError("config 必须是 FilesConfig")
        self.config = config

    # ----------------------------- 权限与路径 -----------------------------
    def _user_dir(self, qq):
        resolved = _norm(os.path.join(self.config.root, qq))
        if not _is_relative_to(resolved, self.config.root) or resolved == self.config.root:
            raise ToolError("用户目录解析后越界（疑似符号链接逃逸），已拒绝")
        return resolved

    def resolve_user(self, requested):
        """把工具参数 user（可能为空）解析成实际 QQ 号，并做权限判断。"""
        self.config.require_owner()
        owner = self.config.owner_qq
        req = "" if requested is None else str(requested).strip()
        if not req or req == owner:
            return owner
        if not USER_QQ_RE.match(req):
            raise ToolError("user 参数不是合法的 QQ 号：%r" % req)
        if self.config.is_superuser():
            return req
        raise ToolError(
            "没有权限看别人的文件夹（当前 QQ %s 只能操作自己的目录，请求的是 %s）"
            % (owner, req)
        )

    def _literal_path(self, base, rel):
        """把用户给的相对路径拼成 base 内的字面路径（静态校验，不跟随符号链接）。"""
        raw = "" if rel is None else str(rel).strip()
        if raw in ("", "."):
            return base
        if "\x00" in raw:
            raise ToolError("路径包含非法字符（NUL）")
        if os.path.isabs(raw) or raw.startswith(("/", "\\")) or (
            len(raw) >= 2 and raw[1] == ":"
        ):
            raise ToolError("不允许使用绝对路径，请传用户目录内的相对路径")
        segments = [seg for seg in raw.replace("\\", "/").split("/") if seg not in ("", ".")]
        if any(seg == ".." for seg in segments):
            raise ToolError("路径不允许包含 ..（禁止目录穿越）")
        if not segments:
            return base
        return os.path.join(base, *segments)

    def _resolve(self, base, rel, allow_root=False):
        """返回 (literal, resolved)；校验相对路径、越界与符号链接逃逸。"""
        literal = self._literal_path(base, rel)
        resolved = _norm(literal)
        if not _is_relative_to(resolved, base):
            raise ToolError(
                "路径越界：realpath 解析后不在允许的目录内"
                "（禁止 ..、绝对路径与符号链接逃逸）"
            )
        if resolved == base and not allow_root:
            raise ToolError("path 不能指向用户根目录本身")
        return literal, resolved

    def _rel_display(self, base, path):
        try:
            rel = os.path.relpath(path, base)
        except ValueError:
            return str(path)
        return "." if rel == os.curdir else rel.replace(os.sep, "/")

    # ------------------------------- 工具 --------------------------------
    def files_list(self, arguments):
        user = self.resolve_user(arguments.get("user"))
        base = self._user_dir(user)
        _, target = self._resolve(base, arguments.get("path"), allow_root=True)
        if not os.path.lexists(target):
            raise ToolError("目录不存在：%s" % self._rel_display(base, target))
        if not os.path.isdir(target):
            raise ToolError("这不是目录，不能列出内容；查看文件详情请用 files_info")
        limit = max(1, min(_to_int(arguments.get("limit"), DEFAULT_LIST_LIMIT), MAX_LIST_LIMIT))

        entries = []
        try:
            with os.scandir(target) as iterator:
                for entry in iterator:
                    try:
                        st = entry.stat(follow_symlinks=False)
                    except OSError:
                        continue
                    is_link = entry.is_symlink()
                    is_dir = entry.is_dir(follow_symlinks=False)
                    item = {
                        "name": entry.name,
                        "path": self._rel_display(base, entry.path),
                        "type": "dir" if is_dir else ("file" if not is_link else "symlink"),
                        "is_dir": is_dir,
                        "is_symlink": is_link,
                        "size": None if is_dir else int(st.st_size),
                        "mtime": _fmt_time(st.st_mtime),
                        "_mtime": float(st.st_mtime),
                    }
                    entries.append(item)
        except OSError as exc:
            raise ToolError("列目录失败：%s" % exc)

        entries.sort(key=lambda x: (-x["_mtime"], x["name"]))
        for item in entries:
            item.pop("_mtime", None)
        shown = entries[:limit]
        return {
            "user": user,
            "path": self._rel_display(base, target),
            "count": len(entries),
            "returned": len(shown),
            "truncated": len(entries) > len(shown),
            "sort": "mtime_desc",
            "entries": shown,
        }

    def files_read(self, arguments):
        user = self.resolve_user(arguments.get("user"))
        base = self._user_dir(user)
        _, target = self._resolve(base, arguments.get("path"))
        if not os.path.lexists(target):
            raise ToolError("文件不存在：%s" % self._rel_display(base, target))
        if os.path.isdir(target):
            raise ToolError("这是目录，不能当文本读取；列目录请用 files_list")
        if not os.path.isfile(target):
            raise ToolError("不是普通文件，无法读取；请用 files_info 查看详情")
        try:
            size = os.path.getsize(target)
        except OSError as exc:
            raise ToolError("读取文件失败：%s" % exc)
        limit_bytes = self.config.max_read_bytes
        if size > limit_bytes:
            raise ToolError(
                "文件过大：%d 字节，超过单次读取上限 %d 字节（可调大 "
                "DSH_FILES_MAX_READ_BYTES），请改用 files_info 查看详情"
                % (size, limit_bytes)
            )
        try:
            with open(target, "rb") as fh:
                data = fh.read(limit_bytes + 1)
        except OSError as exc:
            raise ToolError("读取文件失败：%s" % exc)
        if len(data) > limit_bytes:
            raise ToolError(
                "文件过大：超过单次读取上限 %d 字节（可调大 "
                "DSH_FILES_MAX_READ_BYTES），请改用 files_info 查看详情" % limit_bytes
            )

        text, encoding = _decode_text(data)
        max_chars = max(1, min(_to_int(arguments.get("max_chars"), DEFAULT_MAX_CHARS), MAX_READ_CHARS))
        shown = text[:max_chars]
        payload = {
            "user": user,
            "path": self._rel_display(base, target),
            "size": size,
            "encoding": encoding,
            "total_chars": len(text),
            "returned_chars": len(shown),
            "truncated": len(text) > len(shown),
            "text": shown,
        }
        if payload["truncated"]:
            payload["hint"] = (
                "内容已截断：可调大 max_chars（上限 %d），或先用 files_info 看预览/大小"
                % MAX_READ_CHARS
            )
        return payload

    def files_info(self, arguments):
        user = self.resolve_user(arguments.get("user"))
        base = self._user_dir(user)
        _, target = self._resolve(base, arguments.get("path"))
        if not os.path.lexists(target):
            raise ToolError("路径不存在：%s" % self._rel_display(base, target))
        try:
            st = os.stat(target)
        except OSError as exc:
            raise ToolError("获取文件信息失败：%s" % exc)
        payload = {
            "user": user,
            "path": self._rel_display(base, target),
            "name": os.path.basename(target),
            "mtime": _fmt_time(st.st_mtime),
            "size": int(st.st_size),
        }
        if os.path.isdir(target):
            try:
                with os.scandir(target) as iterator:
                    count = sum(1 for _ in iterator)
            except OSError:
                count = None
            payload.update(
                {
                    "type": "dir",
                    "is_dir": True,
                    "is_text": False,
                    "entry_count": count,
                    "preview": None,
                }
            )
            return payload

        text, encoding = _sample_text(target)
        is_text = text is not None
        preview = None
        preview_truncated = False
        if is_text:
            preview = text[:PREVIEW_CHARS]
            preview_truncated = len(text) > PREVIEW_CHARS
        payload.update(
            {
                "type": "file",
                "is_dir": False,
                "is_text": is_text,
                "encoding": encoding,
                "preview": preview,
                "preview_truncated": preview_truncated,
            }
        )
        if not is_text:
            payload["note"] = "疑似二进制文件，建议用 files_read 之外的方式处理（或先看大小）"
        return payload

    def files_mkdir(self, arguments):
        user = self.resolve_user(arguments.get("user"))
        base = self._user_dir(user)
        literal, target = self._resolve(base, arguments.get("path"))
        rel = self._rel_display(base, target)
        if os.path.lexists(literal) or os.path.lexists(target):
            if os.path.isdir(target):
                return {"user": user, "path": rel, "created": False, "note": "目录已存在"}
            raise ToolError("同名文件已存在，无法创建目录：%s" % rel)
        try:
            os.makedirs(target, exist_ok=True)
        except OSError as exc:
            raise ToolError("创建目录失败：%s" % exc)
        return {"user": user, "path": rel, "created": True}

    def files_move(self, arguments):
        user = self.resolve_user(arguments.get("user"))
        base = self._user_dir(user)
        src_literal, src = self._resolve(base, arguments.get("src"))
        dst_literal, dst = self._resolve(base, arguments.get("dst"))
        if not (os.path.lexists(src_literal) or os.path.lexists(src)):
            raise ToolError("源路径不存在：%s" % self._rel_display(base, src))
        if os.path.lexists(dst_literal) or os.path.lexists(dst):
            raise ToolError("目标已存在，拒绝覆盖：%s" % self._rel_display(base, dst))
        if src == dst:
            raise ToolError("源和目标相同，无需移动")
        if os.path.isdir(src) and _is_relative_to(dst, src):
            raise ToolError("不能把目录移动到它自己或它的子目录里")
        dst_parent = _norm(os.path.dirname(dst_literal))
        if not _is_relative_to(dst_parent, base):
            raise ToolError("目标父目录越界，已拒绝")
        if not os.path.isdir(dst_parent):
            raise ToolError(
                "目标父目录不存在：%s（可先用 files_mkdir 创建）"
                % self._rel_display(base, dst_parent)
            )
        src_op = src_literal if os.path.islink(src_literal) else src
        try:
            os.rename(src_op, dst_literal)
        except OSError as exc:
            raise ToolError("移动失败：%s" % exc)
        return {
            "user": user,
            "src": self._rel_display(base, src),
            "dst": self._rel_display(base, dst),
            "moved": True,
        }

    def files_delete(self, arguments):
        user = self.resolve_user(arguments.get("user"))
        base = self._user_dir(user)
        literal, target = self._resolve(base, arguments.get("path"))
        if not (os.path.lexists(literal) or os.path.lexists(target)):
            raise ToolError("路径不存在：%s" % self._rel_display(base, target))
        recursive = _to_bool(arguments.get("recursive"), False)

        # 删除前二次 realpath 校验：路径解析结果必须稳定且仍在允许根内
        again = _norm(literal)
        if again != target or not _is_relative_to(again, base):
            raise ToolError("删除前校验失败：realpath 解析结果不一致或越界，已拒绝")
        if again == base:
            raise ToolError("不能删除用户根目录本身")

        is_link = os.path.islink(literal)
        op_target = literal if is_link else again
        if os.path.isdir(op_target) and not is_link:
            try:
                with os.scandir(op_target) as iterator:
                    has_children = any(True for _ in iterator)
            except OSError as exc:
                raise ToolError("读取目录失败：%s" % exc)
            if has_children and not recursive:
                raise ToolError(
                    "目录非空，必须传 recursive=true 才能删除：%s"
                    % self._rel_display(base, target)
                )
            try:
                if recursive:
                    shutil.rmtree(op_target)
                else:
                    os.rmdir(op_target)
            except OSError as exc:
                raise ToolError("删除目录失败：%s" % exc)
            kind = "dir"
        else:
            try:
                os.unlink(op_target)
            except OSError as exc:
                raise ToolError("删除文件失败：%s" % exc)
            kind = "symlink" if is_link else "file"
        return {
            "user": user,
            "path": self._rel_display(base, target),
            "type": kind,
            "recursive": recursive,
            "deleted": True,
        }

    def files_users(self, arguments=None):
        self.config.require_owner()
        if not self.config.is_superuser():
            raise ToolError("只有超级管理员可以查看所有用户的文件夹")
        root = self.config.root
        if not os.path.isdir(root):
            raise ToolError("文件根目录不存在：%s" % root)
        users = []
        try:
            with os.scandir(root) as iterator:
                for entry in iterator:
                    try:
                        if not entry.is_dir(follow_symlinks=False):
                            continue
                    except OSError:
                        continue
                    name = entry.name
                    if not USER_QQ_RE.match(name):
                        continue
                    resolved = _norm(entry.path)
                    if resolved == root or not _is_relative_to(resolved, root):
                        # 目录本身是逃逸符号链接：不展示，避免泄露根外内容
                        continue
                    file_count, total_bytes = _tree_usage(resolved)
                    users.append(
                        {
                            "user": name,
                            "file_count": file_count,
                            "total_bytes": total_bytes,
                            "is_owner": name == self.config.owner_qq,
                            "is_superuser": name in self.config.superusers,
                        }
                    )
        except OSError as exc:
            raise ToolError("扫描文件根目录失败：%s" % exc)
        users.sort(key=lambda x: x["user"])
        return {
            "root": root,
            "owner": self.config.owner_qq,
            "count": len(users),
            "total_files": sum(u["file_count"] for u in users),
            "total_bytes": sum(u["total_bytes"] for u in users),
            "users": users,
        }


def _tree_usage(directory):
    """统计目录下普通文件数/总字节数；跳过符号链接与根外内容。"""
    file_count = 0
    total_bytes = 0
    try:
        for current, dirs, names in os.walk(directory, followlinks=False):
            dirs[:] = [
                d
                for d in dirs
                if not os.path.islink(os.path.join(current, d))
            ]
            for name in names:
                path = os.path.join(current, name)
                try:
                    st = os.lstat(path)
                except OSError:
                    continue
                if stat.S_ISLNK(st.st_mode):
                    continue
                if stat.S_ISREG(st.st_mode):
                    file_count += 1
                    total_bytes += int(st.st_size)
    except OSError:
        pass
    return file_count, total_bytes


# --------------------------------------------------------------------------
# MCP 工具定义
# --------------------------------------------------------------------------
_USER_PROP = {
    "type": "string",
    "description": (
        "要操作哪个 QQ 的目录（缺省=本次任务所属 QQ，即自己）。"
        "普通用户只能填自己；只有超级管理员可以填别人的 QQ。"
    ),
}
_PATH_PROP = {
    "type": "string",
    "description": "用户目录内的相对路径（如 '报告.docx' 或 '资料/2024'），不允许绝对路径或 ..",
}

TOOL_DEFS = [
    {
        "name": "files_list",
        "description": (
            "列出某个 QQ 用户目录（默认自己的目录）下的文件，按修改时间倒序返回"
            "名称/大小/修改时间/是否目录。path 可指定子目录。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "user": _USER_PROP,
                "path": {
                    "type": "string",
                    "description": "相对用户目录的目录路径，缺省为用户目录根",
                },
                "limit": {
                    "type": "integer",
                    "description": "最多返回条数，默认 200",
                    "minimum": 1,
                    "maximum": MAX_LIST_LIMIT,
                    "default": DEFAULT_LIST_LIMIT,
                },
            },
        },
    },
    {
        "name": "files_read",
        "description": (
            "读取用户目录内的文本文件内容（默认自己目录）。二进制文件或超过大小"
            "上限的文件会被拒绝，请改用 files_info 看大小/类型。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "user": _USER_PROP,
                "path": _PATH_PROP,
                "max_chars": {
                    "type": "integer",
                    "description": "最多返回字符数，默认 8000",
                    "minimum": 1,
                    "maximum": MAX_READ_CHARS,
                    "default": DEFAULT_MAX_CHARS,
                },
            },
            "required": ["path"],
        },
    },
    {
        "name": "files_info",
        "description": (
            "查看单个文件或目录的详情：大小、修改时间、是否文本文件、前 200 字预览。"
            "不确定文件类型/是否太大时先用这个。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"user": _USER_PROP, "path": _PATH_PROP},
            "required": ["path"],
        },
    },
    {
        "name": "files_mkdir",
        "description": "在用户目录内创建子目录（自动创建多级父目录）。",
        "inputSchema": {
            "type": "object",
            "properties": {"user": _USER_PROP, "path": _PATH_PROP},
            "required": ["path"],
        },
    },
    {
        "name": "files_move",
        "description": (
            "在同一个用户目录内重命名或移动文件/目录。目标不能已存在；"
            "不能跨用户目录移动。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "user": _USER_PROP,
                "src": {
                    "type": "string",
                    "description": "源路径（相对用户目录）",
                },
                "dst": {
                    "type": "string",
                    "description": "目标路径（相对用户目录，不能已存在）",
                },
            },
            "required": ["src", "dst"],
        },
    },
    {
        "name": "files_delete",
        "description": (
            "删除用户目录内的文件或目录。非空目录必须传 recursive=true；"
            "符号链接只删除链接本身。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "user": _USER_PROP,
                "path": _PATH_PROP,
                "recursive": {
                    "type": "boolean",
                    "description": "是否递归删除非空目录，默认 false",
                    "default": False,
                },
            },
            "required": ["path"],
        },
    },
    {
        "name": "files_users",
        "description": (
            "列出所有 QQ 用户目录及其文件数/占用空间。仅超级管理员可用；"
            "普通用户调用会被拒绝。"
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
]

TOOL_NAMES = [tool["name"] for tool in TOOL_DEFS]


# --------------------------------------------------------------------------
# 工具调用分发
# --------------------------------------------------------------------------
def call_tool(name, arguments, config=None):
    """执行一个工具，返回 (text, is_error)。

    text 为 JSON 字符串（成功：{"ok": true, ...}；失败：{"ok": false, "error": ...}）。
    该函数不写 stdout，方便单元测试直接 import 调用。
    """
    if config is None:
        config = get_config()
    args = arguments if isinstance(arguments, dict) else {}
    service = FilesService(config)

    handlers = {
        "files_list": service.files_list,
        "files_read": service.files_read,
        "files_info": service.files_info,
        "files_mkdir": service.files_mkdir,
        "files_move": service.files_move,
        "files_delete": service.files_delete,
        "files_users": service.files_users,
    }

    handler = handlers.get(name)
    if handler is None:
        message = "未知工具：%s（可用工具：%s）" % (name, ", ".join(TOOL_NAMES))
        log(config, name or "?", args, "error 未知工具")
        return _error_text(name, message), True
    try:
        payload = handler(args)
        payload.setdefault("ok", True)
        text = json.dumps(payload, ensure_ascii=False, indent=2)
        log(config, name, args, "ok " + _result_brief(payload))
        return text, False
    except ToolError as exc:
        log(config, name, args, "error " + str(exc))
        return _error_text(name, str(exc)), True
    except Exception as exc:  # noqa: BLE001
        log(config, name, args, "error 内部异常 %r" % (exc,))
        return _error_text(name, "内部错误：%r" % (exc,)), True


def _error_text(name, message):
    return json.dumps(
        {"ok": False, "tool": name, "error": message}, ensure_ascii=False, indent=2
    )


_CONFIG = None


def get_config():
    """进程级配置（首次从环境变量读取后缓存）。"""
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = FilesConfig.from_env()
    return _CONFIG


# --------------------------------------------------------------------------
# MCP stdio 传输层
# --------------------------------------------------------------------------
def respond(msg_id, result=None, error=None):
    payload = {"jsonrpc": "2.0", "id": msg_id}
    if error is not None:
        payload["error"] = error
    else:
        payload["result"] = result
    data = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
    try:
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()
    except AttributeError:  # 极少数被替换成文本流的场景
        sys.stdout.write(data.decode("utf-8"))
        sys.stdout.flush()


def handle(message, config):
    method = str(message.get("method") or "")
    msg_id = message.get("id")
    params = message.get("params") or {}

    if msg_id is None:
        # 通知：initialized / cancelled 等，无需响应
        if method.startswith("notifications/"):
            if method == "notifications/initialized":
                log(config, "notifications/initialized", {}, "ok")
            return
        return

    if method == "initialize":
        client_proto = str(params.get("protocolVersion") or "").strip()
        proto = client_proto or PROTOCOL_VERSION
        respond(
            msg_id,
            {
                "protocolVersion": proto,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                "instructions": (
                    "文件按 QQ 号隔离：默认操作自己的目录（DSH_FILES_OWNER_QQ）；"
                    "普通用户不能访问别人目录，超级管理员可通过 user 参数跨目录查看。"
                    "先用 files_list / files_info 了解文件，再决定读取或处理。"
                ),
            },
        )
        log(config, "initialize", {"protocolVersion": proto}, "ok")
        return

    if method == "ping":
        respond(msg_id, {})
        return

    if method == "tools/list":
        respond(msg_id, {"tools": TOOL_DEFS})
        return

    if method == "tools/call":
        name = str(params.get("name") or "")
        arguments = params.get("arguments") or {}
        if not isinstance(arguments, dict):
            arguments = {}
        text, is_error = call_tool(name, arguments, config)
        respond(
            msg_id,
            {"content": [{"type": "text", "text": text}], "isError": is_error},
        )
        return

    if method in ("resources/list", "prompts/list"):
        respond(msg_id, {"resources": []} if method.startswith("resources") else {"prompts": []})
        return

    if method == "logging/setLevel":
        respond(msg_id, {})
        return

    respond(msg_id, error={"code": -32601, "message": "method not found: %s" % method})


def main():
    global _CONFIG
    _CONFIG = FilesConfig.from_env()
    try:
        os.makedirs(_CONFIG.root, exist_ok=True)
    except Exception:  # noqa: BLE001
        pass
    log(
        _CONFIG,
        "startup",
        {},
        "%s v%s root=%s owner=%s superusers=%s max_read_bytes=%d"
        % (
            SERVER_NAME,
            SERVER_VERSION,
            _CONFIG.root,
            _CONFIG.owner_qq or "(未设置)",
            ",".join(_CONFIG.superusers) or "(空)",
            _CONFIG.max_read_bytes,
        ),
    )

    for raw in sys.stdin.buffer:
        line = raw.decode("utf-8", "replace").strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except Exception as exc:  # noqa: BLE001
            log(_CONFIG, "protocol", {}, "非法 JSON: %r line=%s" % (exc, line[:200]))
            continue
        batch = message if isinstance(message, list) else [message]
        for item in batch:
            if not isinstance(item, dict):
                continue
            try:
                handle(item, _CONFIG)
            except Exception as exc:  # noqa: BLE001
                log(_CONFIG, "protocol", {}, "处理异常: %r" % (exc,))
                if item.get("id") is not None:
                    respond(item["id"], error={"code": -32603, "message": str(exc)})


if __name__ == "__main__":
    main()
