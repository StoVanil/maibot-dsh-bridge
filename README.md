# 麦麦×dsh 桥（maibot-dsh-bridge）

把 [DeepSeek Harness (dsh)](https://github.com/deepseek-ai/dsh) 接进 [麦麦 (MaiBot)](https://github.com/MaiM-with-u/MaiBot)：
在 QQ 里发一条 `/dsh <任务>`，dsh 就在服务器上把活干完——**每个话题一条持久会话、一个独立工作目录**，
过程可以随时看（`/dsh peek`）、随时停（`/dsh stop`），产出的文件直接发回聊天。

> **本仓库是 [ji-or-ji/maibot-dsh-bridge](https://github.com/ji-or-ji/maibot-dsh-bridge) 的派生版本（fork）。**
> 上游 v0.6.1 提供基础桥接（派活 / 转发回传 / 定时任务 / 权限 / 交付物）；
> 本 fork 在其上加了话题制会话、MCP 提问、`stop`/`peek`、不限时策略、异常救回等，详见 [CHANGELOG.md](CHANGELOG.md)。
> 原始版权归 ji-or-ji，遵循 MIT 许可（见 [LICENSE](LICENSE)）。

## 特性

- **话题制会话**：每个话题 = 一条持久 dsh 会话（`--session-id`）+ 独立工作目录 + 编号（`#1`、`#2`…），话题可命名、归档、恢复。
- **指令全收在 `/dsh` 下**：派活、话题管理、停止、看进展都是一个入口；桥拦截后墨不会接话茬。
- **MCP 提问**：dsh 需要拍板时调用 `ask_user` 工具，桥把问题转成 QQ 消息，你回一句话，**dsh 在同一进程里继续**。
- **默认不限时**：模型估不准自己的耗时，所以不设时间预算；`/dsh peek` 看进度、`/dsh stop` 秒级停止。
- **交付与救回**：dsh 用 `present` 声明交付物 → 桥打包发 QQ；任务被停/异常终止时，本轮新产出的文件也会被「救回」发出。
- **Office 文档**：配合 dsh 的 `skill-office` 技能，读/写 Word、PPT、Excel。
- **实验功能（可选）**：设置里临时开启 24 小时，期间墨可自主建/切话题、派活（二次确认、期间 `/dsh` 禁用）。

## 架构

```
QQ ──► MaiBot 宿主 ──► 本插件（桥）
                          │  1) 解析 /dsh，管理话题（topics.json）
                          │  2) 起子进程：dsh --profile headless --json --session-id <id> -
                          │  3) 读 dsh 的 JSON 事件流（步骤/工具/结果），转发或回灌给墨
                          │  4) 扫工作区 ipc/questions/ ← ask MCP 服务写的问题 → 发 QQ
                          ▼
              话题工作目录（AGENTS.md、uploads/、ipc/、产出文件）
```

**dsh 侧需要挂几个插件**（否则提问 / 交付 / Office 不可用）：

| 插件 | 作用 |
|---|---|
| `@deepseek-ai/dsh-mcp-client` | 连本仓库的 `scripts/dsh_ask_mcp.py`，提供 `ask_user` 工具 |
| `@deepseek-ai/dsh-tool-present` | dsh 用 `present` 声明交付文件 |
| `@deepseek-ai/dsh-workspace-changes` | 记录每轮工作区变更 |
| `@deepseek-ai/dsh-skill-office` | Word / PPT / Excel 技能 |

示例补丁见 [`profiles/headless.cordis.patch.yml`](profiles/headless.cordis.patch.yml)。

## 兼容性

- **MaiBot**：1.3.x 实测通过（1.2.x 为上游声明范围，未逐版本验证）。
- **maibot-plugin-sdk**：2.x（`maibot_sdk.types` 提供 `HookMode/HookOrder/ErrorPolicy`）。
- **dsh**：0.2.x（headless profile）。
- **Python**：3.11+。
- **适配器**：发文件依赖 NapCat 的文件接口（`adapter.napcat.file.upload_private_file` / `upload_group_file`）；其它适配器会自动跳过文件，只发文字。
- **平台**：Linux 为开发环境；Windows 亦支持（`taskkill /T /F` 终止进程树）。

## 安装

1. 把本仓库目录放进麦麦的 `plugins/`（目录名建议 `maibot-dsh-bridge`）。
2. 复制 `config.toml.example` 为 `config.toml`，填 `workspace_dir`、QQ 白名单等。
3. 服务器上装好 `dsh`（`npm i -g @deepseek-ai/dsh`）与 `DEEPSEEK_API_KEY`。
4. 给 dsh 的 headless profile 加补丁（见 `profiles/headless.cordis.patch.yml`），并把
   `scripts/dsh_ask_mcp.py` 放到 `<MAIBOT_HOME>/scripts/`。
5. 若要用 Office 技能，另建一个装了 `python-docx / python-pptx / openpyxl` 的 venv，
   并把路径写进话题工作区 `AGENTS.md` 的 Office 段落（默认写的是 `<office-venv 路径>/bin/python`）。
6. 重载麦麦。首次 `/dsh <任务>` 会自动建话题。

## 命令（全部在 `/dsh` 下）

| 命令 | 作用 |
|---|---|
| `/dsh <任务>` | 在当前话题派活（没有话题会自动建） |
| `/dsh #3 <任务>` | 指定话题派活 |
| `/dsh peek` / `/dsh 状态` / `/dsh 进展` | 看它现在在干什么（步骤 / 正在跑的工具 / 最近动静） |
| `/dsh stop` / `/dsh 停止` | 立刻停止当前任务（已产出文件会尽量发回） |
| `/dsh stop 全部` | 连排队中的任务一起清掉 |
| `/dsh new` | 新建话题并设为当前 |
| `/dsh set <名>` | 改当前话题名 |
| `/dsh del [#3]` | 归档（7 天后自动删除，期间可恢复） |
| `/dsh restore #3` | 恢复归档话题 |
| `/dsh list` / `/dsh 话题 #3` | 列出 / 查看并切换话题 |
| `/dsh help` | 用法 |

任务末尾加 `(all)` 可看完整合并转发过程；以 `-- ` 开头的正文强制当任务；`/dsh --timeout 600 <任务>` 可给单次任务设上限。

## 配置要点

完整项见 `config.toml.example`。几个容易误解的：

- `dsh.timeout_default = 0`：**0 = 不限时**（默认）。想设上限填秒数，或用 `/dsh --timeout N`。
- `dsh.heartbeat_seconds = 600`：这是**死锁兜底**（连续无输出且进程静默才判卡死），不是时间预算。
- `dsh.salvage_on_fail = true`：任务被停/异常结束时，把本轮新产出的文件救回发出。
- `topic.enabled / topic.private_only`：话题模式及其适用范围（默认只私聊）。
- `experimental.agent_tools`：实验功能总开关（还需在 QQ 里 `/dsh 实验开启` + 二次确认）。

## 目录结构

```
.
├── plugin.py                       # 桥本体（单文件插件）
├── _manifest.json                  # 插件清单（id/版本/兼容范围）
├── config.toml.example             # 配置示例
├── profiles/headless.cordis.patch.yml  # dsh headless profile 补丁示例
├── scripts/dsh_ask_mcp.py          # MCP stdio 服务：ask_user 工具
├── docs/使用说明书.md               # 面向使用者的完整说明
├── docs/实现方案.md                 # 设计取舍与实施记录
└── tests/bridge_harness.py         # 集成测试（假 Host + 真 dsh）
```

## 测试

`tests/bridge_harness.py` 用一个假 Host 驱动真插件、并起真 dsh，覆盖话题、归档、提问（MCP/文件两种通道）、
实验功能、停止/看进展、救回等 20 个用例：

```bash
BRIDGE_PLUGIN_DIR=/path/to/maibot-dsh-bridge \
BRIDGE_DSH_ARGS="--profile headless" \
python tests/bridge_harness.py
```

## 已知限制

- 全局串行：同一时刻只跑一个 dsh 任务，第二个进队列。
- 会话写锁：同一话题的会话被 dsh Web GUI 打开时，桥无法续跑（会提示先关掉）。
- 文件上传依赖 NapCat 接口；其它适配器只发文字。
- 话题细节、实现取舍见 `docs/` 下两份文档。

## 许可

MIT，见 [LICENSE](LICENSE)。本仓库为 [ji-or-ji/maibot-dsh-bridge](https://github.com/ji-or-ji/maibot-dsh-bridge) 的派生版本，
原版权归 ji-or-ji；fork 内的修改同样以 MIT 发布。
