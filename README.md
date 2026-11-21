# 麦麦×dsh 桥（maibot-dsh-bridge）

[![version](https://img.shields.io/badge/version-0.14.1-blue.svg)](CHANGELOG.md)
[![license](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![python](https://img.shields.io/badge/python-3.11%2B-yellow.svg)](requirements.txt)
[![MaiBot](https://img.shields.io/badge/MaiBot-1.2.x%E2%80%931.3.x-1f6feb.svg)](https://github.com/MaiM-with-u/MaiBot)
[![dsh](https://img.shields.io/badge/dsh-0.2.x-8a2be2.svg)](https://github.com/deepseek-ai/dsh)

把 [DeepSeek Harness (dsh)](https://github.com/deepseek-ai/dsh) 接进 [麦麦 (MaiBot)](https://github.com/MaiM-with-u/MaiBot)：
在 QQ 里发一条 `/dsh <任务>`，dsh（对外叫「大肥鱼」）就在服务器上把活干完——**每个话题一条持久会话、一个独立工作目录**，
过程随时能看（`/dsh peek`）、随时能停（`/dsh stop`），产出的文件直接发回聊天。

默认「实验模式常驻」：墨（麦麦的 Planner）可以自己判断任务、自己建/切话题、派活，用户不用记一堆指令；
也可以随时 `/dsh 实验关闭` 临时收摊，把 `/dsh` 命令收回来自己用。

> **本仓库是 [ji-or-ji/maibot-dsh-bridge](https://github.com/ji-or-ji/maibot-dsh-bridge) 的派生版本（fork）。**
> 上游 v0.6.1 提供基础桥接（派活 / 转发回传 / 定时任务 / 权限 / 交付物）；本 fork 在其上加上了
> 话题制会话、MCP 提问、`stop`/`peek`、不限时策略、异常救回、按 QQ 号隔离的文件柜、分场景帮助、
> 文件管理 MCP 工具、**实验模式常驻（墨自主调度）**、**MCP 脚本自愈** 等，详见 [CHANGELOG.md](CHANGELOG.md)。
> 原始版权归 ji-or-ji，遵循 MIT 许可（见 [LICENSE](LICENSE)）。

> **项目状态**：插件版本 **0.14.1**（2026-11-21 从线上部署归档）。README 与 CHANGELOG 已更新到 0.14；
> `docs/` 下的 4 份长文档仍按 v0.13.0 校对，v0.14 的行为变化以本 README、CHANGELOG 和 `plugin.py` 为准。
> 插件 id：`ji-or-ji.maibot-dsh-bridge`（沿用上游 id，便于覆盖升级）。

## 特性

- **话题制持久会话**：每个话题 = 一条持久 dsh 会话（`--session-id`）+ 独立工作目录 + 编号（`#1`、`#2`…）；
  可命名、归档、恢复；闲置 24 小时自动开新话题。归档 7 天后自动清理，期间可恢复。
- **指令收口在 `/dsh`**：派活、话题管理、文件柜、权限审批、停止、看进展都走同一个入口；桥拦截后墨不会接话茬。
- **墨自主调度（默认常驻）**：`experimental.always_on = true` 时，墨可以用 `dsh_run` 等工具自己建/切/改/删话题、派活；
  `/dsh` 命令默认收起，只保留 `stop` / `peek` / `queue` 和实验开关。关掉实验模式即回到纯指令模式。
- **MCP 提问**：dsh 需要拍板时调用 `ask_user`，桥把问题转成 QQ 消息；用户回一句，**dsh 在同一进程里继续**。
  文件柜的 `files_*` 工具合并进同一个 MCP server，不需要再起第二个 mcp-client。
- **结果回传**：实验模式下成功结果只回灌给墨，由墨组织后汇报（不在聊天里铺原文）；任务末尾带 `(all)` 才发原输出/全过程。
  非实验模式按 `dsh.direct_result` 把结果直接发回聊天。
- **文件柜（按 QQ 号隔离）**：用户发来的文件存到 `files/<QQ号>/`，每人只能看/管自己的，主人（审批人）可跨目录；
  QQ 侧 `/dsh files` 管理，dsh 侧有同款 MCP 工具（`files_list/read/info/mkdir/move/delete/users`）。
- **交付与救回**：dsh 用 `present` 声明交付物（话题工作区或本人文件柜里的文件）→ 桥打包发 QQ
  （单文件直发，多个文件自动打 zip）；任务被停/异常终止时，本轮新产出的文件也会被「救回」发出（救回单文件上限 20 MB）。
- **权限与审批**：白名单/黑名单模式；名单外的人发 `/dsh` 会生成申请单，审批人 `/dsh 批准 A1 [小时]` 后可用；
  授权持久化、到期自动失效；`enforce_tools` 同时对 `dsh_run` / `schedule_*` 等工具生效。
- **定时任务**：QQ 指令、`schedule-*` 工具或工作区 `schedule-requests/*.json` 都能建；
  支持 `after_seconds` / `every_seconds` / `at`（ISO 时刻）/ `cron`（5 字段，Vixie 语义）；触发时重新校验创建者授权。
- **运维友好**：启动与每次开工前自愈 MCP 脚本（宿主升级清空 `scripts/` 不再失效）、`dsh_status` 查凭据与最近任务、
  失败原因回显、Windows 下 `taskkill /T /F` 杀整棵进程树、Office（Word/PPT/Excel）技能。

## 工作流程

```text
QQ ──► MaiBot 宿主 ──► 本插件（桥）
                         │  1) 解析 /dsh（或墨调用 dsh_run），确定话题与工作目录
                         │  2) 起子进程：dsh <extra_args> --json [--session-id <id>] -（任务从 stdin 喂入）
                         │  3) 逐事件读 dsh 的 JSON 输出：步骤 / 工具调用 / 结果，按需转发或回灌给墨
                         │  4) 扫工作区 ipc/questions/ ← ask MCP 写的问题 → 发 QQ；回答写回 → 同一进程续跑
                         │  5) QQ 文件落到 files/<QQ号>/，dsh 侧用 files_* MCP 工具按目录隔离管理
                         │  6) 收尾：present 交付物打包发送；失败/停止时救回本轮新文件
                         ▼
       话题工作目录（AGENTS.md、ipc/、产出文件） + 文件柜（files/<QQ>/）
```

dsh 以 `--json` 输出结构化事件；桥默认把过程收进一条合并转发卡片（实验模式下成功结果只回灌给墨，不铺原文），
避免刷屏。`stop` 先温和 terminate，宽限期后强杀进程树；`peek` 汇总步骤、当前工具、最近动静与最近一次结果。

## 给墨的工具

实验模式下（且 `enforce_tools` 允许时）墨可以直接调用这些工具，不必让用户手打命令：

| 工具 | 作用 |
|---|---|
| `dsh_run` | 把较重任务外包给大肥鱼（`task` 必填，可选 `timeout_seconds` / `topic`） |
| `dsh_status` | 查是否在跑、最近几次任务成败与摘要、定时任务数、凭据状态 |
| `dsh_topic_list` / `dsh_topic_new` / `dsh_topic_rename` / `dsh_topic_archive` / `dsh_topic_restore` | 话题管理（归档需 `confirm=true`） |
| `schedule_task` / `schedule_list` / `schedule_update` / `schedule_cancel` | 定时任务管理 |

## dsh 侧需要挂的插件

否则提问 / 交付 / 文件管理 / Office 不可用。示例补丁见 [`profiles/headless.cordis.patch.yml`](profiles/headless.cordis.patch.yml)。

| 插件 | 作用 |
|---|---|
| `@deepseek-ai/dsh-mcp-client`（serverName `ask`） | 连 `scripts/dsh_ask_mcp.py`，提供 `ask_user`，**并合并提供 `files_*` 文件柜工具**（`mcp__ask__files_list` 等） |
| `@deepseek-ai/dsh-tool-present` | dsh 用 `present` 声明交付文件 |
| `@deepseek-ai/dsh-workspace-changes` | 记录每轮工作区变更 |
| `@deepseek-ai/dsh-skill-office` | Word / PPT / Excel 技能 |

## 兼容性

| 项目 | 要求 |
|---|---|
| MaiBot | 1.3.x 实测通过；manifest 声明范围 1.2.0–1.3.99 |
| maibot-plugin-sdk | 2.x（manifest 声明 2.0.0–2.98.99） |
| dsh | 0.2.x（headless profile） |
| Python | 3.11+（插件本体与 MCP 工具都只用标准库） |
| 平台 | Linux 为开发/部署环境；Windows 亦支持（`taskkill /T /F` 终止进程树、`.cmd` 可直接启动） |
| 适配器 | 发/取 QQ 文件依赖 NapCat 适配器（`adapter.napcat.file.upload_*`、`message.get_msg` → `file.get_file`）；其它适配器自动降级为只发文字 / 取不到文件 |

## 安装

1. 把本仓库目录放进麦麦的 `plugins/`（目录名建议 `maibot-dsh-bridge`）。
2. 复制 `config.toml.example` 为 `config.toml`，至少填 `dsh.workspace_dir`、`permission.allowed_users`；
   需要隔离会话库时再填 `dsh.dsh_home`（如 `/root/.dsh-headless`）。
3. 服务器上装好 `dsh`（`npm i -g @deepseek-ai/dsh`）与 DeepSeek API Key（环境变量 `DEEPSEEK_API_KEY` 或配置 `dsh.api_key`）。
4. 给 dsh 的 headless profile 加补丁（见 `profiles/headless.cordis.patch.yml`）。
   v0.14.1 起桥会把 `<插件目录>/scripts/` 下的两个 MCP 脚本自动同步到 `<MAIBOT_HOME>/scripts/`，不必手动复制。
5. （可选）要用 Office 技能，另建一个装了 `python-docx / python-pptx / openpyxl` 的 venv，
   把解释器路径填进 `dsh.office_python`（桥会写进工作区 `AGENTS.md`）。
6. 重载麦麦。首次 `/dsh <任务>` 会自动建话题与文件柜目录。

**首次自检**

- 插件加载日志里应出现「已载入话题 N 个」「已载入定时任务 N 个」，且没有 ERROR/Traceback。
- `/dsh 实验状态` 能回话；`/dsh peek` 显示空闲。
- 先 `/dsh 实验关闭`，再发一条 `/dsh 自检：只回 PONG` 确认真实任务跑通（跑完记得 `/dsh 实验开启`）；
  实验模式常驻时也可以让墨用 `dsh_run` 发同样的自检任务。
- 在工作区日志里确认 ask MCP 已启动（`ipc/mcp.log` 或插件日志），否则提问/文件柜工具不可用。

## 命令

所有命令都在私聊 `/dsh` 下（实验模式常驻时多数命令会被收起，见本节末尾说明）。

**派活 / 结果**

| 命令 | 作用 |
|---|---|
| `/dsh <任务>` | 在当前话题派活（没有话题会自动建） |
| `/dsh #3 <任务>` | 指定话题派活 |
| `/dsh --timeout 600 <任务>` | 给单次任务设时间上限（默认不限时） |
| 任务末尾加 `(all)` | 要原输出 / 全过程，发合并转发卡片 |
| `-- ` 开头 | 正文以 `new` / `del` 等开头时，强制当任务 |

**进度 / 控制**

| 命令 | 作用 |
|---|---|
| `/dsh peek` / `status` / `状态` / `进展` | 看它现在在干什么（步骤 / 正在跑的工具 / 最近动静 / 最近一次结果） |
| `/dsh queue` / `排队` | 看排队中的任务（全局串行，`topic.queue_max` 控制上限） |
| `/dsh stop` / `停止` | 立刻停止当前任务（已产出文件会尽量发回） |
| `/dsh stop 全部` | 连排队中的任务一起清掉 |

**话题**

| 命令 | 作用 |
|---|---|
| `/dsh new` | 新建话题并设为当前 |
| `/dsh list [#3]` | 列出话题 / 查看并切换到 #3 |
| `/dsh rename <名>` / `set <名>` | 改当前话题名（≤12 字） |
| `/dsh archive [#3]` / `del [#3]` | 归档（7 天后自动删除，期间可恢复） |
| `/dsh restore #3` | 恢复归档话题 |

**文件柜**

| 命令 | 作用 |
|---|---|
| `/dsh files` | 看自己的文件（序号 / 名字 / 大小 / 时间） |
| `/dsh files send <序号\|名字>` | 把自己的文件重新发回聊天 |
| `/dsh files rm <序号\|名字>` | 删自己的文件 |
| `/dsh files mkdir <名字>` | 建子目录 |
| `/dsh files all` | 主人专用：看所有人的文件 |
| `/dsh files users` | 主人专用：看各人占用 |

**权限审批**（审批人专属）

| 命令 | 作用 |
|---|---|
| `/dsh 审批` / `授权列表` | 看待审批申请与已授权的人 |
| `/dsh 批准 A1 [小时]` | 批准申请（省略=默认时长，0=永久） |
| `/dsh 拒绝 A1 [理由]` | 驳回申请 |
| `/dsh 撤销 <QQ>` | 收回某人的 `/dsh` 权限 |

**实验模式**

| 命令 | 作用 |
|---|---|
| `/dsh 实验开启` | 恢复常驻（默认就开着；常驻模式不用确认码） |
| `/dsh 实验关闭` | 临时收摊，`/dsh` 命令马上回来 |
| `/dsh 实验状态` | 看是不是常驻开着 |
| `/dsh 实验确认 <6位码>` | 仅旧版 24 小时临时窗口需要 |

**帮助**

`/dsh help [话题|文件|权限|实验|排障|全部]`，也认数字 1–7 和 `新手` / `故障` / `所有` 等别名；`/dsh help` 默认一屏总览。

> ⚠️ **实验模式常驻说明**：`experimental.always_on = true`（默认）时，`/dsh` 的大部分子命令会被拦截，
> 只保留 `stop` / `peek` / `queue` 和实验开关；要取文件、看话题列表或手动派活，先发 `/dsh 实验关闭`，
> 做完再 `/dsh 实验开启`。把 `experimental.always_on` 改成 `false` 则退回旧的「`实验开启` → `实验确认 <6位码>`」24 小时窗口。

## 配置

完整字段见 [`config.toml.example`](config.toml.example)（所有字段都有注释）。最常改的几项：

| 配置 | 默认 | 说明 |
|---|---|---|
| `dsh.command` | `dsh` | dsh 可执行命令；Windows 写 `dsh.cmd` 或带空格路径也支持 |
| `dsh.extra_args` | `--profile headless` | 桥会自动追加 `--json`、必要时 `--session-id` 和 `-` |
| `dsh.workspace_dir` | 空 | 话题/文件柜的根目录；留空用麦麦 data 目录 |
| `dsh.dsh_home` | 空 | headless 的 `DSH_HOME`；建议独立目录，避免和 dsh Web GUI 抢会话写锁 |
| `dsh.permission_mode` | `danger-full-access` | dsh 沙箱：`danger-full-access` / `workspace-write` / `read-only` |
| `dsh.timeout_default` | `0` | **0 = 不限时**；需要上限填秒数或用 `/dsh --timeout N` |
| `dsh.heartbeat_seconds` | `600` | 死锁兜底（无输出且进程静默），不是时间预算 |
| `dsh.display_mode` | `forward` | `forward` = 合并转发卡片；`text` = 纯文本 |
| `dsh.direct_result` | `true` | 非实验模式：结果直接回聊天；实验模式下统一交给墨组织 |
| `dsh.ask_enabled` / `ask_timeout` / `ask_max_rounds` | `true` / `900` / `5` | MCP 提问开关、等待秒数、单任务最多几轮 |
| `dsh.uploads_enabled` / `upload_max_mb` | `true` / `50` | QQ 文件入站开关与单文件上限 |
| `dsh.upload_url_hosts` | QQ 文件域名白名单 | 只允许这些域名直链下载附件（防 SSRF）；其它链接走 NapCat |
| `dsh.office_python` | `/opt/dsh-office-venv/bin/python` | Office 解释器，会写进工作区 `AGENTS.md` |
| `permission.mode` | `whitelist` | `whitelist` 仅名单内可用 / `blacklist` 名单内不可用 |
| `permission.allowed_users` | 空 | 白名单；留空且启用 = 拒绝所有人（fail-closed） |
| `permission.enforce_tools` | `true` | 对 `dsh_run` / `schedule_*` 等工具也校验名单 |
| `permission.approval_enabled` | `true` | 名单外的人先走申请单，不直接拒绝 |
| `permission.approvers` | 空 | 审批人 QQ；留空 = 白名单第一个人（主人） |
| `experimental.always_on` | `true` | 实验模式常驻（墨自主调度，不会到期）；`false` 退回 24 小时窗口 |
| `topic.idle_hours` / `name_max_chars` / `queue_max` / `archive_keep_days` | `24` / `12` / `3` / `7` | 话题闲置、命名、排队、归档保留 |

## 安全边界（务必读）

- **白名单 = 服务器 shell**：`permission_mode = "danger-full-access"` 时，名单内的用户（以及 `allowed_groups` 群里任何人）
  发一句 `/dsh` 就等于让 dsh 在服务器上执行任意命令。只把你自己和完全信任的人放进白名单。
- 需要更严就改 `permission_mode = "workspace-write"`（部分命令会因沙箱被拒）或 `read-only`。
- dsh 的审批策略在 headless + `danger-full-access` 下是 `never`（headless 没有实时审批通道），
  整条链路只有 QQ 侧一层权限判断，且没有命令/路径粒度。
- 文件按 QQ 号隔离是**应用层隔离**，不是系统级沙箱；获授权的人仍可能通过 dsh 读到别人文件柜的内容。
- dsh 会读工作区 `AGENTS.md` 与文件柜内容；网页/文件可能包含提示注入，桥会把结果包成「外部内容-不可信」回灌给墨，
  但仍建议不要把不可信来源直接丢给高权限任务。
- 定时任务在**触发时**重新校验创建者授权；`allow_self_schedule = true` 允许 dsh 自己建定时任务，风险自负。
- 失败/救回的文件会发回聊天，可能包含敏感信息——注意聊天对象。
- 本插件不做模型/密钥隔离：`api_key` 明文写在配置里时注意文件权限；dsh 子进程会继承宿主环境变量。

## 目录结构

```text
.
├── .github/workflows/ci.yml            # CI：语法检查 + ruff + 离线单测
├── .gitignore                          # 忽略 config.toml、__pycache__ 等
├── .ruff.toml                          # lint 规则
├── CHANGELOG.md
├── LICENSE
├── README.md
├── _manifest.json                      # 插件清单（id / 版本 / 兼容范围 / capabilities）
├── config.toml.example                 # 配置示例（全字段注释）
├── plugin.py                           # 桥本体（单文件插件）
├── requirements.txt                    # 运行只依赖标准库；开发期需要 ruff
├── docs/                               # 长文档（仍按 v0.13 校对，待更新到 0.14）
│   ├── 使用说明书.md
│   ├── 实现方案.md
│   ├── 帮助文案.md
│   └── 文案全集.md
├── profiles/headless.cordis.patch.yml  # dsh headless profile 补丁示例
├── scripts/
│   ├── dsh_ask_mcp.py                  # MCP stdio：ask_user + files_* 文件柜工具（合并）
│   └── dsh_files_mcp.py                # 文件柜工具实现（被 ask server import，也可单独跑）
└── tests/
    ├── bridge_harness.py               # 集成测试（假 Host + 真 dsh）
    └── test_dsh_files_mcp.py           # 文件柜 MCP 离线单测（纯标准库）
```

## 测试

离线单测（不需要 MaiBot/dsh，CI 会跑）：

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

集成测试（假 Host + 真 dsh，在 Linux 服务器上跑）：

```bash
BRIDGE_PLUGIN_DIR=/path/to/maibot-dsh-bridge \
python tests/bridge_harness.py
```

它会用 `/tmp/bridge-harness-*` 做工作区，覆盖话题、归档、提问（MCP/文件两种通道）、实验模式、停止/看进展、
文件柜、分场景帮助、救回等场景。CI 里还会跑语法检查、全仓编译与 ruff（单测在 CI 中非阻塞；
ruff 目前因历史 noqa 风格仍报错，见 Actions）。

## 常见问题

**Q：发 `/dsh help`、`/dsh files` 没反应？**
A：实验模式常驻时这些命令会被收起。先 `/dsh 实验关闭`，用完再 `/dsh 实验开启`；或把 `experimental.always_on` 设为 `false`。

**Q：dsh 里没有 `ask_user` / `mcp__ask__files_*` 工具？**
A：检查 `<MAIBOT_HOME>/scripts/` 是否有两个 MCP 脚本（v0.14.1 起会自动补），以及
`$DSH_HOME/profiles/*/cordis.patch.yml` 是否还引用 `dsh_ask_mcp.py`。改完重载插件或触发一次任务让自愈生效。

**Q：任务成功但聊天里没反应？**
A：实验模式下成功结果默认只回灌给墨，由墨组织后汇报；如果墨没接话就看不到。查 `/dsh peek`、
`dsh_status` 或插件日志里的「触发结果汇报失败」。要原输出就让任务带 `(all)`。

**Q：用户说「把我的文件发回来」但没收到？**
A：`present` 只允许话题工作区内或**当前用户本人文件柜**里的文件；主人可跨目录。确认文件路径在这些范围内，
并确认适配器是 NapCat（其它适配器取不到/发不出 QQ 文件）。

**Q：任务一直没动静 / 提示凭据失败？**
A：`/dsh peek` 看卡在哪一步；确认 `dsh.dsh_home` 下的 `.credentials.yaml` 或 `DEEPSEEK_API_KEY` 有效；
旧 key 失效时任务会静默失败，`dsh_status` 能看最近任务记录。

**Q：升级麦麦后提问/文件柜工具消失？**
A：v0.14.1 的 MCP 脚本自愈会从 `<插件目录>/scripts/` 补回 `<MAIBOT_HOME>/scripts/`。如果插件目录也被删了，
只能从 Git/备份恢复插件目录。

**Q：Windows 上 `/dsh stop` 停不掉？**
A：v0.13 起已改为先 `taskkill /T /F` 杀整棵进程树；仍停不掉时检查麦麦是否以同一用户运行，以及 `dsh` 是否被外部进程占用。

## 已知限制

- 全局串行：同一时刻只跑一个 dsh 任务，第二个进队列（`topic.queue_max` 控制队列长度）。
- 实验模式常驻时 `/dsh help`、`/dsh files ...`、`/dsh list` 等命令会被收起（`stop` / `peek` / `queue` 和实验开关仍可用）。
- 会话写锁：同一话题的会话被 dsh Web GUI 打开时，桥无法续跑（会自动换新会话重试并提示）。
- 文件上传/取回依赖 NapCat 系适配器；其它适配器只发文字、取不到 QQ 文件。
- 没有 `psutil` 时进程存活判断降级（Linux 走 `/proc`，其它平台只看进程是否还在）。
- `docs/` 长文档仍按 v0.13.0 校对，尚未同步 0.14 的行为变化。

## 版本与文档

| 项 | 状态 |
|---|---|
| 插件 / manifest | 0.14.1 |
| README / CHANGELOG | 已更新到 0.14.1 |
| `docs/` 长文档 | v0.13.0（待更新） |
| 版本历史 | 见 [CHANGELOG.md](CHANGELOG.md) |

## 许可

MIT，见 [LICENSE](LICENSE)。本仓库为 [ji-or-ji/maibot-dsh-bridge](https://github.com/ji-or-ji/maibot-dsh-bridge) 的派生版本，
原版权归 ji-or-ji；fork 内的修改同样以 MIT 发布。
