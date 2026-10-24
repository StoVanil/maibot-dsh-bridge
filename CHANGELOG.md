## v0.13.0 — 文件柜（按 QQ 隔离）+ 分场景帮助 + 实验模式指令开启 + 审计修复

**新功能**

- **文件柜 `files/<QQ号>/`**：QQ 用户发给大肥鱼的文件按 QQ 号分目录，跨话题共用、不受话题归档影响。
  - 新增 `/dsh files`（看）、`rm <序号|名字>`（删）、`send <序号|名字>`（取回）、`mkdir <名>`（建子目录）、
    `all` / `users`（主人专用：看所有人 / 看各人占用）。
  - 每个用户只能看/管理自己的文件夹，主人（审批人）可跨目录；路径做了 `..`/绝对路径/符号链接防护。
- **文件管理 MCP 工具** `scripts/dsh_files_mcp.py`（纯标准库 stdio MCP）：给 dsh 提供
  `files_list / files_read / files_info / files_mkdir / files_move / files_delete / files_users`，
  按 `DSH_FILES_OWNER_QQ` 隔离，超管（`DSH_FILES_SUPERUSERS`）可跨目录。
- **文件路由调整**：非实验模式 + 消息里没有 `/dsh` → 文件交给墨自己读和回答；带 `/dsh` 才进大肥鱼。
  实验模式下由墨自行判断（只有「文件 + 文字」才会触发决策，纯文件只回执）。
- **分场景帮助**：`/dsh help 话题 | 文件 | 权限 | 实验 | 排障 | 全部`（也认数字 1–7、`新手`/`故障`/`所有` 等别名）；
  默认帮助一屏总览。文案见 `docs/帮助文案.md`。
- **实验模式改指令开启**：不再需要面板总开关，`/dsh 实验开启` → `/dsh 实验确认 <6位码>` 即可，最长 24 小时、到期自动关。
- 新增 `/dsh queue`（看排队）、`/dsh status`（= peek 英文别名）、`/dsh rename`（= set）、`/dsh archive`（= del）。
- 新增配置 `dsh.office_python`（Office 解释器路径可配置，不再硬编码）与 `dsh.upload_url_hosts`（附件直链域名白名单）。
- 新增 `_manifest.json` 版本 0.13.0、`config.toml.example` 全量字段、`.ruff.toml` 与 CI 工作流。

**审计修复**（`analysis/遗留问题-对照` 报告里仍存在的项）

- A1：**非零退出码一律判失败**（以前吐过 final 的崩溃会被当成功）；`error` 事件在转发卡片里显示为「⚠️ 错误」节点。
- A2：Windows **停止不再"假停止"**：改为先 `taskkill /T /F` 杀整棵进程树，再兜底 `kill()`，不留孤儿 dsh。
- A5：归档保护补全（运行中话题即使先切走也不能归档）；清理归档改为「目录真删掉才删记录」，并修正相对路径 bug。
- A6：cron 改 Vixie 语义（日/周双限取 OR、`7`=周日、非法表达式直接拒绝），不再逐分钟空转 366 天；`at` 无时区按本地时间解释。
- A7：Windows 启动不再经 `cmd /c` 吞引号（带空格路径可启动），`extra_args` 按平台正确拆分。
- A8：重启后不再恢复 ipc 通道的 pending（进程已死）；过期一次性定时任务丢弃、周期任务重算下一次。
- A9：定时任务改为异步派发，不再阻塞调度循环（过期清理/归档/排队照常）。
- A10：没有 psutil 时按 `/proc` 判 CPU 活跃（其它平台看进程存活），不再永远「活跃」。
- A11：修掉 v0.11.5 文案重写引入的 AGENTS.md 每轮追加回归（marker 匹配 + 只追加一次）。
- A12/A13：MCP 提问也受 `ask_max_rounds` 限制；关键 JSON 统一原子写（`.part` + `os.replace`）。
- A14：`tool_result` 用 `tool_call` 记录的工具名补节点标题。
- B2：附件直链只认 `upload_url_hosts` 里的 QQ 文件域名（防 SSRF），拒绝带凭据 URL，非白名单链接一律走 NapCat。
- B3：定时任务**触发时重新校验**创建者授权，插件停用则跳过。
- B4：`dsh_status` / `schedule_list` 补权限校验（不再能被非授权用户经 Planner 调用）。
- B5：黑名单模式取不到发送者 ID 时改为 fail-closed（不再放行）。
- B6：`plugin.enabled=false` 时收件钩子/调度循环也停；`ask_enabled` 关时不再起提问监听。
- B7：附件解码/读取前先判大小（图片 base64 长度、NapCat 临时文件 `stat`），避免内存/磁盘 DoS。
- B8：救回文件加符号链接与工作区边界校验；B9：回灌给墨的内容包成「外部内容-不可信」。
- C4：凭据状态检测跟随 `dsh_home`；Office 解释器路径改成配置项。
- C5：`config_version` 统一 0.13.0；manifest 版本/描述/仓库地址修正；README 恢复「安全边界」章节并更新命令表。

## v0.11.4 — 会话问答与文件收发修复

- 清理上一轮被中断任务遗留的 `ipc/questions`（不再把旧提问当成本轮问题）。
- 文件上传接口探测兼容两种返回形态；发不出去时聊天里明确告警（`⚠️ …没发出去`）。
- 插件重载打断任务时提示用户。
- 集成测试新增用例 24。

## v0.12.1 — 审批权只给主人

- `approvers` 留空时，不再把整份白名单都当审批人，**只认白名单里的第一个人**（视为主人）。
- 配置示例与说明书同步；也可显式写 `approvers = "你的QQ"` 指定多个审批人。

## v0.12.0 — 权限审批

- 新增审批制：名单外的人发 `/dsh` 不再直接拒绝，而是生成申请单（`A1`、`A2`…，默认 30 分钟有效），
  审批人收到申请后 `/dsh 批准 A1 [小时]`（省略=默认时长，0=永久）、`/dsh 拒绝 A1`、`/dsh 撤销 <QQ>`。
- 授权持久化到 `grants.json`（到期自动失效），申请单在 `approvals_pending.json`，
  审批人私聊 stream 记在 `user_streams.json`（审批人先说过话才能收到通知，之后会补发）。
- `/dsh 审批`（=授权列表）查看待审批与已授权；`/dsh 批准 <QQ> [小时]` 可直接给某人授权。
- 授权同样适用于 `dsh_run` / `schedule_*` 工具（`enforce_tools` 打开时）。
- 新增配置：`permission.approval_enabled` / `approvers` / `approval_hours` / `approval_ttl_minutes`。
- 集成测试新增用例 25，共 25 例通过。

## v0.11.5 — 文案重写（大肥鱼 + 墨的口吻 + 颜文字）

- 插件里所有用户可见文案重写：`dsh` → **大肥鱼**，语气改成墨的口吻，点缀颜文字/小表情。
- 覆盖：帮助、受理/排队、`/dsh peek`、`/dsh stop`、结果回执、交付、失败/救回、会话失效、话题命令、实验功能、附件入站、提问排版。
- 新增 `docs/文案全集.md`（368 条，按类型编号），方便后续做文案校对与替换。
- 实现方式：AST 定位字符串节点 + 字节偏移替换，避免手改漏改；未显式改写的用户文案统一做 `dsh→大肥鱼`。
- 回归测试 24 例全部通过。

# 更新日志

本仓库是 [ji-or-ji/maibot-dsh-bridge](https://github.com/ji-or-ji/maibot-dsh-bridge) 的派生版本。
上游历史见原仓库；下面是本 fork 相对上游 v0.6.1 的改动。

## v0.11.3 — 结果直接发到聊天

- 任务成功后，桥把 dsh 的最终答复**直接发到聊天**（`✅ dsh 完成（#N）…`）；
  内容超过 `direct_result_chars`（默认 1500）时先发预览，再把完整内容写成 `.md` 文件发出。
- 空输出也发回执（`✅ 任务完成，但没有文字输出`），不再出现「peek 说成功、聊天没动静」。
- 新增配置 `dsh.direct_result`（默认 true）与 `dsh.direct_result_chars`；关掉则恢复"只回灌给墨"。
- 原因说明：`maisaka.context.append` 只是往墨的上下文里加内容，不一定会触发墨开口，所以静默模式下结果容易丢。
- 集成测试新增用例 23（结果直发 + 空输出回执），共 23 例通过。

## v0.11.2 — DSH_HOME 隔离（从根上避免会话写锁）

- 新增 `dsh.dsh_home`：给桥派活的 dsh 子进程设置独立的 `DSH_HOME`，headless 与 dsh Web GUI
  各用一套会话库/凭据/配置，互不干扰、也不会再出现「会话被 GUI 占用」。
- 会话不存在（被清理/归档）时自动开一条新会话继续，并提示 `🔄 旧会话已不存在…`。
- 凭据缺失（`MISSING_CREDENTIAL` / no API key）时给出可照做的提示：去当前 DSH_HOME 配 `DEEPSEEK_API_KEY`。
- 部署说明：新 DSH_HOME 里需要 `.credentials.yaml` 与 `profiles/headless/`（含本项目的 cordis 补丁）。
- 集成测试新增用例 22（验证 DSH_HOME 注入 + 会话不存在自动恢复）。

## v0.11.1 — 失败不再静默 + 会话占用自动恢复

- **修复「无输出」**：dsh 以 `{"type":"error"}` 直接退出时（典型是会话被别的进程占用），
  旧逻辑把它当成「成功的空输出」，用户什么都看不到。现在会判失败、并在 QQ 里回显报错原文。
- **会话写锁自动恢复**：话题会话被 dsh Web GUI 占用时，桥自动改用一条新会话重跑一次
  （提示「旧会话正被 dsh Web GUI 占用，已改用新会话继续」，旧上下文不继承）。
- 失败原因同时记入 `dsh_status` / `/dsh peek` 的最近一次记录。
- 新增集成测试用例 21（用假 dsh 模拟 error 退出）。

## v0.11.0 — 不限时 + 手动控制

- **取消默认时间预算**：`timeout_default/min/max = 0`（不限时）。模型估不准自己的耗时，硬限时经常砍掉快做完的活。
- **`/dsh stop`**：秒级停止当前任务（轮询 2s），`/dsh stop 全部` 连队列一起清；被停后本轮产出文件仍会救回发送。
- **`/dsh peek`**：运行中快照（已用时 / 轮次步骤 / 正在跑的工具 / 最近动静 / 预算），空闲时给最近一次结果。
- **AGENTS.md v4**：要求 dsh「先 `present` 交付、再迭代」，不要为排版无限打磨。
- 死锁兜底阈值提到 600s（仅为防死锁，不是时间预算）。

## v0.10.1 — 救回与命名

- **异常/超时救回**：任务终止时扫工作区，把本轮新产出的文件（docx/xlsx/pptx/pdf/图片/zip/md…）发回聊天。
- 遗留话题自动命名（首个任务的规划标题覆盖「遗留·xxxx」）。
- 会话被 Web GUI 占用时给人的提示。

## v0.10.0 — MCP 提问 + dsh 插件生态

- 提问通道切到 **MCP**（`@deepseek-ai/dsh-mcp-client` + `scripts/dsh_ask_mcp.py`）：dsh 调 `ask_user` → 桥发 QQ → 你回答 → **同一进程继续**；文件式 `ask-requests/` 保留为兜底。
- 挂上 `tool-present` / `workspace-changes` / `skill-office`：交付物回传、工作区变更摘要、Word/PPT/Excel 技能。
- `present` 声明的交付物由桥打包发回（单文件直发 / 多文件 zip）。

## v0.9.0 — 实验功能

- 设置里临时开启（最长 24h、二次确认、到期自动关闭）后，墨可自主调用话题/派活工具；期间 `/dsh` 命令禁用（`stop`/`peek` 除外）。
- 归档工具要求 `confirm=true`。

## v0.8.0 — 指令收进 /dsh

- 话题指令统一为 `/dsh new|set|del|restore|list|help`；命令处理返回拦截，墨不会接话茬。

## v0.7.0 — 话题制会话

- 话题 = 持久会话 + 独立工作目录 + 编号；闲置 24h 自动新开；归档/恢复/7 天清理/编号复用；任务串行排队。
- 旧目录懒认领（`遗留·<hash>`）。
- `/dsh` 中的 `#N` 指定话题。
