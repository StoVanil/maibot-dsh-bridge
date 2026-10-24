# 更新日志

本仓库是 [ji-or-ji/maibot-dsh-bridge](https://github.com/ji-or-ji/maibot-dsh-bridge) 的派生版本。
上游历史见原仓库；下面是本 fork 相对上游 v0.6.1 的改动。

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
