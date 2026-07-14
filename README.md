# grok-build Local Runtime Skill

`grok-build` v0.3.0 是一个可直接解压使用的 Windows x86_64 Agent Skill。Codex 通过随包发布的 `grok-bridge.exe` 调用本机 Grok Runtime；不需要 Python、安装脚本或额外服务。

Runtime 采用 Orca 风格的持久终端会话。CLI 自动启动每用户单例 Server，通过本地 Windows Named Pipe 交换 NDJSON；Server 持有 Grok CLI 的 ConPTY、会话状态和有界终端输出。RPC 命令向 STDOUT 返回一行 JSON，`terminal` 则打开交互式 egui 终端窗口。

```text
Codex / JSON CLI       egui terminal
          \               /
           Windows Named Pipe (NDJSON)
                       │
              单例 Runtime Server
                       │
                    ConPTY
                       │
                    Grok CLI
```

## 系统要求

- Windows 10 1809 或更高版本；
- Windows x86_64；
- 已安装并登录的 Grok CLI，`grok --version` 可正常执行。

默认从 `PATH` 查找原生 `grok.exe`，避免把 pnpm 的无扩展名脚本误交给 ConPTY。`GROK_BIN` 可指定可信的原生 Grok 可执行文件；`GROK_BRIDGE_ALLOWED_ROOTS` 可限制允许创建会话的仓库根目录。

egui terminal 启动时会验证字体字形覆盖。Windows 默认将 Consolas 放在比例字体和等宽字体链的首位显示英文，再由微软雅黑（`msyh.ttc`）显示中文；因此终端和状态栏均保持英文等宽、中文清晰。代码也为后续 macOS/Linux 构建保留系统字体、Fontconfig、Noto CJK、文泉驿和 Source Han 候选。可用 `GROK_BRIDGE_CJK_FONT` 显式覆盖中文 `.ttf`、`.otf` 或 `.ttc` 字体；字体集合可用 `GROK_BRIDGE_CJK_FONT_INDEX` 指定非负 face index。

## 安装

从 GitHub Releases 下载 `grok-build-skill-v0.3.0.zip` 和对应 `.sha256`，校验后直接解压到用户 Skills 目录：

```powershell
Expand-Archive .\grok-build-skill-v0.3.0.zip "$env:USERPROFILE\.agents\skills" -Force
```

安装后应存在：

```text
%USERPROFILE%\.agents\skills\grok-build\
├── SKILL.md
├── agents\openai.yaml
└── bin\windows-x86_64\grok-bridge.exe
```

重启 Codex 后调用 `$grok-build`。手工安装时也只需复制以上三个文件并保持目录结构。

## Codex 自动化工作流

```powershell
$bridge = "$env:USERPROFILE\.agents\skills\grok-build\bin\windows-x86_64\grok-bridge.exe"

$created = & $bridge create `
    --cwd (Get-Location).Path `
    --prompt '实现当前任务并运行相关测试。' `
    --always-approve | ConvertFrom-Json
$session = $created.result.value.session

& $bridge show --session $session
& $bridge read --session $session --cursor 0 --limit 4096 --wait-ms 5000
& $bridge wait --session $session --for tui-idle --timeout-ms 300000
```

验收后可提交后续输入，或显式终止会话：

```powershell
& $bridge send --session $session --text '只修复验收发现的问题，并重新运行测试。'
& $bridge wait --session $session --for tui-idle --timeout-ms 300000
& $bridge close --session $session
```

Codex 自动化应继续优先使用 `create/read/wait/show/send` 的 JSON 接口，不应依赖需要人工交互且会持续运行到窗口关闭的 GUI。

## 交互式终端

附着到 Server 已持有的会话：

```powershell
& $bridge terminal --session $session
```

也可创建会话后立即打开终端：

```powershell
& $bridge terminal `
    --cwd (Get-Location).Path `
    --prompt '检查当前实现并等待我的后续输入。'
```

`--model`、`--prompt` 和 `--always-approve` 都是可选参数。不带 `--cwd` 时使用当前目录。关闭窗口只会 detach，Grok 和会话仍由 Server 持有；只有终端工具栏的“关闭会话”、`close --session` 或 `server stop` 才会终止会话。

终端使用逐 cell 渲染，支持 ANSI 颜色、粗体、下划线、反色、暗色、宽字符和光标。输入支持中文 IME、文本输入、括号粘贴、控制键、Alt、方向键、Home/End、Insert/Delete、PageUp/PageDown 和 F1-F12。鼠标拖动可选择文本，Ctrl+C 在存在选择时复制，否则发送中断；滚轮浏览有界 scrollback，窗口尺寸变化会同步调整 ConPTY。

## 命令

- `server start|status|stop`：管理单例 Runtime；
- `create`：创建由 Server 持有的 Grok ConPTY 会话；
- `list` / `show`：查询会话列表或当前终端状态；
- `read`：按字节 cursor 增量读取有界原始输出；
- `send --text` / `send --interrupt`：提交高层文本或 Ctrl+C；
- `write --data-base64`：向 ConPTY 写入不经改写的原始字节；
- `resize --cols --rows`：调整 ConPTY 和服务端终端屏幕；
- `wait --for tui-idle|exit`：等待 TUI 空闲或进程退出；
- `close`：终止并移除会话；
- `terminal`：附着现有会话，或创建后打开 egui 终端；
- `doctor`：检查 Grok CLI 和 Server 状态。

除 `server status`、`server stop` 外，会话命令会在需要时自动启动 Server。Server 退出时会关闭其持有的全部会话；Runtime 状态不跨 Server 重启持久化。

低层原始输入示例：

```powershell
$data = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("yes`r"))
& $bridge write --session $session --data-base64 $data
& $bridge resize --session $session --cols 120 --rows 36
```

正常提交 prompt 时优先使用 `send --text`；`write` 不追加回车、不做括号粘贴，也不解释字节内容。

## JSON 与终端状态

每个 RPC 命令向 STDOUT 写一个 `ResponseEnvelope` JSON 对象并以换行结束。成功响应包含 `result`，失败响应包含结构化 `error`。`terminal` 是交互式例外，不输出这一 JSON envelope。内部 Named Pipe 使用相同请求 ID 的单请求、单响应 NDJSON 帧，单帧上限 1 MiB。

`read` 返回 `cursor`、`next_cursor`、Base64 原始增量、当前 `screen`、`truncated` 和 `eof`。保存 `next_cursor`，下一次从该位置继续读取。`show` 的会话状态还包含 `rows`、`cols` 和 `screen_ansi_base64`；GUI 先用该 ANSI 快照恢复当前屏幕，再持续消费 `read` 增量。

`wait --for tui-idle` 遇到可识别的交互提示时返回 `satisfied: false` 和 `blocked_reason`，而不是误报空闲。调用方应先用 `show` 检查当前 screen，再通过 `send` 或人工终端提交明确答案。

## 安全边界

- Grok 及其工具继承当前 Windows 用户权限，Runtime 和 GUI 都不是沙箱。
- 只在可信仓库和可信 prompt 上使用 `--always-approve`。
- prompt、`send --text` 和 `write` 的原始字节可能出现在进程参数或本机内存中，不要传递秘密。
- 不要让 Codex 与人工终端同时修改相同文件；`tui-idle` 只表示终端状态，不代表实现已通过验收。
- 每轮后独立检查 `git status`、`git diff` 并运行项目要求的测试。
- 只从可信 Release 下载 ZIP，并验证 SHA-256。

## 开发与发布

Windows 完成检查：

```text
cargo fmt --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
```

CI 在 Windows x86_64 上执行这些检查。`v*` Tag 触发的 Release workflow 只构建 Windows x86_64，并组装可直接解压的 Skill ZIP；本地 Agent 不自动 commit、push、创建 Tag 或发布 Release。
