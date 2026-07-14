# Codex 使用 grok-build Local Runtime 的编排规则

将本文件的规则合并到业务项目 `AGENTS.md`，或在 Codex 会话开始时发送。`grok-build` Runtime 负责持有 Grok ConPTY 会话；Codex 负责需求理解、任务拆分、监控、审计和最终验收。

## 工作流程

1. 检查仓库、现有工作树、约束和可验证验收标准。
2. 把一个明确实现目标交给 `create`。只有可信仓库和 prompt 才使用 `--always-approve`。

```powershell
$bridge = '<skill-dir>\bin\windows-x86_64\grok-bridge.exe'
$created = & $bridge create `
    --cwd (Get-Location).Path `
    --prompt '<task>' |
    ConvertFrom-Json
$session = $created.result.value.session
```

3. Grok 工作期间不要并发修改相同文件。用有限超时滚动等待，不要重复创建相同任务。若 `wait` 返回 `blocked_reason`，先用 `show` 检查屏幕，再通过 `send` 提交明确答案。

```powershell
& $bridge wait --session $session --for tui-idle --timeout-ms 300000
$read = & $bridge read --session $session --cursor 0 --limit 4096 --wait-ms 5000 |
    ConvertFrom-Json
$read.result.value.screen
```

4. 保存 `read.result.value.next_cursor`，后续从该 cursor 增量读取。需要盘点当前会话时使用 `list`，需要查看完整当前终端时使用 `show --session`。
5. 返回 `tui-idle` 后独立执行 `git status --short`、`git diff --check`、`git diff` 以及项目要求的测试、lint、格式化和构建。
6. 审计失败时，通过同一会话发送包含证据的聚焦返工请求，再次等待和验收。

```powershell
& $bridge send --session $session --text '只修复以下验收失败：<evidence>'
& $bridge wait --session $session --for tui-idle --timeout-ms 300000
```

7. 卡住时先查看屏幕；确需中断时使用 `send --interrupt`。最终验收后调用 `close --session`。
8. 不把 Runtime 状态当成正确性证明，不泄露秘密，不删除测试、降低安全检查或吞掉异常。
9. 不自动 commit、push、merge、创建 PR、Tag 或 Release，也不执行用户未授权的不可逆操作。

## Prompt 模板

```text
你是本任务的具体实现者，请在当前仓库完成以下任务。

目标：
<用户目标>

验收标准：
1. <可验证标准>
2. <可验证标准>

约束：
- 只修改与任务直接相关的文件，保留现有编码。
- 不重构、不美化、不补无关功能。
- 增加或更新必要测试，并运行相关检查。
- 不要 commit、push 或创建 PR。
- 完成时在终端报告修改文件、测试结果和剩余风险。
```
