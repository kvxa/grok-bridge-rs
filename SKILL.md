---
name: grok-build
description: Delegate concrete coding, repair, testing, and follow-up tasks to Grok Build through the bundled Windows x86_64 local Runtime CLI. Use when Codex should plan and audit while Grok works in a persistent ConPTY session, when machine-readable create/read/wait/send control is useful, or when the user explicitly asks for Grok, grok-build, or the bundled wrapper. Supports an optional egui terminal for human takeover. Requires Windows and an authenticated Grok CLI.
---

# Grok Build Local Runtime

Use the executable beside this file:

```text
<skill-dir>/bin/windows-x86_64/grok-bridge.exe
```

Resolve `<skill-dir>` as the directory containing this `SKILL.md`.

## Workflow

1. Inspect the repository, current changes, constraints, and acceptance criteria.
2. Run `<bridge> doctor` if Grok availability is uncertain.
3. Create one focused session. Keep automatic approval disabled unless the repository and prompt are trusted.

```powershell
$bridge = '<skill-dir>\bin\windows-x86_64\grok-bridge.exe'
$created = & $bridge create `
    --cwd (Get-Location).Path `
    --prompt 'Implement the requested change, run relevant checks, and report the result.' |
    ConvertFrom-Json
$session = $created.result.value.session
```

Optional creation arguments are `--model <model>` and `--always-approve`.

4. Wait for the TUI to become idle, then read the terminal state. Save `next_cursor` for incremental reads.

```powershell
$wait = & $bridge wait --session $session --for tui-idle --timeout-ms 300000 |
    ConvertFrom-Json
$read = & $bridge read --session $session --cursor 0 --limit 4096 --wait-ms 5000 |
    ConvertFrom-Json
$nextCursor = $read.result.value.next_cursor
$screen = $read.result.value.screen
```

If `$wait.result.value.blocked_reason` is present, inspect `show` and send the exact answer required by the visible prompt. Do not treat a blocked prompt as task completion.

5. Independently inspect `git status`, `git diff`, and run the repository's required checks. Runtime success or `tui-idle` is not proof that the task passed.
6. Send focused follow-up evidence through the same ConPTY session, then repeat `wait`, `read`, and verification.

```powershell
& $bridge send --session $session --text 'Fix only the verified failures and rerun the checks.'
& $bridge wait --session $session --for tui-idle --timeout-ms 300000
```

7. Interrupt a stuck turn with `send --interrupt`. Close the session after the final audit.

```powershell
& $bridge close --session $session
```

## Human takeover

Open the egui terminal only when the user requests an interactive view or manual takeover:

```powershell
& $bridge terminal --session $session
```

Use `terminal [--cwd <path>] [--prompt <text>] [--model <model>] [--always-approve]` to create a session and open it immediately. Closing the window only detaches; use the explicit close action to terminate Grok. Do not use the GUI as the normal Codex automation path because it waits for human interaction and does not return a JSON result.

## Commands

- `server start|status|stop` manages the per-user singleton Runtime.
- `create`, `list`, `show`, `read`, `send`, `write`, `resize`, `wait`, and `close` use JSON responses and automatically start the Server when needed.
- `read` uses byte cursors; `show` includes `rows`, `cols`, and `screen_ansi_base64` for terminal restoration.
- `send --text` submits bracketed text with Enter; `write --data-base64` writes exact raw bytes and is intended for terminal control or protocol testing.
- `wait --for tui-idle` reports recognized interactive prompts through `blocked_reason`; `wait --for exit` waits for process termination.
- `terminal --session <handle>` attaches the egui terminal to an existing session. Without `--session`, it creates a session first.

Prefer JSON `create/read/wait/show/send` for Codex-driven work. Use `write` and `resize` only when exact terminal bytes or dimensions are required. The Server owns every Grok ConPTY and in-memory session; the GUI is only a client. Do not edit the same files concurrently with Grok, expose secrets in prompts or raw input, or assume sessions survive a Server restart. By default the Runtime resolves `grok.exe`; use `GROK_BIN` only for a trusted native executable and `GROK_BRIDGE_ALLOWED_ROOTS` to restrict accepted working directories.
