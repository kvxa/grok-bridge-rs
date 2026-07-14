# Changelog

## 0.3.0 - 2026-07-15

- Replaced v0.2 state-file workers with one per-user Windows x86_64 Runtime Server backed by a local Windows Named Pipe and bounded NDJSON frames.
- Moved Grok process ownership into the Server, with each session running in a persistent ConPTY and retaining bounded in-memory terminal output.
- Added Orca-style `create`, `list`, `show`, `read`, `send`, `write`, `resize`, `wait`, and `close` JSON commands with automatic detached Server startup.
- Added `terminal --session <handle>` to attach an egui terminal to an existing session, plus `terminal [--cwd --prompt --model --always-approve]` to create and open a session.
- Added a cell-based terminal renderer with ANSI colors and styles, wide characters, cursor rendering, Chinese IME, bracketed paste, terminal key mappings, selection, clipboard copy, bounded scrollback, and live ConPTY resize.
- Added raw Base64 terminal writes and synchronized ConPTY/vt100 resize. `show` now reports `rows`, `cols`, and `screen_ansi_base64` so the GUI can restore the current screen before consuming cursor-based output.
- Defined terminal window closure as detach-only. Grok continues running until the user explicitly closes the session or stops the Runtime Server.
- Added TUI-idle and process-exit waits, blocked interactive prompt detection, follow-up text input, and Ctrl+C interruption.
- Added cross-platform CJK font discovery and verified face selection for the egui terminal, with `GROK_BRIDGE_CJK_FONT` and `GROK_BRIDGE_CJK_FONT_INDEX` overrides.
- Prevented the detached Server from inheriting CLI pipeline handles, normalized Windows verbatim working directories, synchronized PTY EOF with process exit, and made Server Stop reliably reap active Grok processes.
- Retained `GROK_BIN` for trusted native Grok executable selection and `GROK_BRIDGE_ALLOWED_ROOTS` for canonical working-directory restrictions.
- Limited CI and Release builds to Windows x86_64. The Skill ZIP contains only `SKILL.md`, `agents/openai.yaml`, and `bin/windows-x86_64/grok-bridge.exe`.

## 0.2.0 - 2026-07-14

- Replaced the blocking one-request protocol with stateful `start`, `status`, `read`, `wait`, `send`, `stop`, and `list` CLI commands.
- Added detached background workers that consume Grok `streaming-json`, persist bounded result fields, expose cursor-based events, and resume follow-ups with the provider session UUID.
- Added observable heartbeat, activity, answer text, usage, timeout, failure, and stop events while excluding Grok thought text and full prompts.
- Added explicit UTF-8 guidance for Windows PowerShell 5.1, whose default native-pipeline encoding corrupts Chinese text into question marks.
- Added session-state, cursor, Unicode-boundary, streaming-event, UUID, and thought-redaction tests.

## 0.1.0 - 2026-07-14

- Converted the repository root into a directly installable `grok-build` Agent Skill.
- Added a single-request STDIN/STDOUT JSON wrapper with timeout, output capture, prompt redaction, allowed-root checks, and Grok session continuation.
- Added Tag-triggered GitHub Actions builds for Windows ARM64/x86_64, macOS ARM64, and Linux ARM64/x86_64, packaged as one Skill ZIP with SHA-256.
