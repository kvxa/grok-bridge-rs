# Security

`grok-bridge.exe` runs a per-user local Runtime Server. CLI and egui terminal clients connect through a Windows Named Pipe; the Server starts Grok in persistent ConPTY sessions and retains bounded terminal state in memory. Grok and every tool it launches execute with the current Windows user's permissions. Neither the Runtime nor the GUI is a sandbox or privilege boundary.

Recommended controls:

- Keep `--always-approve` disabled for untrusted repositories or prompts.
- Configure `GROK_BRIDGE_ALLOWED_ROOTS` so `create --cwd` and terminal create mode accept only approved canonical directories.
- Set `GROK_BIN` only to a trusted native Grok executable. Do not allow untrusted input to select another program.
- Load CJK fonts only from trusted system or user-controlled paths. `GROK_BRIDGE_CJK_FONT` is parsed in-process, and `GROK_BRIDGE_CJK_FONT_INDEX` selects a face from a font collection.
- Do not place passwords, API tokens, private keys, or other secrets in `--prompt`, `send --text`, or raw `write --data-base64`; process arguments, terminal output, clipboard contents, and same-user memory are not secret storage.
- Treat session handles, `screen`, `screen_ansi_base64`, raw output, and GUI state as local execution context. Do not expose them or the Named Pipe to untrusted automation.
- Raw `write` sends exact bytes to Grok's terminal and can accept prompts, control sequences, or command input. Prefer `send --text` for normal automation and validate any externally supplied Base64 before forwarding it.
- Review `git status` and `git diff`, then run relevant tests after every idle turn. `tui-idle` reports terminal state, not correctness.
- Closing the terminal window only detaches. Use the explicit GUI close action, `close --session`, or `server stop` when Grok must be terminated.
- Download the Skill archive only from a trusted GitHub Release and verify its SHA-256.

The pipe uses a per-user-derived local name and Windows access control, but it is not an authentication boundary against administrators or other processes already running as the same user. The transport rejects oversized frames and validates request IDs, session handles, models, paths, read limits, raw writes, and terminal dimensions; changes must not weaken these checks.

The egui terminal renders cells locally from Server-provided ANSI state and output. Selection copy places terminal text on the system clipboard, where other same-user applications may read it. Keyboard, IME, paste, and resize events are forwarded to the active session only while the window is attached.

Runtime session data is not persisted across Server restarts. Grok CLI may independently store authentication, configuration, diagnostics, or provider session data according to its own behavior; protect and review that data separately.
