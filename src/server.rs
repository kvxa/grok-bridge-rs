use std::{
    env,
    io::{BufRead, BufReader, ErrorKind, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use interprocess::local_socket::{ListenerOptions, Stream, prelude::*};

use crate::{
    protocol::{
        Request, ResponseEnvelope, ResponseResult, ServerInfo, decode_request, decode_write_data,
    },
    session::SessionHost,
    transport::{call, read_frame, runtime_name, write_response},
};

pub(crate) fn run() -> Result<()> {
    let name = runtime_name()?;
    let listener = match ListenerOptions::new().name(name).create_sync() {
        Ok(listener) => listener,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::AddrInUse | ErrorKind::PermissionDenied
            ) =>
        {
            if call(Request::ServerStatus, false).is_ok_and(|response| {
                response.ok && matches!(response.result, Some(ResponseResult::ServerInfo(_)))
            }) {
                return Ok(());
            }
            return Err(error).context("runtime pipe name is occupied by another process");
        }
        Err(error) => return Err(error).context("failed to bind the runtime named pipe"),
    };

    let web_listener = bind_web_ui();
    let web_url = web_listener
        .as_ref()
        .and_then(|listener| listener.local_addr().ok())
        .map(|address| format!("http://{address}/"));
    let state = Arc::new(RuntimeState {
        host: SessionHost::new(),
        started_at_ms: now_millis(),
        stopping: AtomicBool::new(false),
        web_url,
    });
    if let Some(listener) = web_listener {
        let web_state = Arc::clone(&state);
        thread::spawn(move || run_web_ui(listener, web_state));
    }

    for connection in listener.incoming() {
        let connection = match connection {
            Ok(connection) => connection,
            Err(error) => {
                if state.stopping.load(Ordering::Acquire) {
                    break;
                }
                eprintln!("grok-bridge server: failed to accept client: {error}");
                continue;
            }
        };
        if state.stopping.load(Ordering::Acquire) {
            break;
        }
        let state = Arc::clone(&state);
        thread::spawn(move || handle_connection(connection, state));
    }

    state.host.shutdown_all()?;
    Ok(())
}

struct RuntimeState {
    host: SessionHost,
    started_at_ms: u64,
    stopping: AtomicBool,
    web_url: Option<String>,
}

fn handle_connection(stream: Stream, state: Arc<RuntimeState>) {
    let mut connection = BufReader::new(stream);
    let frame = match read_frame(&mut connection) {
        Ok(frame) => frame,
        Err(error) => {
            let response =
                ResponseEnvelope::failure("invalid-request", "invalid_frame", format!("{error:#}"));
            let _ = write_response(connection.get_mut(), &response);
            return;
        }
    };
    let envelope = match decode_request(&frame) {
        Ok(envelope) => envelope,
        Err(error) => {
            let response = ResponseEnvelope::failure(
                "invalid-request",
                "invalid_request",
                format!("{error:#}"),
            );
            let _ = write_response(connection.get_mut(), &response);
            return;
        }
    };

    let request_id = envelope.id;
    let (response, stop_after_response) = match dispatch(&state, envelope.request) {
        Ok((result, stop)) => (ResponseEnvelope::success(request_id, result), stop),
        Err(error) => (
            ResponseEnvelope::failure(request_id, "request_failed", format!("{error:#}")),
            false,
        ),
    };
    let _ = write_response(connection.get_mut(), &response);
    if stop_after_response {
        wake_listener();
    }
}

fn dispatch(state: &RuntimeState, request: Request) -> Result<(ResponseResult, bool)> {
    let result = match request {
        Request::ServerStatus => ResponseResult::ServerInfo(state.server_info()),
        Request::ServerStop => {
            state.stopping.store(true, Ordering::Release);
            state.host.shutdown_all()?;
            return Ok((ResponseResult::Accepted { accepted: true }, true));
        }
        Request::Create {
            cwd,
            prompt,
            model,
            owner,
            always_approve,
        } => ResponseResult::Session(state.host.create(
            &cwd,
            prompt,
            model,
            owner,
            always_approve,
        )?),
        Request::List => ResponseResult::Sessions {
            sessions: state.host.list()?,
        },
        Request::Show { session } => ResponseResult::Session(state.host.show(&session)?),
        Request::Read {
            session,
            cursor,
            limit,
            wait_ms,
        } => ResponseResult::Read(state.host.read(
            &session,
            cursor.unwrap_or(0),
            limit.unwrap_or(4096) as usize,
            wait_ms.unwrap_or(0),
        )?),
        Request::Send { session, input } => {
            ResponseResult::Session(state.host.send(&session, input)?)
        }
        Request::Write {
            session,
            data_base64,
        } => {
            state
                .host
                .write_raw(&session, decode_write_data(&data_base64)?)?;
            ResponseResult::Accepted { accepted: true }
        }
        Request::Resize {
            session,
            cols,
            rows,
        } => {
            state.host.resize(&session, cols, rows)?;
            ResponseResult::Accepted { accepted: true }
        }
        Request::Wait {
            session,
            for_condition,
            timeout_ms,
        } => ResponseResult::Wait(state.host.wait(
            &session,
            for_condition,
            timeout_ms.unwrap_or(300_000),
        )?),
        Request::Close { session } => ResponseResult::Accepted {
            accepted: state.host.close(&session)?,
        },
    };
    Ok((result, false))
}

impl RuntimeState {
    fn server_info(&self) -> ServerInfo {
        ServerInfo {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            process_id: std::process::id(),
            started_at_ms: self.started_at_ms,
            active_sessions: self.host.active_count(),
            web_url: self.web_url.clone(),
            stopping: self.stopping.load(Ordering::Acquire),
        }
    }
}

fn bind_web_ui() -> Option<TcpListener> {
    let address = env::var("GROK_BRIDGE_WEB_ADDR").unwrap_or_else(|_| "127.0.0.1:47653".to_owned());
    match TcpListener::bind(&address) {
        Ok(listener) => Some(listener),
        Err(error) => {
            eprintln!("grok-bridge server: WebUI unavailable at {address}: {error}");
            None
        }
    }
}

fn run_web_ui(listener: TcpListener, state: Arc<RuntimeState>) {
    for connection in listener.incoming() {
        if state.stopping.load(Ordering::Acquire) {
            break;
        }
        match connection {
            Ok(stream) => {
                let state = Arc::clone(&state);
                thread::spawn(move || handle_web_connection(stream, state));
            }
            Err(error) => eprintln!("grok-bridge server: WebUI accept failed: {error}"),
        }
    }
}

fn handle_web_connection(mut stream: TcpStream, state: Arc<RuntimeState>) {
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
    let request = match read_http_request(&mut stream) {
        Ok(request) => request,
        Err(error) => {
            let _ = write_http(
                &mut stream,
                "400 Bad Request",
                "text/plain; charset=utf-8",
                &error,
            );
            return;
        }
    };
    let (method, path, bridge_header) = request;
    match (method.as_str(), path.as_str()) {
        ("GET", "/") => {
            let _ = write_http(&mut stream, "200 OK", "text/html; charset=utf-8", WEB_UI);
        }
        ("GET", "/api/sessions") => match state.host.list().and_then(|sessions| {
            serde_json::to_string(&sessions).context("failed to encode WebUI sessions")
        }) {
            Ok(body) => {
                let _ = write_http(&mut stream, "200 OK", "application/json", &body);
            }
            Err(error) => {
                let _ = write_http(
                    &mut stream,
                    "500 Internal Server Error",
                    "text/plain; charset=utf-8",
                    &format!("{error:#}"),
                );
            }
        },
        ("POST", path) if path.starts_with("/api/sessions/") && path.ends_with("/close") => {
            if !bridge_header {
                let _ = write_http(
                    &mut stream,
                    "403 Forbidden",
                    "text/plain; charset=utf-8",
                    "missing WebUI request header",
                );
                return;
            }
            let handle = &path[14..path.len() - 6];
            match state.host.close(handle) {
                Ok(closed) => {
                    let body = format!(r#"{{"accepted":{closed}}}"#);
                    let _ = write_http(&mut stream, "200 OK", "application/json", &body);
                }
                Err(error) => {
                    let _ = write_http(
                        &mut stream,
                        "404 Not Found",
                        "text/plain; charset=utf-8",
                        &format!("{error:#}"),
                    );
                }
            }
        }
        _ => {
            let _ = write_http(
                &mut stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                "not found",
            );
        }
    }
}

fn read_http_request(
    stream: &mut TcpStream,
) -> std::result::Result<(String, String, bool), String> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|error| error.to_string())?;
    let mut parts = line.split_whitespace();
    let method = parts.next().ok_or("missing HTTP method")?.to_owned();
    let path = parts.next().ok_or("missing HTTP path")?.to_owned();
    let mut bridge_header = false;
    loop {
        line.clear();
        reader
            .read_line(&mut line)
            .map_err(|error| error.to_string())?;
        if line == "\r\n" || line == "\n" || line.is_empty() {
            break;
        }
        if line.trim().eq_ignore_ascii_case("X-Grok-Bridge-WebUI: 1") {
            bridge_header = true;
        }
    }
    Ok((method, path, bridge_header))
}

fn write_http(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\nX-Content-Type-Options: nosniff\r\n\r\n{body}",
        body.len()
    )
}

const WEB_UI: &str = r#"<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Grok Bridge Sessions</title><style>body{font:14px system-ui;margin:24px;background:#111827;color:#e5e7eb}h1{font-size:22px}.group{margin:18px 0;padding:14px;background:#1f2937;border-radius:8px}.session{margin-top:12px;padding:12px;background:#0f172a;border:1px solid #374151;border-radius:6px}.meta{display:flex;gap:14px;flex-wrap:wrap;align-items:center}button{cursor:pointer;padding:6px 10px}code{color:#93c5fd}pre{min-height:120px;max-height:420px;overflow:auto;padding:12px;background:#020617;color:#d1fae5;white-space:pre-wrap;font:13px Consolas,monospace}.muted{color:#9ca3af}</style><h1>Grok Bridge 会话</h1><p class="muted">按 Codex 对话标题归类，每 2 秒刷新终端当前屏幕；确认内容后再关闭对应 Grok。</p><main id="groups"></main><script>const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));async function load(){let a=await fetch('/api/sessions',{cache:'no-store'}).then(r=>r.json()),m=new Map;for(const s of a){const k=s.owner||'未标记的 Codex 对话';if(!m.has(k))m.set(k,[]);m.get(k).push(s)}groups.innerHTML=[...m].map(([owner,list])=>`<section class="group"><h2>${esc(owner)}</h2>${list.map(s=>`<article class="session"><div class="meta"><code>${esc(s.session)}</code><b>${esc(s.phase)}</b><span>闲置 ${Math.max(0,Math.floor((Date.now()-s.updated_at_ms)/1000))} 秒</span><span>${esc(s.cwd)}</span><button data-close="${esc(s.session)}">关闭 Grok</button></div><pre>${esc(s.screen||'(终端尚无输出)')}</pre></article>`).join('')}</section>`).join('')||'<p>暂无会话</p>';document.querySelectorAll('[data-close]').forEach(b=>b.onclick=()=>closeSession(b.dataset.close))}async function closeSession(id){if(!confirm('确认关闭 '+id+' 及其 Grok 进程？'))return;await fetch('/api/sessions/'+encodeURIComponent(id)+'/close',{method:'POST',headers:{'X-Grok-Bridge-WebUI':'1'}});load()}load();setInterval(load,2000)</script></html>"#;

fn wake_listener() {
    if let Ok(name) = runtime_name() {
        let _ = Stream::connect(name);
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
