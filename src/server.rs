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
        validate_owner,
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
        ("POST", path) if path.starts_with("/api/owners/") => {
            let Some(encoded_owner) = close_path_segment(path, "/api/owners/") else {
                let _ = write_http(
                    &mut stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    "not found",
                );
                return;
            };
            if !bridge_header {
                let _ = write_http(
                    &mut stream,
                    "403 Forbidden",
                    "text/plain; charset=utf-8",
                    "missing WebUI request header",
                );
                return;
            }
            let owner = match percent_decode_path_segment(encoded_owner) {
                Ok(owner) => owner,
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
            if let Err(error) = validate_owner(&owner) {
                let _ = write_http(
                    &mut stream,
                    "400 Bad Request",
                    "text/plain; charset=utf-8",
                    &format!("{error:#}"),
                );
                return;
            }
            match state.host.close_owner(&owner) {
                Ok(result) => {
                    let body = serde_json::json!({
                        "matched": result.matched,
                        "closed": result.closed,
                        "failures": result.failures,
                    })
                    .to_string();
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
            }
        }
        ("POST", path) if path.starts_with("/api/sessions/") => {
            let Some(handle) = close_path_segment(path, "/api/sessions/") else {
                let _ = write_http(
                    &mut stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    "not found",
                );
                return;
            };
            if !bridge_header {
                let _ = write_http(
                    &mut stream,
                    "403 Forbidden",
                    "text/plain; charset=utf-8",
                    "missing WebUI request header",
                );
                return;
            }
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

fn close_path_segment<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    let segment = path.strip_prefix(prefix)?.strip_suffix("/close")?;
    if segment.contains('/') || segment.contains('?') {
        return None;
    }
    Some(segment)
}

fn percent_decode_path_segment(value: &str) -> std::result::Result<String, String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err("owner contains an incomplete percent escape".to_owned());
            }
            let high = hex_value(bytes[index + 1])
                .ok_or_else(|| "owner contains an invalid percent escape".to_owned())?;
            let low = hex_value(bytes[index + 2])
                .ok_or_else(|| "owner contains an invalid percent escape".to_owned())?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| "owner is not valid UTF-8".to_owned())
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
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

const WEB_UI: &str = r#"<!doctype html>
<html lang="zh-CN">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width">
<title>Grok Bridge Sessions</title>
<style>
body{font:14px system-ui;margin:24px;background:#111827;color:#e5e7eb}
h1{font-size:22px}
.group{margin:18px 0;padding:14px;background:#1f2937;border-radius:8px}
.group-close{float:right;background:#991b1b;color:#fff;border:1px solid #ef4444}
.session{margin-top:12px;padding:12px;background:#0f172a;border:1px solid #374151;border-radius:6px}
.meta{display:flex;gap:14px;flex-wrap:wrap;align-items:center}
button{cursor:pointer;padding:6px 10px}
code{color:#93c5fd}
pre{min-height:120px;max-height:420px;overflow:auto;padding:12px;background:#020617;color:#d1fae5;white-space:pre-wrap;font:13px Consolas,monospace}
.muted{color:#9ca3af}
</style>
<h1>Grok Bridge 会话</h1>
<p class="muted">按 Codex 对话标题归类，每 2 秒刷新终端当前屏幕；确认内容后再关闭对应 Grok。</p>
<main id="groups"></main>
<script>
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function load(){
  const sessions=await fetch('/api/sessions',{cache:'no-store'}).then(r=>r.json());
  const grouped=new Map();
  for(const session of sessions){
    const owner=session.owner??null;
    if(!grouped.has(owner))grouped.set(owner,[]);
    grouped.get(owner).push(session);
  }
  groups.innerHTML=[...grouped].map(([owner,list])=>`
    <section class="group">
      ${owner===null?'':`<button class="group-close" data-close-owner="${esc(owner)}" data-count="${list.length}">关闭该 Codex 全部 Grok (${list.length})</button>`}
      <h2>${esc(owner??'未标记的 Codex 对话')}</h2>
      ${list.map(session=>`
        <article class="session">
          <div class="meta">
            <code>${esc(session.session)}</code>
            <b>${esc(session.phase)}</b>
            <span>闲置 ${Math.max(0,Math.floor((Date.now()-session.updated_at_ms)/1000))} 秒</span>
            <span>${esc(session.cwd)}</span>
            <button data-close="${esc(session.session)}">关闭 Grok</button>
          </div>
          <pre>${esc(session.screen||'(终端尚无输出)')}</pre>
        </article>`).join('')}
    </section>`).join('')||'<p>暂无会话</p>';
  document.querySelectorAll('[data-close]').forEach(button=>{
    button.onclick=()=>closeSession(button.dataset.close);
  });
  document.querySelectorAll('[data-close-owner]').forEach(button=>{
    button.onclick=()=>closeOwner(button.dataset.closeOwner,Number(button.dataset.count),button);
  });
}
async function closeSession(id){
  if(!confirm('确认关闭 '+id+' 及其 Grok 进程？'))return;
  await fetch('/api/sessions/'+encodeURIComponent(id)+'/close',{
    method:'POST',
    headers:{'X-Grok-Bridge-WebUI':'1'}
  });
  load();
}
async function closeOwner(owner,count,button){
  if(!confirm('确认关闭 Codex“'+owner+'”下的全部 '+count+' 个 Grok 会话？'))return;
  button.disabled=true;
  try{
    const response=await fetch('/api/owners/'+encodeURIComponent(owner)+'/close',{
      method:'POST',
      headers:{'X-Grok-Bridge-WebUI':'1'}
    });
    if(!response.ok)throw new Error(await response.text());
    const result=await response.json();
    if(result.matched===0){
      alert('该 Codex 分组已没有活跃 Grok 会话。');
    }else if(result.failures.length||result.closed!==result.matched){
      alert('已关闭 '+result.closed+'/'+result.matched+' 个会话。\\n失败：\\n'+result.failures.join('\\n'));
    }
  }catch(error){
    alert('关闭失败：'+error);
  }finally{
    load();
  }
}
load();
setInterval(load,2000);
</script>
</html>"#;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_utf8_owner_path_segments_without_form_url_rules() {
        assert_eq!(
            percent_decode_path_segment("Codex-%E5%AF%B9%E8%AF%9D%2F100%25+ready").unwrap(),
            "Codex-对话/100%+ready"
        );
        assert_eq!(percent_decode_path_segment("A%2fb").unwrap(), "A/b");
    }

    #[test]
    fn rejects_malformed_owner_path_segments() {
        for value in ["owner%", "owner%2", "owner%GG", "%FF"] {
            assert!(percent_decode_path_segment(value).is_err(), "{value}");
        }
    }

    #[test]
    fn extracts_close_routes_without_overlapping_prefix_and_suffix() {
        assert_eq!(
            close_path_segment("/api/owners/Codex%20A/close", "/api/owners/"),
            Some("Codex%20A")
        );
        assert_eq!(
            close_path_segment("/api/owners//close", "/api/owners/"),
            Some("")
        );
        assert_eq!(
            close_path_segment("/api/owners/close", "/api/owners/"),
            None
        );
        assert_eq!(
            close_path_segment("/api/owners/a/b/close", "/api/owners/"),
            None
        );
        assert_eq!(
            close_path_segment("/api/sessions/close", "/api/sessions/"),
            None
        );
        assert_eq!(
            close_path_segment("/api/sessions/session-1/close", "/api/sessions/"),
            Some("session-1")
        );
    }

    #[test]
    fn webui_keeps_missing_owner_out_of_batch_close_groups() {
        assert!(WEB_UI.contains("const owner=session.owner??null;"));
        assert!(WEB_UI.contains("owner===null?'':"));
    }
}
