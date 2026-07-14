use std::{
    io::{BufReader, ErrorKind},
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

    let state = Arc::new(RuntimeState {
        host: SessionHost::new(),
        started_at_ms: now_millis(),
        stopping: AtomicBool::new(false),
    });

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
            always_approve,
        } => ResponseResult::Session(state.host.create(&cwd, prompt, model, always_approve)?),
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
            stopping: self.stopping.load(Ordering::Acquire),
        }
    }
}

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
