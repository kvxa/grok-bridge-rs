use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    hash::{Hash, Hasher},
    io::{BufRead, BufReader, ErrorKind, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use interprocess::local_socket::{Stream, prelude::*};
use tungstenite::{
    Message, WebSocket,
    handshake::derive_accept_key,
    protocol::{Role, WebSocketConfig},
};

#[cfg(test)]
use crate::transport::read_frame;
use crate::{
    protocol::{
        DEFAULT_WAIT_TIMEOUT_MS, Request, ResponseEnvelope, ResponseResult, ServerInfo,
        decode_request, decode_write_data, extract_validated_request_id,
        validate_client_session_id, validate_owner, validate_session_handle,
        validate_terminal_size,
    },
    session::{
        OrphanPolicy, SessionHost, WEB_CONTROL_LEASE_MS, WebEventsContinuation,
        web_events_plan_error_code,
    },
    transport::{bind_runtime_listener, runtime_name},
    version_check::{CHECK_INTERVAL, VersionChecker},
};

/// Bound WebSocket text/binary payload size (server and client).
const WEB_EVENTS_MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const WEB_EVENTS_MAX_BATCH_FRAMES: usize = 64;
const WEB_EVENTS_MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
const WEB_EVENTS_BATCH_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

fn web_events_batch_within_budget(
    frames: usize,
    bytes: usize,
    deadline: std::time::Instant,
) -> bool {
    frames <= WEB_EVENTS_MAX_BATCH_FRAMES
        && bytes <= WEB_EVENTS_MAX_BATCH_BYTES
        && std::time::Instant::now() < deadline
}
/// Max idle sleep between client-frame polls so interactive keys stay low-latency.
/// Host Condvar still wakes immediately on session revisions.
const WEB_EVENTS_CLIENT_POLL: Duration = Duration::from_millis(25);
/// Cap inbound request-id length for WebUI command frames.
const WEB_EVENTS_MAX_REQUEST_ID_BYTES: usize = 128;
/// Stable browser client identity (localStorage UUID etc.).
const WEB_EVENTS_MAX_CLIENT_IDENTITY_BYTES: usize = 128;
const WEB_EVENTS_MIN_CLIENT_IDENTITY_BYTES: usize = 8;
/// Bound per-client terminal subscriptions so one browser cannot request an
/// unbound terminal fan-out.
const WEB_EVENTS_MAX_SUBSCRIPTIONS: usize = 256;
/// Bound completed request IDs retained for duplicate command suppression
/// (per WebSocket and cross-reconnect identity cache).
const WEB_EVENTS_REQUEST_CACHE_CAPACITY: usize = 256;
const WEB_EVENTS_REQUEST_CACHE_BYTES: usize = 256 * 1024;
const WEB_EVENTS_GLOBAL_COMMAND_CACHE_BYTES: usize = 4 * 1024 * 1024;
/// Bound distinct client identities retained in the cross-connection cache.
const WEB_EVENTS_MAX_IDENTITY_BUCKETS: usize = 256;
/// How long a completed command result remains replayable after reconnect.
const WEB_EVENTS_RESULT_TTL_MS: u64 = 60_000;
/// Bound command IDs accepted from one WebSocket before acknowledgements drain.
const WEB_EVENTS_MAX_PENDING_COMMANDS: usize = 64;
// In-flight identity reservations are **not** wall-clock TTL purged. A WriteJob
// may block far longer than 30s; evicting pending would let the same id re-reserve
// and double-write. Terminal publish (remember / abort / RAII guard) is the only
// release path; capacity exhaustion returns flow_control instead.
// Web control lease duration: `session::WEB_CONTROL_LEASE_MS` (shared with
// SessionHost orphan reaper so claim/heartbeat and cleanup agree).
/// Server-originated WebSocket Ping interval. Browsers auto-reply with Pong;
/// Pong only proves transport liveness and never refreshes control leases.
const WEB_EVENTS_SERVER_PING_MS: u64 = 5_000;
/// Hard caps for the local WebUI HTTP front door.
const WEB_HTTP_MAX_CONNECTIONS: usize = 64;
const WEB_HTTP_MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
const WEB_HTTP_MAX_HEADERS: usize = 64;
const WEB_HTTP_MAX_HEADER_BYTES: usize = 32 * 1024;
const WEB_HTTP_MAX_HEADER_LINE_BYTES: usize = 8 * 1024;
const WEB_HTTP_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const WEB_HTTP_MAX_JSON_RESPONSE_BYTES: usize = 1024 * 1024;
/// High-entropy WebUI capability length (32 bytes → 64 hex chars).
const WEB_UI_CAPABILITY_BYTES: usize = 32;
const WEB_UI_CAPABILITY_HEX_LEN: usize = WEB_UI_CAPABILITY_BYTES * 2;
/// HttpOnly cookie set after successful `?c=` bootstrap (same-origin only).
const WEBUI_CAPABILITY_COOKIE: &str = "grok_bridge_webui_c";
/// Cap concurrent Runtime IPC workers so half-open peers cannot exhaust threads.
const IPC_MAX_CONNECTIONS: usize = 64;
/// Default first-frame read / ordinary response write timeout on the server.
const IPC_IO_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn run() -> Result<()> {
    let listener = match bind_runtime_listener() {
        Ok(listener) => listener,
        Err(error) if error.to_string().contains("already running") => {
            // Another live singleton owns the endpoint; treat start as success.
            return Ok(());
        }
        Err(error) => return Err(error),
    };

    let web_listener = bind_web_ui();
    let web_capability = generate_webui_capability()?;
    let web_url = web_listener
        .as_ref()
        .and_then(|listener| listener.local_addr().ok())
        .map(|address| {
            // Bootstrap URL carries the capability once for `server ui` open.
            // Token is never logged; CLI redacts when printing status.
            format!("http://{address}/?c={web_capability}")
        });
    let state = Arc::new(RuntimeState {
        host: SessionHost::new(OrphanPolicy::from_env()?),
        started_at_ms: now_millis(),
        stopping: AtomicBool::new(false),
        web_url,
        web_capability,
        version_checker: Arc::new(VersionChecker::new()),
        web_controls: WebControlRegistry::default(),
        web_identities: WebIdentityRegistry::default(),
        web_side_effect_gate: Mutex::new(()),
        next_web_client_id: AtomicU64::new(1),
        web_http_connections: AtomicU64::new(0),
        ipc_connections: AtomicU64::new(0),
    });
    if let Some(listener) = web_listener {
        let web_state = Arc::clone(&state);
        thread::spawn(move || run_web_ui(listener, web_state));
    }
    {
        let reaper_state = Arc::clone(&state);
        thread::spawn(move || run_orphan_reaper(reaper_state));
    }
    {
        let version_state = Arc::clone(&state);
        thread::spawn(move || run_version_checker(version_state));
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
        let active = state.ipc_connections.load(Ordering::Acquire);
        if active as usize >= IPC_MAX_CONNECTIONS {
            // Drop the connection without spawning a worker (half-open DoS guard).
            drop(connection);
            continue;
        }
        state.ipc_connections.fetch_add(1, Ordering::AcqRel);
        let state = Arc::clone(&state);
        thread::spawn(move || {
            handle_connection(connection, Arc::clone(&state));
            state.ipc_connections.fetch_sub(1, Ordering::AcqRel);
        });
    }

    // Listener loop ended: drain remaining sessions with the same bounded stop
    // path. Failures are logged; process still exits.
    if let Err(error) = state.host.shutdown_all() {
        eprintln!("grok-bridge server: shutdown_all after listener exit: {error:#}");
    }
    Ok(())
}

struct RuntimeState {
    host: SessionHost,
    started_at_ms: u64,
    stopping: AtomicBool,
    /// Bootstrap URL including `?c=` capability (for opening the browser only).
    web_url: Option<String>,
    /// High-entropy per-process WebUI capability. Never logged or persisted.
    web_capability: String,
    version_checker: Arc<VersionChecker>,
    web_controls: WebControlRegistry,
    web_identities: WebIdentityRegistry,
    /// Serializes identity takeover with control admission and PTY side effects
    /// so a revoked connection cannot write/resize after attach/takeover.
    web_side_effect_gate: Mutex<()>,
    next_web_client_id: AtomicU64,
    web_http_connections: AtomicU64,
    /// In-flight Runtime IPC workers (accept → handle_connection).
    ipc_connections: AtomicU64,
}

#[derive(Clone, Debug)]
struct ControlOwner {
    connection_id: u64,
    last_heartbeat_ms: u64,
}

#[derive(Default)]
struct WebControlRegistry {
    sessions: Mutex<HashMap<String, ControlOwner>>,
}

impl WebControlRegistry {
    fn expire_stale(&self, sessions: &mut HashMap<String, ControlOwner>, now: u64) {
        sessions
            .retain(|_, owner| now.saturating_sub(owner.last_heartbeat_ms) < WEB_CONTROL_LEASE_MS);
    }

    fn claim(&self, session: &str, client_id: u64) -> Result<bool> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("WebUI control registry lock was poisoned"))?;
        let now = now_millis();
        self.expire_stale(&mut sessions, now);
        match sessions.get(session) {
            Some(owner) if owner.connection_id != client_id => Ok(false),
            _ => {
                sessions.insert(
                    session.to_owned(),
                    ControlOwner {
                        connection_id: client_id,
                        last_heartbeat_ms: now,
                    },
                );
                Ok(true)
            }
        }
    }

    fn owns(&self, session: &str, client_id: u64) -> Result<bool> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("WebUI control registry lock was poisoned"))?;
        let now = now_millis();
        self.expire_stale(&mut sessions, now);
        if let Some(owner) = sessions.get_mut(session)
            && owner.connection_id == client_id
        {
            owner.last_heartbeat_ms = now;
            return Ok(true);
        }
        Ok(false)
    }

    /// Refresh control leases held by `client_id` that are still in `active`
    /// (visible / subscribed sessions). Unsubscribed holds are **not** renewed —
    /// they expire so another client can take over and orphan reaping can resume.
    /// Returns refreshed session handles for SessionHost lease mirroring.
    fn heartbeat(&self, client_id: u64, active: &HashSet<String>) -> Vec<String> {
        let mut refreshed = Vec::new();
        if let Ok(mut sessions) = self.sessions.lock() {
            let now = now_millis();
            self.expire_stale(&mut sessions, now);
            for (session, owner) in sessions.iter_mut() {
                if owner.connection_id == client_id && active.contains(session) {
                    owner.last_heartbeat_ms = now;
                    refreshed.push(session.clone());
                }
            }
        }
        refreshed
    }

    /// Drop every hold by `client_id` for sessions in `sessions`. Returns the
    /// handles actually released (for SessionHost mirror cleanup).
    fn release_sessions(&self, client_id: u64, sessions: &[String]) -> Vec<String> {
        let mut released = Vec::new();
        if sessions.is_empty() {
            return released;
        }
        if let Ok(mut map) = self.sessions.lock() {
            for session in sessions {
                if map
                    .get(session)
                    .is_some_and(|owner| owner.connection_id == client_id)
                {
                    map.remove(session);
                    released.push(session.clone());
                }
            }
        }
        released
    }

    fn release(&self, session: &str, client_id: u64) -> Result<bool> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("WebUI control registry lock was poisoned"))?;
        let now = now_millis();
        self.expire_stale(&mut sessions, now);
        if sessions
            .get(session)
            .is_some_and(|owner| owner.connection_id == client_id)
        {
            sessions.remove(session);
            return Ok(true);
        }
        Ok(false)
    }

    /// Drop all holds for a connection. Returns released session handles.
    fn release_client(&self, client_id: u64) -> Vec<String> {
        let mut released = Vec::new();
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.retain(|session, owner| {
                if owner.connection_id == client_id {
                    released.push(session.clone());
                    false
                } else {
                    true
                }
            });
        }
        released
    }
}

#[derive(Clone, Debug)]
struct CachedCommandResult {
    payload: String,
    /// Fingerprint of the command body (type/session/args), not the request id.
    fingerprint: String,
    expires_at_ms: u64,
}

/// Shared terminal state for one in-flight identity request id. Original
/// apply and same-id retries all wait on this until real completion.
struct PendingCompletion {
    outcome: Mutex<Option<PendingOutcome>>,
    cv: Condvar,
}

impl std::fmt::Debug for PendingCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ready = self
            .outcome
            .lock()
            .map(|guard| guard.is_some())
            .unwrap_or(true);
        f.debug_struct("PendingCompletion")
            .field("ready", &ready)
            .finish()
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
enum PendingOutcome {
    /// JSON command-result payload (success or definitive error).
    Ready(String),
    /// Reservation dropped without a result body (capacity / disconnect).
    Dropped,
}

impl PendingCompletion {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            outcome: Mutex::new(None),
            cv: Condvar::new(),
        })
    }

    fn publish(&self, outcome: PendingOutcome) {
        let mut guard = self
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.is_none() {
            *guard = Some(outcome);
            self.cv.notify_all();
        }
    }

    #[cfg(test)]
    fn wait(&self) -> PendingOutcome {
        let mut guard = self
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while guard.is_none() {
            guard = self
                .cv
                .wait(guard)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        guard
            .clone()
            .expect("pending identity completion published")
    }
}

#[derive(Clone, Debug)]
struct PendingIdentityCommand {
    fingerprint: String,
    /// Connection that reserved the id (diagnostic / unbound detach filter).
    connection_id: u64,
    /// Unique per reserve; completers must present this token to publish/abort.
    reservation_token: u64,
    /// True once a side-effect (WriteJob enqueue / apply) is bound. Detach of the
    /// reserving socket must **not** free bound reservations — the job owns the id.
    bound: bool,
    completion: Arc<PendingCompletion>,
}

/// RAII: every reserved identity command publishes a terminal outcome.
/// - Pre-enqueue (`Reserved`): Drop aborts so the id may safely retry.
/// - Post-enqueue (`Bound`): Drop caches a definitive failure (may be partial write)
///   so the same id never re-enqueues.
struct IdentityReservationGuard {
    cache: WebIdentityRegistry,
    identity: String,
    request_id: String,
    fingerprint: String,
    result_type: &'static str,
    session: Option<String>,
    /// Token from `begin_command` Reserved — required for terminal publish.
    reservation_token: u64,
    /// `false` until WriteJob (or other side effect) is bound / enqueued.
    bound: bool,
    finished: bool,
}

impl IdentityReservationGuard {
    fn new(
        cache: &WebIdentityRegistry,
        identity: &str,
        request_id: &str,
        fingerprint: &str,
        result_type: &'static str,
        session: Option<&str>,
        reservation_token: u64,
    ) -> Self {
        Self {
            cache: cache.clone(),
            identity: identity.to_owned(),
            request_id: request_id.to_owned(),
            fingerprint: fingerprint.to_owned(),
            result_type,
            session: session.map(str::to_owned),
            reservation_token,
            bound: false,
            finished: false,
        }
    }

    fn mark_bound(&mut self) {
        if self.bound {
            return;
        }
        self.bound = true;
        self.cache
            .bind_command(&self.identity, &self.request_id, self.reservation_token);
    }

    /// Revert to pre-enqueue mode so Drop/abort allows a safe same-id retry.
    fn mark_unbound(&mut self) {
        if !self.bound {
            return;
        }
        self.bound = false;
        self.cache
            .unbind_command(&self.identity, &self.request_id, self.reservation_token);
    }

    fn mark_finished(&mut self) {
        self.finished = true;
    }

    fn bind_write_completion(mut self, inflight: &crate::session::SessionWriteInFlight) {
        self.mark_bound();
        self.finished = true;
        let cache = self.cache.clone();
        let identity = self.identity.clone();
        let request_id = self.request_id.clone();
        let fingerprint = self.fingerprint.clone();
        let result_type = self.result_type;
        let session = self.session.clone();
        let token = self.reservation_token;
        inflight.observe(move |outcome| {
            let payload =
                write_completion_payload(result_type, &request_id, session.as_deref(), outcome);
            cache.remember(&identity, &request_id, fingerprint, payload, Some(token));
        });
    }

    /// Publish a non-cached terminal body to joiners (transient pre-enqueue errors).
    fn finish_uncached(&mut self, payload: String) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.cache.complete_without_cache(
            &self.identity,
            &self.request_id,
            payload,
            Some(self.reservation_token),
        );
    }
}

fn write_completion_error_code(message: &str) -> &'static str {
    if message.contains("deadline") || message.contains("outcome was not confirmed") {
        "indeterminate"
    } else {
        "write_failed"
    }
}

fn write_completion_payload(
    result_type: &str,
    request_id: &str,
    session: Option<&str>,
    outcome: Result<(), String>,
) -> String {
    match outcome {
        Ok(()) => build_web_events_command_result(
            result_type,
            Some(request_id),
            session,
            true,
            None,
            None,
        ),
        Err(message) => build_web_events_command_result(
            result_type,
            Some(request_id),
            session,
            false,
            Some(write_completion_error_code(&message)),
            Some(&message),
        ),
    }
}

impl Drop for IdentityReservationGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        if self.bound {
            // Enqueued (or otherwise applied) without an explicit finish — treat as
            // definitive failure. Same id must Replay this, never re-write.
            let payload = build_web_events_command_result(
                self.result_type,
                Some(self.request_id.as_str()),
                self.session.as_deref(),
                false,
                Some("write_failed"),
                Some(
                    "command interrupted after enqueue; PTY write may have partially applied and is not safe to retry",
                ),
            );
            self.cache.remember(
                &self.identity,
                &self.request_id,
                self.fingerprint.clone(),
                payload,
                Some(self.reservation_token),
            );
        } else {
            self.cache.abort_command(
                &self.identity,
                &self.request_id,
                Some(self.reservation_token),
            );
        }
    }
}

#[derive(Clone, Debug, Default)]
struct IdentityCommandBucket {
    completed: HashMap<String, CachedCommandResult>,
    /// In-flight reservations so reconnect/takeover cannot double-apply while
    /// the previous connection has applied but not yet remembered the result.
    pending: HashMap<String, PendingIdentityCommand>,
}

#[derive(Clone, Debug)]
struct LiveIdentity {
    connection_id: u64,
    /// Monotonic generation; increments on each attach/takeover.
    generation: u64,
}

/// Outcome of atomically looking up / reserving a request id for an identity.
#[derive(Debug)]
enum IdentityCommandBegin {
    /// Completed result is already cached — replay without re-applying.
    Replay(String),
    /// Same id+fingerprint is executing; wait on this for the single outcome.
    Join(Arc<PendingCompletion>),
    /// Reserved for this connection; caller must finish or abort with `token`.
    Reserved { token: u64 },
}

/// Structured begin_command failure — never collapse capacity into id_conflict.
#[derive(Clone, Debug, PartialEq, Eq)]
enum IdentityBeginError {
    /// Same request id was used with a different command fingerprint.
    IdConflict(String),
    /// Global identity-bucket capacity exhausted; client may retry later.
    FlowControl(String),
}

impl IdentityBeginError {
    fn error_code(&self) -> &'static str {
        match self {
            Self::IdConflict(_) => "id_conflict",
            Self::FlowControl(_) => "flow_control",
        }
    }

    fn message(&self) -> &str {
        match self {
            Self::IdConflict(message) | Self::FlowControl(message) => message.as_str(),
        }
    }
}

/// Cross-connection result replay + in-flight reservation keyed by stable
/// browser tab identity.
///
/// Pending reservations are owned by the **side-effect job** (WriteJob /
/// completion token), not by the WebSocket connection. Takeover `detach` of an
/// old connection must not free a bound reservation while write_all/flush may
/// still run — otherwise a same-id retry on the new socket can double-enqueue.
#[derive(Clone, Default)]
struct WebIdentityRegistry {
    /// identity → completed results and in-flight reservations (one lock so
    /// reserve/lookup/finish are atomic against takeover).
    commands: Arc<Mutex<HashMap<String, IdentityCommandBucket>>>,
    /// identity currently attached to a live WebSocket connection.
    live: Arc<Mutex<HashMap<String, LiveIdentity>>>,
    /// Monotonic reservation tokens for completer ownership checks.
    next_reservation_token: Arc<AtomicU64>,
}

impl WebIdentityRegistry {
    fn command_cache_bytes(commands: &HashMap<String, IdentityCommandBucket>) -> usize {
        commands
            .iter()
            .map(|(identity, bucket)| {
                identity.len()
                    + bucket
                        .completed
                        .iter()
                        .map(|(id, entry)| id.len() + entry.fingerprint.len() + entry.payload.len())
                        .sum::<usize>()
                    + bucket
                        .pending
                        .iter()
                        .map(|(id, entry)| id.len() + entry.fingerprint.len())
                        .sum::<usize>()
            })
            .sum()
    }

    fn evict_completed_to_byte_budget(commands: &mut HashMap<String, IdentityCommandBucket>) {
        while Self::command_cache_bytes(commands) > WEB_EVENTS_GLOBAL_COMMAND_CACHE_BYTES {
            let victim = commands
                .iter()
                .flat_map(|(identity, bucket)| {
                    bucket
                        .completed
                        .iter()
                        .map(move |(id, entry)| (identity.clone(), id.clone(), entry.expires_at_ms))
                })
                .min_by_key(|(_, _, expires)| *expires);
            let Some((identity, id, _)) = victim else {
                break;
            };
            if let Some(bucket) = commands.get_mut(&identity) {
                bucket.completed.remove(&id);
            }
            if commands
                .get(&identity)
                .is_some_and(|bucket| bucket.completed.is_empty() && bucket.pending.is_empty())
            {
                commands.remove(&identity);
            }
        }
    }

    /// Attach `connection_id` as the sole live owner of `identity`.
    /// Returns the generation for this attachment. Previous owners are revoked.
    fn attach(&self, identity: &str, connection_id: u64) -> Result<(u64, Option<u64>), String> {
        let mut live = self
            .live
            .lock()
            .map_err(|_| "WebUI identity registry lock was poisoned".to_owned())?;
        let revoked = live
            .get(identity)
            .filter(|current| current.connection_id != connection_id)
            .map(|current| current.connection_id);
        let generation = live
            .get(identity)
            .map(|current| current.generation.wrapping_add(1))
            .unwrap_or(1);
        live.insert(
            identity.to_owned(),
            LiveIdentity {
                connection_id,
                generation,
            },
        );
        Ok((generation, revoked))
    }

    fn is_current(&self, identity: &str, connection_id: u64, generation: u64) -> bool {
        let Ok(live) = self.live.lock() else {
            return false;
        };
        live.get(identity).is_some_and(|current| {
            current.connection_id == connection_id && current.generation == generation
        })
    }

    fn detach(&self, identity: Option<&str>, connection_id: u64) {
        let Some(identity) = identity else {
            return;
        };
        if let Ok(mut live) = self.live.lock()
            && live
                .get(identity)
                .is_some_and(|current| current.connection_id == connection_id)
        {
            live.remove(identity);
        }
        // Only drop **unbound** reservations for this socket. Bound pending is
        // owned by the live WriteJob/side-effect until terminal publish
        // (remember / complete / bound Drop) — never by socket lifecycle.
        // Freeing bound slots on detach allows a same-id retry to double-write.
        if let Ok(mut commands) = self.commands.lock()
            && let Some(bucket) = commands.get_mut(identity)
        {
            bucket.pending.retain(|_, pending| {
                if pending.connection_id != connection_id {
                    return true;
                }
                if pending.bound {
                    // Job still owns the id; Joiners on the new connection wait.
                    return true;
                }
                pending.completion.publish(PendingOutcome::Dropped);
                false
            });
            if bucket.completed.is_empty() && bucket.pending.is_empty() {
                commands.remove(identity);
            }
        }
    }

    /// Lookup a cached result. Returns `Ok(payload)` on fingerprint match,
    /// `Err(id_conflict)` when the id exists with a different fingerprint.
    /// In-flight reservations return `Ok(None)` here — use `begin_command` to
    /// distinguish InFlight from missing for admission.
    fn lookup(
        &self,
        identity: &str,
        request_id: &str,
        fingerprint: &str,
    ) -> Result<Option<String>, String> {
        let mut commands = self
            .commands
            .lock()
            .map_err(|_| "WebUI identity registry lock was poisoned".to_owned())?;
        let now = now_millis();
        Self::purge_locked(&mut commands, now);
        let Some(entry) = commands
            .get(identity)
            .and_then(|bucket| bucket.completed.get(request_id))
        else {
            return Ok(None);
        };
        if entry.fingerprint != fingerprint {
            return Err("request id was already used with a different command payload".to_owned());
        }
        Ok(Some(entry.payload.clone()))
    }

    /// Atomically replay, detect in-flight, or reserve `request_id` for identity.
    fn begin_command(
        &self,
        identity: &str,
        connection_id: u64,
        request_id: &str,
        fingerprint: &str,
    ) -> Result<IdentityCommandBegin, IdentityBeginError> {
        let mut commands = self.commands.lock().map_err(|_| {
            IdentityBeginError::FlowControl("WebUI identity registry lock was poisoned".to_owned())
        })?;
        let now = now_millis();
        Self::purge_locked(&mut commands, now);
        // Existing identity: lookup first without allocating a new bucket.
        if let Some(bucket) = commands.get(identity) {
            if let Some(entry) = bucket.completed.get(request_id) {
                if entry.fingerprint != fingerprint {
                    return Err(IdentityBeginError::IdConflict(
                        "request id was already used with a different command payload".to_owned(),
                    ));
                }
                return Ok(IdentityCommandBegin::Replay(entry.payload.clone()));
            }
            if let Some(pending) = bucket.pending.get(request_id) {
                if pending.fingerprint != fingerprint {
                    return Err(IdentityBeginError::IdConflict(
                        "request id was already used with a different command payload".to_owned(),
                    ));
                }
                return Ok(IdentityCommandBegin::Join(Arc::clone(&pending.completion)));
            }
        }
        if !Self::ensure_identity_capacity(&mut commands, identity) {
            return Err(IdentityBeginError::FlowControl(
                "too many client identities in flight; retry later".to_owned(),
            ));
        }
        let reservation_bytes = identity.len() + request_id.len() + fingerprint.len();
        if Self::command_cache_bytes(&commands).saturating_add(reservation_bytes)
            > WEB_EVENTS_GLOBAL_COMMAND_CACHE_BYTES
        {
            return Err(IdentityBeginError::FlowControl(
                "WebUI command cache byte budget is full; retry later".to_owned(),
            ));
        }
        let bucket = commands.entry(identity.to_owned()).or_default();
        // Re-check after capacity (another path may have inserted).
        if let Some(entry) = bucket.completed.get(request_id) {
            if entry.fingerprint != fingerprint {
                return Err(IdentityBeginError::IdConflict(
                    "request id was already used with a different command payload".to_owned(),
                ));
            }
            return Ok(IdentityCommandBegin::Replay(entry.payload.clone()));
        }
        if let Some(pending) = bucket.pending.get(request_id) {
            if pending.fingerprint != fingerprint {
                return Err(IdentityBeginError::IdConflict(
                    "request id was already used with a different command payload".to_owned(),
                ));
            }
            return Ok(IdentityCommandBegin::Join(Arc::clone(&pending.completion)));
        }
        let completion = PendingCompletion::new();
        // Non-zero monotonic token; completers must present it to publish/abort.
        let token = self
            .next_reservation_token
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        bucket.pending.insert(
            request_id.to_owned(),
            PendingIdentityCommand {
                fingerprint: fingerprint.to_owned(),
                connection_id,
                reservation_token: token,
                bound: false,
                completion,
            },
        );
        Ok(IdentityCommandBegin::Reserved { token })
    }

    /// Mark a reserved id as bound to a live side-effect (WriteJob enqueued).
    fn bind_command(&self, identity: &str, request_id: &str, token: u64) {
        let Ok(mut commands) = self.commands.lock() else {
            return;
        };
        if let Some(pending) = commands
            .get_mut(identity)
            .and_then(|bucket| bucket.pending.get_mut(request_id))
            && pending.reservation_token == token
        {
            pending.bound = true;
        }
    }

    /// Clear bound flag after a pre-enqueue failure (same id may retry safely).
    fn unbind_command(&self, identity: &str, request_id: &str, token: u64) {
        let Ok(mut commands) = self.commands.lock() else {
            return;
        };
        if let Some(pending) = commands
            .get_mut(identity)
            .and_then(|bucket| bucket.pending.get_mut(request_id))
            && pending.reservation_token == token
        {
            pending.bound = false;
        }
    }

    /// Commit a cacheable result, notify joiners, and release the reservation.
    /// When `token` is `Some`, only the matching reservation may publish (stale
    /// completers after a replaced unbound slot cannot steal the new job's id).
    fn remember(
        &self,
        identity: &str,
        request_id: &str,
        fingerprint: String,
        payload: String,
        token: Option<u64>,
    ) {
        let Ok(mut commands) = self.commands.lock() else {
            return;
        };
        let now = now_millis();
        Self::purge_locked(&mut commands, now);
        // Never evict pending buckets to make room; drop remember if no slot.
        if !commands.contains_key(identity)
            && !Self::ensure_identity_capacity(&mut commands, identity)
        {
            return;
        }
        let bucket = commands.entry(identity.to_owned()).or_default();
        if let Some(expected) = token {
            match bucket.pending.get(request_id) {
                Some(pending) if pending.reservation_token == expected => {}
                Some(_) => {
                    // Stale completer: a different reservation owns this id.
                    return;
                }
                None => {
                    // Reservation already finished; still allow cache insert if
                    // no conflicting pending (idempotent late remember).
                }
            }
        }
        if let Some(pending) = bucket.pending.remove(request_id) {
            if let Some(expected) = token
                && pending.reservation_token != expected
            {
                // Put back if we raced (should not happen after check above).
                bucket.pending.insert(request_id.to_owned(), pending);
                return;
            }
            pending
                .completion
                .publish(PendingOutcome::Ready(payload.clone()));
        }
        while bucket.completed.len() >= WEB_EVENTS_REQUEST_CACHE_CAPACITY {
            let victim = bucket
                .completed
                .iter()
                .min_by_key(|(_, value)| value.expires_at_ms)
                .map(|(key, _)| key.clone());
            if let Some(key) = victim {
                bucket.completed.remove(&key);
            } else {
                break;
            }
        }
        bucket.completed.insert(
            request_id.to_owned(),
            CachedCommandResult {
                payload,
                fingerprint,
                expires_at_ms: now.saturating_add(WEB_EVENTS_RESULT_TTL_MS),
            },
        );
        Self::evict_completed_to_byte_budget(&mut commands);
    }

    /// Publish a non-cacheable result to joiners and drop the reservation.
    /// Used for definitive-but-uncached failures so attached retries observe
    /// the same payload without re-applying the side effect.
    fn complete_without_cache(
        &self,
        identity: &str,
        request_id: &str,
        payload: String,
        token: Option<u64>,
    ) {
        let Ok(mut commands) = self.commands.lock() else {
            return;
        };
        if let Some(bucket) = commands.get_mut(identity) {
            if let Some(pending) = bucket.pending.get(request_id)
                && let Some(expected) = token
                && pending.reservation_token != expected
            {
                return;
            }
            if let Some(pending) = bucket.pending.remove(request_id) {
                if let Some(expected) = token
                    && pending.reservation_token != expected
                {
                    bucket.pending.insert(request_id.to_owned(), pending);
                    return;
                }
                pending.completion.publish(PendingOutcome::Ready(payload));
            }
            if bucket.completed.is_empty() && bucket.pending.is_empty() {
                commands.remove(identity);
            }
        }
    }

    /// Drop an in-flight reservation without caching (transient failure).
    fn abort_command(&self, identity: &str, request_id: &str, token: Option<u64>) {
        let Ok(mut commands) = self.commands.lock() else {
            return;
        };
        if let Some(bucket) = commands.get_mut(identity) {
            if let Some(pending) = bucket.pending.get(request_id)
                && let Some(expected) = token
                && pending.reservation_token != expected
            {
                return;
            }
            if let Some(pending) = bucket.pending.remove(request_id) {
                if let Some(expected) = token
                    && pending.reservation_token != expected
                {
                    bucket.pending.insert(request_id.to_owned(), pending);
                    return;
                }
                pending.completion.publish(PendingOutcome::Dropped);
            }
            if bucket.completed.is_empty() && bucket.pending.is_empty() {
                commands.remove(identity);
            }
        }
    }

    /// Number of identity buckets currently retained (test helper).
    #[cfg(test)]
    fn identity_bucket_count(&self) -> usize {
        self.commands.lock().map(|guard| guard.len()).unwrap_or(0)
    }

    fn purge_locked(commands: &mut HashMap<String, IdentityCommandBucket>, now: u64) {
        commands.retain(|_, bucket| {
            // Only completed results age out. Pending reservations stay until
            // remember/abort/RAII finish — wall-clock eviction would free the id
            // while a WriteJob is still blocked and allow a double write.
            bucket
                .completed
                .retain(|_, entry| entry.expires_at_ms > now);
            !bucket.completed.is_empty() || !bucket.pending.is_empty()
        });
    }

    /// Cap the global number of identity buckets (not only entries per identity).
    /// Never evicts a bucket that still has in-flight pending reservations.
    /// Returns false when no free slot can be made for a new identity.
    fn ensure_identity_capacity(
        commands: &mut HashMap<String, IdentityCommandBucket>,
        identity: &str,
    ) -> bool {
        if commands.contains_key(identity) {
            return true;
        }
        while commands.len() >= WEB_EVENTS_MAX_IDENTITY_BUCKETS {
            // Only idle buckets (no pending) may be reclaimed.
            let victim = commands
                .iter()
                .filter(|(key, _)| key.as_str() != identity)
                .filter(|(_, bucket)| bucket.pending.is_empty())
                .min_by_key(|(_, bucket)| {
                    bucket
                        .completed
                        .values()
                        .map(|entry| entry.expires_at_ms)
                        .min()
                        .unwrap_or(0)
                })
                .map(|(key, _)| key.clone());
            let Some(key) = victim else {
                // All buckets hold pending work — refuse new identity.
                return false;
            };
            commands.remove(&key);
        }
        true
    }
}

fn validate_web_client_identity(identity: &str) -> Result<(), String> {
    let identity = identity.trim();
    if identity.len() < WEB_EVENTS_MIN_CLIENT_IDENTITY_BYTES
        || identity.len() > WEB_EVENTS_MAX_CLIENT_IDENTITY_BYTES
    {
        return Err("client identity length is invalid".to_owned());
    }
    if !identity
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return Err("client identity contains unsupported characters".to_owned());
    }
    Ok(())
}

fn handle_connection(stream: Stream, state: Arc<RuntimeState>) {
    // Bound first-frame read and response write so a half-open peer cannot pin
    // a worker forever (accept path already caps concurrent workers).
    if let Err(error) = stream.set_nonblocking(true) {
        // Without nonblocking mode Windows named-pipe reads have no timeout.
        // Drop the connection before entering read_frame_until rather than
        // letting a peer pin this worker indefinitely.
        eprintln!("grok-bridge server: failed to enable bounded IPC I/O: {error}");
        return;
    }
    let mut connection = BufReader::new(stream);
    let frame = match crate::transport::read_frame_until(
        &mut connection,
        std::time::Instant::now() + IPC_IO_TIMEOUT,
    ) {
        Ok(frame) => frame,
        Err(error) => {
            let response =
                ResponseEnvelope::failure("invalid-request", "invalid_frame", format!("{error:#}"));
            let _ = crate::transport::write_response_until(
                connection.get_mut(),
                &response,
                std::time::Instant::now() + IPC_IO_TIMEOUT,
            );
            return;
        }
    };
    let envelope = match decode_request(&frame) {
        Ok(envelope) => envelope,
        Err(error) => {
            // Preserve a trustworthy client request id so transport::call can
            // surface the real invalid_request message (e.g. read limit) instead
            // of failing first on response id mismatch. Unparseable / untrusted
            // ids stay on the fixed safe placeholder.
            let response_id = extract_validated_request_id(&frame)
                .unwrap_or_else(|| "invalid-request".to_owned());
            let response =
                ResponseEnvelope::failure(response_id, "invalid_request", format!("{error:#}"));
            let _ = crate::transport::write_response_until(
                connection.get_mut(),
                &response,
                std::time::Instant::now() + IPC_IO_TIMEOUT,
            );
            return;
        }
    };

    // Long wait/read: extend socket timeouts to cover the request budget so the
    // worker is not cut mid-wait by the default IPC_IO_TIMEOUT. Values are already
    // validated by decode_request (no silent clamp).
    let io_timeout = match &envelope.request {
        Request::Wait { timeout_ms, .. } => {
            let wait_ms = timeout_ms.unwrap_or(DEFAULT_WAIT_TIMEOUT_MS);
            Duration::from_millis(wait_ms).saturating_add(Duration::from_secs(30))
        }
        Request::Read {
            wait_ms: Some(wait_ms),
            ..
        } if *wait_ms > 0 => {
            Duration::from_millis(*wait_ms).saturating_add(Duration::from_secs(30))
        }
        _ => IPC_IO_TIMEOUT,
    };

    let request_id = envelope.id;
    let client_session_id = envelope.client_session_id;
    let refresh_after_response = !matches!(envelope.request, Request::CloseCodex);
    if let Some(client_session_id) = client_session_id.as_deref()
        && let Err(error) = state.host.touch_client(client_session_id)
    {
        let response =
            ResponseEnvelope::failure(request_id, "invalid_client_session", format!("{error:#}"));
        let _ = crate::transport::write_response_until(
            connection.get_mut(),
            &response,
            std::time::Instant::now() + io_timeout,
        );
        return;
    }
    let (mut response, stop_after_response) =
        match dispatch(&state, envelope.request, client_session_id.as_deref()) {
            Ok((result, stop)) => (ResponseEnvelope::success(request_id, result), stop),
            Err(error) => {
                let (code, message) = map_rpc_dispatch_error(&error);
                (ResponseEnvelope::failure(request_id, code, message), false)
            }
        };
    // A large Show (or any future response carrying a large snapshot) must
    // still produce one parseable NDJSON response. Do not let encode failure
    // silently close the IPC stream or strand the CLI waiting for a frame.
    response = bounded_rpc_response(response);
    let wrote_response = crate::transport::write_response_until(
        connection.get_mut(),
        &response,
        std::time::Instant::now() + io_timeout,
    )
    .is_ok();
    if wrote_response
        && response.ok
        && refresh_after_response
        && let Some(client_session_id) = client_session_id.as_deref()
    {
        let _ = state.host.touch_client(client_session_id);
    }
    if stop_after_response {
        wake_listener();
    }
}

fn bounded_rpc_response(response: ResponseEnvelope) -> ResponseEnvelope {
    if crate::protocol::encode_frame(&response).is_ok() {
        return response;
    }
    ResponseEnvelope::failure(
        response.id,
        "response_too_large",
        "response exceeds the 1 MiB protocol frame limit",
    )
}

fn dispatch(
    state: &RuntimeState,
    request: Request,
    client_session_id: Option<&str>,
) -> Result<(ResponseResult, bool)> {
    let result = match request {
        Request::ServerStatus => ResponseResult::ServerInfo(state.server_info()),
        Request::ServerStop => {
            // Only mark Runtime stopping + wake the accept loop after every
            // session has been torn down. Partial failure restores accepting
            // so close/server stop can be retried; never false-success.
            state.host.shutdown_all()?;
            state.stopping.store(true, Ordering::Release);
            return Ok((ResponseResult::Accepted { accepted: true }, true));
        }
        Request::Heartbeat => {
            let client_session_id = client_session_id.context(
                "heartbeat requires CODEX_THREAD_ID or CODEX_SESSION_ID in the client environment",
            )?;
            state.host.touch_client(client_session_id)?;
            ResponseResult::Accepted { accepted: true }
        }
        Request::CloseCodex => {
            let client_session_id = client_session_id.context(
                "close_codex requires CODEX_THREAD_ID or CODEX_SESSION_ID in the client environment",
            )?;
            ResponseResult::CloseGroup(state.host.close_client(client_session_id)?)
        }
        Request::Create {
            cwd,
            prompt,
            model,
            owner,
            always_approve,
        } => ResponseResult::Session(Box::new(state.host.create(
            &cwd,
            prompt,
            model,
            owner,
            always_approve,
            client_session_id.map(str::to_owned),
        )?)),
        Request::List => ResponseResult::Sessions {
            // List is a board operation; never include ANSI/screen snapshots
            // that belong to Show and can consume the entire frame budget.
            sessions: state.host.list_web_board()?,
        },
        Request::Show { session } => ResponseResult::Session(Box::new(state.host.show(&session)?)),
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
            ResponseResult::Session(Box::new(state.host.send(&session, input)?))
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
            timeout_ms.unwrap_or(DEFAULT_WAIT_TIMEOUT_MS),
        )?),
        Request::Close { session } => ResponseResult::Accepted {
            accepted: state.host.close(&session)?,
        },
        Request::HookEvent {
            provider_session_id,
            event,
        } => ResponseResult::Accepted {
            accepted: state.host.apply_hook_event(&provider_session_id, event)?,
        },
    };
    Ok((result, false))
}

/// Map business `anyhow` failures to stable RPC error codes so callers need not
/// scrape English message text. Decode failures already use `invalid_request`.
fn map_rpc_dispatch_error(error: &anyhow::Error) -> (&'static str, String) {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    // Order matters: validation messages may contain the substring "timeout"
    // (e.g. timeout_ms bounds) and must classify as invalid_request, not timeout.
    let code = if lower.contains("session not found")
        || (lower.contains("not found") && lower.contains("session"))
    {
        "not_found"
    } else if lower.contains("must be between")
        || lower.contains("must not be")
        || lower.contains("must contain")
        || lower.contains("exceeds")
        || lower.contains("invalid")
        || lower.contains("required")
        || lower.contains("beyond the latest cursor")
        || lower.contains("out of range")
        || lower.contains("empty when provided")
        || lower.contains("cwd must")
        || lower.contains("terminal ")
        || lower.contains("data_base64")
        || lower.contains("provider_session_id")
        || lower.contains("wait_ms")
        || lower.contains("timeout_ms")
        || lower.contains("read limit")
    {
        "invalid_request"
    } else if lower.contains("timed out")
        || lower.contains("deadline exceeded")
        || (lower.contains("timeout") && !lower.contains("timeout_ms"))
    {
        "timeout"
    } else if lower.contains("is closing")
        || lower.contains("was closed during create")
        || lower.contains("owner was closed")
        || lower.contains("client session is closing")
        || lower.contains("owner is closing")
    {
        "closing"
    } else if lower.contains("queue is full")
        || lower.contains("capacity exceeded")
        || lower.contains("too many")
        || lower.contains("flow control")
        || lower.contains("no longer accepts new sessions")
    {
        "flow_control"
    } else if lower.contains("poisoned") || lower.contains("panicked") || lower.contains("internal")
    {
        "internal"
    } else {
        // Unknown business failure: treat as internal so clients do not match
        // on opaque English strings via a single request_failed bucket.
        "internal"
    };
    (code, message)
}

#[cfg(test)]
mod rpc_error_mapping_tests {
    use super::map_rpc_dispatch_error;

    #[test]
    fn map_rpc_dispatch_error_classifies_stable_codes() {
        let cases = [
            ("session not found: gbt-missing", "not_found"),
            ("wait_ms must be between 0 and 300000", "invalid_request"),
            (
                "timeout_ms must be between 1 and 7200000",
                "invalid_request",
            ),
            ("cursor 9 is beyond the latest cursor 3", "invalid_request"),
            ("session input queue is full", "flow_control"),
            (
                "session capacity exceeded (256 live + pending creates)",
                "flow_control",
            ),
            ("client session is closing", "closing"),
            ("owner is closing", "closing"),
            ("batch close deadline exceeded", "timeout"),
            ("session registry lock was poisoned", "internal"),
            ("failed to open PTY", "internal"),
        ];
        for (msg, expected) in cases {
            let (code, body) = map_rpc_dispatch_error(&anyhow::anyhow!("{msg}"));
            assert_eq!(code, expected, "msg={msg}");
            assert_eq!(body, msg);
        }
    }
}

fn run_orphan_reaper(state: Arc<RuntimeState>) {
    while !state.stopping.load(Ordering::Acquire) {
        thread::sleep(Duration::from_secs(5));
        if state.stopping.load(Ordering::Acquire) {
            return;
        }
        // Only real close/removal paths notify the WebUI revision bus.
        if let Err(error) = state.host.reap_orphans() {
            eprintln!("grok-bridge server: orphan cleanup failed: {error:#}");
        }
    }
}

fn run_version_checker(state: Arc<RuntimeState>) {
    loop {
        if state.stopping.load(Ordering::Acquire) {
            return;
        }
        state.version_checker.refresh();
        let mut remaining = CHECK_INTERVAL;
        while remaining > Duration::ZERO {
            if state.stopping.load(Ordering::Acquire) {
                return;
            }
            let slice = remaining.min(Duration::from_secs(30));
            thread::sleep(slice);
            remaining = remaining.saturating_sub(slice);
        }
    }
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

/// Generate a fresh high-entropy WebUI capability for this Runtime process.
fn generate_webui_capability() -> Result<String> {
    let mut bytes = [0u8; WEB_UI_CAPABILITY_BYTES];
    getrandom::fill(&mut bytes).context("failed to generate WebUI capability")?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// Constant-time equality for capability strings (fixed-length hex secrets).
fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

fn web_capability_matches(expected: &str, provided: Option<&str>) -> bool {
    let Some(provided) = provided.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    // Reject non-hex / wrong length early without comparing secret content.
    if provided.len() != expected.len() || !provided.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return false;
    }
    constant_time_eq(expected, provided)
}

/// Redact bootstrap capability from a web_url for logs / diagnostics / CLI print.
pub(crate) fn redact_web_url_capability(url: &str) -> String {
    let Some((base, query)) = url.split_once('?') else {
        return url.to_owned();
    };
    let scrubbed = query
        .split('&')
        .map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or("");
            if key == "c" || key == "capability" {
                format!("{key}=***")
            } else {
                pair.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("&");
    format!("{base}?{scrubbed}")
}

fn bind_web_ui() -> Option<TcpListener> {
    let address = env::var("GROK_BRIDGE_WEB_ADDR").unwrap_or_else(|_| "127.0.0.1:47653".to_owned());
    // Capability auth is per-Runtime secret for same-host multi-user isolation.
    // Still refuse non-loopback binds: capability in URL/header is not TLS.
    if !web_bind_address_is_loopback(&address) {
        eprintln!(
            "grok-bridge server: refusing non-loopback WebUI bind (loopback + capability only): {address}"
        );
        return None;
    }
    match TcpListener::bind(&address) {
        Ok(listener) => Some(listener),
        Err(error) => {
            eprintln!("grok-bridge server: WebUI unavailable at {address}: {error}");
            None
        }
    }
}

fn web_bind_address_is_loopback(address: &str) -> bool {
    // Accept host:port, bare host, or IPv6 in brackets.
    if let Ok(socket) = address.parse::<std::net::SocketAddr>() {
        return socket.ip().is_loopback();
    }
    let host = if let Some((host, port)) = address.rsplit_once(':')
        && !host.is_empty()
        && port.chars().all(|ch| ch.is_ascii_digit())
    {
        host
    } else {
        address
    };
    let host = host.trim_matches(|ch| ch == '[' || ch == ']');
    matches!(host, "127.0.0.1" | "localhost" | "::1")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn run_web_ui(listener: TcpListener, state: Arc<RuntimeState>) {
    for connection in listener.incoming() {
        if state.stopping.load(Ordering::Acquire) {
            break;
        }
        match connection {
            Ok(stream) => {
                // Configure before the overload branch: a client that does not
                // read its 503 must never block the sole accept loop forever.
                let _ = stream.set_write_timeout(Some(WEB_HTTP_WRITE_TIMEOUT));
                let active = state.web_http_connections.load(Ordering::Acquire);
                if active as usize >= WEB_HTTP_MAX_CONNECTIONS {
                    let mut stream = stream;
                    let _ = write_http(
                        &mut stream,
                        "503 Service Unavailable",
                        "text/plain; charset=utf-8",
                        "too many concurrent WebUI connections",
                    );
                    continue;
                }
                state.web_http_connections.fetch_add(1, Ordering::AcqRel);
                let state = Arc::clone(&state);
                thread::spawn(move || {
                    handle_web_connection(stream, Arc::clone(&state));
                    state.web_http_connections.fetch_sub(1, Ordering::AcqRel);
                });
            }
            Err(error) => eprintln!("grok-bridge server: WebUI accept failed: {error}"),
        }
    }
}

fn forbid_web_api(stream: &mut TcpStream) {
    // Fixed wording — never echo tokens or distinguish wrong vs missing.
    // Clients should re-open the Runtime bootstrap URL (`grok-bridge server ui`).
    let _ = write_http(
        stream,
        "403 Forbidden",
        "text/plain; charset=utf-8",
        "forbidden",
    );
}

/// After validating bootstrap `?c=`, set HttpOnly cookie and redirect to a clean URL
/// so the secret never stays in the address bar / Referer for subsequent loads.
fn write_capability_bootstrap_redirect(
    stream: &mut TcpStream,
    capability: &str,
    location: &str,
) -> std::io::Result<()> {
    // SameSite=Strict + HttpOnly: JS cannot read; not sent cross-site.
    // No Secure flag: loopback HTTP bootstrap (127.0.0.1) has no TLS.
    let cookie =
        format!("{WEBUI_CAPABILITY_COOKIE}={capability}; Path=/; HttpOnly; SameSite=Strict");
    write!(
        stream,
        "HTTP/1.1 302 Found\r\n\
         Location: {location}\r\n\
         Set-Cookie: {cookie}\r\n\
         Cache-Control: no-store\r\n\
         Content-Length: 0\r\n\
         Connection: close\r\n\
         X-Content-Type-Options: nosniff\r\n\r\n"
    )
}

fn handle_web_connection(mut stream: TcpStream, state: Arc<RuntimeState>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    // Direct test callers do not pass through run_web_ui; keep this idempotent.
    let _ = stream.set_write_timeout(Some(WEB_HTTP_WRITE_TIMEOUT));
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
    // Bootstrap exchange: first document GET with matching ?c= sets cookie and
    // redirects to a scrubbed URL (reload / Duplicate Tab reuse the cookie).
    if request.method == "GET"
        && matches!(request.path.as_str(), "/" | "/index.html")
        && let Some(bootstrap) = request.capability_query.as_deref()
    {
        if web_capability_matches(&state.web_capability, Some(bootstrap)) {
            let _ = write_capability_bootstrap_redirect(&mut stream, bootstrap, "/");
            return;
        }
        forbid_web_api(&mut stream);
        return;
    }
    if request.method == "GET" && request.path == "/api/events" {
        handle_events_websocket(stream, state, request);
        return;
    }
    // Static assets need no capability (bootstrap HTML/JS loads first).
    // Responses never embed the capability secret.
    if request.method == "GET"
        && let Some(asset) = static_web_asset(&request.path)
    {
        let _ = write_http_bytes(&mut stream, "200 OK", asset.content_type, asset.body);
        return;
    }
    // All /api/* routes require the per-Runtime capability (header, query, or cookie).
    if request.path.starts_with("/api/")
        && !web_capability_matches(&state.web_capability, request.capability.as_deref())
    {
        forbid_web_api(&mut stream);
        return;
    }
    let method = request.method.as_str();
    let path = request.path.as_str();
    match (method, path) {
        ("GET", "/api/sessions") => match state.host.list_web_board() {
            Ok(sessions) => {
                let (status, body) = bounded_http_json_response(&sessions);
                let _ = write_http_bytes(&mut stream, status, "application/json", &body);
            }
            Err(error) => {
                let body = serde_json::to_vec(&serde_json::json!({
                    "error": {
                        "code": "internal",
                        "message": format!("{error:#}"),
                    }
                }))
                .unwrap_or_else(|_| {
                    br#"{"error":{"code":"internal","message":"failed to encode error"}}"#.to_vec()
                });
                let _ = write_http_bytes(
                    &mut stream,
                    "500 Internal Server Error",
                    "application/json",
                    &body,
                );
            }
        },
        ("GET", "/api/version") => {
            let body = serde_json::to_string(&state.version_checker.status())
                .unwrap_or_else(|_| {
                    r#"{"current":"unknown","update_available":false,"release_url":"https://github.com/luodaoyi/grok-bridge-rs/releases/latest"}"#.to_owned()
                });
            let _ = write_http(&mut stream, "200 OK", "application/json", &body);
        }
        ("POST", path) if path.starts_with("/api/clients/") => {
            let Some(encoded_client) = close_path_segment(path, "/api/clients/") else {
                let _ = write_http(
                    &mut stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    "not found",
                );
                return;
            };
            let client_session_id = match percent_decode_path_segment(encoded_client) {
                Ok(client_session_id) => client_session_id,
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
            if let Err(error) = validate_client_session_id(&client_session_id) {
                let _ = write_http(
                    &mut stream,
                    "400 Bad Request",
                    "text/plain; charset=utf-8",
                    &format!("{error:#}"),
                );
                return;
            }
            match state.host.close_client(&client_session_id) {
                Ok(result) => {
                    let body = serde_json::to_string(&result)
                        .unwrap_or_else(|_| r#"{"matched":0,"closed":0,"failures":[]}"#.to_owned());
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
            match state.host.close(handle) {
                Ok(closed) => {
                    let body = format!(r#"{{"accepted":{closed}}}"#);
                    let _ = write_http(&mut stream, "200 OK", "application/json", &body);
                }
                Err(error) => {
                    let (status, body) = map_http_session_close_error(&error);
                    let _ = write_http(&mut stream, status, "text/plain; charset=utf-8", &body);
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

fn handle_events_websocket(
    mut stream: TcpStream,
    state: Arc<RuntimeState>,
    request: ParsedHttpRequest,
) {
    // Capability before upgrade — unauthenticated peers must not reach claim/write.
    if !web_capability_matches(&state.web_capability, request.capability.as_deref()) {
        forbid_web_api(&mut stream);
        return;
    }
    if let Err(error) = validate_events_websocket_request(&request) {
        let status = if error.starts_with("origin") {
            "403 Forbidden"
        } else {
            "400 Bad Request"
        };
        // Never include capability or secret material in error bodies.
        let _ = write_http(&mut stream, status, "text/plain; charset=utf-8", &error);
        return;
    }
    let key = request
        .sec_websocket_key
        .as_deref()
        .expect("validated websocket key");
    let accept = derive_accept_key(key.as_bytes());
    if write!(
        stream,
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    )
    .is_err()
    {
        return;
    }
    // Host Condvar is the primary sleep; keep socket reads short so a quiet
    // client socket cannot delay event pushes after a revision wake-up.
    let _ = stream.set_read_timeout(Some(Duration::from_millis(1)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(30)));

    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(WEB_EVENTS_MAX_MESSAGE_BYTES);
    config.max_frame_size = Some(WEB_EVENTS_MAX_MESSAGE_BYTES);
    // Eager writes so session events are not stuck in the 128 KiB default buffer.
    config.write_buffer_size = 0;
    config.read_buffer_size = 8 * 1024;
    let mut websocket = WebSocket::from_raw_socket(stream, Role::Server, Some(config));
    let client_id = state.next_web_client_id.fetch_add(1, Ordering::Relaxed);
    let client_identity = request.client_identity.clone();
    let mut identity_generation = 0u64;
    if let Some(identity) = client_identity.as_deref() {
        // Takeover under the side-effect gate so no PTY write/resize is in flight
        // for the previous owner while we revoke its control.
        let attach_result = {
            let _gate = state
                .web_side_effect_gate
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let result = state.web_identities.attach(identity, client_id);
            if let Ok((_, Some(old_id))) = &result {
                let released = state.web_controls.release_client(*old_id);
                let _ = state
                    .host
                    .release_web_control_for_connection(*old_id, &released);
            }
            result
        };
        match attach_result {
            Ok((generation, _)) => {
                identity_generation = generation;
            }
            Err(error) => {
                let _ = send_web_events_command_result(
                    &mut websocket,
                    "input_result",
                    None,
                    None,
                    false,
                    Some("invalid_client_identity"),
                    Some(&error),
                );
                let _ = websocket.close(None);
                return;
            }
        }
    }
    run_events_websocket(
        &mut websocket,
        &state,
        client_id,
        client_identity,
        identity_generation,
    );
}

struct PendingWebInput {
    inflight: crate::session::SessionWriteInFlight,
    request_id: String,
    session: String,
    fingerprint: String,
}

struct WebSocketClientState {
    id: u64,
    identity: Option<String>,
    identity_generation: u64,
    /// Explicit subscription set. Empty by default: no terminal bytes until
    /// the client sends terminal_subscribe for visible sessions only.
    subscriptions: HashSet<String>,
    /// Highest applied terminal_subscribe generation (latest-wins).
    subscribe_generation: u64,
    /// Completed command payloads keyed by request id, with insertion order
    /// tracked separately so eviction is deterministic oldest-first.
    completed_commands: HashMap<String, (String, String)>,
    completed_command_order: VecDeque<String>,
    pending_commands: HashSet<String>,
    pending_inputs: Vec<PendingWebInput>,
}

impl WebSocketClientState {
    fn new(id: u64, identity: Option<String>, identity_generation: u64) -> Self {
        Self {
            id,
            identity,
            identity_generation,
            subscriptions: HashSet::new(),
            subscribe_generation: 0,
            completed_commands: HashMap::new(),
            completed_command_order: VecDeque::new(),
            pending_commands: HashSet::new(),
            pending_inputs: Vec::new(),
        }
    }

    /// Persist a completed command result *before* the ack write so reconnect
    /// retries never re-apply a side effect after a successful PTY write.
    fn remember_command(
        &mut self,
        id: &str,
        fingerprint: String,
        payload: String,
        identity_cache: &WebIdentityRegistry,
    ) {
        if let Some(identity) = self.identity.as_deref() {
            // Local client cache path: no reservation token (completed only).
            identity_cache.remember(identity, id, fingerprint.clone(), payload.clone(), None);
        }
        self.remember_local_command(id, fingerprint, payload);
    }

    fn remember_local_command(&mut self, id: &str, fingerprint: String, payload: String) {
        if self.completed_commands.contains_key(id) {
            self.completed_commands
                .insert(id.to_owned(), (fingerprint, payload));
            return;
        }
        while self.completed_command_order.len() >= WEB_EVENTS_REQUEST_CACHE_CAPACITY {
            if let Some(oldest) = self.completed_command_order.pop_front() {
                self.completed_commands.remove(&oldest);
            } else {
                break;
            }
        }
        self.completed_command_order.push_back(id.to_owned());
        self.completed_commands
            .insert(id.to_owned(), (fingerprint, payload));
        while self
            .completed_commands
            .iter()
            .map(|(id, (fingerprint, payload))| id.len() + fingerprint.len() + payload.len())
            .sum::<usize>()
            > WEB_EVENTS_REQUEST_CACHE_BYTES
        {
            if let Some(oldest) = self.completed_command_order.pop_front() {
                self.completed_commands.remove(&oldest);
            } else {
                break;
            }
        }
    }

    /// Lookup completed result for fingerprint. `Err` is id_conflict.
    fn lookup_completed(
        &self,
        id: &str,
        fingerprint: &str,
        identity_cache: &WebIdentityRegistry,
    ) -> Result<Option<String>, String> {
        if let Some((cached_fp, payload)) = self.completed_commands.get(id) {
            if cached_fp != fingerprint {
                return Err(
                    "request id was already used with a different command payload".to_owned(),
                );
            }
            return Ok(Some(payload.clone()));
        }
        if let Some(identity) = self.identity.as_deref() {
            return identity_cache.lookup(identity, id, fingerprint);
        }
        Ok(None)
    }
}

fn release_web_client(state: &RuntimeState, client: &WebSocketClientState) {
    let released = state.web_controls.release_client(client.id);
    let _ = state
        .host
        .release_web_control_for_connection(client.id, &released);
    state
        .web_identities
        .detach(client.identity.as_deref(), client.id);
}

fn run_events_websocket(
    websocket: &mut WebSocket<TcpStream>,
    state: &RuntimeState,
    client_id: u64,
    identity: Option<String>,
    identity_generation: u64,
) {
    let mut client = WebSocketClientState::new(client_id, identity, identity_generation);
    let mut cursors = HashMap::new();
    let mut event_continuation = WebEventsContinuation::default();
    let mut seen_revision = state.host.revision();
    // A revision that started a multi-batch continuation is committed only
    // after its final frame. Appends during the reset then remain observable.
    let mut continuation_revision: Option<u64> = None;
    let mut last_server_ping_ms = now_millis();
    // Initial frame: session metadata only (empty subscriptions → no PTY bytes).
    let Some(more_pending) = send_web_events(
        websocket,
        state,
        &mut cursors,
        &mut event_continuation,
        Some(&client.subscriptions),
        true,
    ) else {
        release_web_client(state, &client);
        return;
    };
    let mut continuation_pending = more_pending;

    while !state.stopping.load(Ordering::Acquire) {
        // Identity takeover by another tab: stop accepting commands immediately.
        if let Some(identity) = client.identity.as_deref()
            && !state
                .web_identities
                .is_current(identity, client.id, client.identity_generation)
        {
            let _ = send_web_events_command_result(
                websocket,
                "input_result",
                None,
                None,
                false,
                Some("identity_revoked"),
                Some("client identity was taken over by another connection"),
            );
            release_web_client(state, &client);
            let _ = websocket.close(None);
            return;
        }

        // Service inbound commands before sleeping so
        // interactive keystrokes never wait behind a multi-second idle timeout.
        match poll_websocket_client(websocket, state, &mut client, &mut cursors) {
            WsClientAction::Continue {
                refresh,
                reset_sessions,
            } => {
                if refresh {
                    event_continuation.reset_sessions(&reset_sessions);
                    let Some(more_pending) = send_web_events(
                        websocket,
                        state,
                        &mut cursors,
                        &mut event_continuation,
                        Some(&client.subscriptions),
                        false,
                    ) else {
                        release_web_client(state, &client);
                        return;
                    };
                    continuation_pending = more_pending;
                }
            }
            WsClientAction::Close => {
                release_web_client(state, &client);
                return;
            }
        }

        // Server Ping keeps the TCP/WebSocket half-open path healthy. Control
        // leases are **not** refreshed here — only subscribed sessions via
        // client_heartbeat / input / resize (see WebControlRegistry::heartbeat).
        let now = now_millis();
        if now.saturating_sub(last_server_ping_ms) >= WEB_EVENTS_SERVER_PING_MS {
            if websocket
                .send(Message::Ping(Vec::new().into()))
                .and_then(|()| websocket.flush())
                .is_err()
            {
                release_web_client(state, &client);
                return;
            }
            last_server_ping_ms = now;
        }

        let lease_deadline = state
            .host
            .next_client_lifecycle_deadline_ms()
            .ok()
            .flatten();
        // Sleep until the next host revision *or* the next pure-time lease
        // transition, but never longer than WEB_EVENTS_CLIENT_POLL so client
        // frames stay low-latency. Timeouts without a revision/lease signal
        // never push frames.
        let wait = match lease_deadline {
            Some(deadline) if deadline > now => Duration::from_millis(deadline - now)
                .min(Duration::from_secs(30))
                .max(Duration::from_millis(1))
                .min(WEB_EVENTS_CLIENT_POLL),
            Some(_) => Duration::from_millis(1),
            None => WEB_EVENTS_CLIENT_POLL,
        };
        let current = state.host.wait_revision(seen_revision, wait);
        match poll_websocket_client(websocket, state, &mut client, &mut cursors) {
            WsClientAction::Continue {
                refresh,
                reset_sessions,
            } => {
                if refresh {
                    event_continuation.reset_sessions(&reset_sessions);
                    let Some(more_pending) = send_web_events(
                        websocket,
                        state,
                        &mut cursors,
                        &mut event_continuation,
                        Some(&client.subscriptions),
                        false,
                    ) else {
                        release_web_client(state, &client);
                        return;
                    };
                    continuation_pending = more_pending;
                }
            }
            WsClientAction::Close => {
                release_web_client(state, &client);
                return;
            }
        }
        if state.stopping.load(Ordering::Acquire) {
            break;
        }
        let now_after = now_millis();
        let lease_due = lease_deadline.is_some_and(|deadline| now_after >= deadline);
        if current == seen_revision && !lease_due && !continuation_pending {
            continue;
        }
        if !continuation_pending && current != seen_revision {
            continuation_revision = Some(current);
        }
        let Some(more_pending) = send_web_events(
            websocket,
            state,
            &mut cursors,
            &mut event_continuation,
            Some(&client.subscriptions),
            false,
        ) else {
            release_web_client(state, &client);
            return;
        };
        continuation_pending = more_pending;
        if !continuation_pending && let Some(revision) = continuation_revision.take() {
            seen_revision = revision;
        }
    }

    release_web_client(state, &client);
    let _ = websocket.close(None);
}

/// Plan frames from immutable cursors, send each frame, and commit cursor
/// advances only after that frame is successfully written. Oversize/encode
/// failures never advance cursors and never silently drop committed bytes.
fn send_web_events(
    websocket: &mut WebSocket<TcpStream>,
    state: &RuntimeState,
    cursors: &mut HashMap<String, u64>,
    continuation: &mut WebEventsContinuation,
    subscriptions: Option<&HashSet<String>>,
    force_reset: bool,
) -> Option<bool> {
    let batch_deadline = std::time::Instant::now() + WEB_EVENTS_BATCH_WRITE_TIMEOUT;
    let mut batch_frames = 0usize;
    let mut batch_bytes = 0usize;
    let plan = match state.host.plan_web_events_batch_with_budget(
        cursors,
        force_reset,
        WEB_EVENTS_MAX_MESSAGE_BYTES,
        subscriptions,
        WEB_EVENTS_MAX_BATCH_FRAMES,
        WEB_EVENTS_MAX_BATCH_BYTES,
        batch_deadline,
        continuation,
    ) {
        Ok(frames) => frames,
        Err(error) => {
            // Permanent plan failure (e.g. sessions metadata oversize): drop the
            // client rather than leave it connected forever without frames.
            eprintln!("grok-bridge server: WebUI events plan failed: {error:#}");
            let code = web_events_plan_error_code(&error).unwrap_or("events_plan_failed");
            let _ = send_web_events_command_result(
                websocket,
                "input_result",
                None,
                None,
                false,
                Some(code),
                Some(&format!("web events plan failed: {error:#}")),
            );
            return None;
        }
    };
    let frames = plan.frames;

    for frame in frames {
        let payload = match serde_json::to_string(&frame.message) {
            Ok(payload) => payload,
            Err(error) => {
                eprintln!("grok-bridge server: WebUI events encode failed: {error}");
                // Do not commit any remaining planned cursors; close so the client
                // reconnects instead of stalling without frames.
                return None;
            }
        };
        if payload.len() > WEB_EVENTS_MAX_MESSAGE_BYTES {
            eprintln!(
                "grok-bridge server: WebUI events frame exceeds {} bytes; closing client",
                WEB_EVENTS_MAX_MESSAGE_BYTES
            );
            return None;
        }
        batch_frames += 1;
        batch_bytes = batch_bytes.saturating_add(payload.len());
        if !web_events_batch_within_budget(batch_frames, batch_bytes, batch_deadline) {
            return None;
        }
        let remaining = batch_deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero()
            || websocket
                .get_mut()
                .set_write_timeout(Some(remaining))
                .is_err()
        {
            return None;
        }
        if websocket
            .send(Message::text(payload))
            .and_then(|()| websocket.flush())
            .is_err()
        {
            return None;
        }
        for (session, cursor) in frame.cursor_commits {
            cursors.insert(session, cursor);
        }
        for session in frame.cursor_drops {
            cursors.remove(&session);
        }
        for reset_commit in frame.reset_commits {
            continuation.commit_reset(reset_commit);
        }
    }
    let _ = websocket
        .get_mut()
        .set_write_timeout(Some(WEB_HTTP_WRITE_TIMEOUT));
    Some(plan.more_pending)
}

enum WsClientAction {
    Continue {
        refresh: bool,
        reset_sessions: HashSet<String>,
    },
    Close,
}

fn poll_websocket_client(
    websocket: &mut WebSocket<TcpStream>,
    state: &RuntimeState,
    client: &mut WebSocketClientState,
    cursors: &mut HashMap<String, u64>,
) -> WsClientAction {
    let mut refresh = false;
    let mut reset_sessions = HashSet::new();
    // Drain a bounded number of control/application frames so a noisy client
    // cannot pin this connection thread forever.
    for _ in 0..32 {
        if flush_pending_web_inputs(websocket, state, client).is_err() {
            return WsClientAction::Close;
        }
        match websocket.read() {
            Ok(Message::Ping(payload)) => {
                // Transport keepalive only — do not refresh control leases.
                if websocket.send(Message::Pong(payload)).is_err() {
                    return WsClientAction::Close;
                }
            }
            Ok(Message::Pong(_)) => {
                // Browser auto-Pong to server Ping: connection liveness only.
            }
            Ok(Message::Close(_)) => {
                let _ = websocket.close(None);
                return WsClientAction::Close;
            }
            Ok(Message::Text(text)) => {
                // Application frames do not bulk-refresh every owned control.
                // Leases renew only for still-subscribed sessions (heartbeat)
                // or the session being written/resized (owns).
                match handle_web_events_client_text(
                    websocket,
                    state,
                    client,
                    cursors,
                    text.as_str(),
                ) {
                    Ok(needs_refresh) => {
                        refresh |= needs_refresh;
                        if needs_refresh
                            && let Ok(Some(WebEventsClientCommand::Resync { session, .. })) =
                                parse_web_events_client_command(text.as_str())
                        {
                            reset_sessions.insert(session);
                        }
                    }
                    Err(()) => return WsClientAction::Close,
                }
            }
            // Binary frames are not part of the JSON command protocol.
            Ok(Message::Binary(_)) | Ok(Message::Frame(_)) => {}
            Err(tungstenite::Error::Io(error))
                if error.kind() == ErrorKind::WouldBlock || error.kind() == ErrorKind::TimedOut =>
            {
                return WsClientAction::Continue {
                    refresh,
                    reset_sessions,
                };
            }
            Err(tungstenite::Error::ConnectionClosed)
            | Err(tungstenite::Error::AlreadyClosed)
            | Err(tungstenite::Error::Protocol(_)) => {
                return WsClientAction::Close;
            }
            Err(_) => return WsClientAction::Close,
        }
    }
    WsClientAction::Continue {
        refresh,
        reset_sessions,
    }
}

/// Client → server command on `/api/events` (JSON text only).
#[derive(Clone, Debug, PartialEq, Eq)]
enum WebEventsClientCommand {
    Subscribe {
        id: Option<String>,
        sessions: Vec<String>,
        /// Monotonic client generation for latest-wins under reordering.
        generation: Option<u64>,
    },
    Claim {
        id: Option<String>,
        session: String,
    },
    Release {
        id: Option<String>,
        session: String,
    },
    Input {
        id: Option<String>,
        session: String,
        data_base64: String,
    },
    Resize {
        id: Option<String>,
        session: String,
        cols: u16,
        rows: u16,
    },
    /// Application-level keepalive for **subscribed** control leases only.
    Heartbeat {
        id: Option<String>,
    },
    /// Force the next events frame to re-emit an ANSI snapshot for one session
    /// without unsubscribing (avoids control-release races of remove→add).
    Resync {
        id: Option<String>,
        session: String,
    },
}

/// Stable fingerprint of command semantics for idempotent replay safety.
fn command_fingerprint(command: &WebEventsClientCommand) -> String {
    match command {
        WebEventsClientCommand::Subscribe {
            sessions,
            generation,
            ..
        } => {
            let gen_label = generation
                .map(|value| value.to_string())
                .unwrap_or_default();
            format!("subscribe:{gen_label}:{}", sessions.join(","))
        }
        WebEventsClientCommand::Claim { session, .. } => format!("claim:{session}"),
        WebEventsClientCommand::Release { session, .. } => format!("release:{session}"),
        WebEventsClientCommand::Input {
            session,
            data_base64,
            ..
        } => format!("input:{session}:{}", fixed_payload_digest(data_base64)),
        WebEventsClientCommand::Resize {
            session,
            cols,
            rows,
            ..
        } => format!("resize:{session}:{cols}:{rows}"),
        WebEventsClientCommand::Heartbeat { .. } => "heartbeat".to_owned(),
        WebEventsClientCommand::Resync { session, .. } => format!("resync:{session}"),
    }
}

fn fixed_payload_digest(value: &str) -> String {
    let mut first = std::collections::hash_map::DefaultHasher::new();
    0x8f3d_2c17_4a91_b605_u64.hash(&mut first);
    value.hash(&mut first);
    let mut second = std::collections::hash_map::DefaultHasher::new();
    0x51a7_e9c3_d842_6b0f_u64.hash(&mut second);
    value.hash(&mut second);
    format!("{:016x}{:016x}", first.finish(), second.finish())
}

#[derive(Debug)]
struct WebCommandError {
    code: &'static str,
    message: String,
}

impl WebCommandError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Parse a single WebUI client command without panicking on junk.
/// Unknown types yield `Ok(None)` (ignored). Malformed known types yield `Err`.
fn parse_web_events_client_command(text: &str) -> Result<Option<WebEventsClientCommand>, String> {
    if text.len() > WEB_EVENTS_MAX_MESSAGE_BYTES {
        return Err("message exceeds size limit".to_owned());
    }
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|error| format!("invalid JSON: {error}"))?;
    let Some(object) = value.as_object() else {
        return Err("message must be a JSON object".to_owned());
    };
    let message_type = object
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let id = match object.get("id") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(value)) => {
            if value.is_empty() || value.len() > WEB_EVENTS_MAX_REQUEST_ID_BYTES {
                return Err("request id is too long".to_owned());
            }
            Some(value.clone())
        }
        Some(_) => return Err("request id must be a string".to_owned()),
    };

    match message_type {
        "terminal_subscribe" => {
            let values = object
                .get("sessions")
                .and_then(|value| value.as_array())
                .ok_or_else(|| "sessions is required and must be an array".to_owned())?;
            if values.len() > WEB_EVENTS_MAX_SUBSCRIPTIONS {
                return Err("too many terminal subscriptions".to_owned());
            }
            let mut sessions = Vec::with_capacity(values.len());
            for value in values {
                let session = value
                    .as_str()
                    .ok_or_else(|| "subscription session must be a string".to_owned())?
                    .to_owned();
                validate_session_handle(&session).map_err(|error| format!("{error:#}"))?;
                if !sessions.iter().any(|current| current == &session) {
                    sessions.push(session);
                }
            }
            let generation = match object.get("generation") {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::Number(n)) => n
                    .as_u64()
                    .ok_or_else(|| "generation must be a non-negative integer".to_owned())
                    .map(Some)?,
                Some(_) => return Err("generation must be a non-negative integer".to_owned()),
            };
            Ok(Some(WebEventsClientCommand::Subscribe {
                id,
                sessions,
                generation,
            }))
        }
        "terminal_claim" => Ok(Some(WebEventsClientCommand::Claim {
            id,
            session: parse_web_events_session(object)?,
        })),
        "terminal_release" => Ok(Some(WebEventsClientCommand::Release {
            id,
            session: parse_web_events_session(object)?,
        })),
        "terminal_input" => {
            let session = parse_web_events_session(object)?;
            let data_base64 = object
                .get("data_base64")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_owned();
            if data_base64.is_empty() {
                return Err("data_base64 is required".to_owned());
            }
            Ok(Some(WebEventsClientCommand::Input {
                id,
                session,
                data_base64,
            }))
        }
        "terminal_resize" => {
            let session = parse_web_events_session(object)?;
            let cols = object
                .get("cols")
                .and_then(|value| value.as_u64())
                .ok_or_else(|| "cols is required".to_owned())?;
            let rows = object
                .get("rows")
                .and_then(|value| value.as_u64())
                .ok_or_else(|| "rows is required".to_owned())?;
            let cols = u16::try_from(cols).map_err(|_| "cols out of range".to_owned())?;
            let rows = u16::try_from(rows).map_err(|_| "rows out of range".to_owned())?;
            Ok(Some(WebEventsClientCommand::Resize {
                id,
                session,
                cols,
                rows,
            }))
        }
        "client_heartbeat" => Ok(Some(WebEventsClientCommand::Heartbeat { id })),
        "terminal_resync" => Ok(Some(WebEventsClientCommand::Resync {
            id,
            session: parse_web_events_session(object)?,
        })),
        // Unknown / push-only types (e.g. future clients): ignore safely.
        _ => Ok(None),
    }
}

fn parse_web_events_session(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, String> {
    let session = object
        .get("session")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_owned();
    validate_session_handle(&session).map_err(|error| format!("{error:#}"))?;
    Ok(session)
}

fn web_command_host_error(operation: &'static str, error: anyhow::Error) -> WebCommandError {
    let message = format!("{error:#}");
    let code = if message.contains("session not found") {
        "session_not_found"
    } else if message.contains("queue is full") {
        // Pre-enqueue: safe to retry with the same id after abort.
        "flow_control"
    } else if operation == "input"
        && (message.contains("write")
            || message.contains("flush")
            || message.contains("partial")
            || message.contains("writer closed")
            || message.contains("broken pipe")
            || message.contains("channel is closed")
            || message.contains("not writable")
            || message.contains("not safe to retry"))
    {
        // Post-enqueue write/flush errors may have delivered some bytes.
        // Terminal for this request id — cache and replay; never re-write.
        "write_failed"
    } else if operation == "input" {
        "input_rejected"
    } else {
        "resize_rejected"
    };
    let message = if code == "write_failed" && !message.contains("not safe to retry") {
        format!("{message}; PTY write may have partially applied and is not safe to retry")
    } else {
        message
    };
    WebCommandError::new(code, message)
}

/// Ensure this connection still owns the identity generation (if any).
fn ensure_identity_current(
    state: &RuntimeState,
    client: &WebSocketClientState,
) -> Result<(), WebCommandError> {
    if let Some(identity) = client.identity.as_deref()
        && !state
            .web_identities
            .is_current(identity, client.id, client.identity_generation)
    {
        return Err(WebCommandError::new(
            "identity_revoked",
            "client identity was taken over by another connection",
        ));
    }
    Ok(())
}

/// Commands that mutate shared control/PTY state. Admission (and for non-input
/// commands the full apply) runs under `web_side_effect_gate`. PTY input only
/// holds the gate through enqueue; write_all+flush waits outside so other
/// sessions are not stalled. Identity pending/Join covers the remember gap.
fn command_needs_side_effect_gate(command: &WebEventsClientCommand) -> bool {
    matches!(
        command,
        WebEventsClientCommand::Claim { .. }
            | WebEventsClientCommand::Release { .. }
            | WebEventsClientCommand::Input { .. }
            | WebEventsClientCommand::Resize { .. }
    )
}

/// Admit control + enqueue one terminal_input WriteJob under the caller's gate.
/// Caller must drop the global side-effect gate before `wait` on the handle.
fn submit_web_events_input(
    state: &RuntimeState,
    client: &WebSocketClientState,
    command: &WebEventsClientCommand,
) -> Result<crate::session::SessionWriteInFlight, WebCommandError> {
    let WebEventsClientCommand::Input {
        session,
        data_base64,
        ..
    } = command
    else {
        return Err(WebCommandError::new(
            "internal_error",
            "submit_web_events_input requires terminal_input",
        ));
    };
    let data = decode_write_data(data_base64)
        .map_err(|error| WebCommandError::new("invalid_input", format!("{error:#}")))?;
    ensure_identity_current(state, client)?;
    if !state
        .web_controls
        .owns(session, client.id)
        .map_err(|error| WebCommandError::new("internal_error", format!("{error:#}")))?
    {
        return Err(WebCommandError::new(
            "control_required",
            "claim terminal control before sending input",
        ));
    }
    // owns refreshed the registry lease; mirror SessionHost control lease
    // (same as the synchronous Input path in apply_web_events_client_command_body).
    let _ = state.host.refresh_web_control(session, client.id);
    state
        .host
        .begin_write_raw(session, data)
        .map_err(|error| web_command_host_error("input", error))
}

fn lock_side_effect_gate(
    state: &RuntimeState,
) -> Result<std::sync::MutexGuard<'_, ()>, WebCommandError> {
    state
        .web_side_effect_gate
        .lock()
        .map_err(|_| WebCommandError::new("internal_error", "side-effect gate poisoned"))
}

/// Test helper: apply with side-effect gate held for the apply body only.
/// Production holds the gate across apply+remember in `handle_web_events_client_text`.
#[cfg(test)]
fn apply_web_events_client_command(
    state: &RuntimeState,
    client: &mut WebSocketClientState,
    command: &WebEventsClientCommand,
) -> Result<bool, WebCommandError> {
    let _gate = if command_needs_side_effect_gate(command) {
        Some(lock_side_effect_gate(state)?)
    } else {
        None
    };
    apply_web_events_client_command_body(state, client, command)
}

/// Body of apply. Side-effect commands require the caller to already hold
/// `web_side_effect_gate` (or to have taken it via `apply_web_events_client_command`).
fn apply_web_events_client_command_body(
    state: &RuntimeState,
    client: &mut WebSocketClientState,
    command: &WebEventsClientCommand,
) -> Result<bool, WebCommandError> {
    match command {
        WebEventsClientCommand::Subscribe {
            sessions,
            generation,
            ..
        } => {
            // Latest-wins: a delayed older generation must not roll subscriptions
            // back after a newer set was already applied.
            if let Some(subscribe_gen) = *generation {
                if subscribe_gen < client.subscribe_generation {
                    return Ok(false);
                }
                client.subscribe_generation = subscribe_gen;
            }
            let next = sessions.iter().cloned().collect::<HashSet<_>>();
            let changed = client.subscriptions != next;
            // Drop control for sessions that left the visible set **before**
            // replacing the set, so orphan reaping can resume immediately.
            let removed: Vec<String> = client.subscriptions.difference(&next).cloned().collect();
            if !removed.is_empty() {
                let released = state.web_controls.release_sessions(client.id, &removed);
                if !released.is_empty() {
                    let _ = state
                        .host
                        .release_web_control_for_connection(client.id, &released);
                }
            }
            client.subscriptions = next;
            Ok(changed)
        }
        WebEventsClientCommand::Claim { session, .. } => {
            ensure_identity_current(state, client)?;
            // Control only for terminals this connection still watches.
            if !client.subscriptions.contains(session) {
                return Err(WebCommandError::new(
                    "control_required",
                    "subscribe to the terminal before claiming control",
                ));
            }
            state
                .host
                .show(session)
                .map_err(|error| web_command_host_error("claim", error))?;
            if !state
                .web_controls
                .claim(session, client.id)
                .map_err(|error| WebCommandError::new("internal_error", format!("{error:#}")))?
            {
                return Err(WebCommandError::new(
                    "control_busy",
                    "terminal control is held by another WebUI client",
                ));
            }
            // Mirror onto Session so orphan reaper honors interactive hold.
            // If mirror fails, roll back *our* registry owner only — release is
            // no-op when another connection already took over.
            if let Err(error) = state.host.acquire_web_control(session, client.id) {
                let _ = state.web_controls.release(session, client.id);
                return Err(WebCommandError::new("internal_error", format!("{error:#}")));
            }
            Ok(false)
        }
        WebEventsClientCommand::Release { session, .. } => {
            ensure_identity_current(state, client)?;
            let released = state
                .web_controls
                .release(session, client.id)
                .map_err(|error| WebCommandError::new("internal_error", format!("{error:#}")))?;
            if !released {
                return Err(WebCommandError::new(
                    "control_not_owned",
                    "terminal control is not held by this WebUI client",
                ));
            }
            let _ = state.host.release_web_control(session, client.id);
            Ok(false)
        }
        WebEventsClientCommand::Input {
            session,
            data_base64,
            ..
        } => {
            let data = decode_write_data(data_base64)
                .map_err(|error| WebCommandError::new("invalid_input", format!("{error:#}")))?;
            // Identity + control admission and PTY enqueue share one gate with
            // attach/takeover so a revoked connection cannot write after steal.
            ensure_identity_current(state, client)?;
            if !state
                .web_controls
                .owns(session, client.id)
                .map_err(|error| WebCommandError::new("internal_error", format!("{error:#}")))?
            {
                return Err(WebCommandError::new(
                    "control_required",
                    "claim terminal control before sending input",
                ));
            }
            // owns refreshed registry lease; mirror Session control lease.
            let _ = state.host.refresh_web_control(session, client.id);
            state
                .host
                .write_raw(session, data)
                .map(|_| false)
                .map_err(|error| web_command_host_error("input", error))
        }
        WebEventsClientCommand::Resize {
            session,
            cols,
            rows,
            ..
        } => {
            validate_terminal_size(*cols, *rows)
                .map_err(|error| WebCommandError::new("invalid_resize", format!("{error:#}")))?;
            ensure_identity_current(state, client)?;
            if !state
                .web_controls
                .owns(session, client.id)
                .map_err(|error| WebCommandError::new("internal_error", format!("{error:#}")))?
            {
                return Err(WebCommandError::new(
                    "control_required",
                    "claim terminal control before resizing",
                ));
            }
            let _ = state.host.refresh_web_control(session, client.id);
            state
                .host
                .resize(session, *cols, *rows)
                .map(|_| false)
                .map_err(|error| web_command_host_error("resize", error))
        }
        WebEventsClientCommand::Heartbeat { .. } => {
            // Renew only controls for terminals still in this connection's
            // subscription set (visible). Hidden sessions age out.
            let held = state
                .web_controls
                .heartbeat(client.id, &client.subscriptions);
            for session in held {
                let _ = state.host.refresh_web_control(&session, client.id);
            }
            Ok(false)
        }
        WebEventsClientCommand::Resync { session, .. } => {
            // Force a full ANSI snapshot on the next events frame without
            // unsubscribing (so control is not released). Cursor drop happens
            // in handle_web_events_client_text when needs_resync is true.
            if !client.subscriptions.contains(session) {
                return Err(WebCommandError::new(
                    "session_not_found",
                    "terminal is not subscribed on this connection",
                ));
            }
            Ok(true)
        }
    }
}

fn web_events_result_type(command: &WebEventsClientCommand) -> &'static str {
    match command {
        WebEventsClientCommand::Subscribe { .. } => "terminal_subscribe_result",
        WebEventsClientCommand::Claim { .. } => "terminal_claim_result",
        WebEventsClientCommand::Release { .. } => "terminal_release_result",
        WebEventsClientCommand::Input { .. } => "input_result",
        WebEventsClientCommand::Resize { .. } => "resize_result",
        WebEventsClientCommand::Heartbeat { .. } => "client_heartbeat_result",
        WebEventsClientCommand::Resync { .. } => "terminal_resync_result",
    }
}

fn web_events_command_session(command: &WebEventsClientCommand) -> Option<&str> {
    match command {
        WebEventsClientCommand::Subscribe { .. } | WebEventsClientCommand::Heartbeat { .. } => None,
        WebEventsClientCommand::Claim { session, .. }
        | WebEventsClientCommand::Release { session, .. }
        | WebEventsClientCommand::Input { session, .. }
        | WebEventsClientCommand::Resize { session, .. }
        | WebEventsClientCommand::Resync { session, .. } => Some(session.as_str()),
    }
}

fn web_events_command_id(command: &WebEventsClientCommand) -> Option<&str> {
    match command {
        WebEventsClientCommand::Subscribe { id, .. }
        | WebEventsClientCommand::Claim { id, .. }
        | WebEventsClientCommand::Release { id, .. }
        | WebEventsClientCommand::Input { id, .. }
        | WebEventsClientCommand::Resize { id, .. }
        | WebEventsClientCommand::Heartbeat { id, .. }
        | WebEventsClientCommand::Resync { id, .. } => id.as_deref(),
    }
}

/// claim/release/input/resize mutate shared control/PTY state and need a stable
/// client identity + request id so reconnects stay idempotent.
fn command_requires_identity_and_id(command: &WebEventsClientCommand) -> bool {
    matches!(
        command,
        WebEventsClientCommand::Claim { .. }
            | WebEventsClientCommand::Release { .. }
            | WebEventsClientCommand::Input { .. }
            | WebEventsClientCommand::Resize { .. }
    )
}

/// Best-effort result type for parse failures so clients can match by type.
fn peek_web_events_result_type(text: &str) -> &'static str {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return "input_result";
    };
    match value.get("type").and_then(|v| v.as_str()) {
        Some("terminal_subscribe") => "terminal_subscribe_result",
        Some("terminal_claim") => "terminal_claim_result",
        Some("terminal_release") => "terminal_release_result",
        Some("terminal_input") => "input_result",
        Some("terminal_resize") => "resize_result",
        Some("client_heartbeat") => "client_heartbeat_result",
        Some("terminal_resync") => "terminal_resync_result",
        _ => "input_result",
    }
}

fn peek_web_events_request_id(text: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    value
        .get("id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty() && s.len() <= WEB_EVENTS_MAX_REQUEST_ID_BYTES)
        .map(str::to_owned)
}

/// Map SessionHost::close failures to accurate HTTP status lines + body text.
fn map_http_session_close_error(error: &anyhow::Error) -> (&'static str, String) {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("session not found") {
        ("404 Not Found", message)
    } else if lower.contains("deadline")
        || lower.contains("did not terminate")
        || lower.contains("still has live members")
        || lower.contains("pty output did not close")
        || lower.contains("terminal session state before the close deadline")
    {
        ("504 Gateway Timeout", message)
    } else if lower.contains("conflict")
        || lower.contains("already closed")
        || lower.contains("busy")
        || lower.contains("in progress")
    {
        ("409 Conflict", message)
    } else {
        ("500 Internal Server Error", message)
    }
}

/// Build a result envelope that never collides with `{ type: "sessions" }` push frames.
fn build_web_events_command_result(
    result_type: &str,
    id: Option<&str>,
    session: Option<&str>,
    ok: bool,
    error_code: Option<&str>,
    error: Option<&str>,
) -> String {
    let mut map = serde_json::Map::new();
    map.insert(
        "type".to_owned(),
        serde_json::Value::String(result_type.to_owned()),
    );
    map.insert("ok".to_owned(), serde_json::Value::Bool(ok));
    if let Some(id) = id {
        map.insert("id".to_owned(), serde_json::Value::String(id.to_owned()));
    }
    if let Some(session) = session {
        map.insert(
            "session".to_owned(),
            serde_json::Value::String(session.to_owned()),
        );
    }
    map.insert(
        "error_code".to_owned(),
        error_code
            .map(|value| serde_json::Value::String(value.to_owned()))
            .unwrap_or(serde_json::Value::Null),
    );
    map.insert(
        "error".to_owned(),
        error
            .map(|value| serde_json::Value::String(value.to_owned()))
            .unwrap_or(serde_json::Value::Null),
    );
    serde_json::Value::Object(map).to_string()
}

fn send_web_events_command_payload(
    websocket: &mut WebSocket<TcpStream>,
    payload: String,
) -> Result<(), ()> {
    if payload.len() > WEB_EVENTS_MAX_MESSAGE_BYTES {
        return Err(());
    }
    websocket
        .send(Message::text(payload))
        .and_then(|()| websocket.flush())
        .map_err(|_| ())
}

fn send_web_events_command_result(
    websocket: &mut WebSocket<TcpStream>,
    result_type: &str,
    id: Option<&str>,
    session: Option<&str>,
    ok: bool,
    error_code: Option<&str>,
    error: Option<&str>,
) -> Result<(), ()> {
    send_web_events_command_payload(
        websocket,
        build_web_events_command_result(result_type, id, session, ok, error_code, error),
    )
}

fn flush_pending_web_inputs(
    websocket: &mut WebSocket<TcpStream>,
    _state: &RuntimeState,
    client: &mut WebSocketClientState,
) -> Result<(), ()> {
    let mut index = 0;
    while index < client.pending_inputs.len() {
        let Some(outcome) = client.pending_inputs[index].inflight.poll() else {
            index += 1;
            continue;
        };
        let pending = client.pending_inputs.swap_remove(index);
        let ok = outcome.is_ok();
        let error_code = outcome
            .as_ref()
            .err()
            .map(|message| write_completion_error_code(message));
        let payload = write_completion_payload(
            "input_result",
            &pending.request_id,
            Some(&pending.session),
            outcome,
        );
        client.pending_commands.remove(&pending.request_id);
        if result_is_cacheable(ok, error_code) {
            // The completion observer already published the authoritative
            // cross-connection result with its reservation token. Keep only a
            // same-socket replay copy here so a timeout cannot be overwritten
            // from indeterminate to write_failed by this display-layer poller.
            client.remember_local_command(
                &pending.request_id,
                pending.fingerprint,
                payload.clone(),
            );
        }
        send_web_events_command_payload(websocket, payload)?;
    }
    Ok(())
}

fn handle_web_events_client_text(
    websocket: &mut WebSocket<TcpStream>,
    state: &RuntimeState,
    client: &mut WebSocketClientState,
    cursors: &mut HashMap<String, u64>,
    text: &str,
) -> Result<bool, ()> {
    let command = match parse_web_events_client_command(text) {
        Ok(None) => return Ok(false),
        Ok(Some(command)) => command,
        Err(error) => {
            // Malformed input never touches the PTY; type/id when peekable.
            let result_type = peek_web_events_result_type(text);
            let id = peek_web_events_request_id(text);
            send_web_events_command_result(
                websocket,
                result_type,
                id.as_deref(),
                None,
                false,
                Some("invalid_request"),
                Some(&error),
            )?;
            return Ok(false);
        }
    };

    let result_type = web_events_result_type(&command);
    let session = web_events_command_session(&command);
    let request_id = web_events_command_id(&command).map(str::to_owned);

    // Reject commands from a superseded connection after identity takeover.
    if let Some(identity) = client.identity.as_deref()
        && !state
            .web_identities
            .is_current(identity, client.id, client.identity_generation)
    {
        send_web_events_command_result(
            websocket,
            result_type,
            request_id.as_deref(),
            session,
            false,
            Some("identity_revoked"),
            Some("client identity was taken over by another connection"),
        )?;
        return Err(());
    }

    // Side-effect commands need a stable identity + request id for cross-reconnect
    // idempotency. Anonymous / id-less peers cannot claim or write the PTY.
    if command_requires_identity_and_id(&command) {
        if client.identity.is_none() {
            send_web_events_command_result(
                websocket,
                result_type,
                request_id.as_deref(),
                session,
                false,
                Some("identity_required"),
                Some("client identity is required for this command"),
            )?;
            return Ok(false);
        }
        if request_id.is_none() {
            send_web_events_command_result(
                websocket,
                result_type,
                None,
                session,
                false,
                Some("request_id_required"),
                Some("request id is required for this command"),
            )?;
            return Ok(false);
        }
    }

    let fingerprint = command_fingerprint(&command);
    // RAII: on every exit after reserve, publish terminal state (abort if not
    // yet enqueued; cached write_failed if bound). Prevents Dropped+re-write.
    let mut identity_guard: Option<IdentityReservationGuard> = None;
    if let Some(id) = request_id.as_deref() {
        // Same-connection completed/local replay first.
        match client.lookup_completed(id, &fingerprint, &state.web_identities) {
            Ok(Some(payload)) => {
                send_web_events_command_payload(websocket, payload)?;
                return Ok(false);
            }
            Ok(None) => {}
            Err(message) => {
                send_web_events_command_result(
                    websocket,
                    result_type,
                    Some(id),
                    session,
                    false,
                    Some("id_conflict"),
                    Some(&message),
                )?;
                return Ok(false);
            }
        }
        if client.pending_commands.contains(id) {
            send_web_events_command_result(
                websocket,
                result_type,
                Some(id),
                session,
                false,
                Some("duplicate_request"),
                Some("request is already being processed"),
            )?;
            return Ok(false);
        }
        // Cross-connection: reserve before apply so a takeover in the
        // apply→remember gap cannot treat the cache as empty and re-apply.
        if let Some(identity) = client.identity.as_deref() {
            match state
                .web_identities
                .begin_command(identity, client.id, id, &fingerprint)
            {
                Ok(IdentityCommandBegin::Replay(payload)) => {
                    send_web_events_command_payload(websocket, payload)?;
                    return Ok(false);
                }
                Ok(IdentityCommandBegin::Join(_completion)) => {
                    // Never wait synchronously here: this function owns the
                    // WebSocket event loop, so waiting for a slow PTY write
                    // would also stop heartbeat, ping/pong and takeover frames.
                    // The caller may retry the same id; once the first command
                    // publishes, begin_command returns Replay without reapply.
                    send_web_events_command_result(
                        websocket,
                        result_type,
                        Some(id),
                        session,
                        false,
                        Some("in_progress"),
                        Some("request is already being processed; retry with the same request id"),
                    )?;
                    return Ok(false);
                }
                Ok(IdentityCommandBegin::Reserved { token }) => {
                    identity_guard = Some(IdentityReservationGuard::new(
                        &state.web_identities,
                        identity,
                        id,
                        &fingerprint,
                        result_type,
                        session,
                        token,
                    ));
                }
                Err(error) => {
                    // Typed mapping: capacity must be flow_control (retryable),
                    // never id_conflict (permanent fingerprint poison).
                    send_web_events_command_result(
                        websocket,
                        result_type,
                        Some(id),
                        session,
                        false,
                        Some(error.error_code()),
                        Some(error.message()),
                    )?;
                    // No reservation was taken; do not remember or poison the id.
                    return Ok(false);
                }
            }
        }
        if client.pending_commands.len() >= WEB_EVENTS_MAX_PENDING_COMMANDS {
            if let Some(guard) = identity_guard.as_mut() {
                let payload = build_web_events_command_result(
                    result_type,
                    Some(id),
                    session,
                    false,
                    Some("flow_control"),
                    Some("too many pending WebUI commands"),
                );
                guard.finish_uncached(payload.clone());
                send_web_events_command_payload(websocket, payload)?;
            } else {
                send_web_events_command_result(
                    websocket,
                    result_type,
                    Some(id),
                    session,
                    false,
                    Some("flow_control"),
                    Some("too many pending WebUI commands"),
                )?;
            }
            return Ok(false);
        }
        client.pending_commands.insert(id.to_owned());
    }

    // Side-effect gate serializes admission (identity/control/enqueue) with
    // attach/takeover. PTY write_all+flush must NOT hold the global gate: a
    // slow writer would stall every other session's claim/input/resize.
    // Identity pending (and Join waiters) keep the request bound until the
    // real write outcome is published via remember / RAII guard.
    let side_effect = command_needs_side_effect_gate(&command);
    let is_pty_input = matches!(command, WebEventsClientCommand::Input { .. });

    let outcome = if is_pty_input {
        // Short gate: admit + enqueue only.
        let submitted = {
            let gate = match lock_side_effect_gate(state) {
                Ok(guard) => guard,
                Err(error) => {
                    if let Some(id) = request_id.as_deref() {
                        client.pending_commands.remove(id);
                    }
                    if let Some(guard) = identity_guard.as_mut() {
                        let payload = build_web_events_command_result(
                            result_type,
                            request_id.as_deref(),
                            session,
                            false,
                            Some(error.code),
                            Some(&error.message),
                        );
                        // Gate failure is pre-enqueue → uncached abort path.
                        guard.finish_uncached(payload.clone());
                        let _ = send_web_events_command_payload(websocket, payload);
                    } else {
                        let _ = send_web_events_command_result(
                            websocket,
                            result_type,
                            request_id.as_deref(),
                            session,
                            false,
                            Some(error.code),
                            Some(&error.message),
                        );
                    }
                    return Ok(false);
                }
            };
            let submitted = submit_web_events_input(state, client, &command);
            drop(gate);
            submitted
        };
        match submitted {
            Ok(inflight) => {
                // Job is on the writer queue: same id must never re-enqueue.
                if let Some(guard) = identity_guard.take() {
                    guard.bind_write_completion(&inflight);
                }
                client.pending_inputs.push(PendingWebInput {
                    inflight,
                    request_id: request_id
                        .clone()
                        .expect("validated terminal_input request id"),
                    session: session
                        .map(str::to_owned)
                        .expect("terminal_input always has a session"),
                    fingerprint,
                });
                // Ack is emitted only after the writer completion becomes ready.
                // Returning keeps this socket able to read heartbeats/commands
                // and push events while the PTY writer is blocked.
                return Ok(false);
            }
            // Pre-enqueue (queue full / control / …): keep unbound so abort/retry.
            Err(error) => Err(error),
        }
    } else {
        let gate = if side_effect {
            match lock_side_effect_gate(state) {
                Ok(guard) => Some(guard),
                Err(error) => {
                    if let Some(id) = request_id.as_deref() {
                        client.pending_commands.remove(id);
                    }
                    if let Some(g) = identity_guard.as_mut() {
                        let payload = build_web_events_command_result(
                            result_type,
                            request_id.as_deref(),
                            session,
                            false,
                            Some(error.code),
                            Some(&error.message),
                        );
                        g.finish_uncached(payload.clone());
                        let _ = send_web_events_command_payload(websocket, payload);
                    } else {
                        let _ = send_web_events_command_result(
                            websocket,
                            result_type,
                            request_id.as_deref(),
                            session,
                            false,
                            Some(error.code),
                            Some(&error.message),
                        );
                    }
                    return Ok(false);
                }
            }
        } else {
            None
        };
        // Non-input side effects (resize/claim) apply under the gate; mark bound
        // once body starts so Drop cannot abort a half-applied mutation.
        if side_effect && let Some(guard) = identity_guard.as_mut() {
            guard.mark_bound();
        }
        let outcome = apply_web_events_client_command_body(state, client, &command);
        drop(gate);
        // Admission failures (control_required / flow_control) never mutated PTY:
        // unbind so same id can retry after abort.
        if let Err(ref err) = outcome
            && !result_is_cacheable(false, Some(err.code))
            && let Some(guard) = identity_guard.as_mut()
        {
            guard.mark_unbound();
        }
        outcome
    };

    let (ok, error_code, error, needs_resync) = match outcome {
        Ok(needs_resync) => (true, None, None, needs_resync),
        Err(error) => (false, Some(error.code), Some(error.message), false),
    };
    if needs_resync {
        // Subscribe changes: drop cursors for unsubscribed sessions.
        cursors.retain(|session, _| client.subscriptions.contains(session));
        // terminal_resync: drop one session cursor so the next plan emits reset
        // without a remove→add subscription (control lease stays intact).
        if let WebEventsClientCommand::Resync { session, .. } = &command {
            cursors.remove(session);
        }
    }
    let payload = build_web_events_command_result(
        result_type,
        request_id.as_deref(),
        session,
        ok,
        error_code,
        error.as_deref(),
    );
    // Terminal publish before ack write. Cacheable (incl. write_failed) →
    // remember. Transient pre-enqueue → uncached so same id may retry.
    if let Some(id) = request_id.as_deref() {
        client.pending_commands.remove(id);
        if result_is_cacheable(ok, error_code) {
            // remember_command publishes identity terminal + local cache.
            if let Some(guard) = identity_guard.as_mut() {
                guard.mark_finished();
            }
            client.remember_command(id, fingerprint, payload.clone(), &state.web_identities);
        } else if let Some(guard) = identity_guard.as_mut() {
            guard.finish_uncached(payload.clone());
        }
    }
    let sent = send_web_events_command_payload(websocket, payload).is_ok();
    if sent { Ok(needs_resync) } else { Err(()) }
}

/// Results that may be replayed on same id + fingerprint without changing semantics.
fn result_is_cacheable(ok: bool, error_code: Option<&str>) -> bool {
    if ok {
        return true;
    }
    match error_code {
        // Transient pre-enqueue / admission failures: same id may re-attempt.
        // identity_revoked is connection-local after takeover — caching it would
        // poison the shared identity cache so the new owner replays a stale failure.
        Some("flow_control")
        | Some("control_busy")
        | Some("control_required")
        | Some("control_not_owned")
        | Some("duplicate_request")
        | Some("identity_revoked")
        | Some("identity_required")
        | Some("request_id_required") => false,
        // write_failed: job was enqueued; bytes may already be on the wire.
        // Cache as terminal so same id only Replays and never re-writes.
        Some("write_failed") => true,
        // Other applied / permanent failures: replay.
        _ => true,
    }
}

fn validate_events_websocket_request(request: &ParsedHttpRequest) -> Result<(), String> {
    if request.method != "GET" {
        return Err("WebSocket upgrade requires GET".to_owned());
    }
    if request.path != "/api/events" {
        return Err("WebSocket path must be /api/events".to_owned());
    }
    if !request.upgrade_websocket || !request.connection_upgrade {
        return Err("missing WebSocket upgrade headers".to_owned());
    }
    if request.sec_websocket_version.as_deref() != Some("13") {
        return Err("unsupported Sec-WebSocket-Version".to_owned());
    }
    let key = request
        .sec_websocket_key
        .as_deref()
        .ok_or_else(|| "missing Sec-WebSocket-Key".to_owned())?;
    if !sec_websocket_key_valid(key) {
        return Err("invalid Sec-WebSocket-Key".to_owned());
    }
    if !web_origin_allowed(request.origin.as_deref(), request.host.as_deref()) {
        return Err("origin not allowed".to_owned());
    }
    Ok(())
}

/// RFC 6455: Sec-WebSocket-Key is a base64-encoded value that decodes to 16 bytes.
fn sec_websocket_key_valid(key: &str) -> bool {
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
    let key = key.trim();
    if key.is_empty() {
        return false;
    }
    match BASE64.decode(key) {
        Ok(bytes) => bytes.len() == 16,
        Err(_) => false,
    }
}

/// Same-origin browser Origin on a loopback Host only.
fn web_origin_allowed(origin: Option<&str>, host: Option<&str>) -> bool {
    let Some(origin) = origin.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    let Some(host) = host.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    let Some(origin_authority) = http_origin_authority(origin) else {
        return false;
    };
    if !authority_is_loopback(&origin_authority) || !authority_is_loopback(host) {
        return false;
    }
    authority_eq(&origin_authority, host)
}

fn http_origin_authority(origin: &str) -> Option<String> {
    let rest = origin.strip_prefix("http://")?;
    if rest.is_empty() || rest.contains('/') || rest.contains('?') || rest.contains('#') {
        return None;
    }
    Some(rest.to_owned())
}

fn authority_is_loopback(authority: &str) -> bool {
    let host = authority_host(authority);
    matches!(host, "127.0.0.1" | "localhost" | "::1" | "[::1]")
}

fn authority_host(authority: &str) -> &str {
    if authority.starts_with('[') {
        if let Some(end) = authority.find(']') {
            return &authority[..=end];
        }
        return authority;
    }
    if let Some((host, port)) = authority.rsplit_once(':')
        && !host.is_empty()
        && port.chars().all(|ch| ch.is_ascii_digit())
    {
        return host;
    }
    authority
}

fn authority_eq(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
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

#[derive(Clone)]
struct ParsedHttpRequest {
    method: String,
    path: String,
    /// Marker header only (not a secret). Capability authorizes the call.
    #[allow(dead_code)]
    bridge_header: bool,
    host: Option<String>,
    origin: Option<String>,
    upgrade_websocket: bool,
    connection_upgrade: bool,
    sec_websocket_key: Option<String>,
    sec_websocket_version: Option<String>,
    /// Stable WebUI client identity from query (`?client=`) or header.
    client_identity: Option<String>,
    /// Capability from query only — used for bootstrap cookie exchange.
    capability_query: Option<String>,
    /// Effective capability: query, header, or HttpOnly bootstrap cookie.
    capability: Option<String>,
}

// capability_query is set by read_http_request; test helpers use None.

/// Read one HTTP line with a hard byte cap (including CRLF). Rejects before
/// unbounded `String` growth when the peer never sends a newline.
fn read_http_line_bounded(
    reader: &mut impl BufRead,
    max_bytes: usize,
) -> std::result::Result<String, String> {
    let mut buf = Vec::new();
    loop {
        if buf.len() > max_bytes {
            return Err("HTTP line exceeds size limit".to_owned());
        }
        let mut byte = [0u8; 1];
        let n = std::io::Read::read(reader, &mut byte).map_err(|error| error.to_string())?;
        if n == 0 {
            break;
        }
        buf.push(byte[0]);
        if byte[0] == b'\n' {
            break;
        }
        if buf.len() > max_bytes {
            return Err("HTTP line exceeds size limit".to_owned());
        }
    }
    String::from_utf8(buf).map_err(|_| "HTTP line is not valid UTF-8".to_owned())
}

fn read_http_request(stream: &mut TcpStream) -> std::result::Result<ParsedHttpRequest, String> {
    let mut reader = BufReader::new(stream);
    let line = read_http_line_bounded(&mut reader, WEB_HTTP_MAX_REQUEST_LINE_BYTES)?;
    if line.is_empty() {
        return Err("missing HTTP request line".to_owned());
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().ok_or("missing HTTP method")?.to_owned();
    let raw_path = parts.next().ok_or("missing HTTP path")?.to_owned();
    let (path, query_identity, query_capability) = split_path_query(&raw_path)?;
    let mut bridge_header = false;
    let mut host = None;
    let mut origin = None;
    let mut upgrade_websocket = false;
    let mut connection_upgrade = false;
    let mut sec_websocket_key = None;
    let mut sec_websocket_version = None;
    let mut header_identity = None;
    let mut header_capability = None;
    let mut cookie_header = None;
    let mut header_count = 0usize;
    let mut header_bytes = 0usize;
    loop {
        let line = read_http_line_bounded(&mut reader, WEB_HTTP_MAX_HEADER_LINE_BYTES)?;
        if line == "\r\n" || line == "\n" || line.is_empty() {
            break;
        }
        header_count += 1;
        header_bytes = header_bytes.saturating_add(line.len());
        if header_count > WEB_HTTP_MAX_HEADERS {
            return Err("too many HTTP headers".to_owned());
        }
        if header_bytes > WEB_HTTP_MAX_HEADER_BYTES {
            return Err("HTTP headers exceed size limit".to_owned());
        }
        let header = line.trim_end_matches(['\r', '\n']);
        let Some((name, value)) = header.split_once(':') else {
            continue;
        };
        let name = name.trim();
        let value = value.trim();
        if name.eq_ignore_ascii_case("X-Grok-Bridge-WebUI") && value == "1" {
            // Marker only — not authorization.
            bridge_header = true;
        } else if name.eq_ignore_ascii_case("X-Grok-Bridge-Capability") {
            if value.len() > WEB_UI_CAPABILITY_HEX_LEN + 16 {
                return Err("capability header is too long".to_owned());
            }
            header_capability = Some(value.to_owned());
        } else if name.eq_ignore_ascii_case("Cookie") {
            if value.len() > WEB_HTTP_MAX_HEADER_LINE_BYTES {
                return Err("Cookie header is too long".to_owned());
            }
            cookie_header = Some(value.to_owned());
        } else if name.eq_ignore_ascii_case("Host") {
            host = Some(value.to_owned());
        } else if name.eq_ignore_ascii_case("Origin") {
            origin = Some(value.to_owned());
        } else if name.eq_ignore_ascii_case("Upgrade") && value.eq_ignore_ascii_case("websocket") {
            upgrade_websocket = true;
        } else if name.eq_ignore_ascii_case("Connection") {
            connection_upgrade = value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case("upgrade"));
        } else if name.eq_ignore_ascii_case("Sec-WebSocket-Key") {
            sec_websocket_key = Some(value.to_owned());
        } else if name.eq_ignore_ascii_case("Sec-WebSocket-Version") {
            sec_websocket_version = Some(value.to_owned());
        } else if name.eq_ignore_ascii_case("X-Grok-Bridge-Client-Id") {
            validate_web_client_identity(value)?;
            header_identity = Some(value.to_owned());
        }
    }
    let client_identity = match (query_identity, header_identity) {
        (Some(query), Some(header)) if query != header => {
            return Err("client identity mismatch between query and header".to_owned());
        }
        (Some(identity), _) | (_, Some(identity)) => Some(identity),
        (None, None) => None,
    };
    let cookie_capability = cookie_header
        .as_deref()
        .and_then(parse_webui_capability_cookie);
    let capability = merge_capability_sources(
        query_capability.as_deref(),
        header_capability.as_deref(),
        cookie_capability.as_deref(),
    )?;
    Ok(ParsedHttpRequest {
        method,
        path,
        bridge_header,
        host,
        origin,
        upgrade_websocket,
        connection_upgrade,
        sec_websocket_key,
        sec_websocket_version,
        client_identity,
        capability_query: query_capability,
        capability,
    })
}

/// Parse `grok_bridge_webui_c=<hex>` from a Cookie header value.
fn parse_webui_capability_cookie(cookie_header: &str) -> Option<String> {
    for part in cookie_header.split(';') {
        let part = part.trim();
        let Some(value) = part
            .strip_prefix(WEBUI_CAPABILITY_COOKIE)
            .and_then(|rest| rest.strip_prefix('='))
        else {
            continue;
        };
        let value = value.trim();
        if value.len() == WEB_UI_CAPABILITY_HEX_LEN && value.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Some(value.to_owned());
        }
    }
    None
}

/// Query, header, and cookie must agree when more than one is present.
fn merge_capability_sources(
    query: Option<&str>,
    header: Option<&str>,
    cookie: Option<&str>,
) -> Result<Option<String>, String> {
    let mut chosen: Option<&str> = None;
    for candidate in [query, header, cookie].into_iter().flatten() {
        match chosen {
            None => chosen = Some(candidate),
            Some(existing) if existing == candidate => {}
            Some(_) => {
                return Err("capability mismatch between query, header, and cookie".to_owned());
            }
        }
    }
    Ok(chosen.map(str::to_owned))
}

/// Split path, optional `client=` identity, and optional `c=` / `capability=`.
fn split_path_query(raw_path: &str) -> Result<(String, Option<String>, Option<String>), String> {
    let Some((path, query)) = raw_path.split_once('?') else {
        return Ok((raw_path.to_owned(), None, None));
    };
    let mut identity = None;
    let mut capability = None;
    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        let key = parts.next().unwrap_or("");
        let value = parts.next().unwrap_or("");
        if key == "client" {
            if value.is_empty() {
                return Err("client identity query is empty".to_owned());
            }
            let decoded = percent_decode_path_segment(value)
                .map_err(|_| "client identity query is not valid".to_owned())?;
            validate_web_client_identity(&decoded)?;
            if identity
                .as_ref()
                .is_some_and(|existing| existing != &decoded)
            {
                return Err("duplicate conflicting client identity query".to_owned());
            }
            identity = Some(decoded);
        } else if key == "c" || key == "capability" {
            if value.is_empty() {
                return Err("capability query is empty".to_owned());
            }
            if value.len() > WEB_UI_CAPABILITY_HEX_LEN + 16 {
                return Err("capability query is too long".to_owned());
            }
            let decoded = percent_decode_path_segment(value)
                .map_err(|_| "capability query is not valid".to_owned())?;
            if capability
                .as_ref()
                .is_some_and(|existing| existing != &decoded)
            {
                return Err("duplicate conflicting capability query".to_owned());
            }
            capability = Some(decoded);
        }
    }
    Ok((path.to_owned(), identity, capability))
}

fn write_http(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> std::io::Result<()> {
    write_http_bytes(stream, status, content_type, body.as_bytes())
}

fn bounded_http_json_response<T: serde::Serialize>(value: &T) -> (&'static str, Vec<u8>) {
    match serde_json::to_vec(value) {
        Ok(body) if body.len() <= WEB_HTTP_MAX_JSON_RESPONSE_BYTES => ("200 OK", body),
        Ok(_) => (
            "413 Content Too Large",
            br#"{"error":{"code":"response_too_large","message":"JSON response exceeds the 1 MiB limit"}}"#.to_vec(),
        ),
        Err(_) => (
            "500 Internal Server Error",
            br#"{"error":{"code":"internal","message":"failed to encode JSON response"}}"#.to_vec(),
        ),
    }
}

fn write_http_bytes(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\nX-Content-Type-Options: nosniff\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)
}

struct StaticWebAsset {
    content_type: &'static str,
    body: &'static [u8],
}

const WEB_UI_HTML: &[u8] = include_bytes!("../webui/dist/index.html");
const WEB_UI_JS: &[u8] = include_bytes!("../webui/dist/assets/app.js");
const WEB_UI_CSS: &[u8] = include_bytes!("../webui/dist/assets/app.css");

fn static_web_asset(path: &str) -> Option<StaticWebAsset> {
    match path {
        "/" => Some(StaticWebAsset {
            content_type: "text/html; charset=utf-8",
            body: WEB_UI_HTML,
        }),
        "/assets/app.js" => Some(StaticWebAsset {
            content_type: "text/javascript; charset=utf-8",
            body: WEB_UI_JS,
        }),
        "/assets/app.css" => Some(StaticWebAsset {
            content_type: "text/css; charset=utf-8",
            body: WEB_UI_CSS,
        }),
        _ => None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_rpc_response_falls_back_with_same_request_id() {
        let oversized = ResponseEnvelope::failure(
            "request-large-show",
            "failed",
            "x".repeat(crate::protocol::MAX_FRAME_BYTES),
        );
        assert!(crate::protocol::encode_frame(&oversized).is_err());
        let bounded = bounded_rpc_response(oversized);
        assert_eq!(bounded.id, "request-large-show");
        assert!(!bounded.ok);
        assert_eq!(bounded.error.as_ref().unwrap().code, "response_too_large");
        assert!(crate::protocol::encode_frame(&bounded).is_ok());
    }

    #[test]
    fn write_timeout_replay_and_current_socket_use_one_terminal_payload() {
        let message = "PTY write/flush outcome was not confirmed before the deadline; delivery may be partial and is not safe to retry";
        let current = write_completion_payload(
            "input_result",
            "same-request",
            Some("gbt-1"),
            Err(message.to_owned()),
        );
        let replay = write_completion_payload(
            "input_result",
            "same-request",
            Some("gbt-1"),
            Err(message.to_owned()),
        );
        assert_eq!(current, replay);
        assert!(current.contains("indeterminate"));

        let effect_error = "ordered state commit failed";
        let current = write_completion_payload(
            "input_result",
            "effect-request",
            Some("gbt-1"),
            Err(effect_error.to_owned()),
        );
        let replay = write_completion_payload(
            "input_result",
            "effect-request",
            Some("gbt-1"),
            Err(effect_error.to_owned()),
        );
        assert_eq!(current, replay);
        assert!(current.contains("write_failed"));
    }

    #[test]
    fn web_events_batch_budget_bounds_frames_bytes_and_deadline() {
        let future = std::time::Instant::now() + Duration::from_secs(1);
        assert!(web_events_batch_within_budget(1, 1, future));
        assert!(web_events_batch_within_budget(
            WEB_EVENTS_MAX_BATCH_FRAMES,
            WEB_EVENTS_MAX_BATCH_BYTES,
            future
        ));
        assert!(!web_events_batch_within_budget(
            WEB_EVENTS_MAX_BATCH_FRAMES + 1,
            1,
            future
        ));
        assert!(!web_events_batch_within_budget(
            1,
            WEB_EVENTS_MAX_BATCH_BYTES + 1,
            future
        ));
        assert!(!web_events_batch_within_budget(
            1,
            1,
            std::time::Instant::now() - Duration::from_millis(1)
        ));
    }
    use std::io::Read;

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
    fn serves_only_bundled_webui_distribution_assets() {
        for (path, content_type, expected_body) in [
            ("/", "text/html; charset=utf-8", WEB_UI_HTML),
            (
                "/assets/app.js",
                "text/javascript; charset=utf-8",
                WEB_UI_JS,
            ),
            ("/assets/app.css", "text/css; charset=utf-8", WEB_UI_CSS),
        ] {
            let asset = static_web_asset(path).expect("static route must exist");
            assert_eq!(asset.content_type, content_type);
            assert_eq!(asset.body, expected_body);
            assert!(!asset.body.is_empty());

            let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
            let response = serve_web_request(request.as_bytes());
            let (headers, body) = split_http_response(&response);
            assert!(headers.starts_with("HTTP/1.1 200 OK\r\n"));
            assert!(headers.contains(&format!("Content-Type: {content_type}")));
            assert_eq!(body, expected_body);
        }

        let html = std::str::from_utf8(WEB_UI_HTML).expect("index.html must be UTF-8");
        assert!(html.contains("/assets/app.js"));
        assert!(html.contains("/assets/app.css"));
        assert!(static_web_asset("/api/sessions").is_none());
        assert!(static_web_asset("/assets/missing.js").is_none());
    }

    #[test]
    fn sessions_api_remains_json_instead_of_static_content() {
        let response = serve_web_request_with_capability(&format!(
            "GET /api/sessions HTTP/1.1\r\nHost: localhost\r\nX-Grok-Bridge-Capability: {TEST_WEBUI_CAPABILITY}\r\n\r\n"
        ));
        let (headers, body) = split_http_response(&response);
        assert!(headers.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(headers.contains("Content-Type: application/json"));
        assert_eq!(body, b"[]");
    }

    #[test]
    fn sessions_api_omits_large_terminal_snapshots_and_stays_bounded() {
        let state = test_runtime_state();
        state.host.test_register_idle_session("gbt-heavy-screen");
        state
            .host
            .test_append_output("gbt-heavy-screen", vec![b'Z'; 2 * 1024 * 1024]);
        let request = format!(
            "GET /api/sessions HTTP/1.1\r\nHost: localhost\r\nX-Grok-Bridge-Capability: {TEST_WEBUI_CAPABILITY}\r\n\r\n"
        );
        let response = serve_web_request_state(request.as_bytes(), state);
        let (headers, body) = split_http_response(&response);
        assert!(headers.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(body.len() <= WEB_HTTP_MAX_JSON_RESPONSE_BYTES);
        let value: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(value[0]["session"], "gbt-heavy-screen");
        assert!(value[0]["screen"].is_null());
        assert_eq!(value[0]["screen_ansi_base64"], "");
        assert!(!String::from_utf8_lossy(body).contains(&"Wg==".repeat(1024)));
    }

    #[test]
    fn concurrent_sessions_api_reads_do_not_amplify_terminal_snapshots() {
        let state = Arc::new(test_runtime_state());
        state.host.test_register_idle_session("gbt-http-shared");
        state
            .host
            .test_append_output("gbt-http-shared", vec![b'Q'; 2 * 1024 * 1024]);
        let request = format!(
            "GET /api/sessions HTTP/1.1\r\nHost: localhost\r\nX-Grok-Bridge-Capability: {TEST_WEBUI_CAPABILITY}\r\n\r\n"
        )
        .into_bytes();
        let readers = (0..16)
            .map(|_| {
                let state = Arc::clone(&state);
                let request = request.clone();
                thread::spawn(move || serve_web_request_arc(&request, state))
            })
            .collect::<Vec<_>>();
        for reader in readers {
            let response = reader.join().unwrap();
            let (headers, body) = split_http_response(&response);
            assert!(headers.starts_with("HTTP/1.1 200 OK\r\n"));
            assert!(body.len() <= WEB_HTTP_MAX_JSON_RESPONSE_BYTES);
            assert!(!String::from_utf8_lossy(body).contains(&"UQ==".repeat(1024)));
        }
    }

    #[test]
    fn bounded_http_json_returns_parseable_response_too_large() {
        let oversized = vec!["x".repeat(WEB_HTTP_MAX_JSON_RESPONSE_BYTES); 2];
        let (status, body) = bounded_http_json_response(&oversized);
        assert_eq!(status, "413 Content Too Large");
        assert!(body.len() <= WEB_HTTP_MAX_JSON_RESPONSE_BYTES);
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "response_too_large");
    }

    #[test]
    fn version_api_reports_current_package_version() {
        let response = serve_web_request_with_capability(&format!(
            "GET /api/version HTTP/1.1\r\nHost: localhost\r\nX-Grok-Bridge-Capability: {TEST_WEBUI_CAPABILITY}\r\n\r\n"
        ));
        let (headers, body) = split_http_response(&response);
        assert!(headers.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(headers.contains("Content-Type: application/json"));
        let value: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(value["current"], env!("CARGO_PKG_VERSION"));
        assert_eq!(value["update_available"], false);
        assert!(
            value["release_url"]
                .as_str()
                .unwrap()
                .contains("github.com/luodaoyi/grok-bridge-rs/releases")
        );
    }

    #[test]
    fn byte_http_writer_uses_raw_body_length() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        let body = [0, 0xff, b'\n', 0x80];

        write_http_bytes(&mut server, "200 OK", "application/octet-stream", &body).unwrap();
        drop(server);

        let mut response = Vec::new();
        client.read_to_end(&mut response).unwrap();
        let separator = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap();
        let headers = std::str::from_utf8(&response[..separator]).unwrap();
        assert!(headers.contains("Content-Length: 4"));
        assert_eq!(&response[separator + 4..], body);
    }

    #[test]
    fn close_api_requires_capability_not_marker_header_alone() {
        let response = serve_web_request(
            b"POST /api/sessions/missing/close HTTP/1.1\r\nHost: localhost\r\nX-Grok-Bridge-WebUI: 1\r\n\r\n",
        );
        assert!(response.starts_with(b"HTTP/1.1 403 Forbidden\r\n"));
        assert!(response.ends_with(b"forbidden"));
    }

    #[test]
    fn web_origin_requires_matching_loopback_host() {
        assert!(web_origin_allowed(
            Some("http://127.0.0.1:47653"),
            Some("127.0.0.1:47653")
        ));
        assert!(web_origin_allowed(
            Some("http://localhost:47653"),
            Some("localhost:47653")
        ));
        assert!(!web_origin_allowed(
            Some("http://evil.example:47653"),
            Some("127.0.0.1:47653")
        ));
        assert!(!web_origin_allowed(
            Some("http://127.0.0.1:47653"),
            Some("127.0.0.1:9")
        ));
        assert!(!web_origin_allowed(None, Some("127.0.0.1:47653")));
        assert!(!web_origin_allowed(
            Some("https://127.0.0.1:47653"),
            Some("127.0.0.1:47653")
        ));
    }

    #[test]
    fn events_websocket_handshake_rejects_bad_origin_and_path() {
        let missing_upgrade = ParsedHttpRequest {
            method: "GET".to_owned(),
            path: "/api/events".to_owned(),
            bridge_header: false,
            host: Some("127.0.0.1:47653".to_owned()),
            origin: Some("http://127.0.0.1:47653".to_owned()),
            upgrade_websocket: false,
            connection_upgrade: false,
            sec_websocket_key: Some("dGhlIHNhbXBsZSBub25jZQ==".to_owned()),
            sec_websocket_version: Some("13".to_owned()),
            client_identity: None,
            capability_query: None,
            capability: None,
        };
        assert!(validate_events_websocket_request(&missing_upgrade).is_err());

        let bad_origin = ParsedHttpRequest {
            upgrade_websocket: true,
            connection_upgrade: true,
            origin: Some("http://evil.example".to_owned()),
            ..missing_upgrade.clone()
        };
        assert_eq!(
            validate_events_websocket_request(&bad_origin).unwrap_err(),
            "origin not allowed"
        );

        let bad_path = ParsedHttpRequest {
            path: "/api/sessions".to_owned(),
            upgrade_websocket: true,
            connection_upgrade: true,
            origin: Some("http://127.0.0.1:47653".to_owned()),
            ..missing_upgrade
        };
        assert!(validate_events_websocket_request(&bad_path).is_err());
    }

    #[test]
    fn events_websocket_handshake_accepts_same_origin_loopback() {
        let request = ParsedHttpRequest {
            method: "GET".to_owned(),
            path: "/api/events".to_owned(),
            bridge_header: false,
            host: Some("127.0.0.1:47653".to_owned()),
            origin: Some("http://127.0.0.1:47653".to_owned()),
            upgrade_websocket: true,
            connection_upgrade: true,
            sec_websocket_key: Some("dGhlIHNhbXBsZSBub25jZQ==".to_owned()),
            sec_websocket_version: Some("13".to_owned()),
            client_identity: None,
            capability_query: None,
            capability: Some(TEST_WEBUI_CAPABILITY.to_owned()),
        };
        assert!(validate_events_websocket_request(&request).is_ok());
    }

    #[test]
    fn sec_websocket_key_must_be_rfc6455_16_byte_base64() {
        // RFC 6455 example key decodes to 16 bytes.
        assert!(sec_websocket_key_valid("dGhlIHNhbXBsZSBub25jZQ=="));
        assert!(!sec_websocket_key_valid(""));
        assert!(!sec_websocket_key_valid("   "));
        assert!(!sec_websocket_key_valid("not-base64!!!"));
        // Valid base64 but wrong decoded length (3 bytes).
        assert!(!sec_websocket_key_valid("YWJj"));
        // Valid base64 of 15 bytes.
        assert!(!sec_websocket_key_valid("AAAAAAAAAAAAAAAAAAAA"));
        // Valid base64 of 17 bytes.
        assert!(!sec_websocket_key_valid("AQIDBAUGBwgJCgsMDQ4PEBE="));

        let mut request = ParsedHttpRequest {
            method: "GET".to_owned(),
            path: "/api/events".to_owned(),
            bridge_header: false,
            host: Some("127.0.0.1:47653".to_owned()),
            origin: Some("http://127.0.0.1:47653".to_owned()),
            upgrade_websocket: true,
            connection_upgrade: true,
            sec_websocket_key: Some("YWJj".to_owned()),
            sec_websocket_version: Some("13".to_owned()),
            client_identity: None,
            capability_query: None,
            capability: Some(TEST_WEBUI_CAPABILITY.to_owned()),
        };
        assert_eq!(
            validate_events_websocket_request(&request).unwrap_err(),
            "invalid Sec-WebSocket-Key"
        );
        request.sec_websocket_key = Some("dGhlIHNhbXBsZSBub25jZQ==".to_owned());
        assert!(validate_events_websocket_request(&request).is_ok());
    }

    #[test]
    fn events_api_without_upgrade_stays_http_error() {
        let response = serve_web_request_with_capability(&format!(
            "GET /api/events?c={TEST_WEBUI_CAPABILITY} HTTP/1.1\r\nHost: 127.0.0.1:47653\r\nOrigin: http://127.0.0.1:47653\r\n\r\n"
        ));
        assert!(response.starts_with(b"HTTP/1.1 400 Bad Request\r\n"));
    }

    #[test]
    fn parse_terminal_input_and_resize_commands() {
        let input = parse_web_events_client_command(
            r#"{"type":"terminal_input","id":"r1","session":"gbt-1","data_base64":"YQ=="}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            input,
            WebEventsClientCommand::Input {
                id: Some("r1".to_owned()),
                session: "gbt-1".to_owned(),
                data_base64: "YQ==".to_owned(),
            }
        );

        let resize = parse_web_events_client_command(
            r#"{"type":"terminal_resize","id":"r2","session":"gbt-1","cols":120,"rows":40}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            resize,
            WebEventsClientCommand::Resize {
                id: Some("r2".to_owned()),
                session: "gbt-1".to_owned(),
                cols: 120,
                rows: 40,
            }
        );

        // Unknown types are ignored (push-only / future).
        assert_eq!(
            parse_web_events_client_command(r#"{"type":"sessions","sessions":[]}"#).unwrap(),
            None
        );
    }

    #[test]
    fn input_fingerprint_uses_fixed_digest_instead_of_retaining_base64() {
        let payload = "c2Vuc2l0aXZlLXRlcm1pbmFsLWlucHV0";
        let command = WebEventsClientCommand::Input {
            id: Some("digest-1".to_owned()),
            session: "gbt-1".to_owned(),
            data_base64: payload.to_owned(),
        };
        let first = command_fingerprint(&command);
        let second = command_fingerprint(&command);
        assert_eq!(first, second);
        assert!(!first.contains(payload));
        assert_eq!(first.rsplit(':').next().unwrap().len(), 32);
    }

    #[test]
    fn parse_rejects_malformed_and_oversized_input_without_command() {
        assert!(parse_web_events_client_command("not-json").is_err());
        assert!(
            parse_web_events_client_command(
                r#"{"type":"terminal_input","session":"","data_base64":"YQ=="}"#
            )
            .is_err()
        );
        assert!(
            parse_web_events_client_command(r#"{"type":"terminal_input","session":"gbt-1"}"#)
                .is_err()
        );
        assert!(
            parse_web_events_client_command(
                r#"{"type":"terminal_resize","session":"gbt-1","cols":1,"rows":40}"#
            )
            .is_ok()
        );
        // cols=1 is parsed; apply-time validate_terminal_size rejects it.
        let cmd = parse_web_events_client_command(
            r#"{"type":"terminal_resize","session":"gbt-1","cols":1,"rows":40}"#,
        )
        .unwrap()
        .unwrap();
        let state = test_runtime_state();
        let mut client = WebSocketClientState::new(1, None, 0);
        assert!(apply_web_events_client_command(&state, &mut client, &cmd).is_err());
    }

    #[test]
    fn apply_terminal_input_uses_decode_write_data_and_rejects_unknown_session() {
        let state = test_runtime_state();
        let mut client = WebSocketClientState::new(1, None, 0);
        // Exact raw bytes for "A" (0x41) via standard base64.
        let cmd = WebEventsClientCommand::Input {
            id: Some("n1".to_owned()),
            session: "missing-session".to_owned(),
            data_base64: "QQ==".to_owned(),
        };
        let data = decode_write_data("QQ==").unwrap();
        assert_eq!(data, vec![0x41]);
        // Unknown session: error, never panics, never writes.
        let err = apply_web_events_client_command(&state, &mut client, &cmd).unwrap_err();
        assert!(!err.message.is_empty());

        // Empty / oversized rejected by decode_write_data before host write.
        let empty = WebEventsClientCommand::Input {
            id: None,
            session: "missing-session".to_owned(),
            data_base64: "".to_owned(),
        };
        // Empty base64 fails at parse (required).
        assert!(
            parse_web_events_client_command(
                r#"{"type":"terminal_input","session":"s","data_base64":""}"#
            )
            .is_err()
        );
        let _ = empty;

        use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
        let oversize = BASE64.encode(vec![0x5a; crate::protocol::MAX_WRITE_BYTES + 1]);
        let over = WebEventsClientCommand::Input {
            id: None,
            session: "missing-session".to_owned(),
            data_base64: oversize,
        };
        assert!(apply_web_events_client_command(&state, &mut client, &over).is_err());
    }

    #[test]
    fn command_result_json_is_not_sessions_type() {
        let payload = build_web_events_command_result(
            "input_result",
            Some("r1"),
            Some("gbt-1"),
            false,
            Some("input_rejected"),
            Some("nope"),
        );
        let value: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value["type"], "input_result");
        assert_eq!(value["ok"], false);
        assert_eq!(value["id"], "r1");
        assert_eq!(value["session"], "gbt-1");
        assert_eq!(value["error"], "nope");
        assert!(value.get("sessions").is_none());
        assert!(value.get("terminals").is_none());
    }

    #[test]
    fn client_poll_interval_is_bounded_for_interactive_latency() {
        // Guard against regressions that reintroduce multi-second waits before
        // reading client frames.
        assert!(WEB_EVENTS_CLIENT_POLL <= Duration::from_millis(100));
        assert!(WEB_EVENTS_CLIENT_POLL >= Duration::from_millis(1));
        const {
            assert!(WEB_EVENTS_MAX_PENDING_COMMANDS >= 32);
        }
    }

    #[test]
    fn remember_command_evicts_oldest_first_when_full() {
        let cache = WebIdentityRegistry::default();
        let mut client = WebSocketClientState::new(1, None, 0);
        for i in 0..WEB_EVENTS_REQUEST_CACHE_CAPACITY {
            client.remember_command(
                &format!("id-{i}"),
                format!("fp-{i}"),
                format!("payload-{i}"),
                &cache,
            );
        }
        assert_eq!(
            client.completed_commands.len(),
            WEB_EVENTS_REQUEST_CACHE_CAPACITY
        );
        assert_eq!(
            client.completed_command_order.len(),
            WEB_EVENTS_REQUEST_CACHE_CAPACITY
        );
        assert_eq!(
            client.completed_command_order.front().map(String::as_str),
            Some("id-0")
        );
        let last_id = format!("id-{}", WEB_EVENTS_REQUEST_CACHE_CAPACITY - 1);
        assert_eq!(
            client.completed_command_order.back().map(String::as_str),
            Some(last_id.as_str())
        );

        client.remember_command(
            "id-new",
            "fp-new".to_owned(),
            "payload-new".to_owned(),
            &cache,
        );
        assert_eq!(
            client.completed_commands.len(),
            WEB_EVENTS_REQUEST_CACHE_CAPACITY
        );
        assert!(!client.completed_commands.contains_key("id-0"));
        assert_eq!(
            client
                .completed_commands
                .get("id-new")
                .map(|(_, payload)| payload.as_str()),
            Some("payload-new")
        );
        assert_eq!(
            client.completed_command_order.front().map(String::as_str),
            Some("id-1")
        );
        assert_eq!(
            client.completed_command_order.back().map(String::as_str),
            Some("id-new")
        );
        // Second overflow still drops the next oldest.
        client.remember_command(
            "id-newer",
            "fp-newer".to_owned(),
            "payload-newer".to_owned(),
            &cache,
        );
        assert!(!client.completed_commands.contains_key("id-1"));
        assert!(client.completed_commands.contains_key("id-2"));
        assert_eq!(
            client.completed_command_order.front().map(String::as_str),
            Some("id-2")
        );
    }

    #[test]
    fn remember_command_duplicate_replacement_keeps_order_and_size() {
        let cache = WebIdentityRegistry::default();
        let mut client = WebSocketClientState::new(1, None, 0);
        for i in 0..WEB_EVENTS_REQUEST_CACHE_CAPACITY {
            client.remember_command(
                &format!("id-{i}"),
                format!("fp-{i}"),
                format!("payload-{i}"),
                &cache,
            );
        }
        let order_before: Vec<String> = client.completed_command_order.iter().cloned().collect();
        let mid = WEB_EVENTS_REQUEST_CACHE_CAPACITY / 2;
        let mid_id = format!("id-{mid}");

        client.remember_command(
            &mid_id,
            format!("fp-{mid}"),
            "payload-replaced".to_owned(),
            &cache,
        );

        assert_eq!(
            client.completed_commands.len(),
            WEB_EVENTS_REQUEST_CACHE_CAPACITY
        );
        assert_eq!(
            client.completed_command_order.len(),
            WEB_EVENTS_REQUEST_CACHE_CAPACITY
        );
        assert_eq!(
            client
                .completed_commands
                .get(&mid_id)
                .map(|(_, payload)| payload.as_str()),
            Some("payload-replaced")
        );
        let order_after: Vec<String> = client.completed_command_order.iter().cloned().collect();
        assert_eq!(order_before, order_after);
        // Oldest entry is still present after a mid-cache replace while full.
        assert!(client.completed_commands.contains_key("id-0"));
        assert_eq!(
            client.completed_command_order.front().map(String::as_str),
            Some("id-0")
        );
    }

    #[test]
    fn local_and_identity_command_caches_respect_shared_byte_budgets() {
        let cache = WebIdentityRegistry::default();
        let mut client = WebSocketClientState::new(1, Some("byte-budget-tab".to_owned()), 1);
        cache.attach("byte-budget-tab", 1).unwrap();
        for index in 0..8 {
            let id = format!("large-{index}");
            let fingerprint = format!("fp-{index}");
            let payload = "p".repeat(192 * 1024);
            client.remember_local_command(&id, fingerprint.clone(), payload.clone());
            cache.remember("byte-budget-tab", &id, fingerprint, payload, None);
        }
        let local_bytes: usize = client
            .completed_commands
            .iter()
            .map(|(id, (fingerprint, payload))| id.len() + fingerprint.len() + payload.len())
            .sum();
        assert!(local_bytes <= WEB_EVENTS_REQUEST_CACHE_BYTES);
        let commands = cache.commands.lock().unwrap();
        assert!(
            WebIdentityRegistry::command_cache_bytes(&commands)
                <= WEB_EVENTS_GLOBAL_COMMAND_CACHE_BYTES
        );
    }

    #[test]
    fn parse_rejects_invalid_session_handles_before_host_touch() {
        let long = "x".repeat(200);
        let bad_cases = [
            "",
            "has space",
            "bad/id",
            long.as_str(),
            "ctrl\n",
            "unicode-\u{4e2d}",
        ];
        for bad in bad_cases {
            let payload = format!(
                r#"{{"type":"terminal_input","session":{session},"data_base64":"YQ=="}}"#,
                session = serde_json::to_string(bad).unwrap()
            );
            let err = parse_web_events_client_command(&payload).unwrap_err();
            assert!(
                err.contains("session handle") || err.contains("session"),
                "bad={bad:?} err={err}"
            );
            let resize = format!(
                r#"{{"type":"terminal_resize","session":{session},"cols":80,"rows":24}}"#,
                session = serde_json::to_string(bad).unwrap()
            );
            assert!(parse_web_events_client_command(&resize).is_err());
        }
        // Valid handle shape is accepted by the parser (host still enforces existence).
        assert!(
            parse_web_events_client_command(
                r#"{"type":"terminal_input","session":"gbt-1","data_base64":"YQ=="}"#
            )
            .unwrap()
            .is_some()
        );
    }

    #[test]
    fn client_close_api_reports_an_exact_empty_group() {
        let response = serve_web_request_with_capability(&format!(
            "POST /api/clients/codex-thread-42/close HTTP/1.1\r\nHost: localhost\r\nX-Grok-Bridge-WebUI: 1\r\nX-Grok-Bridge-Capability: {TEST_WEBUI_CAPABILITY}\r\n\r\n"
        ));
        let (headers, body) = split_http_response(&response);
        assert!(headers.starts_with("HTTP/1.1 200 OK\r\n"));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(body).unwrap(),
            serde_json::json!({ "matched": 0, "closed": 0, "failures": [] })
        );
    }

    #[test]
    fn api_rejects_missing_and_wrong_capability() {
        // Fixed marker header alone is not authorization.
        let missing = serve_web_request(
            b"GET /api/sessions HTTP/1.1\r\nHost: localhost\r\nX-Grok-Bridge-WebUI: 1\r\n\r\n",
        );
        assert!(
            missing.starts_with(b"HTTP/1.1 403 Forbidden\r\n"),
            "missing capability must 403"
        );
        assert!(
            !String::from_utf8_lossy(&missing).contains(TEST_WEBUI_CAPABILITY),
            "response must not leak capability"
        );

        let wrong = serve_web_request(
            b"GET /api/sessions HTTP/1.1\r\nHost: localhost\r\nX-Grok-Bridge-Capability: ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\r\n\r\n",
        );
        assert!(wrong.starts_with(b"HTTP/1.1 403 Forbidden\r\n"));
        let body = String::from_utf8_lossy(&wrong);
        assert!(body.contains("forbidden"));
        assert!(!body.contains(TEST_WEBUI_CAPABILITY));
    }

    #[test]
    fn api_accepts_correct_capability_header_and_query() {
        let by_header = serve_web_request_with_capability(&format!(
            "GET /api/sessions HTTP/1.1\r\nHost: localhost\r\nX-Grok-Bridge-Capability: {TEST_WEBUI_CAPABILITY}\r\n\r\n"
        ));
        assert!(
            by_header.starts_with(b"HTTP/1.1 200 OK\r\n"),
            "got {}",
            String::from_utf8_lossy(&by_header[..by_header.len().min(120)])
        );

        let by_query = serve_web_request_with_capability(&format!(
            "GET /api/sessions?c={TEST_WEBUI_CAPABILITY} HTTP/1.1\r\nHost: localhost\r\n\r\n"
        ));
        assert!(by_query.starts_with(b"HTTP/1.1 200 OK\r\n"));

        let version = serve_web_request_with_capability(&format!(
            "GET /api/version HTTP/1.1\r\nHost: localhost\r\nX-Grok-Bridge-Capability: {TEST_WEBUI_CAPABILITY}\r\n\r\n"
        ));
        assert!(version.starts_with(b"HTTP/1.1 200 OK\r\n"));
    }

    #[test]
    fn websocket_upgrade_requires_capability() {
        let no_cap = serve_web_request(
            b"GET /api/events HTTP/1.1\r\nHost: 127.0.0.1:47653\r\nOrigin: http://127.0.0.1:47653\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
        );
        assert!(
            no_cap.starts_with(b"HTTP/1.1 403 Forbidden\r\n"),
            "WS without capability must 403 before upgrade"
        );

        let with_cap = serve_web_request_with_capability(&format!(
            "GET /api/events?c={TEST_WEBUI_CAPABILITY} HTTP/1.1\r\nHost: 127.0.0.1:47653\r\nOrigin: http://127.0.0.1:47653\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
        ));
        assert!(
            with_cap.starts_with(b"HTTP/1.1 101 Switching Protocols\r\n"),
            "got {}",
            String::from_utf8_lossy(&with_cap[..with_cap.len().min(160)])
        );
    }

    #[test]
    fn static_assets_do_not_require_capability_and_do_not_embed_it() {
        let response = serve_web_request(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
        assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
        let text = String::from_utf8_lossy(&response);
        assert!(!text.contains(TEST_WEBUI_CAPABILITY));
        assert!(text.contains("Cache-Control: no-store"));
    }

    #[test]
    fn bootstrap_query_sets_httponly_cookie_and_redirects_without_secret_in_location() {
        let response = serve_web_request_with_capability(&format!(
            "GET /?c={TEST_WEBUI_CAPABILITY} HTTP/1.1\r\nHost: 127.0.0.1:47653\r\n\r\n"
        ));
        let text = String::from_utf8_lossy(&response);
        assert!(
            text.starts_with("HTTP/1.1 302 Found\r\n"),
            "bootstrap must redirect: {}",
            &text[..text.len().min(160)]
        );
        assert!(text.contains("Location: /\r\n"));
        assert!(text.contains(&format!(
            "Set-Cookie: {WEBUI_CAPABILITY_COOKIE}={TEST_WEBUI_CAPABILITY};"
        )));
        assert!(text.contains("HttpOnly"));
        assert!(text.contains("SameSite=Strict"));
        // Location must not re-embed the secret (Referer-safe after redirect).
        assert!(!text.contains(&format!("Location: /?c={TEST_WEBUI_CAPABILITY}")));
    }

    #[test]
    fn api_and_websocket_accept_capability_cookie_like_reload_or_duplicate_tab() {
        // Simulate reload / second tab: no header, no query — only bootstrap cookie.
        let by_cookie = serve_web_request_with_capability(&format!(
            "GET /api/sessions HTTP/1.1\r\nHost: localhost\r\nCookie: {WEBUI_CAPABILITY_COOKIE}={TEST_WEBUI_CAPABILITY}\r\n\r\n"
        ));
        assert!(
            by_cookie.starts_with(b"HTTP/1.1 200 OK\r\n"),
            "cookie auth must work for reload: {}",
            String::from_utf8_lossy(&by_cookie[..by_cookie.len().min(120)])
        );

        // Second "tab" WS upgrade with the same cookie only.
        let ws_cookie = serve_web_request_with_capability(&format!(
            "GET /api/events HTTP/1.1\r\nHost: 127.0.0.1:47653\r\nOrigin: http://127.0.0.1:47653\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nCookie: {WEBUI_CAPABILITY_COOKIE}={TEST_WEBUI_CAPABILITY}\r\n\r\n"
        ));
        assert!(
            ws_cookie.starts_with(b"HTTP/1.1 101 Switching Protocols\r\n"),
            "WS cookie auth (duplicate tab): {}",
            String::from_utf8_lossy(&ws_cookie[..ws_cookie.len().min(160)])
        );

        // Wrong cookie after Runtime restart → 403 without leaking the real token.
        let wrong_cookie = serve_web_request_with_capability(&format!(
            "GET /api/sessions HTTP/1.1\r\nHost: localhost\r\nCookie: {WEBUI_CAPABILITY_COOKIE}=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\r\n\r\n"
        ));
        assert!(wrong_cookie.starts_with(b"HTTP/1.1 403 Forbidden\r\n"));
        assert!(!String::from_utf8_lossy(&wrong_cookie).contains(TEST_WEBUI_CAPABILITY));
    }

    #[test]
    fn parse_and_merge_capability_cookie_sources() {
        assert_eq!(
            parse_webui_capability_cookie(&format!(
                "other=1; {WEBUI_CAPABILITY_COOKIE}={TEST_WEBUI_CAPABILITY}; x=y"
            ))
            .as_deref(),
            Some(TEST_WEBUI_CAPABILITY)
        );
        assert!(parse_webui_capability_cookie("x=y").is_none());
        assert_eq!(
            merge_capability_sources(
                Some(TEST_WEBUI_CAPABILITY),
                None,
                Some(TEST_WEBUI_CAPABILITY)
            )
            .unwrap()
            .as_deref(),
            Some(TEST_WEBUI_CAPABILITY)
        );
        assert!(merge_capability_sources(Some("aaa"), Some("bbb"), None).is_err());
    }

    #[test]
    fn capability_compare_is_length_sensitive_and_constant_time_eq_works() {
        let a = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert!(constant_time_eq(a, a));
        assert!(!constant_time_eq(a, &a[..a.len() - 1]));
        assert!(!web_capability_matches(a, Some("not-hex!!!!!!!!!!!!!!!!")));
        assert!(!web_capability_matches(a, None));
        assert!(web_capability_matches(a, Some(a)));
    }

    #[test]
    fn redact_web_url_capability_scrubs_query_secret() {
        let raw = format!("http://127.0.0.1:47653/?c={TEST_WEBUI_CAPABILITY}&x=1");
        let redacted = redact_web_url_capability(&raw);
        assert!(!redacted.contains(TEST_WEBUI_CAPABILITY));
        assert!(redacted.contains("c=***"));
        assert!(redacted.contains("x=1"));
    }

    #[test]
    fn web_bind_refuses_non_loopback_addresses() {
        assert!(web_bind_address_is_loopback("127.0.0.1:47653"));
        assert!(web_bind_address_is_loopback("localhost:47653"));
        assert!(web_bind_address_is_loopback("[::1]:47653"));
        assert!(!web_bind_address_is_loopback("0.0.0.0:47653"));
        assert!(!web_bind_address_is_loopback("192.168.1.10:47653"));
        assert!(!web_bind_address_is_loopback("[::]:8080"));
    }

    #[test]
    fn control_lease_expires_for_half_open_owners() {
        let registry = WebControlRegistry::default();
        assert!(registry.claim("gbt-1", 1).unwrap());
        assert!(!registry.claim("gbt-1", 2).unwrap());
        {
            let mut sessions = registry.sessions.lock().unwrap();
            sessions.get_mut("gbt-1").unwrap().last_heartbeat_ms =
                now_millis().saturating_sub(WEB_CONTROL_LEASE_MS + 1);
        }
        // Expired owner is free for deterministic takeover.
        assert!(registry.claim("gbt-1", 2).unwrap());
        assert!(registry.owns("gbt-1", 2).unwrap());
        assert!(!registry.owns("gbt-1", 1).unwrap());
    }

    #[test]
    fn control_heartbeat_only_refreshes_subscribed_sessions() {
        let registry = WebControlRegistry::default();
        assert!(registry.claim("gbt-visible", 1).unwrap());
        assert!(registry.claim("gbt-hidden", 1).unwrap());
        let subscribed = HashSet::from(["gbt-visible".to_owned()]);
        // Age both, then heartbeat with only visible in the active set.
        {
            let mut sessions = registry.sessions.lock().unwrap();
            let old = now_millis().saturating_sub(WEB_CONTROL_LEASE_MS / 2);
            sessions.get_mut("gbt-visible").unwrap().last_heartbeat_ms = old;
            sessions.get_mut("gbt-hidden").unwrap().last_heartbeat_ms = old;
        }
        let refreshed = registry.heartbeat(1, &subscribed);
        assert_eq!(refreshed.len(), 1);
        assert!(refreshed.contains(&"gbt-visible".to_owned()));
        {
            let sessions = registry.sessions.lock().unwrap();
            let visible = sessions.get("gbt-visible").unwrap().last_heartbeat_ms;
            let hidden = sessions.get("gbt-hidden").unwrap().last_heartbeat_ms;
            assert!(visible > hidden, "only subscribed session is renewed");
        }
        // Age past lease: hidden expires; visible stays if still fresh enough.
        {
            let mut sessions = registry.sessions.lock().unwrap();
            sessions.get_mut("gbt-hidden").unwrap().last_heartbeat_ms =
                now_millis().saturating_sub(WEB_CONTROL_LEASE_MS + 1);
        }
        // Another client can claim the unsubscribed hold after expiry.
        assert!(registry.claim("gbt-hidden", 2).unwrap());
        assert!(!registry.claim("gbt-visible", 2).unwrap());
    }

    #[test]
    fn subscribe_removal_releases_control_immediately() {
        let state = test_runtime_state();
        // Seed a live session the claim path can show.
        // Use host with no real session — claim calls show which fails without session.
        // Test registry release_sessions directly + Subscribe path via apply.
        let registry = WebControlRegistry::default();
        assert!(registry.claim("gbt-a", 7).unwrap());
        assert!(registry.claim("gbt-b", 7).unwrap());
        let released = registry.release_sessions(7, &["gbt-a".to_owned()]);
        assert_eq!(released, vec!["gbt-a".to_owned()]);
        assert!(registry.claim("gbt-a", 8).unwrap());
        assert!(!registry.claim("gbt-b", 8).unwrap());
        let _ = state;
    }

    /// claim succeeded in web_controls but host mirror failed → registry owner
    /// rolled back (no phantom). release is owner-id scoped.
    #[test]
    fn claim_rolls_back_registry_when_host_mirror_fails() {
        let state = test_runtime_state();
        state.host.test_register_idle_session("gbt-claim-1");
        let mut client = WebSocketClientState::new(11, Some("tab-claim-rb".to_owned()), 1);
        state.web_identities.attach("tab-claim-rb", 11).unwrap();
        client.subscriptions.insert("gbt-claim-1".to_owned());
        state.host.test_force_next_acquire_web_control_err();
        let err = apply_web_events_client_command(
            &state,
            &mut client,
            &WebEventsClientCommand::Claim {
                id: Some("claim-1".to_owned()),
                session: "gbt-claim-1".to_owned(),
            },
        )
        .unwrap_err();
        assert_eq!(err.code, "internal_error");
        assert!(
            !state.web_controls.owns("gbt-claim-1", 11).unwrap(),
            "phantom registry owner must be rolled back"
        );
        // Retry works after inject is consumed.
        apply_web_events_client_command(
            &state,
            &mut client,
            &WebEventsClientCommand::Claim {
                id: Some("claim-2".to_owned()),
                session: "gbt-claim-1".to_owned(),
            },
        )
        .unwrap();
        assert!(state.web_controls.owns("gbt-claim-1", 11).unwrap());
    }

    /// Rollback release must not free an owner that already transferred.
    #[test]
    fn claim_rollback_does_not_release_transferred_owner() {
        let registry = WebControlRegistry::default();
        assert!(registry.claim("gbt-xfer", 1).unwrap());
        // Transfer to connection 2 before client 1 would roll back.
        {
            let mut sessions = registry.sessions.lock().unwrap();
            sessions.get_mut("gbt-xfer").unwrap().connection_id = 2;
            sessions.get_mut("gbt-xfer").unwrap().last_heartbeat_ms = now_millis();
        }
        assert!(!registry.release("gbt-xfer", 1).unwrap());
        assert!(registry.owns("gbt-xfer", 2).unwrap());
        assert!(!registry.owns("gbt-xfer", 1).unwrap());
    }

    /// Async submit_web_events_input must mirror SessionHost control lease after
    /// web_controls.owns (same as the sync Input path).
    #[test]
    fn submit_web_events_input_refreshes_session_host_control_lease() {
        let state = test_runtime_state();
        state.host.test_register_idle_session("gbt-async-in");
        let mut client = WebSocketClientState::new(21, Some("tab-async-in".to_owned()), 1);
        state.web_identities.attach("tab-async-in", 21).unwrap();
        client.subscriptions.insert("gbt-async-in".to_owned());
        apply_web_events_client_command(
            &state,
            &mut client,
            &WebEventsClientCommand::Claim {
                id: Some("c-async".to_owned()),
                session: "gbt-async-in".to_owned(),
            },
        )
        .unwrap();
        // Age Session mirror (still within lease so hold is not expired away).
        let aged = now_millis().saturating_sub(WEB_CONTROL_LEASE_MS / 3);
        state
            .host
            .test_set_web_control_last_ms("gbt-async-in", aged);
        let command = WebEventsClientCommand::Input {
            id: Some("in-async".to_owned()),
            session: "gbt-async-in".to_owned(),
            data_base64: "YQ==".to_owned(),
        };
        // Enqueue under gate (production path); lease refresh is the assert.
        let submit = {
            let _gate = lock_side_effect_gate(&state).unwrap();
            submit_web_events_input(&state, &client, &command)
        };
        // begin_write may fail without a live writer; refresh must still run first.
        let _ = submit;
        let last = state.host.test_web_control_last_ms("gbt-async-in");
        assert!(
            last > aged,
            "async input must refresh SessionHost control lease; aged={aged} last_ms={last}"
        );
    }

    #[test]
    fn identity_cache_replays_results_across_connections() {
        let cache = WebIdentityRegistry::default();
        let (gen1, revoked) = cache.attach("client-abc-001", 1).unwrap();
        assert_eq!(gen1, 1);
        assert!(revoked.is_none());
        cache.remember(
            "client-abc-001",
            "req-1",
            "input:gbt-1:YQ==".to_owned(),
            r#"{"type":"input_result","ok":true,"id":"req-1"}"#.to_owned(),
            None,
        );
        // New connection id, same identity: still replayable within TTL.
        let (gen2, revoked) = cache.attach("client-abc-001", 2).unwrap();
        assert_eq!(gen2, 2);
        assert_eq!(revoked, Some(1));
        assert!(!cache.is_current("client-abc-001", 1, gen1));
        assert!(cache.is_current("client-abc-001", 2, gen2));
        let payload = cache
            .lookup("client-abc-001", "req-1", "input:gbt-1:YQ==")
            .unwrap()
            .unwrap();
        assert!(payload.contains("req-1"));
        // Different fingerprint with same id is a hard conflict.
        let conflict = cache.lookup("client-abc-001", "req-1", "input:gbt-1:Yg==");
        assert!(conflict.is_err());
        assert!(validate_web_client_identity("short").is_err());
        assert!(validate_web_client_identity("valid-client-id").is_ok());
        assert!(validate_web_client_identity("bad identity!").is_err());
    }

    #[test]
    fn transient_errors_are_not_cached_for_same_id_retry() {
        let cache = WebIdentityRegistry::default();
        let mut client = WebSocketClientState::new(1, Some("tab-a".to_owned()), 1);
        cache.attach("tab-a", 1).unwrap();
        let fp = "input:gbt-1:YQ==".to_owned();
        // Simulate a flow_control failure that must NOT be remembered.
        assert!(!result_is_cacheable(false, Some("flow_control")));
        assert!(!result_is_cacheable(false, Some("control_required")));
        assert!(!result_is_cacheable(false, Some("control_busy")));
        assert!(!result_is_cacheable(false, Some("identity_revoked")));
        // Post-enqueue write failure is terminal (possible partial delivery).
        assert!(result_is_cacheable(false, Some("write_failed")));
        assert!(result_is_cacheable(true, None));
        assert!(result_is_cacheable(false, Some("input_rejected")));
        // Successful apply is cacheable and replayable.
        client.remember_command(
            "id-1",
            fp.clone(),
            r#"{"type":"input_result","ok":true,"id":"id-1"}"#.to_owned(),
            &cache,
        );
        let replay = client
            .lookup_completed("id-1", &fp, &cache)
            .unwrap()
            .unwrap();
        assert!(replay.contains("\"ok\":true"));
    }

    /// Takeover → old connection gets identity_revoked under the gate. That
    /// failure must abort the reservation and must NOT enter the shared cache,
    /// or the new owner replaying the same id/payload would stuck on revoked.
    #[test]
    fn identity_revoked_aborts_reservation_and_is_not_cached() {
        let state = test_runtime_state();
        let identity = "tab-revoke-no-cache";
        let request_id = "req-after-takeover";
        let fingerprint = "input:gbt-1:YQ==";

        state.web_identities.attach(identity, 1).unwrap();
        match state
            .web_identities
            .begin_command(identity, 1, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Reserved { .. } => {}
            other => panic!("expected Reserved, got {other:?}"),
        }

        // New connection steals identity; old generation is revoked.
        let (_, revoked) = state.web_identities.attach(identity, 2).unwrap();
        assert_eq!(revoked, Some(1));
        state.web_controls.release_client(1);

        let mut old = WebSocketClientState::new(1, Some(identity.to_owned()), 1);
        let input = WebEventsClientCommand::Input {
            id: Some(request_id.to_owned()),
            session: "gbt-1".to_owned(),
            data_base64: "YQ==".to_owned(),
        };
        let err = apply_web_events_client_command(&state, &mut old, &input).unwrap_err();
        assert_eq!(err.code, "identity_revoked");
        assert!(
            !result_is_cacheable(false, Some(err.code)),
            "identity_revoked must be non-cacheable"
        );

        // Production path: non-cacheable → abort reservation (not remember).
        state
            .web_identities
            .abort_command(identity, request_id, None);

        // Shared cache must not hold a stale identity_revoked payload.
        assert!(
            state
                .web_identities
                .lookup(identity, request_id, fingerprint)
                .unwrap()
                .is_none()
        );

        // New current connection can reserve the same id+fingerprint and apply.
        match state
            .web_identities
            .begin_command(identity, 2, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Reserved { .. } => {}
            IdentityCommandBegin::Replay(payload) => {
                panic!("must not replay stale failure: {payload}");
            }
            IdentityCommandBegin::Join(_) => {
                panic!("reservation must have been aborted, not left joinable");
            }
        }

        // Contrast: incorrectly remembering identity_revoked would poison retries.
        let stale = r#"{"type":"input_result","ok":false,"id":"req-after-takeover","error_code":"identity_revoked"}"#;
        // After abort we reserved again — finish that reservation cleanly.
        state
            .web_identities
            .abort_command(identity, request_id, None);
        // If cacheable were true, remember would stick; prove current policy rejects it.
        assert!(!result_is_cacheable(false, Some("identity_revoked")));
        let _ = stale;
    }

    #[test]
    fn identity_bucket_count_is_globally_bounded() {
        let cache = WebIdentityRegistry::default();
        for i in 0..(WEB_EVENTS_MAX_IDENTITY_BUCKETS + 32) {
            let identity = format!("bucket-id-{i:04}");
            // Pad to min identity length.
            let identity = format!("{identity}-pad");
            cache.attach(&identity, i as u64 + 1).unwrap();
            cache.remember(
                &identity,
                "r1",
                "input:gbt-1:YQ==".to_owned(),
                format!(r#"{{"ok":true,"id":"r1","i":{i}}}"#),
                None,
            );
        }
        assert!(
            cache.identity_bucket_count() <= WEB_EVENTS_MAX_IDENTITY_BUCKETS,
            "buckets={}",
            cache.identity_bucket_count()
        );
    }

    #[test]
    fn write_failure_is_cacheable_and_same_id_replays_without_rewrite() {
        // After a real PTY write/flush failure (possible partial delivery) the
        // handle path must remember write_failed as terminal for that id.
        assert!(result_is_cacheable(false, Some("write_failed")));
        assert!(result_is_cacheable(true, None));

        let cache = WebIdentityRegistry::default();
        let identity = "tab-write-fail-xx";
        cache.attach(identity, 1).unwrap();
        match cache
            .begin_command(identity, 1, "w1", "input:gbt-1:YQ==")
            .unwrap()
        {
            IdentityCommandBegin::Reserved { .. } => {}
            other => panic!("{other:?}"),
        }
        let err_payload = r#"{"type":"input_result","ok":false,"id":"w1","error_code":"write_failed","error":"PTY write/flush failed after possible partial delivery (not safe to retry)"}"#
            .to_owned();
        cache.remember(
            identity,
            "w1",
            "input:gbt-1:YQ==".to_owned(),
            err_payload.clone(),
            None,
        );
        match cache
            .begin_command(identity, 1, "w1", "input:gbt-1:YQ==")
            .unwrap()
        {
            IdentityCommandBegin::Replay(p) => assert_eq!(p, err_payload),
            IdentityCommandBegin::Reserved { .. } => {
                panic!("must not re-reserve after write_failed — that re-enqueues")
            }
            IdentityCommandBegin::Join(_) => panic!("must not stay joinable after remember"),
        }
    }

    #[test]
    fn pending_reservation_survives_beyond_former_30s_ttl_without_duplicate_reserve() {
        // Wall-clock purge used to Drop pending after 30s while WriteJob still
        // blocked, allowing the same id to re-reserve and double-write.
        let cache = WebIdentityRegistry::default();
        let identity = "tab-long-pending-xx";
        cache.attach(identity, 1).unwrap();
        match cache
            .begin_command(identity, 1, "slow-1", "input:gbt-1:YQ==")
            .unwrap()
        {
            IdentityCommandBegin::Reserved { .. } => {}
            other => panic!("{other:?}"),
        }
        // In-flight pending is never wall-clock TTL purged (no reserved_at field).
        // Any purge-triggering lookup must still Join — never free the slot.
        match cache
            .begin_command(identity, 1, "slow-1", "input:gbt-1:YQ==")
            .unwrap()
        {
            IdentityCommandBegin::Join(_) => {}
            IdentityCommandBegin::Reserved { .. } => {
                panic!("must not re-reserve after >30s while original job is live")
            }
            IdentityCommandBegin::Replay(_) => panic!("unexpected replay before finish"),
        }
        let payload =
            r#"{"type":"input_result","ok":true,"id":"slow-1","session":"gbt-1"}"#.to_owned();
        cache.remember(
            identity,
            "slow-1",
            "input:gbt-1:YQ==".to_owned(),
            payload.clone(),
            None,
        );
        match cache
            .begin_command(identity, 1, "slow-1", "input:gbt-1:YQ==")
            .unwrap()
        {
            IdentityCommandBegin::Replay(p) => assert_eq!(p, payload),
            other => panic!("expected Replay after long job: {other:?}"),
        }
    }

    #[test]
    fn identity_guard_drop_after_bound_caches_terminal_failure() {
        let cache = WebIdentityRegistry::default();
        let identity = "tab-guard-bound-xx";
        cache.attach(identity, 1).unwrap();
        let token = match cache
            .begin_command(identity, 1, "g1", "input:gbt-1:YQ==")
            .unwrap()
        {
            IdentityCommandBegin::Reserved { token } => token,
            other => panic!("{other:?}"),
        };
        {
            let mut guard = IdentityReservationGuard::new(
                &cache,
                identity,
                "g1",
                "input:gbt-1:YQ==",
                "input_result",
                Some("gbt-1"),
                token,
            );
            guard.mark_bound();
            // Drop without remember → terminal write_failed cached.
        }
        match cache
            .begin_command(identity, 1, "g1", "input:gbt-1:YQ==")
            .unwrap()
        {
            IdentityCommandBegin::Replay(p) => {
                assert!(p.contains("write_failed"), "payload={p}");
                assert!(
                    p.contains("not safe to retry") || p.contains("partial"),
                    "payload={p}"
                );
            }
            other => panic!("bound Drop must cache terminal failure: {other:?}"),
        }
    }

    #[test]
    fn successful_write_ack_is_cached_so_retry_does_not_reapply() {
        // Real success is remembered; same id+fingerprint is Replay without a
        // second host write (double-write protection after lost ack).
        let cache = WebIdentityRegistry::default();
        let identity = "tab-write-ok-xx";
        cache.attach(identity, 1).unwrap();
        match cache
            .begin_command(identity, 1, "ok1", "input:gbt-1:YQ==")
            .unwrap()
        {
            IdentityCommandBegin::Reserved { .. } => {}
            other => panic!("{other:?}"),
        }
        let payload =
            r#"{"type":"input_result","ok":true,"id":"ok1","session":"gbt-1"}"#.to_owned();
        cache.remember(
            identity,
            "ok1",
            "input:gbt-1:YQ==".to_owned(),
            payload.clone(),
            None,
        );
        match cache
            .begin_command(identity, 1, "ok1", "input:gbt-1:YQ==")
            .unwrap()
        {
            IdentityCommandBegin::Replay(p) => assert_eq!(p, payload),
            other => panic!("expected Replay after success cache: {other:?}"),
        }
    }

    #[test]
    fn pending_identity_buckets_are_never_evicted_for_capacity() {
        let cache = WebIdentityRegistry::default();
        // Fill capacity with in-flight (pending-only) reservations.
        for i in 0..WEB_EVENTS_MAX_IDENTITY_BUCKETS {
            let identity = format!("pend-id-{i:04}-xx");
            cache.attach(&identity, i as u64 + 1).unwrap();
            match cache
                .begin_command(&identity, i as u64 + 1, "hold", "input:gbt-1:YQ==")
                .unwrap()
            {
                IdentityCommandBegin::Reserved { .. } => {}
                other => panic!("expected Reserved for {identity}: {other:?}"),
            }
        }
        assert_eq!(
            cache.identity_bucket_count(),
            WEB_EVENTS_MAX_IDENTITY_BUCKETS
        );
        // First identity must still be joinable — not evicted.
        match cache
            .begin_command("pend-id-0000-xx", 1, "hold", "input:gbt-1:YQ==")
            .unwrap()
        {
            IdentityCommandBegin::Join(_) => {}
            other => panic!("expected Join for original reservation: {other:?}"),
        }
        // New identity is refused with typed flow_control (not id_conflict).
        let err = cache
            .begin_command("pend-id-overflow-xx", 9999, "hold", "input:gbt-1:YQ==")
            .unwrap_err();
        assert_eq!(err.error_code(), "flow_control");
        assert!(
            err.message().contains("too many client identities"),
            "err={err:?}"
        );
        assert_eq!(
            cache.identity_bucket_count(),
            WEB_EVENTS_MAX_IDENTITY_BUCKETS
        );
    }

    /// End-to-end handler: capacity exhaustion must emit `error_code=flow_control`
    /// on the terminal result frame (not `id_conflict`), leave the request id
    /// uncached, and keep the same id reservable after capacity frees.
    #[test]
    fn handler_identity_capacity_returns_flow_control_not_id_conflict() {
        use std::net::{TcpListener, TcpStream};
        use tungstenite::{Message, accept, client};

        let state = Arc::new(test_runtime_state());
        for i in 0..WEB_EVENTS_MAX_IDENTITY_BUCKETS {
            let identity = format!("cap-id-{i:04}-xx");
            state
                .web_identities
                .attach(&identity, i as u64 + 1)
                .unwrap();
            match state
                .web_identities
                .begin_command(&identity, i as u64 + 1, "hold", "input:gbt-1:YQ==")
                .unwrap()
            {
                IdentityCommandBegin::Reserved { .. } => {}
                other => panic!("expected Reserved: {other:?}"),
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let state_server = Arc::clone(&state);
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut websocket = accept(stream).expect("websocket accept");
            let mut client_state =
                WebSocketClientState::new(9_999, Some("cap-overflow-client".to_owned()), 1);
            state_server
                .web_identities
                .attach("cap-overflow-client", 9_999)
                .unwrap();
            let mut cursors = HashMap::new();
            let text = r#"{"type":"terminal_input","id":"cap-req-1","session":"gbt-1","data_base64":"YQ=="}"#;
            let _ = handle_web_events_client_text(
                &mut websocket,
                &state_server,
                &mut client_state,
                &mut cursors,
                text,
            );
            // Capacity path must not cache success/failure under this id.
            assert!(
                state_server
                    .web_identities
                    .lookup("cap-overflow-client", "cap-req-1", "input:gbt-1:YQ==")
                    .unwrap()
                    .is_none()
            );
            // Free one pending bucket; same request id must not be id_conflict-poisoned.
            state_server
                .web_identities
                .abort_command("cap-id-0000-xx", "hold", None);
            match state_server
                .web_identities
                .begin_command(
                    "cap-overflow-client",
                    9_999,
                    "cap-req-1",
                    "input:gbt-1:YQ==",
                )
                .unwrap()
            {
                IdentityCommandBegin::Reserved { .. } => {}
                other => panic!("id must remain usable after flow_control: {other:?}"),
            }
            let _ = websocket.close(None);
        });

        let stream = TcpStream::connect(addr).unwrap();
        let (mut client_ws, _) = client(format!("ws://{addr}/"), stream).expect("websocket client");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut payload = None;
        while std::time::Instant::now() < deadline {
            match client_ws.read() {
                Ok(Message::Text(text)) => {
                    payload = Some(text.to_string());
                    break;
                }
                Ok(Message::Close(_)) => break,
                Ok(_) => {}
                Err(tungstenite::Error::Io(error))
                    if error.kind() == ErrorKind::WouldBlock
                        || error.kind() == ErrorKind::TimedOut =>
                {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
        let text = payload.expect("handler must send a command result");
        let value: serde_json::Value = serde_json::from_str(&text).expect("json result");
        assert_eq!(
            value.get("type").and_then(|v| v.as_str()),
            Some("input_result"),
            "payload={text}"
        );
        assert_eq!(value.get("ok").and_then(|v| v.as_bool()), Some(false));
        assert_eq!(
            value.get("id").and_then(|v| v.as_str()),
            Some("cap-req-1"),
            "payload={text}"
        );
        assert_eq!(
            value.get("error_code").and_then(|v| v.as_str()),
            Some("flow_control"),
            "capacity must be flow_control not id_conflict; payload={text}"
        );
        assert!(!result_is_cacheable(
            false,
            value.get("error_code").and_then(|v| v.as_str())
        ));
        server.join().expect("server thread");
        let _ = client_ws.close(None);
    }

    #[test]
    fn side_effect_gate_blocks_write_after_identity_takeover() {
        let state = test_runtime_state();
        // Old tab owns control and identity.
        let mut old = WebSocketClientState::new(1, Some("tab-shared".to_owned()), 1);
        state.web_identities.attach("tab-shared", 1).unwrap();
        assert!(state.web_controls.claim("gbt-1", 1).unwrap());
        // New tab takes over identity under the gate and revokes control.
        {
            let _gate = state.web_side_effect_gate.lock().unwrap();
            let (_, revoked) = state.web_identities.attach("tab-shared", 2).unwrap();
            if let Some(old_id) = revoked {
                state.web_controls.release_client(old_id);
            }
        }
        old.identity_generation = 1; // stale generation
        let input = WebEventsClientCommand::Input {
            id: Some("w1".to_owned()),
            session: "gbt-1".to_owned(),
            data_base64: "YQ==".to_owned(),
        };
        let err = apply_web_events_client_command(&state, &mut old, &input).unwrap_err();
        assert_eq!(err.code, "identity_revoked");
        // Even with a faked current generation, control was released.
        old.identity_generation = 2;
        // Generation 2 belongs to connection 2, not 1.
        let err2 = apply_web_events_client_command(&state, &mut old, &input).unwrap_err();
        assert!(
            err2.code == "identity_revoked" || err2.code == "control_required",
            "code={}",
            err2.code
        );
    }

    #[test]
    fn http_line_reader_rejects_oversized_line_without_newline() {
        use std::io::Cursor;
        let huge = vec![b'A'; WEB_HTTP_MAX_REQUEST_LINE_BYTES + 64];
        let mut cursor = Cursor::new(huge);
        let err = read_http_line_bounded(&mut cursor, WEB_HTTP_MAX_REQUEST_LINE_BYTES).unwrap_err();
        assert!(err.contains("size limit"), "err={err}");
        // Partial short line without newline is accepted (EOF).
        let mut short = Cursor::new(b"GET / HTTP/1.1".as_slice());
        let line = read_http_line_bounded(&mut short, WEB_HTTP_MAX_REQUEST_LINE_BYTES).unwrap();
        assert!(line.starts_with("GET /"));
    }

    #[test]
    fn completed_result_persisted_before_ack_is_idempotent_on_retry() {
        let cache = WebIdentityRegistry::default();
        let mut client = WebSocketClientState::new(7, Some("tab-identity-1".to_owned()), 1);
        cache.attach("tab-identity-1", 7).unwrap();
        let fingerprint = "input:gbt-1:YQ==".to_owned();
        let payload =
            r#"{"type":"input_result","ok":true,"id":"same-id","session":"gbt-1"}"#.to_owned();
        // Simulate: side effect applied, remember, then ack send fails.
        client.remember_command("same-id", fingerprint.clone(), payload.clone(), &cache);
        // Reconnect with same identity (takeover of same tab after half-open).
        let client2 = WebSocketClientState::new(8, Some("tab-identity-1".to_owned()), 2);
        cache.attach("tab-identity-1", 8).unwrap();
        let replayed = client2
            .lookup_completed("same-id", &fingerprint, &cache)
            .unwrap()
            .unwrap();
        assert_eq!(replayed, payload);
        // Different command body must not replay or re-apply.
        let conflict = client2.lookup_completed("same-id", "input:gbt-1:Yg==", &cache);
        assert!(conflict.is_err());
    }

    /// Deterministic interleave: reservation is visible to a new connection
    /// before remember, so the same id cannot double-apply across takeover.
    #[test]
    fn identity_pending_blocks_double_apply_across_takeover_barrier() {
        let cache = Arc::new(WebIdentityRegistry::default());
        let identity = "tab-pending-race";
        let request_id = "req-once";
        let fingerprint = "input:gbt-1:YQ==";
        let payload =
            r#"{"type":"input_result","ok":true,"id":"req-once","session":"gbt-1"}"#.to_owned();

        cache.attach(identity, 1).unwrap();
        match cache
            .begin_command(identity, 1, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Reserved { .. } => {}
            other => panic!("expected Reserved, got {other:?}"),
        }

        // B joins the in-flight request and blocks until A remembers — one apply.
        let (joined_tx, joined_rx) = std::sync::mpsc::channel();
        let cache_a = Arc::clone(&cache);
        let payload_a = payload.clone();
        let finisher = thread::spawn(move || {
            joined_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("takeover did not join in-flight");
            thread::sleep(Duration::from_millis(20));
            cache_a.remember(
                identity,
                request_id,
                fingerprint.to_owned(),
                payload_a,
                None,
            );
        });

        let cache_b = Arc::clone(&cache);
        let payload_b = payload.clone();
        let taker = thread::spawn(move || {
            let (generation, revoked) = cache_b.attach(identity, 2).unwrap();
            assert_eq!(generation, 2);
            assert_eq!(revoked, Some(1));
            let join = match cache_b
                .begin_command(identity, 2, request_id, fingerprint)
                .unwrap()
            {
                IdentityCommandBegin::Join(completion) => completion,
                other => panic!("expected Join before remember, got {other:?}"),
            };
            joined_tx.send(()).unwrap();
            match join.wait() {
                PendingOutcome::Ready(replayed) => assert_eq!(replayed, payload_b),
                PendingOutcome::Dropped => panic!("join must receive remembered success"),
            }
            // Subsequent begin is Replay from the completed cache.
            match cache_b
                .begin_command(identity, 2, request_id, fingerprint)
                .unwrap()
            {
                IdentityCommandBegin::Replay(replayed) => {
                    assert_eq!(replayed, payload_b);
                }
                other => panic!("expected Replay after remember, got {other:?}"),
            }
        });

        finisher.join().unwrap();
        taker.join().unwrap();
    }

    /// P0: new connection attaches, old socket detaches late — bound pending must
    /// survive so a same-id retry Joins instead of Reserve+second enqueue.
    #[test]
    fn takeover_old_detach_keeps_bound_pending_write_count_one() {
        let cache = Arc::new(WebIdentityRegistry::default());
        let identity = "tab-detach-bound-xx";
        let request_id = "keystroke-1";
        let fingerprint = "input:gbt-1:YQ==";
        let payload =
            r#"{"type":"input_result","ok":true,"id":"keystroke-1","session":"gbt-1"}"#.to_owned();

        // Count "enqueues": only Reserved counts as a new side-effect admission.
        let write_count = Arc::new(AtomicU64::new(0));

        cache.attach(identity, 1).unwrap();
        let token = match cache
            .begin_command(identity, 1, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Reserved { token } => token,
            other => panic!("expected Reserved: {other:?}"),
        };
        write_count.fetch_add(1, Ordering::SeqCst);
        // Side effect bound (WriteJob enqueued / write_all in flight).
        cache.bind_command(identity, request_id, token);

        // New tab/socket takes over identity (common reconnect path).
        let (gen2, revoked) = cache.attach(identity, 2).unwrap();
        assert_eq!(gen2, 2);
        assert_eq!(revoked, Some(1));
        // Old socket finally runs release_web_client → detach.
        cache.detach(Some(identity), 1);

        // New connection retries the same id (lost ack / reconnect).
        let begin = cache
            .begin_command(identity, 2, request_id, fingerprint)
            .unwrap();
        match &begin {
            IdentityCommandBegin::Join(_) => {}
            IdentityCommandBegin::Reserved { .. } => {
                write_count.fetch_add(1, Ordering::SeqCst);
                panic!("detach must not free bound pending — would double-enqueue");
            }
            IdentityCommandBegin::Replay(_) => panic!("must not Replay before finish"),
        }
        assert_eq!(
            write_count.load(Ordering::SeqCst),
            1,
            "exactly one side-effect admission"
        );

        // Old completion wins; Joiners observe it. Stale Dropped must not fire.
        let join = match begin {
            IdentityCommandBegin::Join(c) => c,
            _ => unreachable!(),
        };
        let cache_c = Arc::clone(&cache);
        let payload_c = payload.clone();
        let publisher = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            cache_c.remember(
                identity,
                request_id,
                fingerprint.to_owned(),
                payload_c,
                Some(token),
            );
        });
        match join.wait() {
            PendingOutcome::Ready(p) => assert_eq!(p, payload),
            PendingOutcome::Dropped => {
                panic!("bound pending must not Drop on old detach")
            }
        }
        publisher.join().unwrap();
        assert_eq!(write_count.load(Ordering::SeqCst), 1);

        // After terminal publish, same id is Replay only.
        match cache
            .begin_command(identity, 2, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Replay(p) => assert_eq!(p, payload),
            other => panic!("expected Replay: {other:?}"),
        }
    }

    /// Unbound (pre-enqueue) pending may be dropped on detach so the id is not
    /// pinned forever by a dead socket that never bound a job.
    #[test]
    fn detach_drops_unbound_pending_but_token_protects_against_stale_abort() {
        let cache = WebIdentityRegistry::default();
        let identity = "tab-detach-unbound";
        let request_id = "pre-enqueue-1";
        let fingerprint = "input:gbt-1:YQ==";
        cache.attach(identity, 1).unwrap();
        let token_old = match cache
            .begin_command(identity, 1, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Reserved { token } => token,
            other => panic!("{other:?}"),
        };
        // Not bound — detach clears it.
        cache.detach(Some(identity), 1);
        cache.attach(identity, 2).unwrap();
        let token_new = match cache
            .begin_command(identity, 2, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Reserved { token } => token,
            other => panic!("expected fresh Reserve after unbound detach: {other:?}"),
        };
        assert_ne!(token_old, token_new);
        // Stale old abort must not free the new reservation.
        cache.abort_command(identity, request_id, Some(token_old));
        match cache
            .begin_command(identity, 2, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Join(_) => {}
            IdentityCommandBegin::Reserved { .. } => {
                panic!("stale abort must not remove new pending")
            }
            other => panic!("{other:?}"),
        }
        // Correct token completes.
        cache.remember(
            identity,
            request_id,
            fingerprint.to_owned(),
            r#"{"ok":true}"#.to_owned(),
            Some(token_new),
        );
        match cache
            .begin_command(identity, 2, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Replay(_) => {}
            other => panic!("{other:?}"),
        }
    }

    /// Old bound completer after new Reserve is impossible while bound survives
    /// detach; still prove token mismatch cannot steal a later reservation.
    #[test]
    fn stale_remember_token_cannot_override_newer_reservation() {
        let cache = WebIdentityRegistry::default();
        let identity = "tab-stale-token";
        let request_id = "req-token";
        let fingerprint = "input:gbt-1:YQ==";
        cache.attach(identity, 1).unwrap();
        let token_old = match cache
            .begin_command(identity, 1, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Reserved { token } => token,
            other => panic!("{other:?}"),
        };
        // Unbound abort frees slot (simulates safe pre-enqueue abandon).
        cache.abort_command(identity, request_id, Some(token_old));
        let token_new = match cache
            .begin_command(identity, 1, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Reserved { token } => token,
            other => panic!("{other:?}"),
        };
        cache.bind_command(identity, request_id, token_new);
        // Late success from the abandoned attempt must not remove new pending.
        cache.remember(
            identity,
            request_id,
            fingerprint.to_owned(),
            r#"{"ok":true,"stale":true}"#.to_owned(),
            Some(token_old),
        );
        match cache
            .begin_command(identity, 1, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Join(_) => {}
            IdentityCommandBegin::Replay(p) => {
                panic!("stale remember must not cache over live job: {p}")
            }
            other => panic!("{other:?}"),
        }
        cache.remember(
            identity,
            request_id,
            fingerprint.to_owned(),
            r#"{"ok":true,"fresh":true}"#.to_owned(),
            Some(token_new),
        );
        match cache
            .begin_command(identity, 1, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Replay(p) => assert!(p.contains("fresh")),
            other => panic!("{other:?}"),
        }
    }

    /// Identity pending + Join (not a multi-second global gate) covers the
    /// apply→cache gap so takeover cannot double-apply the same request id.
    #[test]
    fn identity_join_covers_remember_gap_without_holding_global_gate() {
        let state = Arc::new(test_runtime_state());
        let identity = "tab-gate-remember";
        let request_id = "gate-req-1";
        let fingerprint = "input:gbt-1:YQ==".to_owned();
        let payload =
            r#"{"type":"input_result","ok":true,"id":"gate-req-1","session":"gbt-1"}"#.to_owned();

        state.web_identities.attach(identity, 1).unwrap();
        state
            .web_identities
            .begin_command(identity, 1, request_id, &fingerprint)
            .unwrap();

        let (joined_tx, joined_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();

        let state_a = Arc::clone(&state);
        let payload_a = payload.clone();
        let fingerprint_a = fingerprint.clone();
        let applier = thread::spawn(move || {
            // Simulate PTY write wait *outside* the global gate.
            joined_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("joiner did not attach");
            thread::sleep(Duration::from_millis(30));
            state_a
                .web_identities
                .remember(identity, request_id, fingerprint_a, payload_a, None);
            done_tx.send(()).unwrap();
        });

        let state_b = Arc::clone(&state);
        let payload_b = payload.clone();
        let fingerprint_b = fingerprint.clone();
        let takeover = thread::spawn(move || {
            // Attach does not need to wait on A's slow write — gate is free.
            let attach_started = std::time::Instant::now();
            let _gate = state_b
                .web_side_effect_gate
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let (_, revoked) = state_b.web_identities.attach(identity, 2).unwrap();
            assert_eq!(revoked, Some(1));
            if let Some(old) = revoked {
                state_b.web_controls.release_client(old);
            }
            drop(_gate);
            assert!(
                attach_started.elapsed() < Duration::from_millis(200),
                "attach must not wait for another session's slow writer"
            );
            let join = match state_b
                .web_identities
                .begin_command(identity, 2, request_id, &fingerprint_b)
                .unwrap()
            {
                IdentityCommandBegin::Join(c) => c,
                other => panic!("expected Join in apply→remember gap: {other:?}"),
            };
            joined_tx.send(()).unwrap();
            match join.wait() {
                PendingOutcome::Ready(replayed) => assert_eq!(replayed, payload_b),
                PendingOutcome::Dropped => panic!("expected Ready after remember"),
            }
            done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("applier did not finish");
        });

        applier.join().unwrap();
        takeover.join().unwrap();
    }

    #[test]
    fn abort_command_releases_identity_reservation_for_retry() {
        let cache = WebIdentityRegistry::default();
        cache.attach("tab-abort-1", 1).unwrap();
        assert!(matches!(
            cache
                .begin_command("tab-abort-1", 1, "r1", "input:gbt-1:YQ==")
                .unwrap(),
            IdentityCommandBegin::Reserved { .. }
        ));
        assert!(matches!(
            cache
                .begin_command("tab-abort-1", 1, "r1", "input:gbt-1:YQ==")
                .unwrap(),
            IdentityCommandBegin::Join(_)
        ));
        cache.abort_command("tab-abort-1", "r1", None);
        assert!(matches!(
            cache
                .begin_command("tab-abort-1", 1, "r1", "input:gbt-1:YQ==")
                .unwrap(),
            IdentityCommandBegin::Reserved { .. }
        ));
    }

    #[test]
    fn three_same_id_frames_join_one_write_and_replay_success() {
        // Writer blocked past the old 5s timeout window; three same-id admissions
        // produce one WriteJob and all observe the successful result.
        let cache = Arc::new(WebIdentityRegistry::default());
        let identity = "tab-triple-join-xx";
        let request_id = "triple-1";
        let fingerprint = "input:gbt-1:c2FtZQ==";
        let payload =
            r#"{"type":"input_result","ok":true,"id":"triple-1","session":"gbt-1"}"#.to_owned();
        cache.attach(identity, 1).unwrap();
        match cache
            .begin_command(identity, 1, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Reserved { .. } => {}
            other => panic!("{other:?}"),
        }

        let writes = Arc::new(AtomicU64::new(0));
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let writes_w = Arc::clone(&writes);
        let cache_w = Arc::clone(&cache);
        let payload_w = payload.clone();
        let writer = thread::spawn(move || {
            writes_w.fetch_add(1, Ordering::SeqCst);
            release_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("release");
            // Simulate write completing after a long delay (beyond old timeout).
            cache_w.remember(
                identity,
                request_id,
                fingerprint.to_owned(),
                payload_w,
                None,
            );
        });

        let mut joiners = Vec::new();
        for _ in 0..2 {
            let cache_j = Arc::clone(&cache);
            let payload_j = payload.clone();
            joiners.push(thread::spawn(move || {
                let join = match cache_j
                    .begin_command(identity, 1, request_id, fingerprint)
                    .unwrap()
                {
                    IdentityCommandBegin::Join(c) => c,
                    other => panic!("expected Join: {other:?}"),
                };
                match join.wait() {
                    PendingOutcome::Ready(p) => assert_eq!(p, payload_j),
                    PendingOutcome::Dropped => panic!("expected Ready"),
                }
            }));
        }

        thread::sleep(Duration::from_millis(30));
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        release_tx.send(()).unwrap();
        writer.join().unwrap();
        for j in joiners {
            j.join().unwrap();
        }
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        match cache
            .begin_command(identity, 1, request_id, fingerprint)
            .unwrap()
        {
            IdentityCommandBegin::Replay(p) => assert_eq!(p, payload),
            other => panic!("final frame must Replay success: {other:?}"),
        }
    }

    #[test]
    fn slow_input_write_does_not_block_other_session_side_effects() {
        // While session A holds a simulated long write_all outside the gate,
        // session B must still acquire the gate and complete a claim.
        let state = Arc::new(test_runtime_state());
        let (entered_write_tx, entered_write_rx) = std::sync::mpsc::channel();
        let (release_write_tx, release_write_rx) = std::sync::mpsc::channel();

        let state_a = Arc::clone(&state);
        let slow_writer = thread::spawn(move || {
            // Production input path: short gate for enqueue, then wait outside.
            {
                let _gate = state_a.web_side_effect_gate.lock().unwrap();
                // enqueue under gate (instant)
            }
            entered_write_tx.send(()).unwrap();
            release_write_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("release slow write");
        });

        entered_write_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("slow writer did not start");

        let state_b = Arc::clone(&state);
        let other = thread::spawn(move || {
            let started = std::time::Instant::now();
            let _gate = state_b.web_side_effect_gate.lock().unwrap();
            // Claim under gate for a different session while A is mid-write.
            assert!(state_b.web_controls.claim("gbt-other", 99).unwrap());
            assert!(
                started.elapsed() < Duration::from_millis(200),
                "other session side effect blocked by slow writer"
            );
        });

        other.join().unwrap();
        release_write_tx.send(()).unwrap();
        slow_writer.join().unwrap();
    }

    #[test]
    fn same_websocket_reads_heartbeat_while_input_write_is_pending() {
        use std::net::TcpListener;
        use tungstenite::{Message, accept, client};

        let state = Arc::new(test_runtime_state());
        state.host.test_register_idle_session("gbt-slow-socket");
        let (_entered_rx, release_tx) = state.host.test_gate_next_write("gbt-slow-socket");
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let state_server = Arc::clone(&state);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (allow_release_tx, allow_release_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_millis(5)))
                .unwrap();
            let mut websocket = accept(stream).unwrap();
            let identity = "same-socket-heartbeat";
            let generation = state_server.web_identities.attach(identity, 7).unwrap().0;
            let mut client_state =
                WebSocketClientState::new(7, Some(identity.to_owned()), generation);
            let subscribe = WebEventsClientCommand::Subscribe {
                id: Some("sub-slow".to_owned()),
                sessions: vec!["gbt-slow-socket".to_owned()],
                generation: Some(1),
            };
            apply_web_events_client_command(&state_server, &mut client_state, &subscribe).unwrap();
            let claim = WebEventsClientCommand::Claim {
                id: Some("claim-slow".to_owned()),
                session: "gbt-slow-socket".to_owned(),
            };
            apply_web_events_client_command(&state_server, &mut client_state, &claim).unwrap();
            let mut cursors = HashMap::new();

            assert!(matches!(
                poll_websocket_client(
                    &mut websocket,
                    &state_server,
                    &mut client_state,
                    &mut cursors
                ),
                WsClientAction::Continue { .. }
            ));
            ready_tx.send(()).unwrap();
            allow_release_rx.recv().unwrap();
            release_tx.send(()).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(1);
            while !client_state.pending_inputs.is_empty() && std::time::Instant::now() < deadline {
                let _ = poll_websocket_client(
                    &mut websocket,
                    &state_server,
                    &mut client_state,
                    &mut cursors,
                );
                thread::sleep(Duration::from_millis(2));
            }
            assert!(client_state.pending_inputs.is_empty());
        });

        let (mut websocket, _) =
            client(format!("ws://{addr}/"), TcpStream::connect(addr).unwrap()).unwrap();
        websocket
            .send(Message::Text(
                r#"{"type":"terminal_input","id":"input-slow","session":"gbt-slow-socket","data_base64":"YQ=="}"#.into(),
            ))
            .unwrap();
        websocket
            .send(Message::Text(
                r#"{"type":"client_heartbeat","id":"heartbeat-slow"}"#.into(),
            ))
            .unwrap();
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let heartbeat = websocket.read().unwrap();
        let Message::Text(heartbeat) = heartbeat else {
            panic!("expected heartbeat result, got {heartbeat:?}");
        };
        let heartbeat: serde_json::Value = serde_json::from_str(&heartbeat).unwrap();
        assert_eq!(heartbeat["type"], "client_heartbeat_result");
        assert_eq!(heartbeat["ok"], true);
        allow_release_tx.send(()).unwrap();
        let input = websocket.read().unwrap();
        let Message::Text(input) = input else {
            panic!("expected input result, got {input:?}");
        };
        let input: serde_json::Value = serde_json::from_str(&input).unwrap();
        assert_eq!(input["type"], "input_result");
        assert_eq!(input["id"], "input-slow");
        assert_eq!(input["ok"], true);
        server.join().unwrap();
    }

    #[test]
    fn empty_subscriptions_emit_no_terminal_bytes() {
        let state = test_runtime_state();
        let mut client = WebSocketClientState::new(1, None, 0);
        assert!(client.subscriptions.is_empty());
        // Subscribe to nothing stays empty; resync requested only when set changes.
        let cmd = WebEventsClientCommand::Subscribe {
            id: Some("s1".to_owned()),
            sessions: vec![],
            generation: Some(1),
        };
        assert!(!apply_web_events_client_command(&state, &mut client, &cmd).unwrap());
        let frames = state
            .host
            .plan_web_events_with_subscriptions(
                &HashMap::new(),
                true,
                WEB_EVENTS_MAX_MESSAGE_BYTES,
                Some(&client.subscriptions),
            )
            .unwrap();
        for frame in frames {
            assert!(frame.message.terminals.is_empty());
        }
    }

    #[test]
    fn subscribe_generation_is_latest_wins() {
        let state = test_runtime_state();
        let mut client = WebSocketClientState::new(1, Some("client-abc-001".to_owned()), 1);
        let sub_b = WebEventsClientCommand::Subscribe {
            id: Some("s-b".to_owned()),
            sessions: vec!["gbt-b".to_owned()],
            generation: Some(2),
        };
        assert!(apply_web_events_client_command(&state, &mut client, &sub_b).unwrap());
        assert!(client.subscriptions.contains("gbt-b"));
        assert_eq!(client.subscribe_generation, 2);

        // Stale generation 1 must not roll the set back to A.
        let sub_a = WebEventsClientCommand::Subscribe {
            id: Some("s-a".to_owned()),
            sessions: vec!["gbt-a".to_owned()],
            generation: Some(1),
        };
        assert!(!apply_web_events_client_command(&state, &mut client, &sub_a).unwrap());
        assert!(client.subscriptions.contains("gbt-b"));
        assert!(!client.subscriptions.contains("gbt-a"));
        assert_eq!(client.subscribe_generation, 2);

        // Equal or higher generation applies.
        let sub_c = WebEventsClientCommand::Subscribe {
            id: Some("s-c".to_owned()),
            sessions: vec!["gbt-c".to_owned()],
            generation: Some(2),
        };
        assert!(apply_web_events_client_command(&state, &mut client, &sub_c).unwrap());
        assert!(client.subscriptions.contains("gbt-c"));
        assert!(!client.subscriptions.contains("gbt-b"));
    }

    #[test]
    fn parse_subscribe_accepts_generation() {
        let cmd = parse_web_events_client_command(
            r#"{"type":"terminal_subscribe","id":"s1","sessions":["gbt-1"],"generation":7}"#,
        )
        .unwrap()
        .unwrap();
        match cmd {
            WebEventsClientCommand::Subscribe {
                generation: Some(7),
                sessions,
                ..
            } => assert_eq!(sessions, vec!["gbt-1".to_owned()]),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn map_http_session_close_error_classifies_status() {
        let not_found = anyhow::anyhow!("session not found: gbt-missing");
        let (status, body) = map_http_session_close_error(&not_found);
        assert_eq!(status, "404 Not Found");
        assert!(body.contains("session not found"));

        let timeout =
            anyhow::anyhow!("Grok process tree still has live members before the close deadline");
        let (status, _) = map_http_session_close_error(&timeout);
        assert_eq!(status, "504 Gateway Timeout");

        let conflict = anyhow::anyhow!("close already in progress");
        let (status, _) = map_http_session_close_error(&conflict);
        assert_eq!(status, "409 Conflict");

        let internal = anyhow::anyhow!("session registry lock was poisoned");
        let (status, body) = map_http_session_close_error(&internal);
        assert_eq!(status, "500 Internal Server Error");
        assert!(body.contains("poisoned"));
    }

    #[test]
    fn side_effect_commands_require_identity_and_request_id() {
        assert!(command_requires_identity_and_id(
            &WebEventsClientCommand::Claim {
                id: Some("c1".to_owned()),
                session: "gbt-1".to_owned(),
            }
        ));
        assert!(command_requires_identity_and_id(
            &WebEventsClientCommand::Release {
                id: None,
                session: "gbt-1".to_owned(),
            }
        ));
        assert!(command_requires_identity_and_id(
            &WebEventsClientCommand::Input {
                id: Some("i1".to_owned()),
                session: "gbt-1".to_owned(),
                data_base64: "QQ==".to_owned(),
            }
        ));
        assert!(command_requires_identity_and_id(
            &WebEventsClientCommand::Resize {
                id: Some("r1".to_owned()),
                session: "gbt-1".to_owned(),
                cols: 80,
                rows: 24,
            }
        ));
        assert!(!command_requires_identity_and_id(
            &WebEventsClientCommand::Subscribe {
                id: Some("s1".to_owned()),
                sessions: vec![],
                generation: Some(1),
            }
        ));
        assert!(!command_requires_identity_and_id(
            &WebEventsClientCommand::Heartbeat {
                id: Some("h1".to_owned()),
            }
        ));
        assert_eq!(
            peek_web_events_result_type(r#"{"type":"terminal_claim","session":"gbt-1"}"#),
            "terminal_claim_result"
        );
        assert_eq!(
            peek_web_events_result_type(r#"{"type":"terminal_resize","session":"gbt-1"}"#),
            "resize_result"
        );
        assert_eq!(
            peek_web_events_request_id(r#"{"type":"terminal_claim","id":"abc-1"}"#).as_deref(),
            Some("abc-1")
        );
    }

    #[test]
    fn handler_rejects_side_effect_without_identity_or_id() {
        use std::net::{TcpListener, TcpStream};
        use tungstenite::{Message, accept, client};

        fn exchange(text: &str, identity: Option<&str>) -> serde_json::Value {
            let state = Arc::new(test_runtime_state());
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let state_server = Arc::clone(&state);
            let identity_owned = identity.map(str::to_owned);
            let text_owned = text.to_owned();
            let server = thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                let mut websocket = accept(stream).expect("websocket accept");
                let mut identity_generation = 0u64;
                if let Some(ref id) = identity_owned {
                    // Register so is_current passes; we are testing id/identity
                    // presence gates, not takeover.
                    identity_generation = state_server
                        .web_identities
                        .attach(id, 42)
                        .expect("attach identity")
                        .0;
                }
                let mut client_state =
                    WebSocketClientState::new(42, identity_owned, identity_generation);
                let mut cursors = HashMap::new();
                let _ = handle_web_events_client_text(
                    &mut websocket,
                    &state_server,
                    &mut client_state,
                    &mut cursors,
                    &text_owned,
                );
            });
            let (mut ws, _) = client(format!("ws://{addr}/"), TcpStream::connect(addr).unwrap())
                .expect("client connect");
            let msg = match ws.read().expect("client read") {
                Message::Text(t) => t,
                other => panic!("expected text, got {other:?}"),
            };
            server.join().unwrap();
            serde_json::from_str(&msg).unwrap()
        }

        let value = exchange(
            r#"{"type":"terminal_claim","id":"c-anon","session":"gbt-1"}"#,
            None,
        );
        assert_eq!(value["type"], "terminal_claim_result");
        assert_eq!(value["ok"], false);
        assert_eq!(value["id"], "c-anon");
        assert_eq!(value["error_code"], "identity_required");

        let value = exchange(
            r#"{"type":"terminal_claim","session":"gbt-1"}"#,
            Some("client-abc-002"),
        );
        assert_eq!(value["type"], "terminal_claim_result");
        assert_eq!(value["ok"], false);
        assert_eq!(value["error_code"], "request_id_required");

        let value = exchange(
            r#"{"type":"terminal_resize","id":"rz1","session":"gbt-1","cols":80,"rows":24}"#,
            None,
        );
        assert_eq!(value["type"], "resize_result");
        assert_eq!(value["error_code"], "identity_required");
    }

    #[test]
    fn rejects_conflicting_client_identity_sources() {
        assert!(
            split_path_query("/api/events?client=valid-client-id")
                .unwrap()
                .1
                .is_some()
        );
        assert!(split_path_query("/api/events?client=bad%20id").is_err());
        let (_, _, cap) = split_path_query(&format!(
            "/api/events?c={TEST_WEBUI_CAPABILITY}&client=valid-client-id"
        ))
        .unwrap();
        assert_eq!(cap.as_deref(), Some(TEST_WEBUI_CAPABILITY));
    }

    #[test]
    fn http_request_line_and_header_limits_are_enforced() {
        let huge_line = format!(
            "GET {} HTTP/1.1\r\n\r\n",
            "x".repeat(WEB_HTTP_MAX_REQUEST_LINE_BYTES)
        );
        let response = serve_web_request(huge_line.as_bytes());
        assert!(
            response.starts_with(b"HTTP/1.1 400 Bad Request\r\n"),
            "got {}",
            String::from_utf8_lossy(&response[..response.len().min(200)])
        );
    }

    /// Fixed test capability (64 hex chars). Not a production secret.
    const TEST_WEBUI_CAPABILITY: &str =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn serve_web_request(request: &[u8]) -> Vec<u8> {
        serve_web_request_state(request, test_runtime_state())
    }

    fn serve_web_request_with_capability(request: &str) -> Vec<u8> {
        serve_web_request_state(request.as_bytes(), test_runtime_state())
    }

    fn serve_web_request_state(request: &[u8], state: RuntimeState) -> Vec<u8> {
        serve_web_request_arc(request, Arc::new(state))
    }

    fn serve_web_request_arc(request: &[u8], state: Arc<RuntimeState>) -> Vec<u8> {
        let timeout = Duration::from_secs(10);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        client.set_read_timeout(Some(timeout)).unwrap();
        client.set_write_timeout(Some(timeout)).unwrap();
        server.set_read_timeout(Some(timeout)).unwrap();
        server.set_write_timeout(Some(timeout)).unwrap();
        client.write_all(request).unwrap();
        client.shutdown(std::net::Shutdown::Write).unwrap();

        let handler = std::thread::spawn(move || handle_web_connection(server, state));

        let mut response = Vec::new();
        client.read_to_end(&mut response).unwrap();
        handler.join().unwrap();
        response
    }

    fn test_runtime_state() -> RuntimeState {
        RuntimeState {
            host: SessionHost::new(OrphanPolicy {
                lease_ms: 120_000,
                grace_ms: 600_000,
            }),
            started_at_ms: 0,
            stopping: AtomicBool::new(false),
            web_url: Some(format!("http://127.0.0.1:47653/?c={TEST_WEBUI_CAPABILITY}")),
            web_capability: TEST_WEBUI_CAPABILITY.to_owned(),
            version_checker: Arc::new(VersionChecker::new()),
            web_controls: WebControlRegistry::default(),
            web_identities: WebIdentityRegistry::default(),
            web_side_effect_gate: Mutex::new(()),
            next_web_client_id: AtomicU64::new(1),
            web_http_connections: AtomicU64::new(0),
            ipc_connections: AtomicU64::new(0),
        }
    }

    /// Real local-socket accept → `handle_connection` → NDJSON response.
    /// Exercises the same path CLI `transport::call` uses (not a protocol helper).
    fn serve_ipc_frame(state: RuntimeState, request_frame: &[u8]) -> ResponseEnvelope {
        use crate::protocol::decode_response;
        use std::io::Write;

        let (listener, connect_name) = bind_test_ipc_listener();
        let state = Arc::new(state);
        let server = thread::spawn(move || {
            let conn = listener.accept().expect("accept ipc client");
            handle_connection(conn, state);
        });

        let mut client = connect_test_ipc_client(&connect_name);
        client
            .write_all(request_frame)
            .expect("write ipc request frame");
        client.flush().expect("flush ipc request frame");
        let mut reader = BufReader::new(client);
        let response_frame = read_frame(&mut reader).expect("read ipc response frame");
        let response = decode_response(&response_frame).expect("decode ipc response");
        server.join().expect("handle_connection thread");
        // Best-effort cleanup of the unix socket dir (path ends with /t.sock).
        #[cfg(unix)]
        if let Some(dir) = std::path::Path::new(&connect_name).parent() {
            let _ = std::fs::remove_dir_all(dir);
        }
        response
    }

    fn serve_ipc_request(
        state: RuntimeState,
        envelope: &crate::protocol::RequestEnvelope,
    ) -> ResponseEnvelope {
        use crate::protocol::encode_frame;
        let frame = encode_frame(envelope).expect("encode request");
        serve_ipc_frame(state, &frame)
    }

    /// Returns (listener, opaque connect token for the peer).
    fn bind_test_ipc_listener() -> (interprocess::local_socket::Listener, String) {
        static IPC_TEST_SEQ: AtomicU64 = AtomicU64::new(1);
        let seq = IPC_TEST_SEQ.fetch_add(1, Ordering::Relaxed);
        #[cfg(unix)]
        {
            use interprocess::local_socket::{GenericFilePath, ListenerOptions};
            let dir = std::env::temp_dir().join(format!(
                "gbr-ipc-{}-{}-{}",
                std::process::id(),
                now_millis(),
                seq
            ));
            let _ = std::fs::create_dir_all(&dir);
            let path = dir.join("t.sock");
            let name = path
                .as_os_str()
                .to_fs_name::<GenericFilePath>()
                .expect("filesystem socket name");
            let listener = ListenerOptions::new()
                .name(name)
                .create_sync()
                .expect("bind test ipc socket");
            // Store path for connect + cleanup token (path is the connect identity).
            (listener, path.to_string_lossy().into_owned())
        }
        #[cfg(windows)]
        {
            use interprocess::local_socket::{GenericNamespaced, ListenerOptions};
            let token = format!(
                "grok-bridge-test-ipc-{}-{}-{}",
                std::process::id(),
                now_millis(),
                seq
            );
            let name = token
                .clone()
                .to_ns_name::<GenericNamespaced>()
                .expect("named pipe name");
            let listener = ListenerOptions::new()
                .name(name)
                .create_sync()
                .expect("bind test ipc pipe");
            (listener, token)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = seq;
            panic!("ipc roundtrip tests require unix or windows");
        }
    }

    fn connect_test_ipc_client(connect_name: &str) -> Stream {
        #[cfg(unix)]
        {
            use interprocess::local_socket::GenericFilePath;
            use std::path::Path;
            let path = Path::new(connect_name);
            let mut last = None;
            for _ in 0..100 {
                let name = match path.as_os_str().to_fs_name::<GenericFilePath>() {
                    Ok(n) => n,
                    Err(e) => {
                        last = Some(format!("name: {e}"));
                        break;
                    }
                };
                match Stream::connect(name) {
                    Ok(stream) => return stream,
                    Err(e) => {
                        last = Some(format!("{e}"));
                        thread::sleep(Duration::from_millis(5));
                    }
                }
            }
            panic!("connect test ipc socket {connect_name}: {last:?}");
        }
        #[cfg(windows)]
        {
            use interprocess::local_socket::GenericNamespaced;
            let mut last = None;
            for _ in 0..100 {
                let name = match connect_name.to_ns_name::<GenericNamespaced>() {
                    Ok(n) => n,
                    Err(e) => {
                        last = Some(format!("name: {e}"));
                        break;
                    }
                };
                match Stream::connect(name) {
                    Ok(stream) => return stream,
                    Err(e) => {
                        last = Some(format!("{e}"));
                        thread::sleep(Duration::from_millis(5));
                    }
                }
            }
            panic!("connect test ipc pipe {connect_name}: {last:?}");
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = connect_name;
            panic!("ipc roundtrip tests require unix or windows");
        }
    }

    #[test]
    fn handle_connection_preserves_request_id_when_read_limit_is_invalid() {
        // Mirrors CLI `read --limit 262144`: frame carries a legal req-* id, but
        // parameter validation fails. Response must keep that id so transport
        // does not report id mismatch before the real invalid_request error.
        let request_id = "req-a1b2c3d4-read-limit";
        let envelope = crate::protocol::RequestEnvelope {
            id: request_id.to_owned(),
            client_session_id: None,
            request: Request::Read {
                session: "gbt-test".to_owned(),
                cursor: Some(0),
                limit: Some(262_144),
                wait_ms: None,
            },
        };
        use crate::protocol::encode_frame;
        let frame = encode_frame(&envelope).expect("encode");
        assert!(
            decode_request(&frame).is_err(),
            "decode_request must still reject oversized read limit"
        );

        let response = serve_ipc_request(test_runtime_state(), &envelope);
        assert_eq!(
            response.id, request_id,
            "response id must match the client request id"
        );
        assert!(!response.ok);
        let error = response.error.expect("invalid_request error body");
        assert_eq!(error.code, "invalid_request");
        assert!(
            error.message.contains("65536") || error.message.to_ascii_lowercase().contains("limit"),
            "expected read-limit validation message, got {}",
            error.message
        );
        // Same check transport::call_over_stream performs before surfacing errors.
        assert_eq!(response.id, envelope.id);
    }

    #[test]
    fn handle_connection_uses_fixed_id_when_request_id_is_untrusted() {
        // Spaces fail validate_identifier — must not echo untrusted id material.
        let frame = br#"{"id":"bad id","request":{"method":"read","params":{"session":"gbt-1","cursor":0,"limit":262144}}}"#;
        let mut with_nl = frame.to_vec();
        with_nl.push(b'\n');
        let response = serve_ipc_frame(test_runtime_state(), &with_nl);
        assert_eq!(response.id, "invalid-request");
        assert!(!response.ok);
        assert_eq!(
            response.error.as_ref().map(|e| e.code.as_str()),
            Some("invalid_request")
        );
    }

    #[test]
    fn handle_connection_uses_fixed_id_for_malformed_json_frame() {
        let response = serve_ipc_frame(test_runtime_state(), b"{not-json\n");
        assert_eq!(response.id, "invalid-request");
        assert!(!response.ok);
        assert_eq!(
            response.error.as_ref().map(|e| e.code.as_str()),
            Some("invalid_request")
        );
    }

    fn split_http_response(response: &[u8]) -> (&str, &[u8]) {
        let separator = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap();
        (
            std::str::from_utf8(&response[..separator + 2]).unwrap(),
            &response[separator + 4..],
        )
    }
}
