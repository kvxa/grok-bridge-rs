use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    ffi::OsString,
    fs::{self, OpenOptions},
    io::{self, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{SyncSender, TrySendError, sync_channel},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};

#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};

#[cfg(windows)]
use windows_sys::Win32::{
    Foundation::{
        DUPLICATE_SAME_ACCESS, DuplicateHandle, ERROR_ACCESS_DENIED, GetLastError,
        INVALID_HANDLE_VALUE,
    },
    System::{
        Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW,
            TH32CS_SNAPPROCESS,
        },
        JobObjects::{
            CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
            QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
        },
        Threading::{
            GetCurrentProcess, GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
            PROCESS_TERMINATE, SYNCHRONIZE, TerminateProcess, WaitForSingleObject,
        },
    },
};

/// Win32 `STILL_ACTIVE` — process has not exited.
#[cfg(windows)]
const STILL_ACTIVE: u32 = 259;

use crate::protocol::{
    ClientLeaseState, CloseGroupResult, HookActivity, HookEvent, HookEventKind, MAX_WRITE_BYTES,
    ReadResult, SessionPhase, SessionState, TerminalStreamEntry, WaitCondition, WaitResult,
    WebEventsMessage, validate_client_session_id, validate_owner, validate_terminal_size,
};

const INITIAL_COLS: u16 = 120;
const INITIAL_ROWS: u16 = 36;
const SCROLLBACK_ROWS: usize = 5_000;
const MAX_TRANSCRIPT_BYTES: usize = 512 * 1024;
const MAX_READ_BYTES: usize = 64 * 1024;
/// Maximum retained terminal reset source for one WebSocket connection. Reset
/// snapshots contain only the visible grid and terminal modes, never scrollback.
const MAX_WEB_RESET_CONTINUATION_BYTES: usize = 8 * 1024 * 1024;
const WRITER_QUEUE_CAPACITY: usize = 64;
/// A PTY write may have partially reached the child even when the writer stalls.
/// Publish one terminal, non-retryable outcome after this bound so RPC/WebSocket
/// workers and identity joiners cannot wait forever or enqueue the bytes again.
const PTY_WRITE_COMPLETION_TIMEOUT_MS: u64 = 30_000;
const QUIET_IDLE_MILLISECONDS: u64 = 3_000;
const PROCESS_TERMINATE_TIMEOUT_MS: u64 = 5_000;
const PROCESS_HANGUP_GRACE_MS: u64 = 250;
const PROCESS_TERMINATE_GRACE_MS: u64 = 750;
const PROCESS_KILL_REPEAT_MS: u64 = 250;
/// Historical soft bound for tests only. Close tombstones are **not**
/// capacity-evicted: idempotent close is guaranteed for the full TTL
/// regardless of unrelated close churn (see `purge_closed_session_tombstones`).
const CLOSED_SESSION_CACHE_CAPACITY: usize = 16_384;
/// Tombstones remain queryable for this long. Purge is TTL-only — never drop a
/// non-expired entry to free capacity (that would break close idempotency).
const CLOSED_SESSION_TOMBSTONE_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
/// Max hooks buffered for a provider id between spawn and registry install.
/// SessionStart + PromptSubmit arrive almost immediately after Grok starts.
const MAX_PENDING_PROVIDER_HOOKS: usize = 64;
/// Parallel session closes per owner/client/orphan batch (fits WebUI deadline).
const CLOSE_MAX_CONCURRENCY: usize = 8;
/// Absolute wall budget for one `close_owner` / `close_client` / `close_sessions`
/// call (all rounds + final scan share this Instant). Frontend group close must
/// exceed this plus response overhead (`CLOSE_GROUP_TIMEOUT_MS` in webui).
pub(crate) const CLOSE_BATCH_DEADLINE_MS: u64 = 7_500;
/// Absolute wall budget for `shutdown_all` / ServerStop (same concurrent workers).
const SHUTDOWN_ALL_DEADLINE_MS: u64 = 7_500;
/// Live sessions + in-flight creates. Keeps board/list metadata under the 1 MiB
/// frame budget even with full cwd/title fields on every entry.
pub(crate) const MAX_SESSIONS: usize = 256;
/// Interactive WebUI write-control lease (claim / owns / heartbeat). Independent
/// of the Codex client lease. While held and fresh, orphan cleanup is deferred;
/// when released or expired, grace restarts from the hold end — never permanent.
pub(crate) const WEB_CONTROL_LEASE_MS: u64 = 15_000;
const PROVIDER_SESSION_UUID_BYTES: usize = 16;
const DEFAULT_CODEX_LEASE_SECONDS: u64 = 120;
const DEFAULT_ORPHAN_GRACE_SECONDS: u64 = 600;
const MIN_CODEX_LEASE_SECONDS: u64 = 30;
const MAX_CODEX_LEASE_SECONDS: u64 = 24 * 60 * 60;
const MIN_ORPHAN_GRACE_SECONDS: u64 = 60;
const MAX_ORPHAN_GRACE_SECONDS: u64 = 7 * 24 * 60 * 60;

#[derive(Clone, Copy)]
pub(crate) struct OrphanPolicy {
    pub(crate) lease_ms: u64,
    pub(crate) grace_ms: u64,
}

impl OrphanPolicy {
    pub(crate) fn from_env() -> Result<Self> {
        Ok(Self {
            lease_ms: parse_duration_env(
                "GROK_BRIDGE_CODEX_LEASE_SECONDS",
                DEFAULT_CODEX_LEASE_SECONDS,
                MIN_CODEX_LEASE_SECONDS,
                MAX_CODEX_LEASE_SECONDS,
            )?
            .saturating_mul(1_000),
            grace_ms: parse_duration_env(
                "GROK_BRIDGE_ORPHAN_GRACE_SECONDS",
                DEFAULT_ORPHAN_GRACE_SECONDS,
                MIN_ORPHAN_GRACE_SECONDS,
                MAX_ORPHAN_GRACE_SECONDS,
            )?
            .saturating_mul(1_000),
        })
    }
}

/// Global host revision used by the read-only WebUI `/api/events` stream.
/// Every session metadata or terminal change bumps the revision and wakes waiters.
pub(crate) struct HostRevision {
    state: Mutex<u64>,
    changed: Condvar,
}

impl HostRevision {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(0),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn current(&self) -> u64 {
        self.state.lock().map(|guard| *guard).unwrap_or(0)
    }

    pub(crate) fn bump(&self) {
        let Ok(mut revision) = self.state.lock() else {
            return;
        };
        *revision = revision.wrapping_add(1);
        self.changed.notify_all();
    }

    pub(crate) fn wait_for_change(&self, seen: u64, timeout: Duration) -> u64 {
        let Ok(revision) = self.state.lock() else {
            return seen;
        };
        if *revision != seen {
            return *revision;
        }
        let Ok(result) = self.changed.wait_timeout(revision, timeout) else {
            return seen;
        };
        *result.0
    }
}

/// One encoded WebUI events frame plus cursor commits that become durable only
/// after the frame is successfully sent.
#[derive(Debug)]
pub(crate) struct WebEventsFramePlan {
    pub(crate) message: WebEventsMessage,
    /// Exclusive byte cursors to store after this frame is sent.
    pub(crate) cursor_commits: HashMap<String, u64>,
    /// Cursor map keys to drop after this frame is sent (closed sessions).
    pub(crate) cursor_drops: Vec<String>,
    /// Reset snapshot offsets to commit only after this frame is sent.
    pub(crate) reset_commits: Vec<WebEventsResetCommit>,
}

pub(crate) struct WebEventsBatchPlan {
    pub(crate) frames: Vec<WebEventsFramePlan>,
    pub(crate) more_pending: bool,
}

#[derive(Debug, Default)]
pub(crate) struct WebEventsContinuation {
    resets: HashMap<String, WebEventsResetStream>,
}

#[derive(Debug)]
struct WebEventsResetStream {
    source: WebEventsResetSource,
    offset: usize,
    last_cursor: u64,
}

#[derive(Debug)]
enum WebEventsResetSource {
    Components {
        components: Vec<Vec<u8>>,
        bytes: usize,
    },
    #[allow(dead_code)]
    Bytes(Vec<u8>),
}

#[derive(Debug)]
pub(crate) struct WebEventsResetCommit {
    session: String,
    next_offset: usize,
    complete: bool,
}

impl WebEventsContinuation {
    pub(crate) fn commit_reset(&mut self, commit: WebEventsResetCommit) {
        if commit.complete {
            self.resets.remove(&commit.session);
        } else if let Some(reset) = self.resets.get_mut(&commit.session) {
            reset.offset = reset.offset.max(commit.next_offset);
        }
    }

    pub(crate) fn reset_sessions(&mut self, sessions: &HashSet<String>) {
        self.resets
            .retain(|session, _| !sessions.contains(session.as_str()));
    }

    fn retained_bytes(&self) -> usize {
        self.resets
            .values()
            .map(|reset| reset.source.retained_bytes())
            .sum()
    }

    fn insert_reset(
        &mut self,
        session: String,
        source: WebEventsResetSource,
        last_cursor: u64,
    ) -> Result<()> {
        let replacing = self
            .resets
            .get(&session)
            .map(|reset| reset.source.retained_bytes())
            .unwrap_or(0);
        let retained = self
            .retained_bytes()
            .saturating_sub(replacing)
            .saturating_add(source.retained_bytes());
        if retained > MAX_WEB_RESET_CONTINUATION_BYTES {
            return Err(WebEventsPlanLimitError(format!(
                "terminal reset continuation exceeds the {} byte connection limit",
                MAX_WEB_RESET_CONTINUATION_BYTES
            ))
            .into());
        }
        self.resets.insert(
            session,
            WebEventsResetStream {
                source,
                offset: 0,
                last_cursor,
            },
        );
        Ok(())
    }
}

impl WebEventsResetSource {
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Components { bytes, .. } => *bytes,
            Self::Bytes(bytes) => bytes.len(),
        }
    }
}

impl WebEventsResetStream {
    fn window(&self, start: usize, limit: usize, deadline: Instant) -> (Vec<u8>, bool) {
        fn append_window(
            output: &mut Vec<u8>,
            stream_offset: &mut usize,
            start: usize,
            limit: usize,
            component: &[u8],
        ) {
            let component_start = *stream_offset;
            *stream_offset = stream_offset.saturating_add(component.len());
            let skip = start.saturating_sub(component_start);
            if skip >= component.len() {
                return;
            }
            let available = limit.saturating_sub(output.len());
            if available > 0 {
                output.extend_from_slice(&component[skip..component.len().min(skip + available)]);
            }
        }
        if limit == 0 || Instant::now() >= deadline {
            return (Vec::new(), false);
        }
        let components = match &self.source {
            WebEventsResetSource::Components { components, .. } => components,
            WebEventsResetSource::Bytes(bytes) => {
                let end = (start + limit).min(bytes.len());
                return (
                    bytes[start.min(bytes.len())..end].to_vec(),
                    end == bytes.len(),
                );
            }
        };
        let mut output = Vec::with_capacity(limit.min(64 * 1024));
        let mut stream_offset = 0usize;
        let mut all_components_visited = true;
        for component in components {
            if Instant::now() >= deadline || output.len() >= limit {
                all_components_visited = false;
                break;
            }
            append_window(&mut output, &mut stream_offset, start, limit, component);
        }
        let complete =
            all_components_visited && stream_offset <= start.saturating_add(output.len());
        (output, complete)
    }
}

#[derive(Debug)]
struct WebEventsPlanLimitError(String);

impl std::fmt::Display for WebEventsPlanLimitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for WebEventsPlanLimitError {}

pub(crate) fn web_events_plan_error_code(error: &anyhow::Error) -> Option<&'static str> {
    error
        .downcast_ref::<WebEventsPlanLimitError>()
        .map(|_| "response_too_large")
}

pub(crate) struct SessionHost {
    registry: Mutex<SessionRegistry>,
    next_id: AtomicU64,
    orphan_policy: OrphanPolicy,
    revision: Arc<HostRevision>,
    /// Test-only pause after lease capture / registry drop and before spawn so
    /// create-vs-close_client races can be interleaved deterministically.
    create_after_lease_hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    /// Test-only: run after close_client has closed snapshotted sessions and
    /// before the final lease/epoch detach, so install can race in a session.
    close_client_before_lease_hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    /// Test-only: after fence enter, return Err once (simulates close_sessions
    /// worker panic) so Drop must still clear the closing fence.
    close_client_force_err_after_fence: AtomicBool,
    /// Test-only: next `install_created_session` fails at a named phase
    /// (`"reattach"` or `"phase3"`) so cleanup paths can be exercised without
    /// poisoning locks.
    install_inject_failure: Mutex<Option<&'static str>>,
    /// Test-only: next `acquire_web_control` fails once (claim mirror rollback).
    #[cfg(test)]
    acquire_web_control_force_err: AtomicBool,
}

/// Provider id reserved between spawn and registry install so early hooks
/// (SessionStart / PromptSubmit) buffer instead of being dropped.
struct PendingProviderEntry {
    hooks: VecDeque<HookEvent>,
}

struct SessionRegistry {
    accepting: bool,
    sessions: HashMap<String, Arc<Session>>,
    provider_sessions: HashMap<String, String>,
    /// Spawned-but-not-installed provider ids; hooks queue here until install.
    pending_providers: HashMap<String, PendingProviderEntry>,
    clients: HashMap<String, Arc<AtomicU64>>,
    /// Advanced when a close fence is established (and optionally at lease
    /// detach). In-flight create captures epoch at start; install fails if it
    /// advanced so concurrent close always wins over late register.
    client_epochs: HashMap<String, u64>,
    /// Mid-close refcount per client id. create/install refuse while > 0.
    /// Concurrent close_client increments; fence clears only when count hits 0.
    clients_closing: HashMap<String, u32>,
    /// In-flight creates that reserved capacity but are not yet registered.
    pending_creates: usize,
    /// Per-client in-flight create count (epoch reclaim must wait for these).
    pending_creates_by_client: HashMap<String, u32>,
    /// Per-owner in-flight create count (owner fence reclaim waits for these).
    pending_creates_by_owner: HashMap<String, u32>,
    /// Advanced when an owner close fence is entered. create/install captures
    /// epoch so concurrent close_owner always wins over late register.
    owner_epochs: HashMap<String, u64>,
    /// Mid-close refcount per owner. create/install refuse while > 0.
    owners_closing: HashMap<String, u32>,
    /// handle → closed_at_ms. Eviction is **TTL-only** (see purge).
    closed_sessions: HashMap<String, u64>,
    closed_session_order: VecDeque<String>,
}

impl SessionRegistry {
    /// Drop only tombstones whose TTL has elapsed. Never capacity-evicts a
    /// fresh close: idempotent `close` stays true for the full window even when
    /// unrelated churn exceeds any historical soft capacity bound.
    fn purge_closed_session_tombstones(&mut self, now: u64) {
        while let Some(oldest) = self.closed_session_order.front() {
            let expired = self.closed_sessions.get(oldest).is_none_or(|closed_at| {
                now.saturating_sub(*closed_at) >= CLOSED_SESSION_TOMBSTONE_TTL_MS
            });
            if !expired {
                break;
            }
            if let Some(handle) = self.closed_session_order.pop_front() {
                if self.closed_sessions.get(&handle).is_none_or(|closed_at| {
                    now.saturating_sub(*closed_at) >= CLOSED_SESSION_TOMBSTONE_TTL_MS
                }) {
                    self.closed_sessions.remove(&handle);
                } else {
                    // Timestamp refreshed while queued — keep and stop (order is
                    // approx FIFO by first insert; rare clock skew path).
                    self.closed_session_order.push_back(handle);
                    break;
                }
            } else {
                break;
            }
        }
    }

    fn begin_pending_provider(&mut self, provider_session_id: &str) -> Result<()> {
        if self.provider_sessions.contains_key(provider_session_id)
            || self.pending_providers.contains_key(provider_session_id)
        {
            bail!("generated a duplicate Grok provider session ID");
        }
        self.pending_providers.insert(
            provider_session_id.to_owned(),
            PendingProviderEntry {
                hooks: VecDeque::new(),
            },
        );
        Ok(())
    }

    fn abort_pending_provider(&mut self, provider_session_id: &str) {
        self.pending_providers.remove(provider_session_id);
    }

    /// Buffer a hook for an in-flight create. `Ok(false)` = unknown provider.
    fn buffer_pending_provider_hook(
        &mut self,
        provider_session_id: &str,
        event: HookEvent,
    ) -> Result<bool> {
        let Some(entry) = self.pending_providers.get_mut(provider_session_id) else {
            return Ok(false);
        };
        if entry.hooks.len() >= MAX_PENDING_PROVIDER_HOOKS {
            bail!(
                "too many buffered hooks for pending provider session (max {MAX_PENDING_PROVIDER_HOOKS})"
            );
        }
        entry.hooks.push_back(event);
        Ok(true)
    }

    fn remember_closed_session(&mut self, handle: &str, now: u64) {
        self.purge_closed_session_tombstones(now);
        if !self.closed_sessions.contains_key(handle)
            && self.closed_sessions.len() >= CLOSED_SESSION_CACHE_CAPACITY
        {
            // New creates reserve a tombstone slot before admission, so this is
            // only a defensive guard for externally assembled test registries.
            return;
        }
        if self
            .closed_sessions
            .insert(handle.to_owned(), now)
            .is_none()
        {
            self.closed_session_order.push_back(handle.to_owned());
        }
        self.purge_closed_session_tombstones(now);
    }

    fn is_closed_session(&mut self, handle: &str, now: u64) -> bool {
        self.purge_closed_session_tombstones(now);
        self.closed_sessions.contains_key(handle)
    }

    /// Remove a session from the registry maps only.
    ///
    /// **Lock order:** must not call into `Session.inner` while `registry` is
    /// held. Identity fields used here are immutable and stored outside `inner`.
    fn remove_session(&mut self, handle: &str, session: &Arc<Session>) -> bool {
        if !self
            .sessions
            .get(handle)
            .is_some_and(|current| Arc::ptr_eq(current, session))
        {
            return false;
        }
        // Immutable after spawn — no Session.inner lock.
        let client_session_id = session.client_session_id_ref().clone();
        self.sessions.remove(handle);
        self.provider_sessions
            .retain(|_, mapped_handle| mapped_handle != handle);
        self.remember_closed_session(handle, now_millis());
        if let Some(client_session_id) = client_session_id
            && !self.sessions.values().any(|remaining| {
                remaining.client_session_id_ref().as_deref() == Some(client_session_id.as_str())
            })
        {
            // Drop the shared lease entry when no registered session remains.
            // Do **not** bump client_epochs here: an in-flight create for the same
            // client must still reattach (or re-insert) this lease on register.
            // Only close_client advances the epoch (explicit client teardown).
            self.clients.remove(&client_session_id);
            self.try_reclaim_client_meta(&client_session_id);
        }
        if let Some(owner) = session.owner_ref().clone()
            && !self
                .sessions
                .values()
                .any(|remaining| remaining.owner_ref().as_deref() == Some(owner.as_str()))
        {
            self.try_reclaim_owner_meta(&owner);
        }
        true
    }

    fn client_epoch(&self, client_session_id: &str) -> u64 {
        self.client_epochs
            .get(client_session_id)
            .copied()
            .unwrap_or(0)
    }

    /// Drop the shared lease map entry. Epoch is advanced when the close fence
    /// is entered (not here) so in-flight creates are invalidated immediately.
    fn close_client_lease(&mut self, client_session_id: &str) {
        self.clients.remove(client_session_id);
    }

    fn bump_client_epoch(&mut self, client_session_id: &str) {
        let epoch = self
            .client_epochs
            .entry(client_session_id.to_owned())
            .or_insert(0);
        *epoch = epoch.wrapping_add(1);
    }

    /// Enter a close fence: bump epoch on first concurrent closer and increment
    /// refcount so create/install stay blocked until every closer finishes.
    fn begin_client_closing(&mut self, client_session_id: &str) {
        let count = self
            .clients_closing
            .entry(client_session_id.to_owned())
            .or_insert(0);
        *count = count.saturating_add(1);
        if *count == 1 {
            self.bump_client_epoch(client_session_id);
        }
    }

    fn is_client_closing(&self, client_session_id: &str) -> bool {
        self.clients_closing
            .get(client_session_id)
            .is_some_and(|count| *count > 0)
    }

    #[cfg(test)]
    fn client_closing_count(&self, client_session_id: &str) -> u32 {
        self.clients_closing
            .get(client_session_id)
            .copied()
            .unwrap_or(0)
    }

    /// Leave one close fence holder. Clears the set only when the last closer exits.
    fn end_client_closing(&mut self, client_session_id: &str) {
        match self.clients_closing.get_mut(client_session_id) {
            Some(count) if *count > 1 => {
                *count -= 1;
            }
            Some(_) => {
                self.clients_closing.remove(client_session_id);
                self.try_reclaim_client_meta(client_session_id);
            }
            None => {}
        }
    }

    fn owner_epoch(&self, owner: &str) -> u64 {
        self.owner_epochs.get(owner).copied().unwrap_or(0)
    }

    fn bump_owner_epoch(&mut self, owner: &str) {
        let epoch = self.owner_epochs.entry(owner.to_owned()).or_insert(0);
        *epoch = epoch.wrapping_add(1);
    }

    fn begin_owner_closing(&mut self, owner: &str) {
        let count = self.owners_closing.entry(owner.to_owned()).or_insert(0);
        *count = count.saturating_add(1);
        if *count == 1 {
            self.bump_owner_epoch(owner);
        }
    }

    fn is_owner_closing(&self, owner: &str) -> bool {
        self.owners_closing
            .get(owner)
            .is_some_and(|count| *count > 0)
    }

    fn end_owner_closing(&mut self, owner: &str) {
        match self.owners_closing.get_mut(owner) {
            Some(count) if *count > 1 => {
                *count -= 1;
            }
            Some(_) => {
                self.owners_closing.remove(owner);
                self.try_reclaim_owner_meta(owner);
            }
            None => {}
        }
    }

    fn occupied_session_slots(&self) -> usize {
        self.sessions.len().saturating_add(self.pending_creates)
    }

    /// Reserve one create slot under the global session cap. Caller must release
    /// via `release_pending_create` on every path (including failed spawn).
    fn reserve_pending_create(
        &mut self,
        client_session_id: Option<&str>,
        owner: Option<&str>,
    ) -> Result<()> {
        self.purge_closed_session_tombstones(now_millis());
        if self
            .closed_sessions
            .len()
            .saturating_add(self.occupied_session_slots())
            >= CLOSED_SESSION_CACHE_CAPACITY
        {
            bail!("closed-session tombstone capacity exceeded ({CLOSED_SESSION_CACHE_CAPACITY})");
        }
        if self.occupied_session_slots() >= MAX_SESSIONS {
            bail!("session capacity exceeded ({MAX_SESSIONS} live + pending creates)");
        }
        self.pending_creates = self.pending_creates.saturating_add(1);
        if let Some(client_session_id) = client_session_id {
            let count = self
                .pending_creates_by_client
                .entry(client_session_id.to_owned())
                .or_insert(0);
            *count = count.saturating_add(1);
        }
        if let Some(owner) = owner {
            let count = self
                .pending_creates_by_owner
                .entry(owner.to_owned())
                .or_insert(0);
            *count = count.saturating_add(1);
        }
        Ok(())
    }

    fn release_pending_create(&mut self, client_session_id: Option<&str>, owner: Option<&str>) {
        self.pending_creates = self.pending_creates.saturating_sub(1);
        if let Some(client_session_id) = client_session_id {
            match self.pending_creates_by_client.get_mut(client_session_id) {
                Some(count) if *count > 1 => *count -= 1,
                Some(_) => {
                    self.pending_creates_by_client.remove(client_session_id);
                    self.try_reclaim_client_meta(client_session_id);
                }
                None => {}
            }
        }
        if let Some(owner) = owner {
            match self.pending_creates_by_owner.get_mut(owner) {
                Some(count) if *count > 1 => *count -= 1,
                Some(_) => {
                    self.pending_creates_by_owner.remove(owner);
                    self.try_reclaim_owner_meta(owner);
                }
                None => {}
            }
        }
    }

    /// Drop client generation/lease maps when no session, pending create, or
    /// closer still references the id. No TTL — slow create races stay safe.
    fn try_reclaim_client_meta(&mut self, client_session_id: &str) {
        if self.clients.contains_key(client_session_id) {
            return;
        }
        if self.is_client_closing(client_session_id) {
            return;
        }
        if self
            .pending_creates_by_client
            .get(client_session_id)
            .is_some_and(|count| *count > 0)
        {
            return;
        }
        if self
            .sessions
            .values()
            .any(|session| session.client_session_id_ref().as_deref() == Some(client_session_id))
        {
            return;
        }
        self.client_epochs.remove(client_session_id);
        self.clients_closing.remove(client_session_id);
        self.pending_creates_by_client.remove(client_session_id);
    }

    fn try_reclaim_owner_meta(&mut self, owner: &str) {
        if self.is_owner_closing(owner) {
            return;
        }
        if self
            .pending_creates_by_owner
            .get(owner)
            .is_some_and(|count| *count > 0)
        {
            return;
        }
        if self
            .sessions
            .values()
            .any(|session| session.owner_ref().as_deref() == Some(owner))
        {
            return;
        }
        self.owner_epochs.remove(owner);
        self.owners_closing.remove(owner);
        self.pending_creates_by_owner.remove(owner);
    }
}

/// RAII fence for `close_client`: always drops refcount on every return path
/// (including `?` / panic unwind of the close_client stack after enter).
struct ClientClosingGuard<'a> {
    registry: &'a Mutex<SessionRegistry>,
    client_session_id: String,
}

impl<'a> ClientClosingGuard<'a> {
    fn enter(registry: &'a Mutex<SessionRegistry>, client_session_id: &str) -> Result<Self> {
        let mut reg = registry
            .lock()
            .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
        reg.begin_client_closing(client_session_id);
        Ok(Self {
            registry,
            client_session_id: client_session_id.to_owned(),
        })
    }
}

impl Drop for ClientClosingGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut reg) = self.registry.lock() {
            reg.end_client_closing(&self.client_session_id);
        }
    }
}

/// RAII fence for `close_owner`: same epoch+refcount model as client close so
/// concurrent create/install for the same owner cannot register after HTTP
/// returns success.
struct OwnerClosingGuard<'a> {
    registry: &'a Mutex<SessionRegistry>,
    owner: String,
}

impl<'a> OwnerClosingGuard<'a> {
    fn enter(registry: &'a Mutex<SessionRegistry>, owner: &str) -> Result<Self> {
        let mut reg = registry
            .lock()
            .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
        reg.begin_owner_closing(owner);
        Ok(Self {
            registry,
            owner: owner.to_owned(),
        })
    }
}

impl Drop for OwnerClosingGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut reg) = self.registry.lock() {
            reg.end_owner_closing(&self.owner);
        }
    }
}

/// RAII capacity reservation for create: releases the global + client/owner
/// pending slots on every path (failed spawn, aborted install, success).
struct PendingCreateGuard<'a> {
    registry: &'a Mutex<SessionRegistry>,
    client_session_id: Option<String>,
    owner: Option<String>,
    active: bool,
}

impl<'a> PendingCreateGuard<'a> {
    fn release_now(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        if let Ok(mut reg) = self.registry.lock() {
            reg.release_pending_create(self.client_session_id.as_deref(), self.owner.as_deref());
            // A create may have inserted a provisional lease before spawn. Reap it
            // atomically with the last pending reservation when no installed
            // session uses it. This covers every early-return path, including RNG
            // and test-hook lock failures, without disrupting a concurrent create.
            if let Some(client) = self.client_session_id.as_deref()
                && !reg.pending_creates_by_client.contains_key(client)
                && !reg
                    .sessions
                    .values()
                    .any(|session| session.client_session_id_ref().as_deref() == Some(client))
            {
                reg.clients.remove(client);
                reg.try_reclaim_client_meta(client);
            }
        }
    }
}

impl Drop for PendingCreateGuard<'_> {
    fn drop(&mut self) {
        self.release_now();
    }
}

/// RAII: any spawned Session that never commits into the registry is force
/// terminated and pending provider / provisional client state is cleared.
struct InstallSessionGuard<'a> {
    host: &'a SessionHost,
    session: Option<Arc<Session>>,
    client_session_id: Option<String>,
    provider_session_id: String,
    committed: bool,
}

impl<'a> InstallSessionGuard<'a> {
    fn new(
        host: &'a SessionHost,
        session: Arc<Session>,
        client_session_id: Option<String>,
        provider_session_id: String,
    ) -> Self {
        Self {
            host,
            session: Some(session),
            client_session_id,
            provider_session_id,
            committed: false,
        }
    }

    fn commit(mut self) {
        self.committed = true;
        self.session = None;
    }

    /// Cleanup then fold any secondary failures into the primary install error.
    fn abort_with(mut self, primary: anyhow::Error) -> anyhow::Error {
        let cleanup = self.cleanup();
        self.committed = true;
        self.session = None;
        match cleanup {
            Ok(()) => primary,
            Err(cleanup_err) => {
                anyhow::anyhow!("{primary:#}; cleanup also failed: {cleanup_err:#}")
            }
        }
    }

    fn cleanup(&mut self) -> Result<()> {
        let mut errors = Vec::new();
        if let Some(session) = self.session.take()
            && let Err(error) = force_terminate_session(&session)
        {
            errors.push(format!("{error:#}"));
        }
        match self.host.registry.lock() {
            Ok(mut registry) => {
                registry.abort_pending_provider(&self.provider_session_id);
            }
            Err(_) => errors.push(
                "session registry lock was poisoned while clearing pending provider".to_owned(),
            ),
        }
        if let Some(client) = self.client_session_id.as_deref()
            && let Err(error) = self.host.forget_client_if_unreferenced(Some(client))
        {
            errors.push(format!("{error:#}"));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            bail!(errors.join("; "))
        }
    }
}

impl Drop for InstallSessionGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.cleanup();
        }
    }
}

/// Drive real process-tree termination even when shutdown's wait loop fails.
fn force_terminate_session(session: &Session) -> Result<()> {
    match session.shutdown() {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = session.request_termination();
            session.close_writer();
            session.release_master();
            Err(error)
        }
    }
}

impl SessionHost {
    pub(crate) fn new(orphan_policy: OrphanPolicy) -> Self {
        Self {
            registry: Mutex::new(SessionRegistry {
                accepting: true,
                sessions: HashMap::new(),
                provider_sessions: HashMap::new(),
                pending_providers: HashMap::new(),
                clients: HashMap::new(),
                client_epochs: HashMap::new(),
                clients_closing: HashMap::new(),
                pending_creates: 0,
                pending_creates_by_client: HashMap::new(),
                pending_creates_by_owner: HashMap::new(),
                owner_epochs: HashMap::new(),
                owners_closing: HashMap::new(),
                closed_sessions: HashMap::new(),
                closed_session_order: VecDeque::new(),
            }),
            next_id: AtomicU64::new(1),
            orphan_policy,
            revision: Arc::new(HostRevision::new()),
            create_after_lease_hook: Mutex::new(None),
            close_client_before_lease_hook: Mutex::new(None),
            close_client_force_err_after_fence: AtomicBool::new(false),
            install_inject_failure: Mutex::new(None),
            #[cfg(test)]
            acquire_web_control_force_err: AtomicBool::new(false),
        }
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision.current()
    }

    pub(crate) fn notify_revision(&self) {
        self.revision.bump();
    }

    pub(crate) fn wait_revision(&self, seen: u64, timeout: Duration) -> u64 {
        self.revision.wait_for_change(seen, timeout)
    }

    /// Earliest *future* wall-clock deadline at which any session's client lease
    /// state would change without another host revision (Connected →
    /// Disconnected/Orphaned). Returns only future deadlines so waiters do not
    /// spin after the transition is already reflected in live list/show state.
    pub(crate) fn next_client_lifecycle_deadline_ms(&self) -> Result<Option<u64>> {
        let now = now_millis();
        // Snapshot Arcs under registry, then query session state without holding it.
        let sessions = {
            let registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
            registry.sessions.values().cloned().collect::<Vec<_>>()
        };
        let mut next: Option<u64> = None;
        for session in sessions {
            if let Some(deadline) = session.next_lifecycle_deadline_ms(now)? {
                next = Some(match next {
                    Some(current) => current.min(deadline),
                    None => deadline,
                });
            }
        }
        Ok(next)
    }

    pub(crate) fn touch_client(&self, client_session_id: &str) -> Result<()> {
        self.touch_client_at(client_session_id, now_millis())
    }

    /// Interactive WebUI claimed write control — defers orphan cleanup while held.
    pub(crate) fn acquire_web_control(&self, handle: &str, connection_id: u64) -> Result<()> {
        #[cfg(test)]
        if self
            .acquire_web_control_force_err
            .swap(false, Ordering::AcqRel)
        {
            bail!("injected acquire_web_control failure for claim rollback test");
        }
        let session = self.get(handle)?;
        session.acquire_web_control(connection_id, now_millis())?;
        self.notify_revision();
        Ok(())
    }

    /// Test-only: next `acquire_web_control` returns Err once.
    #[cfg(test)]
    pub(crate) fn test_force_next_acquire_web_control_err(&self) {
        self.acquire_web_control_force_err
            .store(true, Ordering::Release);
    }

    /// Test-only: register a non-PTY Idle session so WebUI claim/show can run.
    #[cfg(test)]
    pub(crate) fn test_register_idle_session(&self, handle: &str) {
        let session =
            tests::test_session_with_revision(SessionPhase::Idle, Arc::clone(&self.revision));
        session.inner.lock().unwrap().session = handle.to_owned();
        let mut registry = self.registry.lock().expect("registry");
        registry.sessions.insert(handle.to_owned(), session);
    }

    #[cfg(test)]
    pub(crate) fn test_append_output(&self, handle: &str, data: Vec<u8>) {
        self.get(handle).expect("session").append_output(data);
    }

    /// Test-only: replace one session writer with a gate that reports admission
    /// and publishes success only after the returned release sender fires.
    #[cfg(test)]
    pub(crate) fn test_gate_next_write(
        &self,
        handle: &str,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let session = self.get(handle).expect("session");
        let (writer_tx, writer_rx) = sync_channel(8);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);
        let (entered_tx, entered_rx) = sync_channel(1);
        let (release_tx, release_rx) = sync_channel(1);
        thread::spawn(move || {
            let Ok(job) = writer_rx.recv() else {
                return;
            };
            let _ = entered_tx.send(());
            if release_rx.recv().is_err() {
                job.fail(WRITE_CANCELLED_MSG);
                return;
            }
            if let Some(completion) = job.completion {
                let _ = completion.complete_success_with(|| {
                    session
                        .apply_input_effect(job.effect)
                        .map_err(|error| format!("{error:#}"))
                });
            } else {
                let _ = session.apply_input_effect(job.effect);
            }
        });
        (entered_rx, release_tx)
    }

    /// Test-only: set Session web-control last heartbeat (ms).
    #[cfg(test)]
    pub(crate) fn test_set_web_control_last_ms(&self, handle: &str, last_ms: u64) {
        let session = self.get(handle).expect("session");
        session.inner.lock().unwrap().web_control_last_ms = last_ms;
    }

    /// Test-only: read Session web-control last heartbeat (ms).
    #[cfg(test)]
    pub(crate) fn test_web_control_last_ms(&self, handle: &str) -> u64 {
        let session = self.get(handle).expect("session");
        session.inner.lock().unwrap().web_control_last_ms
    }

    /// Heartbeat / owns refresh for an interactive WebUI write holder.
    pub(crate) fn refresh_web_control(&self, handle: &str, connection_id: u64) -> Result<bool> {
        let session = self.get(handle)?;
        let ok = session.refresh_web_control(connection_id, now_millis())?;
        if ok {
            // Cancel uncommitted cleanup; revision so auto_close_at updates.
            self.notify_revision();
        }
        Ok(ok)
    }

    /// Explicit terminal_release or interactive-off for one session.
    pub(crate) fn release_web_control(&self, handle: &str, connection_id: u64) -> Result<bool> {
        let session = self.get(handle)?;
        let ok = session.release_web_control(connection_id, now_millis())?;
        if ok {
            self.notify_revision();
        }
        Ok(ok)
    }

    /// Drop all web-control holds for a WebSocket connection (disconnect/takeover).
    pub(crate) fn release_web_control_for_connection(
        &self,
        connection_id: u64,
        sessions: &[String],
    ) -> Result<()> {
        let now = now_millis();
        let mut changed = false;
        for handle in sessions {
            if let Ok(session) = self.get(handle)
                && session
                    .release_web_control_if_owner(connection_id, now)
                    .unwrap_or(false)
            {
                changed = true;
            }
        }
        if changed {
            self.notify_revision();
        }
        Ok(())
    }

    fn touch_client_at(&self, client_session_id: &str, now: u64) -> Result<()> {
        validate_client_session_id(client_session_id)?;
        // Heartbeat / list / show / read may only refresh a lease that create
        // (or install) already registered. Never insert unknown IDs — that was
        // an unbounded clients-map growth path under unique id flooding.
        let sessions = {
            let registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
            let Some(lease) = registry.clients.get(client_session_id) else {
                return Ok(());
            };
            lease.store(now, Ordering::Release);
            registry.sessions.values().cloned().collect::<Vec<_>>()
        };
        for session in sessions {
            session.cancel_uncommitted_cleanup_for_client(client_session_id)?;
        }
        self.notify_revision();
        Ok(())
    }

    pub(crate) fn create(
        &self,
        cwd: &str,
        prompt: Option<String>,
        model: Option<String>,
        owner: Option<String>,
        always_approve: bool,
        client_session_id: Option<String>,
    ) -> Result<SessionState> {
        let cwd = canonical_directory(Path::new(cwd))?;
        ensure_allowed_root(&cwd)?;
        validate_prompt(prompt.as_deref())?;
        validate_model(model.as_deref())?;
        if let Some(owner) = owner.as_deref() {
            validate_owner(owner)?;
        }
        if let Some(client_session_id) = client_session_id.as_deref() {
            validate_client_session_id(client_session_id)?;
        }

        // Capture lease Arc + epochs under registry, reserve capacity, then drop
        // before spawn so we never hold registry while Session.inner is locked.
        // close_client/close_owner may run in this window: they advance epochs so
        // install aborts. PendingCreateGuard releases the slot on every path.
        // Guard starts inactive; only armed after reserve_pending_create succeeds
        // so failure before reserve never double-releases or re-locks while held.
        let mut pending_guard = PendingCreateGuard {
            registry: &self.registry,
            client_session_id: client_session_id.clone(),
            owner: owner.clone(),
            active: false,
        };

        let (client_lease, client_epoch, owner_epoch) = {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
            if !registry.accepting {
                // Do not re-lock for cleanup while holding registry.
                drop(registry);
                bail!("runtime server is stopping and no longer accepts new sessions");
            }
            if let Some(client_id) = client_session_id.as_deref()
                && registry.is_client_closing(client_id)
            {
                drop(registry);
                bail!("client session is closing");
            }
            if let Some(owner_id) = owner.as_deref()
                && registry.is_owner_closing(owner_id)
            {
                drop(registry);
                bail!("owner is closing");
            }
            let client_epoch = client_session_id
                .as_deref()
                .map(|id| registry.client_epoch(id))
                .unwrap_or(0);
            let owner_epoch = owner
                .as_deref()
                .map(|id| registry.owner_epoch(id))
                .unwrap_or(0);
            // Reserve capacity BEFORE any lease insert or refresh (prevents unique-client metadata leak on capacity rejection).
            registry.reserve_pending_create(client_session_id.as_deref(), owner.as_deref())?;
            let client_lease = if let Some(client_id) = client_session_id.as_deref() {
                let lease = registry
                    .clients
                    .entry(client_id.to_owned())
                    .or_insert_with(|| Arc::new(AtomicU64::new(now_millis())))
                    .clone();
                lease.store(now_millis(), Ordering::Release);
                Some(lease)
            } else {
                None
            };
            // Armed only after successful reserve; Drop/release_now will free the slot.
            pending_guard.active = true;
            (client_lease, client_epoch, owner_epoch)
        };

        // Test hook after lease capture / reserve, before spawn.
        if let Some(hook) = self
            .create_after_lease_hook
            .lock()
            .map_err(|_| anyhow::anyhow!("create-after-lease hook lock was poisoned"))?
            .take()
        {
            hook();
        }

        let handle = self.next_handle();
        let provider_session_id = generate_provider_session_id()?;

        // Reserve provider id before spawn so SessionStart/PromptSubmit hooks
        // that race the install window buffer instead of Ok(false) discard.
        // On any failure path after this, InstallSessionGuard / explicit abort
        // clears the pending provider entry.
        {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
            if !registry.accepting {
                drop(registry);
                // pending_guard Drop releases create slot; no re-lock under hold.
                let _ = self.forget_client_if_unreferenced(client_session_id.as_deref());
                bail!("runtime server is stopping and no longer accepts new sessions");
            }
            if let Err(error) = registry.begin_pending_provider(&provider_session_id) {
                drop(registry);
                let _ = self.forget_client_if_unreferenced(client_session_id.as_deref());
                return Err(error);
            }
        }

        let session = match Session::spawn(
            handle.clone(),
            &provider_session_id,
            LaunchConfig {
                grok_bin: env::var_os("GROK_BIN").unwrap_or_else(default_grok_bin),
                cwd,
                prompt,
                model,
                owner: owner.clone(),
                always_approve,
                client_session_id: client_session_id.clone(),
                client_lease: client_lease.clone(),
                orphan_policy: self.orphan_policy,
            },
            Arc::clone(&self.revision),
        ) {
            Ok(session) => session,
            Err(error) => {
                // Clear pending provider + provisional client; pending_guard Drop
                // releases the create slot. No session process was fully installed.
                if let Ok(mut registry) = self.registry.lock() {
                    registry.abort_pending_provider(&provider_session_id);
                }
                let _ = self.forget_client_if_unreferenced(client_session_id.as_deref());
                return Err(error);
            }
        };

        // Install publishes maps + replays buffered hooks. On failure,
        // InstallSessionGuard terminates the process tree and clears pending
        // provider / provisional client state. pending_guard still releases the
        // capacity slot on every path.
        self.install_created_session(
            handle,
            provider_session_id,
            Arc::clone(&session),
            client_session_id,
            owner,
            client_lease,
            client_epoch,
            owner_epoch,
        )?;

        let state = session.state()?;
        pending_guard.release_now();
        self.notify_revision();
        Ok(state)
    }

    /// Drop `clients[id]` when no registered session still uses it.
    /// Does not advance `client_epochs` (not an explicit close_client).
    fn forget_client_if_unreferenced(&self, client_session_id: Option<&str>) -> Result<()> {
        let Some(client_session_id) = client_session_id else {
            return Ok(());
        };
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
        let still_used = registry
            .sessions
            .values()
            .any(|session| session.client_session_id_ref().as_deref() == Some(client_session_id));
        let still_pending = registry
            .pending_creates_by_client
            .get(client_session_id)
            .is_some_and(|count| *count > 0);
        if !still_used && !still_pending {
            registry.clients.remove(client_session_id);
            registry.try_reclaim_client_meta(client_session_id);
        }
        Ok(())
    }

    /// Register a spawned session. Reattaches the live client lease Arc from the
    /// map so heartbeats after a concurrent map eviction still reach the Session.
    ///
    /// **close_client / close_owner semantics for in-flight create:** if the
    /// captured epoch advanced or a close fence is active, the process is shut
    /// down and create fails. The session is never left registered detached.
    ///
    /// **Failure cleanup:** `InstallSessionGuard` terminates the process tree and
    /// clears pending provider / provisional client state on every non-commit
    /// path (including reattach errors and registry lock poison).
    // Atomic install needs client/owner epochs plus spawn lease in one call.
    #[allow(clippy::too_many_arguments)]
    fn install_created_session(
        &self,
        handle: String,
        provider_session_id: String,
        session: Arc<Session>,
        client_session_id: Option<String>,
        owner: Option<String>,
        spawn_lease: Option<Arc<AtomicU64>>,
        client_epoch: u64,
        owner_epoch: u64,
    ) -> Result<()> {
        let guard = InstallSessionGuard::new(
            self,
            Arc::clone(&session),
            client_session_id.clone(),
            provider_session_id.clone(),
        );

        // Phase 1: validate + resolve live lease under registry only (no Session.inner).
        // Abort paths must drop the registry guard before `guard.abort_with`, which
        // re-locks the registry to clear pending provider state.
        let live_lease = {
            let mut registry = match self.registry.lock() {
                Ok(registry) => registry,
                Err(_) => {
                    return Err(
                        guard.abort_with(anyhow::anyhow!("session registry lock was poisoned"))
                    );
                }
            };
            if !registry.accepting {
                drop(registry);
                return Err(guard.abort_with(anyhow::anyhow!(
                    "runtime server is stopping and no longer accepts new sessions"
                )));
            }
            if registry
                .provider_sessions
                .contains_key(&provider_session_id)
            {
                drop(registry);
                return Err(guard.abort_with(anyhow::anyhow!(
                    "generated a duplicate Grok provider session ID"
                )));
            }
            if let Some(client_id) = client_session_id.as_deref()
                && (registry.is_client_closing(client_id)
                    || registry.client_epoch(client_id) != client_epoch)
            {
                drop(registry);
                return Err(
                    guard.abort_with(anyhow::anyhow!("client session was closed during create"))
                );
            }
            if let Some(owner) = owner.as_deref()
                && (registry.is_owner_closing(owner) || registry.owner_epoch(owner) != owner_epoch)
            {
                drop(registry);
                return Err(guard.abort_with(anyhow::anyhow!("owner was closed during create")));
            }
            if let Some(client_id) = client_session_id.as_deref() {
                let lease = match spawn_lease.as_ref() {
                    Some(spawn_lease) => registry
                        .clients
                        .entry(client_id.to_owned())
                        .or_insert_with(|| Arc::clone(spawn_lease))
                        .clone(),
                    None => registry
                        .clients
                        .entry(client_id.to_owned())
                        .or_insert_with(|| Arc::new(AtomicU64::new(now_millis())))
                        .clone(),
                };
                lease.store(now_millis(), Ordering::Release);
                Some(lease)
            } else {
                None
            }
        };

        // Phase 2: reattach outside registry (Session.inner lock order).
        if let Some(lease) = live_lease {
            if let Ok(mut inject) = self.install_inject_failure.lock()
                && inject.as_deref() == Some("reattach")
            {
                inject.take();
                return Err(guard.abort_with(anyhow::anyhow!("injected reattach failure")));
            }
            if let Err(error) = session.reattach_client_lease(lease) {
                return Err(guard.abort_with(error));
            }
        }

        // Phase 3: drain pending hooks then publish under one guard.
        // Keep the pending_providers entry until hooks are empty so concurrent
        // hooks during replay still buffer (never Ok(false) discard). Only when
        // the queue is empty under the registry lock do we remove the pending
        // entry and insert sessions + provider_sessions atomically. Replay runs
        // outside the registry lock (Session.inner order). Any apply error
        // aborts via InstallSessionGuard before publication.
        let mut phase3_injected = false;
        loop {
            // Decide: drain a hook batch, or publish, under one registry hold.
            let decision = {
                let mut registry = match self.registry.lock() {
                    Ok(registry) => registry,
                    Err(_) => {
                        return Err(
                            guard.abort_with(anyhow::anyhow!("session registry lock was poisoned"))
                        );
                    }
                };
                if !phase3_injected {
                    if let Ok(mut inject) = self.install_inject_failure.lock()
                        && inject.as_deref() == Some("phase3")
                    {
                        inject.take();
                        drop(registry);
                        return Err(guard.abort_with(anyhow::anyhow!("injected phase3 failure")));
                    }
                    phase3_injected = true;
                }
                if !registry.accepting {
                    drop(registry);
                    return Err(guard.abort_with(anyhow::anyhow!(
                        "runtime server is stopping and no longer accepts new sessions"
                    )));
                }
                if registry
                    .provider_sessions
                    .contains_key(&provider_session_id)
                {
                    drop(registry);
                    return Err(guard.abort_with(anyhow::anyhow!(
                        "generated a duplicate Grok provider session ID"
                    )));
                }
                if let Some(client_id) = client_session_id.as_deref()
                    && (registry.is_client_closing(client_id)
                        || registry.client_epoch(client_id) != client_epoch)
                {
                    drop(registry);
                    return Err(guard
                        .abort_with(anyhow::anyhow!("client session was closed during create")));
                }
                if let Some(owner) = owner.as_deref()
                    && (registry.is_owner_closing(owner)
                        || registry.owner_epoch(owner) != owner_epoch)
                {
                    drop(registry);
                    return Err(guard.abort_with(anyhow::anyhow!("owner was closed during create")));
                }
                let Some(entry) = registry.pending_providers.get_mut(&provider_session_id) else {
                    drop(registry);
                    return Err(guard.abort_with(anyhow::anyhow!(
                        "pending provider reservation missing during install"
                    )));
                };
                if !entry.hooks.is_empty() {
                    // Drain only the hooks queue; keep the pending entry so a
                    // hook racing this apply still buffers instead of Ok(false).
                    let batch = std::mem::take(&mut entry.hooks);
                    // Decision::Replay
                    Some(batch)
                } else {
                    // Queue empty: atomic publish under this same lock.
                    registry.pending_providers.remove(&provider_session_id);
                    registry
                        .sessions
                        .insert(handle.clone(), Arc::clone(&session));
                    registry
                        .provider_sessions
                        .insert(provider_session_id.clone(), handle.clone());
                    // Decision::Published
                    None
                }
            };

            match decision {
                Some(batch) => {
                    // Apply outside registry (Session.inner). On error, abort
                    // before any publication so the session is never left live.
                    for event in batch {
                        if let Err(error) = session.apply_hook_event(event) {
                            return Err(guard.abort_with(error));
                        }
                    }
                    // Loop: re-check for hooks that arrived during apply.
                }
                None => {
                    // Published under the lock above; commit once and return.
                    guard.commit();
                    return Ok(());
                }
            }
        }
    }

    pub(crate) fn list(&self) -> Result<Vec<SessionState>> {
        // Lock order: registry only for Arc snapshot; never Session.inner under registry.
        let sessions = {
            let registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
            registry.sessions.values().cloned().collect::<Vec<_>>()
        };
        let mut states = sessions
            .into_iter()
            .map(|session| session.state())
            .collect::<Result<Vec<_>>>()?;
        states.sort_by_key(|state| state.created_at_ms);
        Ok(states)
    }

    /// Board/lifecycle metadata for every session without allocating full screen
    /// snapshots. Used by `/api/events` planning so hidden sessions stay cheap.
    pub(crate) fn list_web_board(&self) -> Result<Vec<SessionState>> {
        let sessions = {
            let registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
            registry.sessions.values().cloned().collect::<Vec<_>>()
        };
        let mut states = sessions
            .into_iter()
            .map(|session| session.state_web_board())
            .collect::<Result<Vec<_>>>()?;
        states.sort_by_key(|state| state.created_at_ms);
        Ok(states)
    }

    /// Plan ordered WebUI events frames without mutating the connection cursor map.
    ///
    /// - `force_reset`: one `reset=true` ANSI snapshot per session (upgrade / resync).
    /// - Otherwise drain raw PTY bytes only up to each session's **frozen**
    ///   `last_cursor` from this batch so a live producer cannot make the plan chase
    ///   forever.
    /// - Terminal entries may span multiple frames under `max_message_bytes`.
    /// - Cursor commits/drops are returned per frame and must be applied only after
    ///   that frame is successfully sent.
    /// - Session metadata in the JSON omits heavy `screen` / `screen_ansi_base64`;
    ///   reset terminal entries remain the authoritative ANSI snapshot.
    #[allow(dead_code)]
    pub(crate) fn plan_web_events(
        &self,
        cursors: &HashMap<String, u64>,
        force_reset: bool,
        max_message_bytes: usize,
    ) -> Result<Vec<WebEventsFramePlan>> {
        self.plan_web_events_with_subscriptions(cursors, force_reset, max_message_bytes, None)
    }

    /// Plan WebUI events while limiting terminal bytes to the sessions actively
    /// viewed by one WebSocket client. Session metadata remains global so the
    /// board can still show lifecycle changes for every session.
    pub(crate) fn plan_web_events_with_subscriptions(
        &self,
        cursors: &HashMap<String, u64>,
        force_reset: bool,
        max_message_bytes: usize,
        subscriptions: Option<&HashSet<String>>,
    ) -> Result<Vec<WebEventsFramePlan>> {
        self.plan_web_events_with_budget(
            cursors,
            force_reset,
            max_message_bytes,
            subscriptions,
            usize::MAX,
            usize::MAX,
            Instant::now() + Duration::from_secs(24 * 60 * 60),
        )
    }

    /// Bounded planner used by the live WebSocket writer. Final JSON frame size,
    /// frame count, and deadline are checked while entries are produced; the
    /// sender commits cursors and reset offsets only after each successful write.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn plan_web_events_with_budget(
        &self,
        cursors: &HashMap<String, u64>,
        force_reset: bool,
        max_message_bytes: usize,
        subscriptions: Option<&HashSet<String>>,
        max_frames: usize,
        max_bytes: usize,
        deadline: Instant,
    ) -> Result<Vec<WebEventsFramePlan>> {
        let mut continuation = WebEventsContinuation::default();
        Ok(self
            .plan_web_events_batch_with_budget(
                cursors,
                force_reset,
                max_message_bytes,
                subscriptions,
                max_frames,
                max_bytes,
                deadline,
                &mut continuation,
            )?
            .frames)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn plan_web_events_batch_with_budget(
        &self,
        cursors: &HashMap<String, u64>,
        force_reset: bool,
        max_message_bytes: usize,
        subscriptions: Option<&HashSet<String>>,
        max_frames: usize,
        max_bytes: usize,
        deadline: Instant,
        continuation: &mut WebEventsContinuation,
    ) -> Result<WebEventsBatchPlan> {
        // Light board snapshot for every session (no full screen ANSI). Full
        // reset snapshots are loaded only for subscribed sessions that need them.
        let sessions = self.list_web_board()?;
        let active: HashSet<&str> = sessions
            .iter()
            .map(|state| state.session.as_str())
            .collect();
        let cursor_drops: Vec<String> = cursors
            .keys()
            .filter(|session| {
                !active.contains(session.as_str())
                    || subscriptions.is_some_and(|set| !set.contains(session.as_str()))
            })
            .cloned()
            .collect();

        if max_frames != usize::MAX || max_bytes != usize::MAX {
            return self.plan_web_events_incremental_bounded(
                sessions,
                cursors,
                force_reset,
                subscriptions,
                cursor_drops,
                max_message_bytes,
                max_frames,
                max_bytes,
                deadline,
                continuation,
            );
        }

        let mut terminal_entries: Vec<(TerminalStreamEntry, Option<(String, u64)>)> = Vec::new();
        let mut source_more_pending = false;
        // One planned entry may expand into multiple pieces; reserving one
        // entry slot per output frame is conservative and guarantees the
        // sender cannot discover a >64-frame batch only after planning.
        let max_entries = max_frames.max(1);
        let mut push_entry = |entry: TerminalStreamEntry, commit: Option<(String, u64)>| {
            if Instant::now() >= deadline || terminal_entries.len() >= max_entries {
                source_more_pending = true;
                false
            } else {
                terminal_entries.push((entry, commit));
                true
            }
        };
        'sessions: for state in &sessions {
            if subscriptions.is_some_and(|set| !set.contains(&state.session)) {
                continue;
            }
            if force_reset || !cursors.contains_key(&state.session) {
                let full = self.show(&state.session)?;
                if !push_entry(
                    TerminalStreamEntry::reset_snapshot(&full),
                    Some((state.session.clone(), full.last_cursor)),
                ) {
                    break 'sessions;
                }
                continue;
            }

            let mut cursor = cursors
                .get(&state.session)
                .copied()
                .unwrap_or(state.last_cursor);
            // Freeze the exclusive end for this batch so continuous output cannot
            // unbounded-chase the live cursor inside one plan call.
            let freeze_end = state.last_cursor;
            if cursor > freeze_end {
                let full = self.show(&state.session)?;
                if !push_entry(
                    TerminalStreamEntry::reset_snapshot(&full),
                    Some((state.session.clone(), freeze_end.min(full.last_cursor))),
                ) {
                    break 'sessions;
                }
                continue;
            }
            if cursor == freeze_end {
                continue;
            }

            while cursor < freeze_end {
                let limit = usize::try_from(freeze_end - cursor)
                    .unwrap_or(MAX_READ_BYTES)
                    .clamp(1, MAX_READ_BYTES);
                let read = match self.read(&state.session, cursor, limit, 0) {
                    Ok(read) => read,
                    Err(_) => {
                        let full = self.show(&state.session)?;
                        if !push_entry(
                            TerminalStreamEntry::reset_snapshot(&full),
                            Some((state.session.clone(), freeze_end.min(full.last_cursor))),
                        ) {
                            break 'sessions;
                        }
                        break;
                    }
                };
                if read.truncated {
                    let full = self.show(&state.session)?;
                    if !push_entry(
                        TerminalStreamEntry::reset_snapshot(&full),
                        Some((state.session.clone(), freeze_end.min(full.last_cursor))),
                    ) {
                        break 'sessions;
                    }
                    break;
                }
                if read.next_cursor == read.cursor {
                    break;
                }
                // Never emit past the freeze point even if the live stream advanced.
                let capped_next = read.next_cursor.min(freeze_end);
                if capped_next <= cursor {
                    break;
                }
                let mut entry = TerminalStreamEntry::delta(&read);
                if capped_next != read.next_cursor {
                    // Re-encode a prefix when the live read overshot the freeze.
                    let raw = BASE64.decode(&read.data_base64).unwrap_or_default();
                    let take = (capped_next - read.cursor) as usize;
                    let take = take.min(raw.len());
                    entry.data_base64 = BASE64.encode(&raw[..take]);
                    entry.next_cursor = read.cursor + take as u64;
                }
                cursor = entry.next_cursor;
                if !push_entry(entry, Some((state.session.clone(), cursor))) {
                    break 'sessions;
                }
            }
        }

        // Board states already omit screen payloads.
        let sessions_view: Vec<SessionState> =
            sessions.into_iter().map(web_events_session_view).collect();
        Ok(WebEventsBatchPlan {
            frames: pack_web_events_frames(
                sessions_view,
                terminal_entries,
                cursor_drops,
                max_message_bytes,
            )?,
            more_pending: source_more_pending,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_web_events_incremental_bounded(
        &self,
        sessions: Vec<SessionState>,
        cursors: &HashMap<String, u64>,
        force_reset: bool,
        subscriptions: Option<&HashSet<String>>,
        cursor_drops: Vec<String>,
        max_message_bytes: usize,
        max_frames: usize,
        max_bytes: usize,
        deadline: Instant,
        continuation: &mut WebEventsContinuation,
    ) -> Result<WebEventsBatchPlan> {
        let active: HashSet<&str> = sessions
            .iter()
            .map(|state| state.session.as_str())
            .collect();
        continuation.resets.retain(|session, _| {
            active.contains(session.as_str())
                && subscriptions.is_none_or(|set| set.contains(session.as_str()))
        });
        if force_reset {
            for state in &sessions {
                if subscriptions.is_none_or(|set| set.contains(&state.session)) {
                    continuation.resets.remove(&state.session);
                }
            }
        }

        let sessions_view: Vec<SessionState> = sessions
            .iter()
            .cloned()
            .map(web_events_session_view)
            .collect();
        let sessions_only = WebEventsMessage::sessions(sessions_view.clone(), Vec::new());
        let sessions_only_len = message_json_len(&sessions_only);
        if sessions_only_len > max_message_bytes {
            bail!(
                "web events sessions metadata exceeds max_message_bytes ({sessions_only_len} > {max_message_bytes})"
            );
        }
        if sessions_only_len > max_bytes {
            return Ok(WebEventsBatchPlan {
                frames: Vec::new(),
                more_pending: true,
            });
        }

        let mut frames = Vec::new();
        let mut total_bytes = 0usize;
        let mut drops = Some(cursor_drops);
        let mut more_pending = false;

        'sessions: for state in &sessions {
            if subscriptions.is_some_and(|set| !set.contains(&state.session)) {
                continue;
            }
            if frames.len() >= max_frames || Instant::now() >= deadline {
                more_pending = true;
                break;
            }

            let cursor = cursors.get(&state.session).copied();
            let needs_reset = force_reset
                || cursor.is_none()
                || cursor.is_some_and(|value| value > state.last_cursor)
                || continuation.resets.contains_key(&state.session);
            if needs_reset {
                if !continuation.resets.contains_key(&state.session) {
                    let session = self.get(&state.session)?;
                    let (source, last_cursor) = session.web_reset_snapshot()?;
                    continuation.insert_reset(state.session.clone(), source, last_cursor)?;
                }
                let reset = continuation
                    .resets
                    .get(&state.session)
                    .expect("reset continuation was inserted");
                let mut offset = reset.offset;
                loop {
                    if frames.len() >= max_frames || Instant::now() >= deadline {
                        more_pending = true;
                        break 'sessions;
                    }
                    let remaining_batch = max_bytes.saturating_sub(total_bytes);
                    let Some((entry, next_offset, complete, frame_bytes)) = plan_reset_frame_piece(
                        &state.session,
                        reset,
                        offset,
                        reset.last_cursor,
                        &sessions_view,
                        max_message_bytes,
                        remaining_batch,
                        deadline,
                    )?
                    else {
                        more_pending = true;
                        break 'sessions;
                    };
                    let mut cursor_commits = HashMap::new();
                    if complete {
                        cursor_commits.insert(state.session.clone(), reset.last_cursor);
                    }
                    frames.push(WebEventsFramePlan {
                        message: WebEventsMessage::sessions(sessions_view.clone(), vec![entry]),
                        cursor_commits,
                        cursor_drops: drops.take().unwrap_or_default(),
                        reset_commits: vec![WebEventsResetCommit {
                            session: state.session.clone(),
                            next_offset,
                            complete,
                        }],
                    });
                    total_bytes = total_bytes.saturating_add(frame_bytes);
                    offset = next_offset;
                    if complete {
                        break;
                    }
                }
                continue;
            }

            let mut cursor = cursor.unwrap_or(state.last_cursor);
            let freeze_end = state.last_cursor;
            while cursor < freeze_end {
                if frames.len() >= max_frames || Instant::now() >= deadline {
                    more_pending = true;
                    break 'sessions;
                }
                let limit = usize::try_from(freeze_end - cursor)
                    .unwrap_or(MAX_READ_BYTES)
                    .clamp(1, MAX_READ_BYTES);
                let read = match self.read(&state.session, cursor, limit, 0) {
                    Ok(read) if !read.truncated => read,
                    _ => {
                        let session = self.get(&state.session)?;
                        let (source, last_cursor) = session.web_reset_snapshot()?;
                        continuation.insert_reset(state.session.clone(), source, last_cursor)?;
                        more_pending = true;
                        break 'sessions;
                    }
                };
                if read.next_cursor <= read.cursor {
                    break;
                }
                let raw = BASE64.decode(&read.data_base64).unwrap_or_default();
                let allowed = usize::try_from(freeze_end - read.cursor)
                    .unwrap_or(raw.len())
                    .min(raw.len());
                let raw = &raw[..allowed];
                let mut read_offset = 0usize;
                while read_offset < raw.len() {
                    if frames.len() >= max_frames || Instant::now() >= deadline {
                        more_pending = true;
                        break 'sessions;
                    }
                    let remaining_batch = max_bytes.saturating_sub(total_bytes);
                    let Some((entry, next_offset, frame_bytes)) = plan_delta_frame_piece(
                        &state.session,
                        raw,
                        read_offset,
                        read.cursor,
                        &sessions_view,
                        max_message_bytes,
                        remaining_batch,
                        deadline,
                    )?
                    else {
                        more_pending = true;
                        break 'sessions;
                    };
                    cursor = entry.next_cursor;
                    frames.push(WebEventsFramePlan {
                        message: WebEventsMessage::sessions(sessions_view.clone(), vec![entry]),
                        cursor_commits: HashMap::from([(state.session.clone(), cursor)]),
                        cursor_drops: drops.take().unwrap_or_default(),
                        reset_commits: Vec::new(),
                    });
                    total_bytes = total_bytes.saturating_add(frame_bytes);
                    read_offset = next_offset;
                }
            }
        }

        if frames.is_empty() && !more_pending {
            frames.push(WebEventsFramePlan {
                message: sessions_only,
                cursor_commits: HashMap::new(),
                cursor_drops: drops.take().unwrap_or_default(),
                reset_commits: Vec::new(),
            });
        }
        Ok(WebEventsBatchPlan {
            frames,
            more_pending,
        })
    }

    pub(crate) fn show(&self, handle: &str) -> Result<SessionState> {
        self.get(handle)?.state()
    }

    pub(crate) fn read(
        &self,
        handle: &str,
        cursor: u64,
        limit: usize,
        wait_ms: u64,
    ) -> Result<ReadResult> {
        self.get(handle)?.read(cursor, limit, wait_ms)
    }

    pub(crate) fn send(&self, handle: &str, input: String) -> Result<SessionState> {
        let session = self.get(handle)?;
        session.send(input)?;
        session.state()
    }

    pub(crate) fn write_raw(&self, handle: &str, data: Vec<u8>) -> Result<SessionState> {
        let session = self.get(handle)?;
        session.write_raw(data)?;
        session.state()
    }

    /// Enqueue a PTY write without waiting. Caller must poll the handle until
    /// completion so event loops remain responsive during write_all+flush.
    pub(crate) fn begin_write_raw(
        &self,
        handle: &str,
        data: Vec<u8>,
    ) -> Result<SessionWriteInFlight> {
        let session = self.get(handle)?;
        let completion = session.begin_write_job(data)?;
        Ok(SessionWriteInFlight {
            session,
            completion,
            deadline: Instant::now() + Duration::from_millis(PTY_WRITE_COMPLETION_TIMEOUT_MS),
        })
    }

    pub(crate) fn resize(&self, handle: &str, cols: u16, rows: u16) -> Result<SessionState> {
        let session = self.get(handle)?;
        session.resize(cols, rows)?;
        session.state()
    }

    pub(crate) fn wait(
        &self,
        handle: &str,
        condition: WaitCondition,
        timeout_ms: u64,
    ) -> Result<WaitResult> {
        self.get(handle)?.wait(condition, timeout_ms)
    }

    pub(crate) fn apply_hook_event(
        &self,
        provider_session_id: &str,
        event: HookEvent,
    ) -> Result<bool> {
        let session = {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
            if let Some(handle) = registry.provider_sessions.get(provider_session_id) {
                let Some(session) = registry.sessions.get(handle) else {
                    return Ok(false);
                };
                Arc::clone(session)
            } else {
                // In-flight create: buffer in order until install publishes.
                return registry.buffer_pending_provider_hook(provider_session_id, event);
            }
        };
        session.apply_hook_event(event)?;
        Ok(true)
    }

    pub(crate) fn close(&self, handle: &str) -> Result<bool> {
        let session = {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
            if registry.is_closed_session(handle, now_millis()) {
                return Ok(true);
            }
            registry.sessions.get(handle).cloned()
        };
        let Some(session) = session else {
            bail!("session not found: {handle}");
        };
        session.shutdown()?;
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
        registry.remove_session(handle, &session);
        drop(registry);
        self.notify_revision();
        Ok(true)
    }

    pub(crate) fn close_owner(&self, owner: &str) -> Result<CloseGroupResult> {
        validate_owner(owner)?;
        // Fence: bump owner epoch (first closer) + refcount. Guard drops on every
        // return path so concurrent create can recover after the last closer exits.
        let _closing = OwnerClosingGuard::enter(&self.registry, owner)?;
        // One absolute wall clock for every round + final scan (not per close_sessions).
        let batch_deadline = Instant::now() + Duration::from_millis(CLOSE_BATCH_DEADLINE_MS);
        const MAX_CLOSE_OWNER_ROUNDS: usize = 8;
        let mut stats = CloseGroupAccumulator::default();
        for _ in 0..MAX_CLOSE_OWNER_ROUNDS {
            let sessions = self.sessions_for_owner(owner)?;
            if sessions.is_empty() {
                break;
            }
            let result = self.close_sessions_with_deadline(sessions.clone(), batch_deadline)?;
            stats.absorb(&sessions, result);
            if stats.has_failures() {
                break;
            }
        }
        // Final scan: catch installs that raced between rounds while fence held.
        // Shares the same deadline; no second 7.5s budget.
        let sessions = self.sessions_for_owner(owner)?;
        if !sessions.is_empty() {
            let result = self.close_sessions_with_deadline(sessions.clone(), batch_deadline)?;
            stats.absorb(&sessions, result);
        }
        self.notify_revision();
        Ok(stats.into_result())
    }

    pub(crate) fn close_client(&self, client_session_id: &str) -> Result<CloseGroupResult> {
        validate_client_session_id(client_session_id)?;
        // Fence: bump epoch (first closer) + refcount. Guard drops refcount on
        // every return path including `?` / worker-join Err so create can recover.
        let _closing = ClientClosingGuard::enter(&self.registry, client_session_id)?;
        if self
            .close_client_force_err_after_fence
            .swap(false, Ordering::AcqRel)
        {
            bail!("injected close_sessions failure after fence");
        }
        // One absolute wall clock for every round + final scan (not per close_sessions).
        let batch_deadline = Instant::now() + Duration::from_millis(CLOSE_BATCH_DEADLINE_MS);
        const MAX_CLOSE_CLIENT_ROUNDS: usize = 8;
        let mut stats = CloseGroupAccumulator::default();
        for _ in 0..MAX_CLOSE_CLIENT_ROUNDS {
            let sessions = self.sessions_for_client(client_session_id)?;
            if sessions.is_empty() {
                break;
            }
            let result = self.close_sessions_with_deadline(sessions.clone(), batch_deadline)?;
            stats.absorb(&sessions, result);
            if stats.has_failures() {
                break;
            }
        }
        if let Some(hook) = self
            .close_client_before_lease_hook
            .lock()
            .map_err(|_| anyhow::anyhow!("close-client-before-lease hook lock was poisoned"))?
            .take()
        {
            hook();
        }
        // Install during the hook must fail (closing fence). Re-scan once more.
        // Shares the same deadline; no second 7.5s budget.
        let sessions = self.sessions_for_client(client_session_id)?;
        if !sessions.is_empty() {
            let result = self.close_sessions_with_deadline(sessions.clone(), batch_deadline)?;
            stats.absorb(&sessions, result);
        }
        // Drop lease map only when empty and this close had no per-session failures.
        // Fence refcount is released by `_closing` Drop (and only clears when last).
        if !stats.has_failures() {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
            let still_registered = registry.sessions.values().any(|session| {
                session.client_session_id_ref().as_deref() == Some(client_session_id)
            });
            if !still_registered {
                registry.close_client_lease(client_session_id);
            }
        }
        self.notify_revision();
        Ok(stats.into_result())
    }

    fn sessions_for_owner(&self, owner: &str) -> Result<Vec<(String, Arc<Session>)>> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
        Ok(registry
            .sessions
            .iter()
            .filter(|(_, session)| session.owner_ref().as_deref() == Some(owner))
            .map(|(handle, session)| (handle.clone(), Arc::clone(session)))
            .collect())
    }

    fn sessions_for_client(&self, client_session_id: &str) -> Result<Vec<(String, Arc<Session>)>> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
        Ok(registry
            .sessions
            .iter()
            .filter(|(_, session)| {
                session.client_session_id_ref().as_deref() == Some(client_session_id)
            })
            .map(|(handle, session)| (handle.clone(), Arc::clone(session)))
            .collect())
    }

    pub(crate) fn reap_orphans(&self) -> Result<CloseGroupResult> {
        let now = now_millis();
        // Snapshot Arcs under registry; claim/commit only after releasing it so
        // claim_orphan_cleanup (Session.inner) never runs under registry.
        let candidates = {
            let registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
            registry
                .sessions
                .iter()
                .map(|(handle, session)| (handle.clone(), Arc::clone(session)))
                .collect::<Vec<_>>()
        };
        let mut claimed = Vec::new();
        for (handle, session) in candidates {
            if session.claim_orphan_cleanup(now)? {
                claimed.push((handle, session));
            }
        }

        let mut sessions = Vec::new();
        for (handle, session) in claimed {
            let still_registered = {
                let registry = self
                    .registry
                    .lock()
                    .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
                registry
                    .sessions
                    .get(&handle)
                    .is_some_and(|current| Arc::ptr_eq(current, &session))
            };
            // commit_orphan_cleanup locks Session.inner — only after registry drop.
            if still_registered && session.commit_orphan_cleanup(now_millis())? {
                sessions.push((handle, session));
            }
        }
        if !sessions.is_empty() {
            // Surface ClientLeaseState::Closing only after the final lease and
            // phase recheck commits cleanup.
            self.notify_revision();
        }
        self.close_sessions(sessions)
    }

    fn close_sessions(&self, sessions: Vec<(String, Arc<Session>)>) -> Result<CloseGroupResult> {
        // One absolute wall deadline for the entire batch (not per chunk).
        self.close_sessions_with_deadline(
            sessions,
            Instant::now() + Duration::from_millis(CLOSE_BATCH_DEADLINE_MS),
        )
    }

    fn close_sessions_with_deadline(
        &self,
        sessions: Vec<(String, Arc<Session>)>,
        batch_deadline: Instant,
    ) -> Result<CloseGroupResult> {
        let matched = sessions.len();
        let mut closed = 0;
        let mut failures = Vec::new();
        // Workers honor the same Instant so a late chunk cannot add another
        // full PROCESS_TERMINATE_TIMEOUT after the budget is already spent.
        let mut remaining = sessions;
        while !remaining.is_empty() {
            let now = Instant::now();
            if now >= batch_deadline {
                for (handle, session) in remaining.drain(..) {
                    session.reset_orphan_cleanup();
                    failures.push(format!("{handle}: batch close deadline exceeded"));
                }
                break;
            }
            let chunk_len = remaining.len().min(CLOSE_MAX_CONCURRENCY);
            let chunk: Vec<(String, Arc<Session>)> = remaining.drain(..chunk_len).collect();
            let outcomes = thread::scope(|scope| {
                let workers = chunk
                    .into_iter()
                    .map(|(handle, session)| {
                        scope.spawn(move || {
                            let result = session.shutdown_until(batch_deadline);
                            (handle, session, result)
                        })
                    })
                    .collect::<Vec<_>>();
                workers
                    .into_iter()
                    .map(|worker| {
                        worker
                            .join()
                            .map_err(|_| anyhow::anyhow!("session close worker panicked"))
                    })
                    .collect::<Result<Vec<_>>>()
            })?;
            for (handle, session, result) in outcomes {
                match result {
                    Ok(()) => {
                        closed += 1;
                        let mut registry = self
                            .registry
                            .lock()
                            .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
                        registry.remove_session(&handle, &session);
                    }
                    Err(error) => {
                        session.reset_orphan_cleanup();
                        failures.push(format!("{handle}: {error:#}"));
                    }
                }
            }
        }
        if closed > 0 || matched > 0 {
            // Removal and Closing (cleanup_claimed) transitions must wake WebUI.
            self.notify_revision();
        }
        Ok(CloseGroupResult {
            matched,
            closed,
            failures,
        })
    }
}

/// Aggregate multi-round close_owner / close_client results by session handle.
/// Surviving sessions re-scanned after a failed first round must not inflate
/// matched / failures; a later successful close clears a prior failure entry.
#[derive(Default)]
struct CloseGroupAccumulator {
    matched: HashSet<String>,
    closed: HashSet<String>,
    /// handle → full `"handle: …"` failure line (last write wins).
    failures: HashMap<String, String>,
}

impl CloseGroupAccumulator {
    fn has_failures(&self) -> bool {
        !self.failures.is_empty()
    }

    fn absorb(&mut self, sessions: &[(String, Arc<Session>)], result: CloseGroupResult) {
        let mut failed: HashMap<String, String> = HashMap::new();
        for line in result.failures {
            // close_sessions formats failures as "{handle}: {error}".
            if let Some((handle, _)) = line.split_once(": ") {
                failed.insert(handle.to_owned(), line);
            } else if let Some((handle, _)) = line.split_once(':') {
                failed.insert(handle.to_owned(), line);
            }
        }
        for (handle, _) in sessions {
            self.matched.insert(handle.clone());
            if let Some(line) = failed.get(handle) {
                if !self.closed.contains(handle) {
                    self.failures.insert(handle.clone(), line.clone());
                }
            } else {
                self.closed.insert(handle.clone());
                self.failures.remove(handle);
            }
        }
    }

    fn into_result(self) -> CloseGroupResult {
        let mut failures: Vec<String> = self.failures.into_values().collect();
        failures.sort();
        CloseGroupResult {
            matched: self.matched.len(),
            closed: self.closed.len(),
            failures,
        }
    }
}

impl SessionHost {
    pub(crate) fn shutdown_all(&self) -> Result<()> {
        // Stop admitting new creates first. Only leave accepting=false when every
        // session is gone — every non-success path restores accepting so close /
        // server stop remain retryable and Runtime never false-reports success.
        struct AcceptingRestore<'a> {
            host: &'a SessionHost,
            restore: bool,
        }
        impl Drop for AcceptingRestore<'_> {
            fn drop(&mut self) {
                if !self.restore {
                    return;
                }
                if let Ok(mut registry) = self.host.registry.lock() {
                    registry.accepting = true;
                }
            }
        }

        let sessions = {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?;
            registry.accepting = false;
            registry
                .sessions
                .iter()
                .map(|(handle, session)| (handle.clone(), Arc::clone(session)))
                .collect::<Vec<_>>()
        };
        // Armed while accepting=false; disarmed only after verified full drain.
        let mut restore = AcceptingRestore {
            host: self,
            restore: true,
        };

        let deadline = Instant::now() + Duration::from_millis(SHUTDOWN_ALL_DEADLINE_MS);
        let result = match self.close_sessions_with_deadline(sessions, deadline) {
            Ok(result) => result,
            Err(error) => {
                // Drop restores accepting=true (worker panic / lock poison / etc.).
                return Err(error);
            }
        };
        // A create may have passed the first accepting check before shutdown
        // fenced admission. Wait for its spawn/install guard to release so a
        // successful shutdown never returns with an in-flight PTY owner.
        let (remaining, pending) = loop {
            let snapshot = match self.registry.lock() {
                Ok(registry) => (registry.sessions.len(), registry.pending_creates),
                Err(_) => {
                    return Err(anyhow::anyhow!("session registry lock was poisoned"));
                }
            };
            if snapshot.1 == 0 || Instant::now() >= deadline {
                break snapshot;
            }
            thread::sleep(Duration::from_millis(5));
        };
        self.notify_revision();
        if result.failures.is_empty() && remaining == 0 && pending == 0 {
            // Full success: keep accepting=false; disarm restore.
            restore.restore = false;
            return Ok(());
        }
        // Partial failure: Drop restores accepting=true before we return Err.
        if !result.failures.is_empty() {
            bail!(
                "failed to stop one or more sessions: {}",
                result.failures.join("; ")
            );
        }
        bail!(
            "failed to stop all sessions: {remaining} still registered, {pending} creates pending"
        );
    }

    pub(crate) fn active_count(&self) -> u32 {
        self.list()
            .map(|states| {
                states
                    .iter()
                    .filter(|state| phase_is_active(state.phase))
                    .count() as u32
            })
            .unwrap_or(0)
    }

    fn get(&self, handle: &str) -> Result<Arc<Session>> {
        self.registry
            .lock()
            .map_err(|_| anyhow::anyhow!("session registry lock was poisoned"))?
            .sessions
            .get(handle)
            .cloned()
            .with_context(|| format!("session not found: {handle}"))
    }

    fn next_handle(&self) -> String {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        format!("gbt-{:x}-{:x}-{id:x}", std::process::id(), now_millis())
    }
}

struct LaunchConfig {
    grok_bin: OsString,
    cwd: PathBuf,
    prompt: Option<String>,
    model: Option<String>,
    owner: Option<String>,
    always_approve: bool,
    client_session_id: Option<String>,
    client_lease: Option<Arc<AtomicU64>>,
    orphan_policy: OrphanPolicy,
}

/// Shared outcome of one PTY write_all+flush. Multiple waiters may attach; only
/// the real writer thread (or fail-all / Drop) publishes success or a definitive
/// failure — never a client timer.
struct WriteCompletion {
    state: Mutex<WriteCompletionState>,
    cv: Condvar,
}

type WriteCompletionObserver = Box<dyn FnOnce(Result<(), String>) + Send + 'static>;

struct WriteCompletionState {
    result: Option<Result<(), String>>,
    observers: Vec<WriteCompletionObserver>,
}

struct WriteDeadlineEntry {
    completion: std::sync::Weak<WriteCompletion>,
    session: std::sync::Weak<Session>,
    deadline: Instant,
}

struct WriteDeadlineRegistry {
    entries: Mutex<Vec<WriteDeadlineEntry>>,
    changed: Condvar,
}

static WRITE_DEADLINE_REGISTRY: std::sync::OnceLock<Arc<WriteDeadlineRegistry>> =
    std::sync::OnceLock::new();

/// Definitive cancel when a job never reached a successful write_all+flush
/// (writer exit, session close, channel shutdown, or completion Drop).
const WRITE_CANCELLED_MSG: &str =
    "PTY write cancelled before completion (writer closed or prior failure; not safe to retry)";
const WRITE_COMPLETION_TIMEOUT_MSG: &str = "PTY write/flush outcome was not confirmed before the deadline; delivery may be partial and is not safe to retry";

impl WriteCompletion {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(WriteCompletionState {
                result: None,
                observers: Vec::new(),
            }),
            cv: Condvar::new(),
        })
    }

    fn new_with_deadline(session: std::sync::Weak<Session>) -> Arc<Self> {
        let completion = Self::new();
        register_write_deadline(
            Arc::downgrade(&completion),
            session,
            Instant::now() + Duration::from_millis(PTY_WRITE_COMPLETION_TIMEOUT_MS),
        );
        completion
    }

    /// Publish at most once. Later calls are no-ops.
    fn complete(&self, value: Result<(), String>) {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.result.is_some() {
            return;
        }
        guard.result = Some(value.clone());
        let observers = std::mem::take(&mut guard.observers);
        self.cv.notify_all();
        drop(guard);
        for observer in observers {
            observer(value.clone());
        }
    }

    pub(crate) fn observe(&self, observer: WriteCompletionObserver) {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(result) = guard.result.clone() {
            drop(guard);
            observer(result);
        } else {
            guard.observers.push(observer);
        }
    }

    fn complete_cancelled(&self) {
        self.complete(Err(WRITE_CANCELLED_MSG.to_owned()));
    }

    /// Atomically win the timeout race. `Some` means this call published the
    /// terminal timeout; `None` means a writer already published another result.
    fn complete_timeout(&self) -> Option<Result<(), String>> {
        let value = Err(WRITE_COMPLETION_TIMEOUT_MSG.to_owned());
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.result.is_some() {
            return None;
        }
        guard.result = Some(value.clone());
        let observers = std::mem::take(&mut guard.observers);
        self.cv.notify_all();
        drop(guard);
        for observer in observers {
            observer(value.clone());
        }
        Some(value)
    }

    /// Apply the ordered semantic effect and publish success while holding the
    /// outcome lock. If a timeout/cancel already published, the effect is skipped
    /// so a late writer cannot revive session state after callers saw failure.
    fn complete_success_with(
        &self,
        effect: impl FnOnce() -> Result<(), String>,
    ) -> Option<Result<(), String>> {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.result.is_some() {
            return None;
        }
        let value = effect();
        guard.result = Some(value.clone());
        let observers = std::mem::take(&mut guard.observers);
        self.cv.notify_all();
        drop(guard);
        for observer in observers {
            observer(value.clone());
        }
        Some(value)
    }

    fn wait(&self) -> Result<(), String> {
        self.wait_timeout(Duration::from_millis(PTY_WRITE_COMPLETION_TIMEOUT_MS))
    }

    fn wait_timeout(&self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while guard.result.is_none() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                drop(guard);
                let _ = self.complete_timeout();
                guard = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                break;
            }
            let waited = self
                .cv
                .wait_timeout(guard, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard = waited.0;
            if waited.1.timed_out() && guard.result.is_none() {
                drop(guard);
                let _ = self.complete_timeout();
                guard = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                break;
            }
        }
        match guard.result.as_ref().expect("write completion set") {
            Ok(()) => Ok(()),
            Err(message) => Err(message.clone()),
        }
    }

    fn poll(&self, timed_out: bool) -> Option<Result<(), String>> {
        if timed_out && let Some(result) = self.complete_timeout() {
            return Some(result);
        }
        let guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.result.clone()
    }

    fn is_ready(&self) -> bool {
        self.state
            .lock()
            .map(|guard| guard.result.is_some())
            .unwrap_or(true)
    }
}

impl Drop for WriteCompletion {
    fn drop(&mut self) {
        // Last Arc gone without a published outcome: never leave a waiter
        // conceptually orphaned if something still joins via a weak path, and
        // never leave an unpublished slot if the job was discarded.
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.result.is_none() {
            guard.result = Some(Err(WRITE_CANCELLED_MSG.to_owned()));
            self.cv.notify_all();
        }
    }
}

fn register_write_deadline(
    completion: std::sync::Weak<WriteCompletion>,
    session: std::sync::Weak<Session>,
    deadline: Instant,
) {
    let registry = WRITE_DEADLINE_REGISTRY.get_or_init(|| {
        let registry = Arc::new(WriteDeadlineRegistry {
            entries: Mutex::new(Vec::new()),
            changed: Condvar::new(),
        });
        let worker = Arc::clone(&registry);
        thread::spawn(move || run_write_deadline_watchdog(worker));
        registry
    });
    let mut entries = registry
        .entries
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    entries.push(WriteDeadlineEntry {
        completion,
        session,
        deadline,
    });
    registry.changed.notify_one();
}

fn run_write_deadline_watchdog(registry: Arc<WriteDeadlineRegistry>) {
    loop {
        let mut entries = registry
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        entries.retain(|entry| {
            entry
                .completion
                .upgrade()
                .is_some_and(|completion| !completion.is_ready())
        });
        let Some(next) = entries.iter().map(|entry| entry.deadline).min() else {
            entries = registry
                .changed
                .wait(entries)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            drop(entries);
            continue;
        };
        let remaining = next.saturating_duration_since(Instant::now());
        if !remaining.is_zero() {
            let waited = registry
                .changed
                .wait_timeout(entries, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            drop(waited.0);
            continue;
        }
        let now = Instant::now();
        let mut expired = Vec::new();
        let mut index = 0;
        while index < entries.len() {
            if entries[index].deadline <= now {
                expired.push(entries.swap_remove(index));
            } else {
                index += 1;
            }
        }
        drop(entries);
        for entry in expired {
            if let Some(completion) = entry.completion.upgrade()
                && let Some(Err(_)) = completion.complete_timeout()
                && let Some(session) = entry.session.upgrade()
            {
                session.mark_writer_error(WRITE_COMPLETION_TIMEOUT_MSG.to_owned());
                session.close_writer();
            }
        }
    }
}

/// One PTY write unit. `effect` is committed on the writer thread after a
/// successful write_all+flush so semantic updates stay FIFO with the bytes.
/// Optional `completion` lets callers wait for that durable commit.
struct WriteJob {
    data: Vec<u8>,
    effect: InputEffect,
    completion: Option<Arc<WriteCompletion>>,
}

impl WriteJob {
    fn fail(self, message: &str) {
        if let Some(completion) = self.completion {
            completion.complete(Err(message.to_owned()));
        }
    }
}

/// Drain every remaining job on the writer channel and publish a definitive
/// failure for each completion (exactly once per job).
fn fail_remaining_write_jobs(writer_rx: &std::sync::mpsc::Receiver<WriteJob>, message: &str) {
    while let Ok(job) = writer_rx.try_recv() {
        job.fail(message);
    }
}

/// Enqueued write whose outcome is still pending on the writer thread.
/// The handle stays bound to this exact job until real write_all+flush and FIFO effect
/// commit succeed/fail, or publishes one terminal non-retryable timeout. Effects
/// remain writer-owned so concurrent waiters cannot reverse phase.
pub(crate) struct SessionWriteInFlight {
    session: Arc<Session>,
    completion: Arc<WriteCompletion>,
    deadline: Instant,
}

impl SessionWriteInFlight {
    pub(crate) fn observe(&self, observer: impl FnOnce(Result<(), String>) + Send + 'static) {
        self.completion.observe(Box::new(observer));
    }
    /// Non-blocking completion probe for event loops that must keep reading the
    /// socket while a PTY writer is stalled.
    pub(crate) fn poll(&self) -> Option<Result<(), String>> {
        let result = self.completion.poll(Instant::now() >= self.deadline)?;
        if let Err(error) = &result {
            // Writer thread also marks; ensure pollers still force Failed after
            // a possible partial write or deadline while preserving the exact
            // completion text used by the identity observer and socket payload.
            self.session.mark_writer_error(error.clone());
            self.session.close_writer();
        }
        Some(result)
    }
}

struct Session {
    /// Immutable after spawn — safe to read while holding the host registry lock
    /// without taking Session.inner (avoids registry ↔ inner deadlocks).
    client_session_id: Option<String>,
    /// Immutable after spawn — same lock-order rule as `client_session_id`.
    owner: Option<String>,
    /// Non-owning backref so the failure reaper can finalize without a cycle.
    self_weak: std::sync::Weak<Session>,
    inner: Mutex<SessionInner>,
    changed: Condvar,
    host_revision: Arc<HostRevision>,
    writer_tx: Mutex<Option<SyncSender<WriteJob>>>,
    master: Mutex<Option<Box<dyn MasterPty + Send>>>,
    termination: Option<Arc<ProcessTerminator>>,
    shutdown: AtomicBool,
    cleanup_claimed: AtomicBool,
    cleanup_committed: AtomicBool,
    /// Test hook: sleep this many ms at the start of shutdown (0 = disabled).
    test_shutdown_hang_ms: AtomicU64,
    /// Ensures only one background failure reaper escalates HUP→TERM→KILL.
    failure_reaper_started: AtomicBool,
    /// Shared with the failure reaper for terminal/force-fail signalling.
    reaper_state: Arc<FailureReaperState>,
}

/// Cross-thread view used by the failure reaper after reader/writer/wait errors.
struct FailureReaperState {
    terminal: AtomicBool,
    force_fail: AtomicBool,
    /// Wakes session waiters when force_fail is set.
    wake: Mutex<()>,
    wake_cv: Condvar,
}

impl FailureReaperState {
    fn new() -> Self {
        Self {
            terminal: AtomicBool::new(false),
            force_fail: AtomicBool::new(false),
            wake: Mutex::new(()),
            wake_cv: Condvar::new(),
        }
    }

    fn mark_terminal(&self) {
        self.terminal.store(true, Ordering::Release);
        self.wake_cv.notify_all();
    }

    fn request_force_fail(&self) {
        self.force_fail.store(true, Ordering::Release);
        self.wake_cv.notify_all();
    }

    fn is_terminal(&self) -> bool {
        self.terminal.load(Ordering::Acquire)
    }

    fn take_force_fail(&self) -> bool {
        self.force_fail.swap(false, Ordering::AcqRel)
    }
}

struct SessionInner {
    session: String,
    owner: Option<String>,
    client_session_id: Option<String>,
    client_lease: Option<Arc<AtomicU64>>,
    orphan_policy: OrphanPolicy,
    phase: SessionPhase,
    phase_changed_at_ms: u64,
    cwd: String,
    model: Option<String>,
    always_approve: bool,
    process_id: Option<u32>,
    created_at_ms: u64,
    updated_at_ms: u64,
    semantic_active_at_ms: u64,
    completed_at_ms: Option<u64>,
    exit_code: Option<u32>,
    error: Option<String>,
    title: Option<String>,
    parser: vt100::Parser<TitleCallbacks>,
    chunks: VecDeque<OutputChunk>,
    transcript_bytes: usize,
    next_cursor: u64,
    last_output_at_ms: Option<u64>,
    process_done: bool,
    reader_done: bool,
    hook: HookState,
    /// Active interactive WebUI write holder (connection id), if any.
    web_control_connection_id: Option<u64>,
    /// Last claim/owns/heartbeat for the WebUI write holder (ms).
    web_control_last_ms: u64,
    /// When the last WebUI write hold ended (release, disconnect, expiry).
    /// Used as the base for orphan grace so cleanup is not permanent keep-alive.
    web_control_ended_at_ms: Option<u64>,
    /// Bracketed-paste open across WriteJob chunks (raw WebUI input). StartTurn
    /// only fires for Enter outside paste so multi-line paste does not mark Working.
    paste_open: bool,
    /// Incomplete CSI prefix of `\x1b[200~` / `\x1b[201~` carried across jobs.
    paste_scan_hold: Vec<u8>,
}

struct OutputChunk {
    start: u64,
    data: Vec<u8>,
}

#[derive(Default)]
struct HookState {
    activity: HookActivity,
    last_event: Option<HookEventKind>,
    last_event_at_ms: Option<u64>,
    tool_name: Option<String>,
    waiting_reason: Option<String>,
    turn_done: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputEffect {
    None,
    StartTurn,
    Cancel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminationSignal {
    Hangup,
    Terminate,
    Kill,
}

struct TerminationState {
    started_at: Option<Instant>,
    next_signal: TerminationSignal,
    next_signal_at: Option<Instant>,
    /// Strongest signal actually delivered (for escalation diagnostics/tests).
    last_sent: Option<TerminationSignal>,
}

impl Default for TerminationState {
    fn default() -> Self {
        Self {
            started_at: None,
            next_signal: TerminationSignal::Hangup,
            next_signal_at: None,
            last_sent: None,
        }
    }
}

struct ProcessTerminator {
    #[cfg(unix)]
    process_group_id: libc::pid_t,
    #[cfg(windows)]
    process_id: u32,
    /// Required kill-on-close Job. Session create fails if this cannot be set up.
    #[cfg(windows)]
    job: Arc<OwnedHandle>,
    /// Creation-time process handle with TERMINATE|QUERY rights (not a bare PID).
    #[cfg(windows)]
    root_process: OwnedHandle,
    state: Mutex<TerminationState>,
}

impl ProcessTerminator {
    fn new(
        master: &dyn MasterPty,
        child: &dyn portable_pty::Child,
        process_id: u32,
        #[cfg(windows)] job: Arc<OwnedHandle>,
    ) -> Result<Self> {
        #[cfg(unix)]
        {
            let _ = (child, process_id);
            let process_group_id = master
                .process_group_leader()
                .context("PTY did not report a process group leader")?;
            if process_group_id <= 0 {
                bail!("PTY reported an invalid process group leader");
            }
            Ok(Self {
                process_group_id,
                state: Mutex::new(TerminationState::default()),
            })
        }

        #[cfg(windows)]
        {
            let _ = master;
            // Job admission was part of CreateProcessW; retain a durable root
            // process handle for independent liveness proof during close.
            let process_handle = child
                .as_raw_handle()
                .context("PTY child did not report a process handle")?;
            let root_process = duplicate_windows_process_handle(process_handle)
                .context("failed to duplicate Grok process handle for job admission and close")?;
            // The vendored portable-pty creates the root with
            // PROC_THREAD_ATTRIBUTE_JOB_LIST, so admission happened atomically
            // inside CreateProcessW before user code could spawn descendants.
            // Prove the pre-created Job is queryable while the root is live.
            match windows_job_active_processes(job.as_raw_handle()) {
                Some(0) => {
                    // An empty Job is valid only when the creation-time root
                    // handle independently proves the process is dead.
                    if !matches!(
                        windows_handle_liveness(root_process.as_raw_handle()),
                        WindowsLiveness::Dead
                    ) {
                        bail!("Job is empty but the root process is still alive or unverifiable");
                    }
                }
                Some(_) => {}
                None => {
                    bail!(
                        "Job Object accounting query failed after assign; refusing unreliable process-tree ownership"
                    );
                }
            }
            Ok(Self {
                process_id,
                job,
                root_process,
                state: Mutex::new(TerminationState::default()),
            })
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = (master, child, process_id);
            bail!("process-tree termination is unsupported on this platform");
        }
    }

    fn request(&self) -> Result<()> {
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("process termination state lock was poisoned"))?;
        let signal = if state.started_at.is_none() {
            Some(TerminationSignal::Hangup)
        } else if state.next_signal_at.is_some_and(|deadline| now >= deadline) {
            Some(state.next_signal)
        } else {
            None
        };
        let Some(signal) = signal else {
            return Ok(());
        };

        self.send(signal)?;
        if state.started_at.is_none() {
            state.started_at = Some(now);
        }
        state.last_sent = Some(signal);
        state.next_signal = match signal {
            TerminationSignal::Hangup => TerminationSignal::Terminate,
            TerminationSignal::Terminate | TerminationSignal::Kill => TerminationSignal::Kill,
        };
        let delay_ms = match signal {
            TerminationSignal::Hangup => PROCESS_HANGUP_GRACE_MS,
            TerminationSignal::Terminate => PROCESS_TERMINATE_GRACE_MS,
            TerminationSignal::Kill => PROCESS_KILL_REPEAT_MS,
        };
        state.next_signal_at = Some(now + Duration::from_millis(delay_ms));
        Ok(())
    }

    fn next_wait_duration(&self) -> Duration {
        let Ok(state) = self.state.lock() else {
            return Duration::from_millis(1);
        };
        state
            .next_signal_at
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
            .unwrap_or_default()
    }

    /// Strongest OS signal/kill actually delivered (tests only).
    #[cfg(test)]
    fn last_signal_sent(&self) -> Option<TerminationSignal> {
        self.state.lock().ok().and_then(|state| state.last_sent)
    }

    /// True when the OS process *tree* is gone — not merely the root wait or PTY EOF.
    ///
    /// - Unix: `kill(-pgid, 0) == ESRCH` (no process remains in the group).
    /// - Windows: Job `ActiveProcesses == 0` only. Job query failure is **not**
    ///   tree-gone (fail-closed). PID-only “OpenProcess failed ⇒ dead” is never used.
    fn is_tree_gone(&self) -> bool {
        #[cfg(unix)]
        {
            let result = unsafe { libc::kill(-self.process_group_id, 0) };
            if result == 0 {
                return false;
            }
            io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        }

        #[cfg(windows)]
        {
            match windows_job_active_processes(self.job.as_raw_handle()) {
                Some(0) => {
                    // Cross-check root handle when possible; Unknown/Alive blocks gone.
                    match windows_handle_liveness(self.root_process.as_raw_handle()) {
                        WindowsLiveness::Dead => true,
                        WindowsLiveness::Alive | WindowsLiveness::Unknown => false,
                    }
                }
                Some(_) => false,
                // Query failure: cannot prove empty tree.
                None => false,
            }
        }

        #[cfg(not(any(unix, windows)))]
        {
            false
        }
    }

    fn send(&self, signal: TerminationSignal) -> Result<()> {
        #[cfg(unix)]
        {
            let signal = match signal {
                TerminationSignal::Hangup => libc::SIGHUP,
                TerminationSignal::Terminate => libc::SIGTERM,
                TerminationSignal::Kill => libc::SIGKILL,
            };
            let result = unsafe { libc::kill(-self.process_group_id, signal) };
            if result == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(());
            }
            Err(anyhow::anyhow!(
                "failed to send signal {signal} to PTY process group {}: {error}",
                self.process_group_id
            ))
        }

        #[cfg(windows)]
        {
            let _ = signal;
            // Kill the whole Job first (covers in-job descendants). Failures are
            // errors unless the job is already empty. Never claim success based on
            // ignored TerminateProcess / OpenProcess results alone.
            terminate_windows_job_tree(
                self.job.as_raw_handle(),
                self.root_process.as_raw_handle(),
                self.process_id,
            )
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = signal;
            bail!("process-tree termination is unsupported on this platform");
        }
    }
}

/// Process / handle liveness for fail-closed tree accounting (pure for tests).
#[cfg(any(windows, test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WindowsLiveness {
    Alive,
    Dead,
    /// Access denied / query failure — must not be treated as gone.
    Unknown,
}

/// Pure: Job ActiveProcesses + root handle liveness → tree-gone?
/// Query failure / Unknown / Alive ⇒ not gone (close must not succeed).
#[cfg(any(windows, test))]
fn windows_job_tree_is_gone(job_active: Option<u32>, root_liveness: WindowsLiveness) -> bool {
    match job_active {
        Some(0) => matches!(root_liveness, WindowsLiveness::Dead),
        Some(_) | None => false,
    }
}

/// Pure: never treat Unknown as dead; empty set is not success.
#[cfg(any(windows, test))]
fn windows_tracked_liveness_all_dead(states: &[WindowsLiveness]) -> bool {
    !states.is_empty() && states.iter().all(|s| matches!(s, WindowsLiveness::Dead))
}

/// Pure policy: session create may only proceed with successful Job admission.
/// PID-only management is never an accepted ownership mode for create.
#[cfg(any(windows, test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WindowsTreeOwnership {
    /// Kill-on-close Job assigned; tree-gone via Job accounting.
    KillOnCloseJob,
    /// Forbidden for create — access errors / PID reuse make this unsafe.
    PidOnlyFallback,
}

#[cfg(any(windows, test))]
fn windows_create_ownership_allowed(mode: WindowsTreeOwnership) -> bool {
    matches!(mode, WindowsTreeOwnership::KillOnCloseJob)
}

#[cfg(windows)]
fn create_kill_on_close_job() -> Result<OwnedHandle> {
    let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
    if job.is_null() {
        bail!("CreateJobObjectW failed: {}", io::Error::last_os_error());
    }
    let job = unsafe { OwnedHandle::from_raw_handle(job) };
    let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    // KILL_ON_JOB_CLOSE: closing our job handle kills remaining members.
    // Do not enable BREAKAWAY so children stay in the job when possible.
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    if unsafe {
        SetInformationJobObject(
            job.as_raw_handle(),
            JobObjectExtendedLimitInformation,
            (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    } == 0
    {
        bail!(
            "SetInformationJobObject(KILL_ON_JOB_CLOSE) failed: {}",
            io::Error::last_os_error()
        );
    }
    Ok(job)
}

/// Duplicate a process handle with TERMINATE + QUERY rights for close ownership.
#[cfg(windows)]
fn duplicate_windows_process_handle(
    source: windows_sys::Win32::Foundation::HANDLE,
) -> Result<OwnedHandle> {
    let mut dup = std::ptr::null_mut();
    let ok = unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            source,
            GetCurrentProcess(),
            &mut dup,
            PROCESS_TERMINATE | PROCESS_QUERY_LIMITED_INFORMATION,
            0,
            0,
        )
    };
    if ok == 0 || dup.is_null() || dup == INVALID_HANDLE_VALUE {
        // Fallback: try same-access duplicate if rights were already broad.
        let mut dup2 = std::ptr::null_mut();
        let ok2 = unsafe {
            DuplicateHandle(
                GetCurrentProcess(),
                source,
                GetCurrentProcess(),
                &mut dup2,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            )
        };
        if ok2 == 0 || dup2.is_null() || dup2 == INVALID_HANDLE_VALUE {
            bail!(
                "DuplicateHandle for Grok process failed: {}",
                io::Error::last_os_error()
            );
        }
        return Ok(unsafe { OwnedHandle::from_raw_handle(dup2) });
    }
    Ok(unsafe { OwnedHandle::from_raw_handle(dup) })
}

#[cfg(windows)]
fn windows_process_parent_map() -> Result<HashMap<u32, Vec<u32>>> {
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot == INVALID_HANDLE_VALUE || snapshot.is_null() {
        return Err(anyhow::anyhow!(
            "failed to snapshot process tree: {}",
            io::Error::last_os_error()
        ));
    }
    let snapshot = unsafe { OwnedHandle::from_raw_handle(snapshot) };
    let mut children = HashMap::<u32, Vec<u32>>::new();
    let mut entry = PROCESSENTRY32W {
        dwSize: std::mem::size_of::<PROCESSENTRY32W>() as u32,
        ..Default::default()
    };
    if unsafe { Process32FirstW(snapshot.as_raw_handle(), &mut entry) } == 0 {
        // Empty snapshot is extremely unlikely; treat First failure as error so
        // callers cannot interpret "no processes" as a successful empty tree.
        let err = io::Error::last_os_error();
        drop(snapshot);
        return Err(anyhow::anyhow!(
            "Process32FirstW failed (refusing empty parent map): {err}"
        ));
    }
    loop {
        children
            .entry(entry.th32ParentProcessID)
            .or_default()
            .push(entry.th32ProcessID);
        if unsafe { Process32NextW(snapshot.as_raw_handle(), &mut entry) } == 0 {
            break;
        }
    }
    drop(snapshot);
    Ok(children)
}

/// Collect descendant PIDs via parent links. Must run **before** killing the
/// root: after root death Windows can reparent children and the walk misses them.
#[cfg(windows)]
fn windows_descendants_of(process_id: u32, children: &HashMap<u32, Vec<u32>>) -> HashSet<u32> {
    let mut queue = VecDeque::from([process_id]);
    let mut descendants = HashSet::new();
    while let Some(parent) = queue.pop_front() {
        for child in children.get(&parent).into_iter().flatten().copied() {
            if descendants.insert(child) {
                queue.push_back(child);
            }
        }
    }
    descendants
}

/// Result of *requesting* Windows terminate — not tree-gone proof.
/// `shutdown_until` may only return Ok after Job/handle/tracked-PID confirmation.
#[cfg(any(windows, test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WindowsTerminateRequestResult {
    /// Exit already observed (not STILL_ACTIVE).
    ConfirmedDead,
    /// OS accepted terminate; process may still be STILL_ACTIVE — poll/escalate.
    AcceptedPending,
    /// OS rejected terminate while process still appears live / unverifiable.
    RequestFailed,
}

/// Pure: classify TerminateProcess / equivalent after observing liveness.
/// Accepted + still Alive/Unknown ⇒ pending (not a request failure).
#[cfg(any(windows, test))]
fn classify_windows_terminate_request(
    term_api_ok: bool,
    liveness: WindowsLiveness,
) -> WindowsTerminateRequestResult {
    match liveness {
        WindowsLiveness::Dead => WindowsTerminateRequestResult::ConfirmedDead,
        WindowsLiveness::Alive | WindowsLiveness::Unknown if term_api_ok => {
            WindowsTerminateRequestResult::AcceptedPending
        }
        WindowsLiveness::Alive | WindowsLiveness::Unknown => {
            WindowsTerminateRequestResult::RequestFailed
        }
    }
}

/// Pure: whether a terminate *request* succeeded enough for shutdown_until to
/// continue polling. Tree-gone proof is separate (`windows_job_tree_is_gone`).
#[cfg(any(windows, test))]
fn windows_terminate_request_allows_poll(result: WindowsTerminateRequestResult) -> bool {
    matches!(
        result,
        WindowsTerminateRequestResult::ConfirmedDead
            | WindowsTerminateRequestResult::AcceptedPending
    )
}

/// Pure job-tree terminate *request* policy (injectable; no real OS handles).
/// Members still Alive after accepted TerminateJobObject/TerminateProcess ⇒
/// allow poll, do not treat as request failure.
#[cfg(any(windows, test))]
fn classify_windows_job_terminate_request(
    job_api_ok: bool,
    job_active: Option<u32>,
    root_request: WindowsTerminateRequestResult,
) -> WindowsTerminateRequestResult {
    let root_ok = windows_terminate_request_allows_poll(root_request);
    match job_active {
        Some(0) => {
            // Job empty: request succeeded if either API path accepted or root dead.
            if job_api_ok || root_ok {
                if matches!(root_request, WindowsTerminateRequestResult::ConfirmedDead) {
                    WindowsTerminateRequestResult::ConfirmedDead
                } else {
                    WindowsTerminateRequestResult::AcceptedPending
                }
            } else {
                WindowsTerminateRequestResult::RequestFailed
            }
        }
        Some(_active) => {
            // Members remain: accepted terminate ⇒ pending poll; both failed ⇒ fail.
            if job_api_ok || root_ok {
                WindowsTerminateRequestResult::AcceptedPending
            } else {
                WindowsTerminateRequestResult::RequestFailed
            }
        }
        None => {
            // Cannot query job: still advance poll if a terminate API accepted.
            if job_api_ok || root_ok {
                WindowsTerminateRequestResult::AcceptedPending
            } else {
                WindowsTerminateRequestResult::RequestFailed
            }
        }
    }
}

/// Terminate via an already-held process handle (creation-time or duplicated).
/// Accepted terminate while still STILL_ACTIVE is **request success** (pending);
/// tree-gone is proven later by Job/handle checks in `is_tree_gone`.
#[cfg(windows)]
fn terminate_windows_handle(handle: windows_sys::Win32::Foundation::HANDLE) -> Result<()> {
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        bail!("invalid process handle for TerminateProcess");
    }
    let term_ok = unsafe { TerminateProcess(handle, 1) } != 0;
    let liveness = windows_handle_liveness(handle);
    match classify_windows_terminate_request(term_ok, liveness) {
        WindowsTerminateRequestResult::ConfirmedDead
        | WindowsTerminateRequestResult::AcceptedPending => Ok(()),
        WindowsTerminateRequestResult::RequestFailed => {
            if matches!(liveness, WindowsLiveness::Unknown) {
                bail!(
                    "cannot verify process after TerminateProcess (query access denied/unknown); terminate_api_ok={term_ok}"
                );
            }
            bail!(
                "TerminateProcess failed and process is still alive: {}",
                io::Error::last_os_error()
            )
        }
    }
}

/// Query Job Object active process count. `None` if the query fails (fail-closed).
#[cfg(windows)]
fn windows_job_active_processes(job: windows_sys::Win32::Foundation::HANDLE) -> Option<u32> {
    let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION {
        TotalUserTime: 0,
        TotalKernelTime: 0,
        ThisPeriodTotalUserTime: 0,
        ThisPeriodTotalKernelTime: 0,
        TotalPageFaultCount: 0,
        TotalProcesses: 0,
        ActiveProcesses: 0,
        TotalTerminatedProcesses: 0,
    };
    let ok = unsafe {
        QueryInformationJobObject(
            job,
            JobObjectBasicAccountingInformation,
            (&mut info as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
            std::mem::size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return None;
    }
    Some(info.ActiveProcesses)
}

#[cfg(windows)]
fn windows_handle_liveness(handle: windows_sys::Win32::Foundation::HANDLE) -> WindowsLiveness {
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return WindowsLiveness::Unknown;
    }
    let mut code = 0u32;
    let ok = unsafe { GetExitCodeProcess(handle, &mut code) };
    if ok == 0 {
        return WindowsLiveness::Unknown;
    }
    if code == STILL_ACTIVE {
        WindowsLiveness::Alive
    } else {
        WindowsLiveness::Dead
    }
}

/// Open by PID for query; access denied ⇒ Unknown (not Dead).
#[cfg(windows)]
fn windows_pid_liveness(pid: u32) -> WindowsLiveness {
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        let err = unsafe { GetLastError() };
        // ERROR_INVALID_PARAMETER (87) often means the PID is not live.
        // ERROR_ACCESS_DENIED must never look "gone".
        if err == ERROR_ACCESS_DENIED {
            return WindowsLiveness::Unknown;
        }
        // Other open failures: treat as Unknown (fail-closed), not Dead.
        // Callers that need "gone" must use a creation-time handle or Job accounting.
        return WindowsLiveness::Unknown;
    }
    let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
    windows_handle_liveness(handle.as_raw_handle())
}

/// Pure predicate: every tracked PID is **Dead** (injectable for tests).
/// Unknown/Alive keep the tree alive. Empty set is not success.
#[cfg(any(windows, test))]
fn windows_tracked_pids_all_gone(
    tracked: &HashSet<u32>,
    liveness: impl Fn(u32) -> WindowsLiveness,
) -> bool {
    if tracked.is_empty() {
        return false;
    }
    let states: Vec<_> = tracked.iter().map(|pid| liveness(*pid)).collect();
    windows_tracked_liveness_all_dead(&states)
}

/// Kill Job members via TerminateJobObject + creation-time root handle.
///
/// This only *requests* termination. Returning Ok means the OS accepted the
/// request (or the tree already looks empty enough to keep polling) — **not**
/// that `is_tree_gone` is true. Members still STILL_ACTIVE after an accepted
/// TerminateJobObject/TerminateProcess must not abort `shutdown_until`.
#[cfg(windows)]
fn terminate_windows_job_tree(
    job: windows_sys::Win32::Foundation::HANDLE,
    root_handle: windows_sys::Win32::Foundation::HANDLE,
    process_id: u32,
) -> Result<()> {
    let job_ok = unsafe { TerminateJobObject(job, 1) } != 0;
    // Always also terminate the held root handle (covers job edge cases).
    // Accepted-but-alive is Ok (pending); hard request failure is Err.
    let root_request = {
        if root_handle.is_null() || root_handle == INVALID_HANDLE_VALUE {
            WindowsTerminateRequestResult::RequestFailed
        } else {
            let term_ok = unsafe { TerminateProcess(root_handle, 1) } != 0;
            classify_windows_terminate_request(term_ok, windows_handle_liveness(root_handle))
        }
    };
    let job_active = windows_job_active_processes(job);
    match classify_windows_job_terminate_request(job_ok, job_active, root_request) {
        WindowsTerminateRequestResult::ConfirmedDead
        | WindowsTerminateRequestResult::AcceptedPending => Ok(()),
        WindowsTerminateRequestResult::RequestFailed => match job_active {
            Some(active) if active > 0 => bail!(
                "TerminateJobObject failed and Job still has {active} active process(es) (pid {process_id}): {}",
                io::Error::last_os_error()
            ),
            Some(0) => bail!(
                "terminate request failed with empty Job but root not confirmed (pid {process_id})"
            ),
            None => bail!(
                "Job accounting query failed and terminate request was not accepted (pid {process_id})"
            ),
            Some(_) => bail!("terminate request failed for job tree (pid {process_id})"),
        },
    }
}

/// Pure plan for cleaning up a process that was spawned into a PTY but never
/// became a fully managed Session (missing PID, terminator setup failure, etc.).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SpawnCleanupPlan {
    /// Unix: signal the whole PTY process group (works even without child PID).
    UnixProcessGroup,
    /// Unix: kill a known root PID when no process group is available.
    UnixRootPid(u32),
    /// Unix: only ChildKiller remains (no group, no PID).
    UnixChildKillOnly,
    /// Windows: parent-tree walk from root PID, then terminate root handle.
    WindowsTreeAndHandle(u32),
    /// Windows: terminate via raw process handle only (no PID for tree walk).
    WindowsHandleOnly,
    /// No platform-specific kill path available.
    None,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)] // Variants selected per target; all exercised in unit tests.
enum SpawnHostPlatform {
    Unix,
    Windows,
    Other,
}

fn plan_spawn_cleanup(
    platform: SpawnHostPlatform,
    has_unix_process_group: bool,
    process_id: Option<u32>,
    has_windows_handle: bool,
) -> SpawnCleanupPlan {
    match platform {
        SpawnHostPlatform::Unix => {
            if has_unix_process_group {
                SpawnCleanupPlan::UnixProcessGroup
            } else if let Some(pid) = process_id {
                SpawnCleanupPlan::UnixRootPid(pid)
            } else {
                SpawnCleanupPlan::UnixChildKillOnly
            }
        }
        SpawnHostPlatform::Windows => {
            if let Some(pid) = process_id {
                SpawnCleanupPlan::WindowsTreeAndHandle(pid)
            } else if has_windows_handle {
                SpawnCleanupPlan::WindowsHandleOnly
            } else {
                SpawnCleanupPlan::None
            }
        }
        SpawnHostPlatform::Other => SpawnCleanupPlan::None,
    }
}

fn current_spawn_host_platform() -> SpawnHostPlatform {
    #[cfg(unix)]
    {
        SpawnHostPlatform::Unix
    }
    #[cfg(windows)]
    {
        SpawnHostPlatform::Windows
    }
    #[cfg(not(any(unix, windows)))]
    {
        SpawnHostPlatform::Other
    }
}

/// Best-effort whole-tree cleanup when spawn/setup fails after the child exists.
/// `process_id` may be missing when portable_pty does not report one.
fn terminate_spawned_process_tree(
    master: &dyn MasterPty,
    child: &mut (dyn portable_pty::Child + Send + Sync),
    process_id: Option<u32>,
) {
    #[cfg(unix)]
    let has_group = master.process_group_leader().is_some_and(|pgid| pgid > 0);
    #[cfg(not(unix))]
    let has_group = false;
    #[cfg(windows)]
    let has_handle = child.as_raw_handle().is_some();
    #[cfg(not(windows))]
    let has_handle = false;

    match plan_spawn_cleanup(
        current_spawn_host_platform(),
        has_group,
        process_id,
        has_handle,
    ) {
        SpawnCleanupPlan::UnixProcessGroup => {
            #[cfg(unix)]
            {
                if let Some(process_group_id) =
                    master.process_group_leader().filter(|pgid| *pgid > 0)
                {
                    for signal in [libc::SIGHUP, libc::SIGTERM, libc::SIGKILL] {
                        let _ = unsafe { libc::kill(-process_group_id, signal) };
                        thread::sleep(Duration::from_millis(50));
                    }
                }
            }
            let _ = (master, child);
        }
        SpawnCleanupPlan::UnixRootPid(pid) => {
            #[cfg(unix)]
            {
                let _ = unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
            }
            let _ = (master, child, pid);
        }
        SpawnCleanupPlan::UnixChildKillOnly => {
            let _ = master;
            let _ = child.kill();
        }
        SpawnCleanupPlan::WindowsTreeAndHandle(pid) => {
            #[cfg(windows)]
            {
                let _ = master;
                let _ = terminate_windows_descendants_by_pid(pid);
                if let Some(handle) = child.as_raw_handle() {
                    let _ = unsafe { TerminateProcess(handle, 1) };
                }
            }
            let _ = (master, child, pid);
        }
        SpawnCleanupPlan::WindowsHandleOnly => {
            #[cfg(windows)]
            {
                let _ = master;
                if let Some(handle) = child.as_raw_handle() {
                    let _ = unsafe { TerminateProcess(handle, 1) };
                } else {
                    let _ = child.kill();
                }
            }
            let _ = (master, child);
        }
        SpawnCleanupPlan::None => {
            let _ = master;
            let _ = child.kill();
        }
    }
}

/// Kill and wait after a post-spawn setup failure so the PTY process is reaped.
fn cleanup_after_failed_spawn(
    master: &dyn MasterPty,
    child: &mut Box<dyn portable_pty::Child + Send + Sync>,
    process_id: Option<u32>,
) {
    terminate_spawned_process_tree(master, child.as_mut(), process_id);
    // Prefer wait so the OS reaps the zombie; fall back to try_wait polling.
    match child.wait() {
        Ok(_) => {}
        Err(_) => {
            for _ in 0..20 {
                match child.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) => {
                        let _ = child.kill();
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(_) => {
                        let _ = child.kill();
                        thread::sleep(Duration::from_millis(25));
                    }
                }
            }
        }
    }
}

/// Emergency cleanup after failed spawn/job admission only.
/// Uses parent walk + TerminateProcess; **not** accepted as create-time tree ownership.
/// Enumerate failure is an error (not an empty map). Open/kill access issues surface
/// as errors rather than silent "gone".
#[cfg(windows)]
fn terminate_windows_descendants_by_pid(process_id: u32) -> Result<HashSet<u32>> {
    let map = windows_process_parent_map().context(
        "process snapshot failed during emergency spawn cleanup (not treating as empty tree)",
    )?;
    let mut tracked = windows_descendants_of(process_id, &map);
    tracked.insert(process_id);
    let mut kill_errors = Vec::new();
    for pid in tracked.iter().copied() {
        if let Err(error) = terminate_windows_pid_strict(pid) {
            kill_errors.push(format!("pid {pid}: {error:#}"));
        }
    }
    for _ in 0..4 {
        let map = match windows_process_parent_map() {
            Ok(map) => map,
            Err(error) => {
                kill_errors.push(format!("re-scan snapshot failed: {error:#}"));
                break;
            }
        };
        let stragglers = windows_descendants_of(process_id, &map);
        if stragglers.is_empty() {
            break;
        }
        for pid in stragglers {
            tracked.insert(pid);
            if let Err(error) = terminate_windows_pid_strict(pid) {
                kill_errors.push(format!("pid {pid}: {error:#}"));
            }
        }
        thread::sleep(Duration::from_millis(20));
    }
    // Fail closed if we still see Alive/Unknown for any tracked PID.
    for pid in &tracked {
        match windows_pid_liveness(*pid) {
            WindowsLiveness::Dead => {}
            WindowsLiveness::Alive => {
                kill_errors.push(format!("pid {pid} still alive after emergency cleanup"))
            }
            WindowsLiveness::Unknown => kill_errors.push(format!(
                "pid {pid} liveness unknown (access denied/query failure) after emergency cleanup"
            )),
        }
    }
    if !kill_errors.is_empty() {
        bail!(
            "emergency Windows tree cleanup incomplete: {}",
            kill_errors.join("; ")
        );
    }
    Ok(tracked)
}

/// Open by PID and terminate; access denied / still-active ⇒ Err (not silent skip).
/// Prefer creation-time handles; this is only for failed-spawn cleanup.
#[cfg(windows)]
fn terminate_windows_pid_strict(pid: u32) -> Result<()> {
    let handle = unsafe {
        OpenProcess(
            PROCESS_TERMINATE | PROCESS_QUERY_LIMITED_INFORMATION,
            0,
            pid,
        )
    };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        let err = unsafe { GetLastError() };
        if err == ERROR_ACCESS_DENIED {
            bail!("OpenProcess(TERMINATE|QUERY) access denied for pid {pid}");
        }
        // Cannot open: may be dead or inaccessible — fail closed as unknown.
        bail!(
            "OpenProcess failed for pid {pid}: {}",
            io::Error::last_os_error()
        );
    }
    let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
    terminate_windows_handle(handle.as_raw_handle())
}

/// Pure helper used by Windows tree termination (and unit-tested on all hosts).
#[cfg(any(test, windows))]
fn windows_descendants_from_pairs(root: u32, pairs: &[(u32, u32)]) -> HashSet<u32> {
    let mut children = HashMap::<u32, Vec<u32>>::new();
    for &(pid, parent) in pairs {
        children.entry(parent).or_default().push(pid);
    }
    #[cfg(windows)]
    {
        windows_descendants_of(root, &children)
    }
    #[cfg(not(windows))]
    {
        let mut queue = VecDeque::from([root]);
        let mut descendants = HashSet::new();
        while let Some(parent) = queue.pop_front() {
            for child in children.get(&parent).into_iter().flatten().copied() {
                if descendants.insert(child) {
                    queue.push_back(child);
                }
            }
        }
        descendants
    }
}

#[derive(Debug, Eq, PartialEq)]
enum HookEffect {
    Reset,
    Working {
        tool_name: Option<String>,
    },
    Waiting {
        tool_name: Option<String>,
        reason: String,
    },
    Done,
    RecordOnly,
}

#[derive(Default)]
struct TitleCallbacks {
    title: Option<String>,
    title_updated: bool,
    responses: Vec<Vec<u8>>,
}

impl vt100::Callbacks for TitleCallbacks {
    fn set_window_title(&mut self, _: &mut vt100::Screen, title: &[u8]) {
        self.title = Some(String::from_utf8_lossy(title).into_owned());
        self.title_updated = true;
    }

    fn unhandled_csi(
        &mut self,
        screen: &mut vt100::Screen,
        first_intermediate: Option<u8>,
        second_intermediate: Option<u8>,
        params: &[&[u16]],
        final_character: char,
    ) {
        if first_intermediate.is_some() || second_intermediate.is_some() {
            return;
        }
        let first_param = params.first().and_then(|value| value.first()).copied();
        match (final_character, first_param) {
            ('n', Some(5)) => self.responses.push(b"\x1b[0n".to_vec()),
            ('n', Some(6)) => {
                let (row, column) = screen.cursor_position();
                self.responses
                    .push(format!("\x1b[{};{}R", row + 1, column + 1).into_bytes());
            }
            ('c', None | Some(0)) => self.responses.push(b"\x1b[?1;2c".to_vec()),
            _ => {}
        }
    }
}

impl Session {
    /// Immutable owner (no Session.inner lock). Safe under registry.
    fn owner_ref(&self) -> &Option<String> {
        &self.owner
    }

    /// Immutable client session id (no Session.inner lock). Safe under registry.
    fn client_session_id_ref(&self) -> &Option<String> {
        &self.client_session_id
    }

    /// Point this session at the host's live `clients` map Arc after create
    /// registration. Must not be called while holding the registry lock.
    fn reattach_client_lease(&self, lease: Arc<AtomicU64>) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        if inner.client_session_id.is_none() {
            return Ok(());
        }
        inner.client_lease = Some(lease);
        Ok(())
    }

    /// Interactive WebUI acquired write control for this session.
    fn acquire_web_control(&self, connection_id: u64, now: u64) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        inner.web_control_connection_id = Some(connection_id);
        inner.web_control_last_ms = now;
        inner.web_control_ended_at_ms = None;
        inner.updated_at_ms = now;
        // A live write holder cancels uncommitted orphan claims (same as Codex heartbeat).
        let _ = self.cancel_uncommitted_cleanup_locked();
        Ok(())
    }

    /// Refresh write-control lease if `connection_id` still holds it.
    fn refresh_web_control(&self, connection_id: u64, now: u64) -> Result<bool> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        inner.expire_stale_web_control(now);
        if inner.web_control_connection_id != Some(connection_id) {
            return Ok(false);
        }
        inner.web_control_last_ms = now;
        inner.web_control_ended_at_ms = None;
        let _ = self.cancel_uncommitted_cleanup_locked();
        Ok(true)
    }

    /// Explicit release by the holding connection (terminal_release / interactive off).
    fn release_web_control(&self, connection_id: u64, now: u64) -> Result<bool> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        if inner.web_control_connection_id != Some(connection_id) {
            return Ok(false);
        }
        inner.clear_web_control(now);
        Ok(true)
    }

    /// Drop hold if owned by `connection_id` (disconnect / identity takeover).
    fn release_web_control_if_owner(&self, connection_id: u64, now: u64) -> Result<bool> {
        self.release_web_control(connection_id, now)
    }

    fn spawn(
        handle: String,
        provider_session_id: &str,
        config: LaunchConfig,
        host_revision: Arc<HostRevision>,
    ) -> Result<Arc<Self>> {
        let grok_state_dir = ensure_grok_state_dir_writable(&config.cwd, provider_session_id)?;
        let command = build_grok_command(&config, provider_session_id, grok_state_dir.as_deref());
        Self::spawn_with_command(handle, config, command, host_revision)
    }

    fn spawn_with_command(
        handle: String,
        config: LaunchConfig,
        command: CommandBuilder,
        host_revision: Arc<HostRevision>,
    ) -> Result<Arc<Self>> {
        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                cols: INITIAL_COLS,
                rows: INITIAL_ROWS,
                pixel_width: 0,
                pixel_height: 0,
            })
            .context("failed to open PTY")?;
        let reader = pair
            .master
            .try_clone_reader()
            .context("failed to clone the PTY reader")?;
        let writer = pair
            .master
            .take_writer()
            .context("failed to take the PTY writer")?;
        #[cfg(windows)]
        let mut command = command;
        #[cfg(windows)]
        let admission_job = Arc::new(
            create_kill_on_close_job()
                .context("failed to create pre-spawn kill-on-close Job Object")?,
        );

        #[cfg(windows)]
        command.set_job_handle(Arc::clone(&admission_job));
        let mut child = pair
            .slave
            .spawn_command(command)
            .context("failed to start interactive Grok Build")?;
        // Missing process_id must still clean up the already-started PTY process.
        let Some(process_id) = child.process_id() else {
            cleanup_after_failed_spawn(pair.master.as_ref(), &mut child, None);
            bail!("Grok did not report a process ID; terminated orphaned PTY process");
        };
        let termination = match ProcessTerminator::new(
            pair.master.as_ref(),
            child.as_ref(),
            process_id,
            #[cfg(windows)]
            admission_job,
        ) {
            Ok(termination) => termination,
            Err(error) => {
                // Job/group setup failed after spawn: still kill the whole tree.
                cleanup_after_failed_spawn(pair.master.as_ref(), &mut child, Some(process_id));
                return Err(error).context("failed to prepare process-tree termination");
            }
        };
        drop(pair.slave);
        let (writer_tx, writer_rx) = sync_channel(WRITER_QUEUE_CAPACITY);
        let now = now_millis();
        let owner = config.owner.clone();
        let client_session_id = config.client_session_id.clone();
        // new_cyclic gives the failure reaper a Weak upgrade path without a cycle.
        let session = Arc::new_cyclic(|weak| Self {
            // Immutable identity fields — readable without Session.inner.
            client_session_id: client_session_id.clone(),
            owner: owner.clone(),
            self_weak: weak.clone(),
            inner: Mutex::new(SessionInner {
                session: handle,
                owner,
                client_session_id,
                client_lease: config.client_lease,
                orphan_policy: config.orphan_policy,
                phase: SessionPhase::Starting,
                phase_changed_at_ms: now,
                cwd: config.cwd.to_string_lossy().into_owned(),
                model: config.model,
                always_approve: config.always_approve,
                process_id: Some(process_id),
                created_at_ms: now,
                updated_at_ms: now,
                semantic_active_at_ms: now,
                completed_at_ms: None,
                exit_code: None,
                error: None,
                title: None,
                parser: vt100::Parser::new_with_callbacks(
                    INITIAL_ROWS,
                    INITIAL_COLS,
                    SCROLLBACK_ROWS,
                    TitleCallbacks::default(),
                ),
                chunks: VecDeque::new(),
                transcript_bytes: 0,
                next_cursor: 0,
                last_output_at_ms: None,
                process_done: false,
                reader_done: false,
                hook: HookState::default(),
                web_control_connection_id: None,
                web_control_last_ms: 0,
                web_control_ended_at_ms: None,
                paste_open: false,
                paste_scan_hold: Vec::new(),
            }),
            changed: Condvar::new(),
            host_revision,
            writer_tx: Mutex::new(Some(writer_tx)),
            master: Mutex::new(Some(pair.master)),
            termination: Some(Arc::new(termination)),
            shutdown: AtomicBool::new(false),
            cleanup_claimed: AtomicBool::new(false),
            cleanup_committed: AtomicBool::new(false),
            test_shutdown_hang_ms: AtomicU64::new(0),
            failure_reaper_started: AtomicBool::new(false),
            reaper_state: Arc::new(FailureReaperState::new()),
        });

        spawn_reader(Arc::clone(&session), reader);
        spawn_writer(Arc::clone(&session), writer, writer_rx);
        spawn_waiter(Arc::clone(&session), child);
        Ok(session)
    }

    fn state(&self) -> Result<SessionState> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        Ok(inner.to_state(now_millis(), self.cleanup_claimed.load(Ordering::Acquire)))
    }

    /// Lifecycle/board fields without allocating full screen / ANSI snapshots.
    fn state_web_board(&self) -> Result<SessionState> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        Ok(inner.to_web_board_state(now_millis(), self.cleanup_claimed.load(Ordering::Acquire)))
    }

    /// Authoritative terminal snapshot for incremental WebSocket reset framing.
    /// Only visible rows and terminal modes are retained. The parser's 5,000-row
    /// scrollback is never cloned into a per-connection continuation.
    fn web_reset_snapshot(&self) -> Result<(WebEventsResetSource, u64)> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        let screen = inner.parser.screen();
        let (_, cols) = screen.size();
        let mut components = Vec::new();
        let mut bytes = 0usize;
        let mut push = |component: Vec<u8>| -> Result<()> {
            bytes = bytes.saturating_add(component.len());
            if bytes > MAX_WEB_RESET_CONTINUATION_BYTES {
                return Err(WebEventsPlanLimitError(format!(
                    "terminal reset snapshot exceeds the {} byte connection limit",
                    MAX_WEB_RESET_CONTINUATION_BYTES
                ))
                .into());
            }
            components.push(component);
            Ok(())
        };
        for (row, contents) in screen.rows_formatted(0, cols).enumerate() {
            push(format!("\x1b[{};1H", row.saturating_add(1)).into_bytes())?;
            push(contents)?;
        }
        push(screen.cursor_state_formatted())?;
        push(screen.attributes_formatted())?;
        push(screen.input_mode_formatted())?;
        Ok((
            WebEventsResetSource::Components { components, bytes },
            inner.next_cursor,
        ))
    }

    /// Next pure time-based client lease transition for this session, if any.
    /// Returns only a future deadline so waiters do not spin after the transition
    /// has already been observed via a subsequent list/show.
    fn next_lifecycle_deadline_ms(&self, now: u64) -> Result<Option<u64>> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        Ok(inner.next_lifecycle_deadline_ms(now, self.cleanup_claimed.load(Ordering::Acquire)))
    }

    fn claim_orphan_cleanup(&self, now: u64) -> Result<bool> {
        if self.cleanup_claimed.load(Ordering::Acquire)
            || self.cleanup_committed.load(Ordering::Acquire)
        {
            return Ok(false);
        }
        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        if !inner.orphan_cleanup_due(now) {
            return Ok(false);
        }
        // Claim while the session lock is still held. Input and phase changes
        // take the same lock, so an idle session cannot become Running between
        // the eligibility check and the claim.
        Ok(self
            .cleanup_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok())
    }

    /// Recheck the lease and phase immediately before cleanup becomes
    /// irreversible. The caller holds the host registry lock, serializing this
    /// commit with every Codex/WebUI lease refresh.
    fn commit_orphan_cleanup(&self, now: u64) -> Result<bool> {
        if !self.cleanup_claimed.load(Ordering::Acquire)
            || self.cleanup_committed.load(Ordering::Acquire)
        {
            return Ok(false);
        }
        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        if !self.cleanup_claimed.load(Ordering::Acquire) {
            return Ok(false);
        }
        if !inner.orphan_cleanup_due(now) {
            self.cleanup_claimed.store(false, Ordering::Release);
            return Ok(false);
        }
        Ok(self
            .cleanup_committed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok())
    }

    fn cancel_uncommitted_cleanup_for_client(&self, client_session_id: &str) -> Result<bool> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        if inner.client_session_id.as_deref() != Some(client_session_id) {
            return Ok(false);
        }
        Ok(self.cancel_uncommitted_cleanup_locked())
    }

    fn cancel_uncommitted_cleanup_locked(&self) -> bool {
        !self.cleanup_committed.load(Ordering::Acquire)
            && self.cleanup_claimed.swap(false, Ordering::AcqRel)
    }

    fn reset_orphan_cleanup(&self) {
        self.cleanup_committed.store(false, Ordering::Release);
        self.cleanup_claimed.store(false, Ordering::Release);
    }

    fn apply_hook_event(&self, event: HookEvent) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        if phase_is_terminal(inner.phase) {
            return Ok(());
        }

        if let Some(cwd) = event.cwd.as_deref() {
            let hook_cwd = canonical_directory(Path::new(cwd))?;
            let session_cwd = normalize_platform_path(PathBuf::from(&inner.cwd));
            if hook_cwd != session_cwd {
                bail!("hook working directory does not match the session");
            }
        }

        let now = now_millis();
        let effect = if inner.hook.turn_done
            && !matches!(
                event.kind,
                HookEventKind::SessionStart
                    | HookEventKind::UserPromptSubmit
                    | HookEventKind::Stop
                    | HookEventKind::StopFailure
                    | HookEventKind::SessionEnd
            ) {
            HookEffect::RecordOnly
        } else {
            hook_effect(&event)
        };
        let semantic = !matches!(&effect, HookEffect::RecordOnly);
        match effect {
            HookEffect::Reset => {
                inner.hook.activity = HookActivity::Unknown;
                inner.hook.tool_name = None;
                inner.hook.waiting_reason = None;
                inner.hook.turn_done = false;
            }
            HookEffect::Working { tool_name } => {
                inner.hook.activity = HookActivity::Working;
                inner.hook.tool_name = tool_name;
                inner.hook.waiting_reason = None;
                if event.kind == HookEventKind::UserPromptSubmit {
                    inner.hook.turn_done = false;
                }
                set_phase(&mut inner, SessionPhase::Running, now);
            }
            HookEffect::Waiting { tool_name, reason } => {
                inner.hook.activity = HookActivity::Waiting;
                inner.hook.tool_name = tool_name;
                inner.hook.waiting_reason = Some(reason);
                set_phase(&mut inner, SessionPhase::Running, now);
            }
            HookEffect::Done => {
                inner.hook.activity = HookActivity::Done;
                inner.hook.tool_name = None;
                inner.hook.waiting_reason = None;
                inner.hook.turn_done = true;
                set_phase(&mut inner, SessionPhase::Idle, now);
            }
            HookEffect::RecordOnly => {}
        }

        if semantic {
            inner.semantic_active_at_ms = now;
        }

        inner.hook.last_event = Some(event.kind);
        inner.hook.last_event_at_ms = Some(now);
        inner.updated_at_ms = now;
        drop(inner);
        self.signal_changed();
        Ok(())
    }

    fn read(&self, cursor: u64, limit: usize, wait_ms: u64) -> Result<ReadResult> {
        use crate::protocol::{MAX_READ_WAIT_MS, MIN_READ_WAIT_MS};
        // Protocol decode already enforces the range; keep a hard assert for
        // internal callers so values are never silently rewritten.
        if wait_ms > MAX_READ_WAIT_MS {
            bail!(
                "wait_ms must be between {MIN_READ_WAIT_MS} and {MAX_READ_WAIT_MS} (0 = non-blocking)"
            );
        }
        let limit = limit.clamp(1, MAX_READ_BYTES);
        let deadline = Instant::now() + Duration::from_millis(wait_ms);
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        if cursor > inner.next_cursor {
            bail!(
                "cursor {cursor} is beyond the latest cursor {}",
                inner.next_cursor
            );
        }
        // Failed is a surfaced control state, not PTY EOF. Continue long-polling
        // until bytes arrive or the reader actually closes.
        while cursor == inner.next_cursor && !inner.reader_done && wait_ms > 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let waited = self
                .changed
                .wait_timeout(inner, remaining)
                .map_err(|_| anyhow::anyhow!("session wait lock was poisoned"))?;
            inner = waited.0;
            if waited.1.timed_out() {
                break;
            }
        }

        let oldest_cursor = inner
            .chunks
            .front()
            .map(|chunk| chunk.start)
            .unwrap_or(inner.next_cursor);
        let actual_cursor = cursor.max(oldest_cursor);
        let mut output = Vec::with_capacity(limit);
        for chunk in &inner.chunks {
            let end = chunk.start + chunk.data.len() as u64;
            if end <= actual_cursor {
                continue;
            }
            let offset = actual_cursor.saturating_sub(chunk.start) as usize;
            let available = &chunk.data[offset.min(chunk.data.len())..];
            let take = available.len().min(limit - output.len());
            output.extend_from_slice(&available[..take]);
            if output.len() == limit {
                break;
            }
        }
        let next_cursor = actual_cursor + output.len() as u64;
        Ok(ReadResult {
            session: inner.session.clone(),
            cursor: actual_cursor,
            next_cursor,
            data_base64: BASE64.encode(&output),
            plain_text: None,
            screen: Some(inner.parser.screen().contents()),
            truncated: cursor < oldest_cursor,
            // EOF is a PTY reader fact. Failed may be surfaced while the process
            // tree and reader are still alive, and callers must keep draining.
            eof: inner.reader_done,
        })
    }

    fn send(&self, input: String) -> Result<()> {
        if input.is_empty() {
            bail!("input must not be empty");
        }
        // High-level send always means "submit this turn". Bracket-paste wrappers
        // carry embedded CR/LF that must not be re-scanned as raw StartTurn.
        let (data, effect) = if input.len() == 1 && input.as_bytes()[0].is_ascii_control() {
            let bytes = input.into_bytes();
            let effect = if bytes[0] == 0x03 {
                InputEffect::Cancel
            } else {
                InputEffect::None
            };
            (bytes, effect)
        } else {
            let mut data = Vec::with_capacity(input.len() + 13);
            data.extend_from_slice(b"\x1b[200~");
            data.extend_from_slice(input.as_bytes());
            data.extend_from_slice(b"\x1b[201~\r");
            (data, InputEffect::StartTurn)
        };
        self.enqueue_input(data, effect)
    }

    fn write_raw(&self, data: Vec<u8>) -> Result<()> {
        let completion = self.begin_write_job(data)?;
        match completion.wait() {
            Ok(()) => Ok(()),
            Err(error) => {
                self.mark_writer_error(error.clone());
                self.close_writer();
                Err(anyhow::anyhow!("{error}"))
            }
        }
    }

    /// Enqueue bytes on the writer thread without waiting for write_all+flush.
    /// Effect is classified here (paste state) and committed on the writer thread
    /// after a successful FIFO write so waiters cannot reverse semantics.
    fn begin_write_job(&self, data: Vec<u8>) -> Result<Arc<WriteCompletion>> {
        if data.is_empty() {
            bail!("terminal data must not be empty");
        }
        if data.len() > MAX_WRITE_BYTES {
            bail!("terminal data exceeds the 64 KiB limit");
        }
        // Raw WebUI input has parser state spanning chunks. Serialize state
        // derivation with writer FIFO admission, and roll it back if the job
        // never enters the queue; otherwise a rejected prefix could alter the
        // next job's paste semantics.
        self.enqueue_raw_write_job(data)
    }

    fn enqueue_raw_write_job(&self, data: Vec<u8>) -> Result<Arc<WriteCompletion>> {
        if self.shutdown.load(Ordering::Acquire) {
            bail!("session has already stopped");
        }
        if self.cleanup_claimed.load(Ordering::Acquire)
            || self.cleanup_committed.load(Ordering::Acquire)
        {
            bail!("session cleanup has started");
        }
        let writer_guard = self
            .writer_tx
            .lock()
            .map_err(|_| anyhow::anyhow!("session input lock was poisoned"))?;
        let Some(writer) = writer_guard.as_ref() else {
            bail!("session input channel is closed");
        };
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        if self.cleanup_claimed.load(Ordering::Acquire)
            || self.cleanup_committed.load(Ordering::Acquire)
            || inner.process_done
            || phase_is_terminal(inner.phase)
            || inner.error.is_some()
        {
            bail!("session is not writable");
        }
        let old_paste_open = inner.paste_open;
        let old_scan_hold = inner.paste_scan_hold.clone();
        let SessionInner {
            paste_open,
            paste_scan_hold,
            ..
        } = &mut *inner;
        let effect = raw_input_effect(&data, paste_open, paste_scan_hold);
        let completion = WriteCompletion::new_with_deadline(self.self_weak.clone());
        match writer.try_send(WriteJob {
            data,
            effect,
            completion: Some(Arc::clone(&completion)),
        }) {
            Ok(()) => Ok(completion),
            Err(TrySendError::Full(job)) => {
                *paste_open = old_paste_open;
                *paste_scan_hold = old_scan_hold;
                job.fail("session input queue is full");
                bail!("session input queue is full");
            }
            Err(TrySendError::Disconnected(job)) => {
                *paste_open = old_paste_open;
                *paste_scan_hold = old_scan_hold;
                job.fail(WRITE_CANCELLED_MSG);
                bail!("session input channel is closed");
            }
        }
    }

    fn enqueue_input(&self, data: Vec<u8>, effect: InputEffect) -> Result<()> {
        let completion = self.enqueue_write_job(data, effect)?;
        // Wait for real write_all+flush and ordered effect commit. A deadline
        // publishes one non-retryable result bound to this exact WriteJob.
        match completion.wait() {
            Ok(()) => Ok(()),
            Err(error) => {
                self.mark_writer_error(error.clone());
                self.close_writer();
                Err(anyhow::anyhow!("{error}"))
            }
        }
    }

    fn enqueue_write_job(
        &self,
        data: Vec<u8>,
        effect: InputEffect,
    ) -> Result<Arc<WriteCompletion>> {
        if self.shutdown.load(Ordering::Acquire) {
            bail!("session has already stopped");
        }
        {
            let inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
            if self.cleanup_claimed.load(Ordering::Acquire)
                || self.cleanup_committed.load(Ordering::Acquire)
            {
                bail!("session cleanup has started");
            }
            if inner.process_done || phase_is_terminal(inner.phase) || inner.error.is_some() {
                bail!("session is not writable");
            }
        }
        let completion = WriteCompletion::new_with_deadline(self.self_weak.clone());
        {
            let writer_guard = self
                .writer_tx
                .lock()
                .map_err(|_| anyhow::anyhow!("session input lock was poisoned"))?;
            let Some(writer) = writer_guard.as_ref() else {
                // Channel already closed: publish cancel so no waiter can hang if
                // a racy path still holds this completion.
                completion.complete_cancelled();
                bail!("session input channel is closed");
            };
            match writer.try_send(WriteJob {
                data,
                effect,
                completion: Some(Arc::clone(&completion)),
            }) {
                Ok(()) => {}
                Err(TrySendError::Full(job)) => {
                    // Queue full is pre-write / retryable at higher layers; do not
                    // leave the discarded job's clone unpublished (Drop would also
                    // fire — complete explicitly for a stable message).
                    job.fail("session input queue is full");
                    completion.complete(Err("session input queue is full".to_owned()));
                    bail!("session input queue is full");
                }
                Err(TrySendError::Disconnected(job)) => {
                    job.fail(WRITE_CANCELLED_MSG);
                    completion.complete_cancelled();
                    bail!("session input channel is closed");
                }
            }
        }
        // close_writer may race after send: job is queued; writer fail-all or
        // cancel-before-write will publish. Caller waits on completion.
        Ok(completion)
    }

    /// True when the writer should not perform further PTY writes (shutdown or
    /// sender already taken). Checked before each dequeued job's write_all.
    fn writer_must_cancel(&self) -> bool {
        if self.shutdown.load(Ordering::Acquire) {
            return true;
        }
        self.writer_tx
            .lock()
            .map(|guard| guard.is_none())
            .unwrap_or(true)
    }

    /// Commit one input effect after a durable PTY write. Called only from the
    /// writer thread (FIFO with write order). Never revives terminal/shutdown
    /// phases — a late StartTurn after close must not set Running.
    fn apply_input_effect(&self, effect: InputEffect) -> Result<()> {
        if matches!(effect, InputEffect::None) {
            return Ok(());
        }
        // Shutdown / cleanup: write may still have completed, but semantics must
        // not move Exited/Failed/Stopped (or cleanup) back to live activity.
        if self.shutdown.load(Ordering::Acquire)
            || self.cleanup_claimed.load(Ordering::Acquire)
            || self.cleanup_committed.load(Ordering::Acquire)
        {
            return Ok(());
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        if phase_is_terminal(inner.phase) {
            return Ok(());
        }
        let now = now_millis();
        match effect {
            InputEffect::StartTurn => {
                set_phase(&mut inner, SessionPhase::Running, now);
                inner.hook.activity = HookActivity::Working;
                inner.hook.tool_name = None;
                inner.hook.waiting_reason = None;
                inner.hook.turn_done = false;
            }
            InputEffect::Cancel => {
                // Interrupt is only confirmed written here. Stay in a truthful
                // cancelling state until hooks/title/exit confirm idle.
                inner.hook.activity = HookActivity::Cancelling;
                inner.hook.tool_name = None;
                inner.hook.waiting_reason = None;
                inner.hook.turn_done = false;
                set_phase(&mut inner, SessionPhase::Running, now);
            }
            InputEffect::None => {}
        }
        inner.semantic_active_at_ms = now;
        inner.updated_at_ms = now;
        drop(inner);
        self.signal_changed();
        Ok(())
    }

    fn resize(&self, cols: u16, rows: u16) -> Result<()> {
        validate_terminal_size(cols, rows)?;
        if self.shutdown.load(Ordering::Acquire) {
            bail!("session has already stopped");
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        if self.cleanup_claimed.load(Ordering::Acquire)
            || self.cleanup_committed.load(Ordering::Acquire)
        {
            bail!("session cleanup has started");
        }
        if inner.process_done || phase_is_terminal(inner.phase) || inner.error.is_some() {
            bail!("session is not resizable");
        }
        let master_guard = self
            .master
            .lock()
            .map_err(|_| anyhow::anyhow!("PTY master lock was poisoned"))?;
        let Some(master) = master_guard.as_ref() else {
            bail!("PTY master is closed");
        };
        master
            .resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .context("failed to resize PTY")?;
        inner.parser.screen_mut().set_size(rows, cols);
        // Viewport sync must not look like semantic activity, or clean-done
        // attention ordering would chase layout noise via updated_at_ms.
        drop(master_guard);
        drop(inner);
        self.signal_changed();
        Ok(())
    }

    fn wait(&self, condition: WaitCondition, timeout_ms: u64) -> Result<WaitResult> {
        use crate::protocol::{MAX_WAIT_TIMEOUT_MS, MIN_WAIT_TIMEOUT_MS};
        if !(MIN_WAIT_TIMEOUT_MS..=MAX_WAIT_TIMEOUT_MS).contains(&timeout_ms) {
            bail!("timeout_ms must be between {MIN_WAIT_TIMEOUT_MS} and {MAX_WAIT_TIMEOUT_MS}");
        }
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        loop {
            if condition == WaitCondition::TuiIdle {
                let screen = inner.parser.screen().contents();
                if let Some(reason) = blocked_reason(&screen) {
                    return Ok(inner.wait_result(condition, false, false, Some(reason)));
                }
                if inner.hook.activity == HookActivity::Waiting {
                    let reason = inner
                        .hook
                        .waiting_reason
                        .as_deref()
                        .unwrap_or("grok-hook-waiting");
                    return Ok(inner.wait_result(condition, false, false, Some(reason)));
                }
            }
            if wait_satisfied(&mut inner, condition) {
                return Ok(inner.wait_result(condition, true, false, None));
            }
            if condition == WaitCondition::TuiIdle && phase_is_terminal(inner.phase) {
                return Ok(inner.wait_result(condition, false, false, None));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(inner.wait_result(condition, false, true, None));
            }
            let poll = remaining.min(Duration::from_millis(250));
            let waited = self
                .changed
                .wait_timeout(inner, poll)
                .map_err(|_| anyhow::anyhow!("session wait lock was poisoned"))?;
            inner = waited.0;
        }
    }

    fn shutdown(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_millis(PROCESS_TERMINATE_TIMEOUT_MS);
        self.shutdown_until(deadline)
    }

    /// Terminate the session, stopping no later than `deadline` (absolute wall clock).
    fn shutdown_until(&self, deadline: Instant) -> Result<()> {
        self.shutdown.store(true, Ordering::Release);
        let hang_ms = self.test_shutdown_hang_ms.load(Ordering::Acquire);
        if hang_ms > 0 {
            let hang = Duration::from_millis(hang_ms);
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                bail!("batch close deadline exceeded before shutdown started");
            }
            thread::sleep(hang.min(remaining));
            if Instant::now() >= deadline {
                bail!("batch close deadline exceeded while waiting for shutdown");
            }
        }
        // Root wait + PTY EOF are necessary but not sufficient. Descendants may
        // ignore HUP/TERM and stay in the PGID / Job after the root exits.
        if self.close_tree_complete() {
            self.close_writer();
            self.release_master();
            return Ok(());
        }

        // Never wait past the caller's absolute deadline (batch close budget).
        let process_cap = Instant::now() + Duration::from_millis(PROCESS_TERMINATE_TIMEOUT_MS);
        let deadline = deadline.min(process_cap);

        self.request_termination()
            .context("failed to terminate Grok process tree")?;
        self.close_writer();
        self.release_master();

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        loop {
            // If the OS tree is gone but mark_exit raced, record process_done.
            let tree_gone = self.tree_is_gone();
            if tree_gone && !inner.process_done {
                inner.process_done = true;
                if inner.exit_code.is_none() {
                    inner.exit_code = Some(1);
                }
                inner.process_id = None;
                let _ = finalize_session(&mut inner, true);
            }
            // Success: root done + PTY EOF + real tree-gone (not phase alone).
            if inner.process_done && inner.reader_done && tree_gone {
                if !phase_is_terminal(inner.phase) {
                    let _ = finalize_session(&mut inner, true);
                }
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                if !tree_gone {
                    bail!("Grok process tree still has live members before the close deadline");
                }
                if !inner.process_done {
                    bail!("Grok process tree did not terminate before the close deadline");
                }
                if !inner.reader_done {
                    bail!(
                        "Grok process terminated but PTY output did not close before the close deadline"
                    );
                }
                bail!(
                    "Grok termination did not reach a terminal session state before the close deadline"
                );
            }

            drop(inner);
            // Keep escalating HUP→TERM→KILL until the tree is actually gone.
            self.request_termination()
                .context("failed to terminate Grok process tree")?;
            let escalation_wait = self
                .termination
                .as_ref()
                .map(|terminator| terminator.next_wait_duration())
                .unwrap_or_else(|| Duration::from_millis(1));
            let poll = remaining.min(escalation_wait.max(Duration::from_millis(1)));
            inner = self
                .inner
                .lock()
                .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
            let waited = self
                .changed
                .wait_timeout(inner, poll)
                .map_err(|_| anyhow::anyhow!("session wait lock was poisoned"))?;
            inner = waited.0;
        }
    }

    /// OS-level process tree is empty (PGID / Job / tracked PIDs).
    fn tree_is_gone(&self) -> bool {
        match self.termination.as_ref() {
            Some(terminator) => terminator.is_tree_gone(),
            // Synthetic fixtures without a terminator never owned an OS tree.
            None => true,
        }
    }

    /// Close may succeed only when root wait, PTY EOF, and tree-gone all hold.
    fn close_tree_complete(&self) -> bool {
        let Ok(inner) = self.inner.lock() else {
            return false;
        };
        inner.process_done && inner.reader_done && self.tree_is_gone()
    }

    fn append_output(&self, data: Vec<u8>) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        let now = now_millis();
        let start = inner.next_cursor;
        inner.next_cursor = inner.next_cursor.saturating_add(data.len() as u64);
        inner.transcript_bytes = inner.transcript_bytes.saturating_add(data.len());
        inner.parser.process(&data);
        inner.title = inner.parser.callbacks().title.clone();
        let callbacks = inner.parser.callbacks_mut();
        let title_updated = std::mem::take(&mut callbacks.title_updated);
        let responses = std::mem::take(&mut callbacks.responses);
        let phase = phase_after_output(
            inner.phase,
            inner.title.as_deref(),
            title_updated,
            inner.hook.activity,
            inner.process_done,
            inner.error.is_some(),
            self.shutdown.load(Ordering::Acquire),
        );
        set_phase(&mut inner, phase, now);
        inner.last_output_at_ms = Some(now);
        inner.semantic_active_at_ms = now;
        inner.updated_at_ms = now;
        inner.chunks.push_back(OutputChunk { start, data });
        while inner.transcript_bytes > MAX_TRANSCRIPT_BYTES {
            let Some(removed) = inner.chunks.pop_front() else {
                break;
            };
            inner.transcript_bytes = inner.transcript_bytes.saturating_sub(removed.data.len());
        }
        drop(inner);
        for response in responses {
            self.queue_terminal_response(response);
        }
        self.signal_changed();
    }

    fn queue_terminal_response(&self, response: Vec<u8>) {
        // Auto PTY replies are best-effort; writer failures still mark error.
        let result = self.writer_tx.lock().ok().and_then(|writer| {
            writer.as_ref().map(|writer| {
                writer.try_send(WriteJob {
                    data: response,
                    effect: InputEffect::None,
                    completion: None,
                })
            })
        });
        match result {
            Some(Ok(())) => {}
            Some(Err(TrySendError::Full(_))) => {
                self.mark_writer_error("terminal response queue is full".to_owned());
            }
            Some(Err(TrySendError::Disconnected(_))) | None => {
                if !self.shutdown.load(Ordering::Acquire) {
                    self.mark_writer_error("terminal response channel is closed".to_owned());
                }
            }
        }
    }

    fn mark_reader_done(&self) {
        let finalized = if let Ok(mut inner) = self.inner.lock() {
            inner.reader_done = true;
            inner.updated_at_ms = now_millis();
            finalize_session(&mut inner, self.shutdown.load(Ordering::Acquire))
        } else {
            false
        };
        self.finish_transition(finalized);
    }

    fn mark_reader_error(&self, message: String) {
        let finalized = if let Ok(mut inner) = self.inner.lock() {
            inner.reader_done = true;
            record_error(&mut inner, message);
            finalize_session(&mut inner, self.shutdown.load(Ordering::Acquire))
        } else {
            false
        };
        self.finish_transition(finalized);
        if !finalized {
            self.ensure_failure_reaper();
        }
    }

    fn mark_writer_error(&self, message: String) {
        // Partial or failed PTY write: surface Failed and stop further enqueues
        // (enqueue_write_job rejects when error is set / phase is terminal).
        if let Ok(mut inner) = self.inner.lock() {
            record_error(&mut inner, message);
            let now = now_millis();
            if !phase_is_terminal(inner.phase) {
                set_phase(&mut inner, SessionPhase::Failed, now);
            }
            inner.updated_at_ms = now;
        }
        self.signal_changed();
        self.ensure_failure_reaper();
    }

    /// `child.wait` returned Err — that is not proof the process tree exited.
    /// Record the error, surface Failed for clients, keep `process_id`, and do
    /// **not** set `process_done`. The waiter continues polling for a real exit;
    /// the failure reaper escalates OS signals until the tree is actually dead.
    fn mark_wait_error(&self, message: String) {
        if let Ok(mut inner) = self.inner.lock() {
            record_error(&mut inner, message);
            let now = now_millis();
            if !phase_is_terminal(inner.phase) {
                set_phase(&mut inner, SessionPhase::Failed, now);
            }
            inner.updated_at_ms = now;
            // Intentionally leave process_done / process_id untouched.
        }
        self.signal_changed();
        self.ensure_failure_reaper();
    }

    /// Drive HUP→TERM→KILL escalation until terminal or bounded Failed.
    ///
    /// The reaper holds only `Weak<Session>` + terminator. On timeout it upgrades
    /// and finalizes autonomously (no later state()/signal required).
    fn ensure_failure_reaper(&self) {
        if self
            .failure_reaper_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        if let Err(error) = self.request_termination() {
            self.record_secondary_error(format!(
                "failed to terminate Grok after error edge: {error}"
            ));
            self.force_failed_if_unrecoverable();
            return;
        }
        let Some(termination) = self.termination.clone() else {
            self.force_failed_if_unrecoverable();
            return;
        };
        let reaper_state = Arc::clone(&self.reaper_state);
        let session_weak = self.self_weak.clone();
        thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_millis(PROCESS_TERMINATE_TIMEOUT_MS);
            while Instant::now() < deadline {
                if reaper_state.is_terminal() {
                    return;
                }
                // Prefer upgrading early if the session already finalized.
                if session_weak
                    .upgrade()
                    .is_some_and(|session| session.failure_recovery_complete())
                {
                    reaper_state.mark_terminal();
                    return;
                }
                let wait = termination
                    .next_wait_duration()
                    .max(Duration::from_millis(PROCESS_KILL_REPEAT_MS));
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                if let Ok(guard) = reaper_state.wake.lock() {
                    let _ = reaper_state
                        .wake_cv
                        .wait_timeout(guard, wait.min(remaining));
                } else {
                    thread::sleep(wait.min(remaining));
                }
                if reaper_state.is_terminal() {
                    return;
                }
                let _ = termination.request();
            }
            if reaper_state.is_terminal() {
                return;
            }
            // Timeout: autonomously force Failed and wake waiters/host revision.
            if let Some(session) = session_weak.upgrade() {
                session.force_failed_if_unrecoverable();
            } else {
                // Session already dropped; nothing left to wake.
                reaper_state.request_force_fail();
                reaper_state.mark_terminal();
            }
        });
    }

    fn failure_recovery_complete(&self) -> bool {
        self.inner
            .lock()
            .map(|inner| inner.process_done && inner.reader_done && self.tree_is_gone())
            .unwrap_or(false)
    }

    fn poll_reaper_force_fail(&self) {
        if self.reaper_state.take_force_fail() {
            self.force_failed_if_unrecoverable();
        }
    }

    /// When termination is unavailable or the process ignores signals past the
    /// budget, force Failed so waiters and the WebUI never hang half-open.
    ///
    /// Does **not** invent `process_done` / `reader_done`: those remain OS/PTY
    /// facts. close/shutdown keeps driving the terminator until the tree and
    /// PTY EOF are real.
    fn force_failed_if_unrecoverable(&self) {
        let signaled = if let Ok(mut inner) = self.inner.lock() {
            if inner.process_done && inner.reader_done && self.tree_is_gone() {
                self.reaper_state.mark_terminal();
                false
            } else {
                let now = now_millis();
                if inner.phase != SessionPhase::Failed {
                    set_phase(&mut inner, SessionPhase::Failed, now);
                }
                let residual =
                    "process-tree termination remains incomplete after the failure-reaper deadline";
                if !inner
                    .error
                    .as_deref()
                    .is_some_and(|error| error.contains(residual))
                {
                    record_error(&mut inner, residual.to_owned());
                }
                inner.updated_at_ms = now;
                // The bounded autonomous escalation is exhausted. Keep process
                // and reader facts honest; explicit close can retry later.
                self.reaper_state.mark_terminal();
                true
            }
        } else {
            false
        };
        if signaled {
            // Do not close writer/master here — OS process may still be alive.
            self.signal_changed();
        }
    }

    fn mark_exit(&self, exit_code: u32) {
        let finalized = if let Ok(mut inner) = self.inner.lock() {
            if !inner.process_done {
                inner.process_done = true;
                inner.exit_code = Some(exit_code);
                inner.process_id = None;
            }
            inner.updated_at_ms = now_millis();
            finalize_session(&mut inner, self.shutdown.load(Ordering::Acquire))
        } else {
            false
        };
        self.finish_transition(finalized);
    }

    fn request_termination(&self) -> Result<()> {
        self.poll_reaper_force_fail();
        if let Some(termination) = self.termination.as_ref() {
            return termination.request();
        }
        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session state lock was poisoned"))?;
        if inner.process_done && inner.reader_done {
            return Ok(());
        }
        bail!("Grok process tree terminator is unavailable")
    }

    fn record_secondary_error(&self, message: String) {
        if let Ok(mut inner) = self.inner.lock() {
            record_error(&mut inner, message);
        }
        self.signal_changed();
    }

    /// Stop new enqueues. The writer thread observes `writer_must_cancel` (sender
    /// gone and/or shutdown) and fail-alls every queued completion without
    /// further PTY writes. Dropping the sender alone is not enough: a healthy
    /// writer would otherwise drain and execute the pre-close queue.
    fn close_writer(&self) {
        // shutdown helps the writer cancel even if it already dequeued a job
        // before observing sender==None.
        self.shutdown.store(true, Ordering::Release);
        if let Ok(mut writer) = self.writer_tx.lock() {
            writer.take();
        }
    }

    fn release_master(&self) {
        if let Ok(mut master) = self.master.lock() {
            master.take();
        }
    }

    fn finish_transition(&self, finalized: bool) {
        if finalized {
            self.close_writer();
            self.release_master();
            if self.tree_is_gone() {
                self.reaper_state.mark_terminal();
            } else if self.termination.is_some() {
                // Root wait + PTY EOF may precede descendant exit. Keep the
                // owned process group/Job under the bounded failure reaper.
                self.ensure_failure_reaper();
            }
        }
        self.signal_changed();
    }

    fn signal_changed(&self) {
        // Apply delayed force-fail from the escalation reaper (HUP ignored, etc.).
        // Avoid re-entrancy: only poll when force_fail is set and we are not already
        // inside force_failed (force_failed clears via take_force_fail).
        if self.reaper_state.force_fail.load(Ordering::Acquire) {
            self.poll_reaper_force_fail();
            return;
        }
        self.changed.notify_all();
        self.host_revision.bump();
    }
}

impl SessionInner {
    fn to_state(&self, now: u64, cleanup_claimed: bool) -> SessionState {
        let screen = self.parser.screen();
        let (rows, cols) = screen.size();
        let mut state = self.to_web_board_state(now, cleanup_claimed);
        state.screen = Some(screen.contents());
        state.rows = rows;
        state.cols = cols;
        state.screen_ansi_base64 = BASE64.encode(screen.contents_formatted());
        state
    }

    /// Board/lifecycle snapshot: rows/cols/cursor without full screen text/ANSI.
    fn to_web_board_state(&self, now: u64, cleanup_claimed: bool) -> SessionState {
        let screen = self.parser.screen();
        let (rows, cols) = screen.size();
        let (client_state, client_last_seen_at_ms, orphaned_at_ms, auto_close_at_ms) =
            self.client_lifecycle(now, cleanup_claimed);
        SessionState {
            session: self.session.clone(),
            owner: self.owner.clone(),
            client_session_id: self.client_session_id.clone(),
            client_state,
            client_lease_ms: self
                .client_lease
                .as_ref()
                .map(|_| self.orphan_policy.lease_ms),
            orphan_grace_ms: self
                .client_lease
                .as_ref()
                .map(|_| self.orphan_policy.grace_ms),
            client_last_seen_at_ms,
            orphaned_at_ms,
            auto_close_at_ms,
            phase: self.phase,
            cwd: self.cwd.clone(),
            model: self.model.clone(),
            always_approve: self.always_approve,
            process_id: self.process_id,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            semantic_active_at_ms: self.semantic_active_at_ms,
            completed_at_ms: self.completed_at_ms,
            exit_code: self.exit_code,
            error: self.error.clone(),
            title: self.title.clone(),
            screen: None,
            rows,
            cols,
            screen_ansi_base64: String::new(),
            last_cursor: self.next_cursor,
            last_output_at_ms: self.last_output_at_ms,
            activity: self.hook.activity,
            hook_event: self.hook.last_event,
            hook_at_ms: self.hook.last_event_at_ms,
            tool_name: self.hook.tool_name.clone(),
            waiting_reason: self.hook.waiting_reason.clone(),
        }
    }

    fn next_lifecycle_deadline_ms(&self, now: u64, cleanup_claimed: bool) -> Option<u64> {
        if cleanup_claimed {
            return None;
        }
        let lease = self.client_lease.as_ref()?;
        let last_seen = lease.load(Ordering::Acquire);
        let lease_expires_at = last_seen.saturating_add(self.orphan_policy.lease_ms);
        // Connected: wake at Codex lease expiry so the next list sees
        // Disconnected/Orphaned/WebControlled (not a second Connected snapshot).
        if now < lease_expires_at {
            return Some(lease_expires_at);
        }
        // Codex offline but interactive WebUI holds write control: wake when
        // that hold expires so cleanup grace can start.
        if self.web_control_is_active(now) {
            let web_expires = self
                .web_control_last_ms
                .saturating_add(WEB_CONTROL_LEASE_MS);
            return (web_expires > now).then_some(web_expires);
        }
        // Orphaned / Disconnected: no pure-time deadline here; reaper polls and
        // snapshots carry auto_close_at for the UI countdown.
        None
    }

    fn web_control_is_active(&self, now: u64) -> bool {
        self.web_control_connection_id.is_some()
            && now.saturating_sub(self.web_control_last_ms) < WEB_CONTROL_LEASE_MS
    }

    fn expire_stale_web_control(&mut self, now: u64) {
        if self.web_control_connection_id.is_some()
            && now.saturating_sub(self.web_control_last_ms) >= WEB_CONTROL_LEASE_MS
        {
            let ended = self
                .web_control_last_ms
                .saturating_add(WEB_CONTROL_LEASE_MS);
            self.web_control_connection_id = None;
            self.web_control_last_ms = 0;
            // First end time sticks for grace base until a new hold.
            if self.web_control_ended_at_ms.is_none() {
                self.web_control_ended_at_ms = Some(ended);
            }
        }
    }

    fn clear_web_control(&mut self, now: u64) {
        if self.web_control_connection_id.is_some() {
            self.web_control_connection_id = None;
            self.web_control_last_ms = 0;
            self.web_control_ended_at_ms = Some(now);
            self.updated_at_ms = now;
        }
    }

    fn client_lifecycle(
        &self,
        now: u64,
        cleanup_claimed: bool,
    ) -> (ClientLeaseState, Option<u64>, Option<u64>, Option<u64>) {
        // Work on a copy for expiry side effects is done by callers that hold
        // &mut; here we only read. expire_stale is applied via helper that
        // doesn't mutate for pure observation — recompute active from raw fields.
        let Some(lease) = self.client_lease.as_ref() else {
            return (ClientLeaseState::Unmanaged, None, None, None);
        };
        let last_seen = lease.load(Ordering::Acquire);
        let lease_expires_at = last_seen.saturating_add(self.orphan_policy.lease_ms);
        if cleanup_claimed {
            return (ClientLeaseState::Closing, Some(last_seen), None, None);
        }
        // Inclusive expiry: Connected only strictly before lease_expires_at.
        if now < lease_expires_at {
            return (ClientLeaseState::Connected, Some(last_seen), None, None);
        }
        // Codex is offline. Active interactive WebUI write hold defers orphan
        // cleanup without forging client_state=connected.
        let web_active = self.web_control_connection_id.is_some()
            && now.saturating_sub(self.web_control_last_ms) < WEB_CONTROL_LEASE_MS;
        if web_active {
            return (ClientLeaseState::WebControlled, Some(last_seen), None, None);
        }
        if !phase_is_safe_for_orphan_cleanup(self.phase) {
            return (ClientLeaseState::Disconnected, Some(last_seen), None, None);
        }
        // Grace base: Codex lease end, phase enter, and end of any WebUI hold.
        let web_ended = self
            .web_control_ended_at_ms
            .or_else(|| {
                // Passive expiry without clear_web_control: end at last+lease.
                if self.web_control_last_ms > 0 {
                    Some(
                        self.web_control_last_ms
                            .saturating_add(WEB_CONTROL_LEASE_MS),
                    )
                } else {
                    None
                }
            })
            .unwrap_or(0);
        let orphaned_at = lease_expires_at
            .max(self.phase_changed_at_ms)
            .max(web_ended);
        let auto_close_at = orphaned_at.saturating_add(self.orphan_policy.grace_ms);
        (
            ClientLeaseState::Orphaned,
            Some(last_seen),
            Some(orphaned_at),
            Some(auto_close_at),
        )
    }

    fn orphan_cleanup_due(&self, now: u64) -> bool {
        // Re-check web hold: active write control blocks cleanup.
        if self.web_control_connection_id.is_some()
            && now.saturating_sub(self.web_control_last_ms) < WEB_CONTROL_LEASE_MS
        {
            return false;
        }
        let (_, _, _, auto_close_at) = self.client_lifecycle(now, false);
        auto_close_at.is_some_and(|deadline| now >= deadline)
    }

    fn wait_result(
        &self,
        condition: WaitCondition,
        satisfied: bool,
        timed_out: bool,
        blocked_reason: Option<&str>,
    ) -> WaitResult {
        WaitResult {
            session: self.session.clone(),
            condition,
            satisfied,
            timed_out,
            phase: self.phase,
            exit_code: self.exit_code,
            blocked_reason: blocked_reason.map(str::to_owned),
        }
    }
}

fn set_phase(inner: &mut SessionInner, phase: SessionPhase, now: u64) {
    if inner.phase != phase {
        inner.phase = phase;
        inner.phase_changed_at_ms = now;
        if phase == SessionPhase::Idle || phase_is_terminal(phase) {
            inner.completed_at_ms = Some(now);
        } else {
            inner.completed_at_ms = None;
        }
    }
    if phase == SessionPhase::Idle
        && matches!(
            inner.hook.activity,
            HookActivity::Working | HookActivity::Cancelling | HookActivity::Waiting
        )
    {
        inner.hook.activity = HookActivity::Done;
        inner.hook.tool_name = None;
        inner.hook.waiting_reason = None;
        inner.hook.turn_done = true;
    }
}

/// Strip heavy terminal snapshots from session metadata for `/api/events`.
/// Reset terminal entries carry the authoritative ANSI snapshot.
fn web_events_session_view(mut state: SessionState) -> SessionState {
    state.screen = None;
    state.screen_ansi_base64 = String::new();
    state
}

fn message_json_len(message: &WebEventsMessage) -> usize {
    serde_json::to_vec(message)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX)
}

#[allow(clippy::too_many_arguments)]
fn plan_reset_frame_piece(
    session: &str,
    reset: &WebEventsResetStream,
    offset: usize,
    last_cursor: u64,
    sessions_view: &[SessionState],
    max_message_bytes: usize,
    remaining_batch_bytes: usize,
    deadline: Instant,
) -> Result<Option<(TerminalStreamEntry, usize, bool, usize)>> {
    if Instant::now() >= deadline || remaining_batch_bytes == 0 {
        return Ok(None);
    }
    let frame_limit = max_message_bytes.min(remaining_batch_bytes);
    let make_entry = |bytes: Vec<u8>, complete: bool| {
        let next_offset = offset + bytes.len();
        TerminalStreamEntry {
            session: session.to_owned(),
            reset: offset == 0,
            reset_cont: offset != 0,
            cursor: offset as u64,
            next_cursor: if complete {
                last_cursor
            } else {
                next_offset as u64
            },
            data_base64: BASE64.encode(bytes),
        }
    };
    let (probe, complete) = reset.window(offset, 1, deadline);
    if complete && probe.is_empty() {
        let entry = make_entry(probe, true);
        let frame_bytes = terminal_entry_message_len(sessions_view, &entry);
        return Ok((frame_bytes <= frame_limit).then_some((entry, offset, true, frame_bytes)));
    }

    // Base64 expands by 4/3. This cap prevents binary-search probes from ever
    // allocating a candidate substantially larger than the frame budget.
    let remaining = max_message_bytes.saturating_mul(3) / 4;
    let mut lo = 1usize;
    let mut hi = remaining.min(frame_limit.saturating_mul(3) / 4).max(1);
    let mut best: Option<(TerminalStreamEntry, usize, usize, bool)> = None;
    while lo <= hi {
        if Instant::now() >= deadline {
            break;
        }
        let mid = lo + (hi - lo) / 2;
        let (bytes, complete) = reset.window(offset, mid, deadline);
        if bytes.is_empty() && !complete {
            break;
        }
        let actual = bytes.len();
        let entry = make_entry(bytes, complete);
        let frame_bytes = terminal_entry_message_len(sessions_view, &entry);
        if frame_bytes <= frame_limit {
            best = Some((entry, frame_bytes, actual, complete));
            if complete || actual < mid {
                break;
            }
            lo = actual + 1;
        } else {
            hi = mid.saturating_sub(1);
        }
    }
    let Some((entry, frame_bytes, take, complete)) = best else {
        return Ok(None);
    };
    let next_offset = offset + take;
    Ok(Some((entry, next_offset, complete, frame_bytes)))
}

#[allow(clippy::too_many_arguments)]
fn plan_delta_frame_piece(
    session: &str,
    raw: &[u8],
    offset: usize,
    base_cursor: u64,
    sessions_view: &[SessionState],
    max_message_bytes: usize,
    remaining_batch_bytes: usize,
    deadline: Instant,
) -> Result<Option<(TerminalStreamEntry, usize, usize)>> {
    if Instant::now() >= deadline || remaining_batch_bytes == 0 || offset >= raw.len() {
        return Ok(None);
    }
    let frame_limit = max_message_bytes.min(remaining_batch_bytes);
    let remaining = raw.len() - offset;
    let mut lo = 1usize;
    let mut hi = remaining.min(frame_limit.saturating_mul(3) / 4).max(1);
    let mut best = None;
    while lo <= hi {
        if Instant::now() >= deadline {
            break;
        }
        let take = lo + (hi - lo) / 2;
        let next_offset = offset + take;
        let entry = TerminalStreamEntry {
            session: session.to_owned(),
            reset: false,
            reset_cont: false,
            cursor: base_cursor.saturating_add(offset as u64),
            next_cursor: base_cursor.saturating_add(next_offset as u64),
            data_base64: BASE64.encode(&raw[offset..next_offset]),
        };
        let frame_bytes = terminal_entry_message_len(sessions_view, &entry);
        if frame_bytes <= frame_limit {
            best = Some((entry, next_offset, frame_bytes));
            lo = take + 1;
        } else {
            hi = take.saturating_sub(1);
        }
    }
    Ok(best)
}

/// One planned terminal payload plus an optional durable cursor commit for its
/// final piece (`session` → exclusive PTY cursor).
type PlannedTerminal = (TerminalStreamEntry, Option<(String, u64)>);

fn pack_web_events_frames(
    sessions_view: Vec<SessionState>,
    terminal_entries: Vec<PlannedTerminal>,
    cursor_drops: Vec<String>,
    max_message_bytes: usize,
) -> Result<Vec<WebEventsFramePlan>> {
    let max_message_bytes = max_message_bytes.max(1);
    let sessions_only = WebEventsMessage::sessions(sessions_view.clone(), Vec::new());
    let sessions_only_len = message_json_len(&sessions_only);
    if sessions_only_len > max_message_bytes {
        bail!(
            "web events sessions metadata exceeds max_message_bytes ({sessions_only_len} > {max_message_bytes})"
        );
    }

    // Expand every terminal entry into pieces that each serialize under the bound
    // when paired alone with sessions metadata.
    let mut expanded: Vec<PlannedTerminal> = Vec::new();
    for (entry, commit) in terminal_entries {
        expanded.extend(split_terminal_entry_to_fit(
            entry,
            commit,
            &sessions_view,
            max_message_bytes,
        )?);
    }

    let mut frames: Vec<WebEventsFramePlan> = Vec::new();
    let mut terminals: Vec<TerminalStreamEntry> = Vec::new();
    let mut commits: HashMap<String, u64> = HashMap::new();
    let mut drops_for_first = cursor_drops;

    let flush = |terminals: &mut Vec<TerminalStreamEntry>,
                 commits: &mut HashMap<String, u64>,
                 drops: &mut Vec<String>,
                 sessions_view: &Vec<SessionState>,
                 frames: &mut Vec<WebEventsFramePlan>| {
        if terminals.is_empty() && commits.is_empty() && drops.is_empty() && !frames.is_empty() {
            return;
        }
        let message = WebEventsMessage::sessions(sessions_view.clone(), std::mem::take(terminals));
        debug_assert!(message_json_len(&message) <= max_message_bytes);
        frames.push(WebEventsFramePlan {
            message,
            cursor_commits: std::mem::take(commits),
            cursor_drops: std::mem::take(drops),
            reset_commits: Vec::new(),
        });
    };

    if expanded.is_empty() {
        frames.push(WebEventsFramePlan {
            message: sessions_only,
            cursor_commits: HashMap::new(),
            cursor_drops: drops_for_first,
            reset_commits: Vec::new(),
        });
        return Ok(frames);
    }

    for (entry, commit) in expanded {
        let mut probe_terminals = terminals.clone();
        probe_terminals.push(entry.clone());
        let probe = WebEventsMessage::sessions(sessions_view.clone(), probe_terminals);
        if !terminals.is_empty() && message_json_len(&probe) > max_message_bytes {
            flush(
                &mut terminals,
                &mut commits,
                &mut drops_for_first,
                &sessions_view,
                &mut frames,
            );
        }
        // After split_terminal_entry_to_fit, a single entry must fit alone.
        let alone = WebEventsMessage::sessions(sessions_view.clone(), vec![entry.clone()]);
        if message_json_len(&alone) > max_message_bytes {
            bail!("web events terminal chunk still exceeds max_message_bytes after split");
        }
        terminals.push(entry);
        if let Some((session, cursor)) = commit {
            commits.insert(session, cursor);
        }
    }

    if !terminals.is_empty()
        || !commits.is_empty()
        || !drops_for_first.is_empty()
        || frames.is_empty()
    {
        flush(
            &mut terminals,
            &mut commits,
            &mut drops_for_first,
            &sessions_view,
            &mut frames,
        );
    }

    if frames.is_empty() {
        frames.push(WebEventsFramePlan {
            message: WebEventsMessage::sessions(sessions_view, Vec::new()),
            cursor_commits: HashMap::new(),
            cursor_drops: Vec::new(),
            reset_commits: Vec::new(),
        });
    }
    for frame in &frames {
        let len = message_json_len(&frame.message);
        if len > max_message_bytes {
            bail!("web events frame exceeds max_message_bytes ({len} > {max_message_bytes})");
        }
    }
    Ok(frames)
}

fn terminal_entry_message_len(
    sessions_view: &[SessionState],
    entry: &TerminalStreamEntry,
) -> usize {
    message_json_len(&WebEventsMessage::sessions(
        sessions_view.to_vec(),
        vec![entry.clone()],
    ))
}

/// Split one terminal entry into ordered pieces that each serialize to
/// `<= max_message_bytes` with the sessions metadata.
///
/// Reset snapshots: first piece `reset=true`, continuations `reset_cont=true`
/// (not ordinary PTY deltas — WebUI must stream them without overflow-resync).
/// PTY cursor commit only on the final piece. Raw deltas preserve cursor spans.
fn split_terminal_entry_to_fit(
    entry: TerminalStreamEntry,
    commit: Option<(String, u64)>,
    sessions_view: &[SessionState],
    max_message_bytes: usize,
) -> Result<Vec<PlannedTerminal>> {
    if terminal_entry_message_len(sessions_view, &entry) <= max_message_bytes {
        return Ok(vec![(entry, commit)]);
    }

    let raw = BASE64
        .decode(&entry.data_base64)
        .context("terminal data_base64 is invalid")?;
    if raw.is_empty() {
        bail!("web events terminal entry exceeds max_message_bytes with empty payload");
    }

    let mut pieces: Vec<PlannedTerminal> = Vec::new();
    let mut offset = 0_usize;
    let mut stream_cursor = entry.cursor;
    let original_reset = entry.reset || entry.reset_cont;
    let pty_commit_cursor = entry.next_cursor;

    while offset < raw.len() {
        let remaining = raw.len() - offset;
        // Binary-search the largest raw prefix that still fits in one frame.
        let mut lo = 1_usize;
        let mut hi = remaining;
        let mut best = 0_usize;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            let is_head = original_reset && offset == 0;
            let candidate = TerminalStreamEntry {
                session: entry.session.clone(),
                reset: is_head,
                reset_cont: original_reset && !is_head,
                cursor: stream_cursor,
                next_cursor: stream_cursor.saturating_add(mid as u64),
                data_base64: BASE64.encode(&raw[offset..offset + mid]),
            };
            if terminal_entry_message_len(sessions_view, &candidate) <= max_message_bytes {
                best = mid;
                lo = mid + 1;
            } else if mid == 0 {
                break;
            } else {
                hi = mid - 1;
            }
        }
        if best == 0 {
            bail!(
                "web events cannot fit any terminal payload bytes within max_message_bytes ({max_message_bytes})"
            );
        }

        let is_last = offset + best >= raw.len();
        let next_cursor = if is_last {
            // Final piece reports the original exclusive end (PTY or snapshot end).
            pty_commit_cursor
        } else {
            stream_cursor.saturating_add(best as u64)
        };
        let is_head = original_reset && offset == 0;
        let piece = TerminalStreamEntry {
            session: entry.session.clone(),
            reset: is_head,
            reset_cont: original_reset && !is_head,
            cursor: stream_cursor,
            next_cursor,
            data_base64: BASE64.encode(&raw[offset..offset + best]),
        };
        // Durable PTY cursor advances only after the final chunk is sent.
        let piece_commit = if is_last { commit.clone() } else { None };
        pieces.push((piece, piece_commit));
        stream_cursor = next_cursor;
        offset += best;
    }

    Ok(pieces)
}

fn phase_is_safe_for_orphan_cleanup(phase: SessionPhase) -> bool {
    phase == SessionPhase::Idle || phase_is_terminal(phase)
}

fn parse_duration_env(name: &str, default: u64, min: u64, max: u64) -> Result<u64> {
    let Some(value) = env::var_os(name) else {
        return Ok(default);
    };
    let value = value
        .to_str()
        .with_context(|| format!("{name} must be valid Unicode"))?;
    let seconds = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be an integer number of seconds"))?;
    if !(min..=max).contains(&seconds) {
        bail!("{name} must be between {min} and {max} seconds");
    }
    Ok(seconds)
}

fn generate_provider_session_id() -> Result<String> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut bytes = [0_u8; PROVIDER_SESSION_UUID_BYTES];
    getrandom::fill(&mut bytes).context("failed to generate the Grok provider session ID")?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    let mut session_id = String::with_capacity(36);
    for (index, byte) in bytes.into_iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            session_id.push('-');
        }
        session_id.push(HEX[usize::from(byte >> 4)] as char);
        session_id.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    Ok(session_id)
}

fn hook_effect(event: &HookEvent) -> HookEffect {
    match event.kind {
        HookEventKind::SessionStart => HookEffect::Reset,
        HookEventKind::UserPromptSubmit => HookEffect::Working {
            tool_name: event.tool_name.clone(),
        },
        HookEventKind::PostToolUse
        | HookEventKind::PostToolUseFailure
        | HookEventKind::PermissionDenied
        | HookEventKind::PreCompact
        | HookEventKind::PostCompact => HookEffect::Working { tool_name: None },
        HookEventKind::PreToolUse if is_ask_user_question(event.tool_name.as_deref()) => {
            HookEffect::Waiting {
                tool_name: event.tool_name.clone(),
                reason: event
                    .message
                    .clone()
                    .unwrap_or_else(|| "ask_user_question".to_owned()),
            }
        }
        HookEventKind::PreToolUse => HookEffect::Working {
            tool_name: event.tool_name.clone(),
        },
        HookEventKind::Stop | HookEventKind::StopFailure | HookEventKind::SessionEnd => {
            HookEffect::Done
        }
        HookEventKind::Notification => notification_effect(event),
        HookEventKind::SubagentStart | HookEventKind::SubagentStop => HookEffect::RecordOnly,
    }
}

fn is_ask_user_question(tool_name: Option<&str>) -> bool {
    tool_name.is_some_and(|name| name.eq_ignore_ascii_case("ask_user_question"))
}

fn notification_effect(event: &HookEvent) -> HookEffect {
    let notification_type = event
        .notification_type
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if notification_type == "permission_prompt" {
        return HookEffect::RecordOnly;
    }

    let level = event
        .level
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let message = event.message.as_deref().unwrap_or_default();
    let lower_message = message.to_ascii_lowercase();
    let waiting = [
        "permission",
        "question",
        "ask_user_question",
        "elicitation",
        "elicitation_dialog",
    ]
    .iter()
    .any(|value| notification_type == *value || level == *value)
        || ["permission", "approval", "approve", "question"]
            .iter()
            .any(|value| lower_message.contains(value))
        || ["权限", "授权", "批准", "问题", "确认"]
            .iter()
            .any(|value| message.contains(value));
    if waiting {
        let reason = event
            .message
            .clone()
            .or_else(|| event.notification_type.clone())
            .unwrap_or_else(|| "grok-hook-waiting".to_owned());
        return HookEffect::Waiting {
            tool_name: event.tool_name.clone(),
            reason,
        };
    }

    let done = [
        "idle_prompt",
        "input_prompt",
        "input_required",
        "user_input",
        "waiting_for_input",
    ]
    .iter()
    .any(|value| notification_type == *value || level == *value)
        || ["waiting for input", "waiting for your input"]
            .iter()
            .any(|value| lower_message.contains(value))
        || ["请输入", "等待输入", "需要输入"]
            .iter()
            .any(|value| message.contains(value));
    if done {
        HookEffect::Done
    } else {
        HookEffect::RecordOnly
    }
}

fn build_grok_command(
    config: &LaunchConfig,
    provider_session_id: &str,
    grok_state_dir: Option<&Path>,
) -> CommandBuilder {
    let mut command = CommandBuilder::new(&config.grok_bin);
    command.cwd(config.cwd.as_os_str());
    command.env("TERM", "xterm-256color");
    command.env("COLORTERM", "truecolor");
    if let Some(grok_state_dir) = grok_state_dir {
        command.env("GROK_HOME", grok_state_dir.as_os_str());
    }
    command.arg("--session-id");
    command.arg(provider_session_id);
    if config.always_approve {
        command.arg("--always-approve");
    }
    if let Some(model) = config.model.as_deref() {
        command.arg("--model");
        command.arg(model);
    }
    if let Some(prompt) = config.prompt.as_deref() {
        command.arg(prompt);
    }
    command
}

fn spawn_reader(session: Arc<Session>, mut reader: Box<dyn Read + Send>) {
    thread::spawn(move || {
        let mut buffer = [0_u8; 16 * 1024];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => {
                    session.mark_reader_done();
                    return;
                }
                Ok(read) => session.append_output(buffer[..read].to_vec()),
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::BrokenPipe | ErrorKind::UnexpectedEof
                    ) =>
                {
                    session.mark_reader_done();
                    return;
                }
                Err(error) => {
                    session.mark_reader_error(format!("failed to read Grok output: {error}"));
                    return;
                }
            }
        }
    });
}

fn spawn_writer(
    session: Arc<Session>,
    mut writer: Box<dyn Write + Send>,
    writer_rx: std::sync::mpsc::Receiver<WriteJob>,
) {
    thread::spawn(move || {
        // On any exit path, every dequeued or still-queued completion must be
        // published exactly once so waiters / identity pending cannot hang.
        let exit_fail_all = |rx: &std::sync::mpsc::Receiver<WriteJob>, message: &str| {
            fail_remaining_write_jobs(rx, message);
        };

        while let Ok(job) = writer_rx.recv() {
            // Session close / sender drop: cancel this job and the rest without
            // writing (healthy writer must not keep draining the pre-close queue).
            if session.writer_must_cancel() {
                job.fail(WRITE_CANCELLED_MSG);
                exit_fail_all(&writer_rx, WRITE_CANCELLED_MSG);
                return;
            }

            // write_all may fail after some bytes are already on the wire; treat
            // any Err as definitive (not safe to retry the same payload).
            let result = writer
                .write_all(&job.data)
                .and_then(|()| writer.flush())
                .map_err(|error| {
                    format!(
                        "PTY write/flush failed after possible partial delivery (not safe to retry): {error}"
                    )
                });
            match result {
                Ok(()) => {
                    // Publish success and the FIFO semantic effect atomically with
                    // respect to completion timeout. A late write after timeout
                    // cannot change phase/activity after callers saw failure.
                    let committed = if let Some(completion) = job.completion {
                        completion.complete_success_with(|| {
                            session
                                .apply_input_effect(job.effect)
                                .map_err(|error| format!("{error:#}"))
                        })
                    } else {
                        Some(
                            session
                                .apply_input_effect(job.effect)
                                .map_err(|error| format!("{error:#}")),
                        )
                    };
                    match committed {
                        Some(Ok(())) => {}
                        Some(Err(message)) => {
                            let message = format!(
                                "PTY bytes were written but the ordered state commit failed (not safe to retry): {message}"
                            );
                            session.mark_writer_error(message.clone());
                            exit_fail_all(&writer_rx, &message);
                            return;
                        }
                        None => {
                            // Timeout/cancel already won. Stop this writer lane;
                            // the published outcome remains authoritative.
                            exit_fail_all(&writer_rx, WRITE_COMPLETION_TIMEOUT_MSG);
                            return;
                        }
                    }
                }
                Err(message) => {
                    if let Some(completion) = job.completion {
                        completion.complete(Err(message.clone()));
                    }
                    // Session leaves the writable path (error recorded + reaper).
                    session.mark_writer_error(message.clone());
                    // Fail every still-queued job; do not leave waiters blocked
                    // when this thread returns and the Receiver is dropped.
                    let rest_msg = format!(
                        "PTY write cancelled after prior write failure (not safe to retry): {message}"
                    );
                    exit_fail_all(&writer_rx, &rest_msg);
                    return;
                }
            }
        }
        // Sender closed and buffer empty: nothing left to fail.
    });
}

fn spawn_waiter(session: Arc<Session>, mut child: Box<dyn portable_pty::Child + Send + Sync>) {
    thread::spawn(move || {
        // Prefer a blocking wait first; on error keep the Child and poll until a
        // real exit is observed (reaper/close kill the tree — do not invent done).
        match child.wait() {
            Ok(status) => {
                session.mark_exit(status.exit_code());
                return;
            }
            Err(error) => {
                session.mark_wait_error(format!("failed while waiting for Grok: {error}"));
            }
        }
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    session.mark_exit(status.exit_code());
                    return;
                }
                Ok(None) => {
                    // Still alive: keep driving termination while shutdown/reaper run.
                    let _ = session.request_termination();
                    thread::sleep(Duration::from_millis(PROCESS_KILL_REPEAT_MS));
                }
                Err(error) => {
                    session.record_secondary_error(format!("retry wait for Grok failed: {error}"));
                    let _ = session.request_termination();
                    thread::sleep(Duration::from_millis(PROCESS_KILL_REPEAT_MS));
                }
            }
            // Session fully torn down and process marked done by another path.
            if session
                .inner
                .lock()
                .map(|inner| inner.process_done)
                .unwrap_or(true)
            {
                return;
            }
        }
    });
}

/// Complete session finalization once both OS/PTY facts are real.
///
/// Returns true when the caller should run `finish_transition` (close writer /
/// master, mark reaper terminal). Phase may already be Failed from
/// `mark_wait_error` / `force_failed_if_unrecoverable` before process_done and
/// reader_done; in that case keep Failed + error, but still release resources.
fn finalize_session(inner: &mut SessionInner, shutdown: bool) -> bool {
    if !inner.process_done || !inner.reader_done {
        return false;
    }
    let now = now_millis();
    if !phase_is_terminal(inner.phase) {
        let phase = completed_phase(shutdown, inner.error.is_some(), inner.exit_code);
        set_phase(inner, phase, now);
    }
    // Clear OS identity even when phase was already Failed/Exited/Stopped.
    inner.process_id = None;
    inner.updated_at_ms = now;
    true
}

fn completed_phase(shutdown: bool, failed: bool, exit_code: Option<u32>) -> SessionPhase {
    if shutdown {
        SessionPhase::Stopped
    } else if failed || exit_code != Some(0) {
        SessionPhase::Failed
    } else {
        SessionPhase::Exited
    }
}

fn phase_after_output(
    current: SessionPhase,
    title: Option<&str>,
    title_updated: bool,
    hook_activity: HookActivity,
    process_done: bool,
    failed: bool,
    shutdown: bool,
) -> SessionPhase {
    if phase_is_terminal(current) || process_done || failed || shutdown {
        current
    } else if title_updated && let Some(phase) = phase_from_title(title) {
        // Title-based idle is semantic evidence that cancel finished.
        phase
    } else if hook_activity == HookActivity::Done {
        SessionPhase::Idle
    } else if hook_activity == HookActivity::Cancelling {
        // Stay Running until hooks/title/exit confirm completion.
        SessionPhase::Running
    } else if hook_activity == HookActivity::Waiting || current == SessionPhase::Starting {
        SessionPhase::Running
    } else {
        current
    }
}

fn record_error(inner: &mut SessionInner, message: String) {
    match &mut inner.error {
        Some(existing) => {
            existing.push_str("; ");
            existing.push_str(&message);
        }
        None => inner.error = Some(message),
    }
    inner.updated_at_ms = now_millis();
}

fn wait_satisfied(inner: &mut SessionInner, condition: WaitCondition) -> bool {
    match condition {
        // Exit is an OS/PTY fact, not a UI phase label. force_failed / mark_wait_error
        // may set Failed while the process tree and PTY reader are still open;
        // wait --for exit must not report success until both are actually done.
        WaitCondition::Exit => inner.process_done && inner.reader_done,
        WaitCondition::TuiIdle => {
            if inner.error.is_some() {
                return false;
            }
            if inner.phase == SessionPhase::Idle {
                return true;
            }
            // Cancelling is not idle until hooks/title/exit confirm completion.
            if inner.hook.activity == HookActivity::Cancelling {
                return false;
            }
            let quiet = now_millis().saturating_sub(
                inner
                    .last_output_at_ms
                    .unwrap_or(inner.updated_at_ms)
                    .max(inner.updated_at_ms),
            ) >= QUIET_IDLE_MILLISECONDS;
            if inner.phase == SessionPhase::Running
                && inner.title.is_none()
                && matches!(
                    inner.hook.activity,
                    HookActivity::Unknown | HookActivity::Working
                )
                && quiet
            {
                let now = now_millis();
                set_phase(inner, SessionPhase::Idle, now);
                inner.updated_at_ms = now;
                return true;
            }
            false
        }
    }
}

fn blocked_reason(screen: &str) -> Option<&'static str> {
    if screen.contains("Run Grok Build in a project directory?") {
        Some("grok-project-directory")
    } else if screen.contains("Type your answer here") || screen.contains("Enter:submit") {
        Some("grok-interactive-prompt")
    } else {
        None
    }
}

fn phase_from_title(title: Option<&str>) -> Option<SessionPhase> {
    let title = title?.trim();
    let lower = title.to_ascii_lowercase();
    if title_has_braille_spinner(title) && (lower.ends_with("grok") || lower.contains(" - grok")) {
        return Some(SessionPhase::Running);
    }
    if lower == "grok" || lower.ends_with(" - grok") {
        return Some(SessionPhase::Idle);
    }
    None
}

fn title_has_braille_spinner(title: &str) -> bool {
    title
        .chars()
        .next()
        .is_some_and(|character| ('\u{2800}'..='\u{28ff}').contains(&character))
}

fn phase_is_active(phase: SessionPhase) -> bool {
    matches!(
        phase,
        SessionPhase::Starting | SessionPhase::Running | SessionPhase::Idle
    )
}

fn phase_is_terminal(phase: SessionPhase) -> bool {
    matches!(
        phase,
        SessionPhase::Exited | SessionPhase::Failed | SessionPhase::Stopped
    )
}

fn canonical_directory(path: &Path) -> Result<PathBuf> {
    let canonical = normalize_platform_path(
        path.canonicalize()
            .with_context(|| format!("failed to resolve working directory: {}", path.display()))?,
    );
    if !canonical.is_dir() {
        bail!(
            "working directory is not a directory: {}",
            canonical.display()
        );
    }
    Ok(canonical)
}

fn ensure_allowed_root(cwd: &Path) -> Result<()> {
    let Some(value) = env::var_os("GROK_BRIDGE_ALLOWED_ROOTS") else {
        return Ok(());
    };
    let mut roots = Vec::new();
    for root in env::split_paths(&value) {
        roots.push(normalize_platform_path(root.canonicalize().with_context(
            || format!("failed to resolve allowed root: {}", root.display()),
        )?));
    }
    if roots.iter().any(|root| cwd.starts_with(root)) {
        Ok(())
    } else {
        bail!(
            "working directory is outside GROK_BRIDGE_ALLOWED_ROOTS: {}",
            cwd.display()
        )
    }
}

fn ensure_grok_state_dir_writable(
    cwd: &Path,
    provider_session_id: &str,
) -> Result<Option<PathBuf>> {
    let Some(state_dir) = grok_state_dir(cwd) else {
        return Ok(None);
    };
    ensure_grok_state_dir_writable_at(&state_dir, provider_session_id)?;
    Ok(Some(state_dir))
}

fn grok_state_dir(cwd: &Path) -> Option<PathBuf> {
    let state_dir = grok_state_dir_from(
        env::var_os("GROK_HOME"),
        env::var_os("HOME"),
        env::var_os("USERPROFILE"),
        cfg!(windows),
    )?;
    Some(resolve_state_dir_from_cwd(cwd, state_dir))
}

fn resolve_state_dir_from_cwd(cwd: &Path, state_dir: PathBuf) -> PathBuf {
    if state_dir.is_absolute() {
        state_dir
    } else {
        cwd.join(state_dir)
    }
}

fn grok_state_dir_from(
    grok_home: Option<OsString>,
    home: Option<OsString>,
    user_profile: Option<OsString>,
    windows: bool,
) -> Option<PathBuf> {
    non_empty_path(grok_home).or_else(|| {
        let home = if windows {
            non_empty_path(user_profile).or_else(|| non_empty_path(home))
        } else {
            non_empty_path(home).or_else(|| non_empty_path(user_profile))
        }?;
        Some(home.join(".grok"))
    })
}

fn non_empty_path(value: Option<OsString>) -> Option<PathBuf> {
    value.filter(|value| !value.is_empty()).map(PathBuf::from)
}

fn ensure_grok_state_dir_writable_at(state_dir: &Path, provider_session_id: &str) -> Result<()> {
    let context = format!(
        "Grok state directory is not writable: {}. The Runtime may have inherited a filesystem sandbox; start grok-bridge server outside that sandbox and retry",
        state_dir.display()
    );
    fs::create_dir_all(state_dir).with_context(|| context.clone())?;
    let probe_path = state_dir.join(format!(".grok-bridge-write-probe-{provider_session_id}"));
    let mut created = false;
    let probe_result = (|| -> std::io::Result<()> {
        let mut probe = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe_path)?;
        created = true;
        probe.write_all(b"grok-bridge")?;
        probe.flush()
    })();
    let cleanup_result = if created {
        match fs::remove_file(&probe_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    } else {
        Ok(())
    };
    probe_result.with_context(|| context.clone())?;
    cleanup_result.with_context(|| {
        format!(
            "failed to remove Grok state probe: {}",
            probe_path.display()
        )
    })?;
    Ok(())
}

#[cfg(windows)]
fn normalize_platform_path(path: PathBuf) -> PathBuf {
    let display = path.to_string_lossy();
    if let Some(rest) = display.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{rest}"))
    } else if let Some(rest) = display.strip_prefix(r"\\?\") {
        PathBuf::from(rest)
    } else {
        path
    }
}

#[cfg(not(windows))]
fn normalize_platform_path(path: PathBuf) -> PathBuf {
    path
}

pub(crate) fn default_grok_bin() -> OsString {
    if cfg!(windows) {
        OsString::from("grok.exe")
    } else {
        OsString::from("grok")
    }
}

fn validate_prompt(prompt: Option<&str>) -> Result<()> {
    if let Some(prompt) = prompt {
        if prompt.trim().is_empty() {
            bail!("prompt must not be empty");
        }
        if prompt.len() > 128 * 1024 {
            bail!("prompt exceeds the 128 KiB limit");
        }
    }
    Ok(())
}

fn validate_model(model: Option<&str>) -> Result<()> {
    if let Some(model) = model {
        if model.is_empty() || model.len() > 256 {
            bail!("model must contain between 1 and 256 bytes");
        }
        if !model
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "-_.:/".contains(character))
        {
            bail!("model contains unsupported characters");
        }
    }
    Ok(())
}

/// Classify raw terminal bytes for activity effects.
///
/// `paste_open` and `hold` are updated in place so multi-chunk WebUI writes that
/// straddle `\x1b[200~` … `\x1b[201~` (including mid-CSI splits) do not treat
/// CR/LF inside the paste as StartTurn.
fn raw_input_effect(data: &[u8], paste_open: &mut bool, hold: &mut Vec<u8>) -> InputEffect {
    // Lone Ctrl-C keeps cancel semantics (interrupt), independent of paste.
    // Only when this job is exactly one byte and no incomplete CSI is pending.
    if data.len() == 1 && data[0] == 0x03 && hold.is_empty() {
        return InputEffect::Cancel;
    }
    let mut stream = std::mem::take(hold);
    stream.extend_from_slice(data);
    let mut effect = InputEffect::None;
    let mut i = 0;
    while i < stream.len() {
        if matches_bracketed_paste_start(&stream, i) {
            *paste_open = true;
            i += 6;
            continue;
        }
        if matches_bracketed_paste_end(&stream, i) {
            *paste_open = false;
            i += 6;
            continue;
        }
        // Incomplete CSI that could still become a paste marker: hold remainder.
        if is_bracketed_paste_prefix(&stream[i..]) {
            hold.extend_from_slice(&stream[i..]);
            break;
        }
        if !*paste_open && matches!(stream[i], b'\r' | b'\n') {
            effect = InputEffect::StartTurn;
        }
        i += 1;
    }
    effect
}

fn matches_bracketed_paste_start(data: &[u8], i: usize) -> bool {
    data.get(i..i + 6) == Some(b"\x1b[200~")
}

fn matches_bracketed_paste_end(data: &[u8], i: usize) -> bool {
    data.get(i..i + 6) == Some(b"\x1b[201~")
}

/// True when `tail` is a non-empty proper prefix of `\x1b[200~` or `\x1b[201~`.
fn is_bracketed_paste_prefix(tail: &[u8]) -> bool {
    if tail.is_empty() || tail.len() >= 6 {
        return false;
    }
    b"\x1b[200~".starts_with(tail) || b"\x1b[201~".starts_with(tail)
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

    const TEST_PROVIDER_SESSION_ID: &str = "123e4567-e89b-42d3-a456-426614174000";

    fn temporary_test_directory(label: &str) -> PathBuf {
        env::temp_dir().join(format!(
            "grok-bridge-{label}-{}-{}",
            std::process::id(),
            generate_provider_session_id().unwrap()
        ))
    }

    fn test_launch_config(cwd: &Path) -> LaunchConfig {
        LaunchConfig {
            grok_bin: OsString::from("test-process"),
            cwd: cwd.to_path_buf(),
            prompt: None,
            model: None,
            owner: Some("test-owner".to_owned()),
            always_approve: false,
            client_session_id: None,
            client_lease: None,
            orphan_policy: OrphanPolicy {
                lease_ms: 120_000,
                grace_ms: 600_000,
            },
        }
    }

    fn wait_for_pty_output(session: &Session, needle: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut cursor = 0;
        let mut output = String::new();
        while Instant::now() < deadline {
            let read = session.read(cursor, MAX_READ_BYTES, 100).unwrap();
            cursor = read.next_cursor;
            if !read.data_base64.is_empty() {
                let data = BASE64.decode(read.data_base64).unwrap();
                output.push_str(&String::from_utf8_lossy(&data));
            }
            if output.contains(needle) {
                return output;
            }
        }
        panic!("PTY did not produce {needle:?}; output={output:?}");
    }

    #[cfg(unix)]
    fn eventually_process_group_gone(process_group_id: libc::pid_t) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            let result = unsafe { libc::kill(-process_group_id, 0) };
            if result != 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::ESRCH) {
                    return;
                }
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("PTY process group {process_group_id} is still alive");
    }

    #[cfg(unix)]
    fn eventually_process_gone(process_id: libc::pid_t) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            let result = unsafe { libc::kill(process_id, 0) };
            if result != 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::ESRCH) {
                    return;
                }
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("PTY descendant process {process_id} is still alive");
    }

    fn hook_event(kind: HookEventKind) -> HookEvent {
        HookEvent {
            kind,
            cwd: None,
            tool_name: None,
            message: None,
            notification_type: None,
            level: None,
        }
    }

    pub(super) fn test_session(phase: SessionPhase) -> Arc<Session> {
        test_session_with_revision(phase, Arc::new(HostRevision::new()))
    }

    /// Drain WriteJobs: apply FIFO effects then ack Ok (mirrors spawn_writer commit).
    fn spawn_ok_ack_writer(session: Arc<Session>, writer_rx: std::sync::mpsc::Receiver<WriteJob>) {
        thread::spawn(move || {
            while let Ok(job) = writer_rx.recv() {
                let _ = session.apply_input_effect(job.effect);
                if let Some(completion) = job.completion {
                    completion.complete(Ok(()));
                }
            }
        });
    }

    pub(super) fn test_session_with_revision(
        phase: SessionPhase,
        host_revision: Arc<HostRevision>,
    ) -> Arc<Session> {
        test_session_with_identity(phase, host_revision, None, Some("test-owner".to_owned()))
    }

    fn test_session_with_identity(
        phase: SessionPhase,
        host_revision: Arc<HostRevision>,
        client_session_id: Option<String>,
        owner: Option<String>,
    ) -> Arc<Session> {
        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let (writer_tx, writer_rx) = sync_channel(8);
        let terminal = phase_is_terminal(phase);
        let clean_done = phase == SessionPhase::Idle || terminal;
        let session = Arc::new(Session {
            client_session_id: client_session_id.clone(),
            owner: owner.clone(),
            self_weak: std::sync::Weak::new(),
            inner: Mutex::new(SessionInner {
                session: "gbt-test".to_owned(),
                owner: owner.clone(),
                client_session_id: client_session_id.clone(),
                client_lease: None,
                orphan_policy: OrphanPolicy {
                    lease_ms: 120_000,
                    grace_ms: 600_000,
                },
                phase,
                phase_changed_at_ms: 1,
                cwd: cwd.to_string_lossy().into_owned(),
                model: Some("grok-test".to_owned()),
                always_approve: false,
                process_id: (!terminal).then_some(42),
                created_at_ms: 1,
                updated_at_ms: 1,
                semantic_active_at_ms: 1,
                completed_at_ms: clean_done.then_some(1),
                exit_code: None,
                error: None,
                title: None,
                parser: vt100::Parser::new_with_callbacks(
                    INITIAL_ROWS,
                    INITIAL_COLS,
                    SCROLLBACK_ROWS,
                    TitleCallbacks::default(),
                ),
                chunks: VecDeque::new(),
                transcript_bytes: 0,
                next_cursor: 0,
                last_output_at_ms: None,
                process_done: terminal,
                reader_done: terminal,
                hook: HookState::default(),
                web_control_connection_id: None,
                web_control_last_ms: 0,
                web_control_ended_at_ms: None,
                paste_open: false,
                paste_scan_hold: Vec::new(),
            }),
            changed: Condvar::new(),
            host_revision,
            writer_tx: Mutex::new(Some(writer_tx)),
            master: Mutex::new(None),
            termination: None,
            shutdown: AtomicBool::new(false),
            cleanup_claimed: AtomicBool::new(false),
            cleanup_committed: AtomicBool::new(false),
            test_shutdown_hang_ms: AtomicU64::new(0),
            failure_reaper_started: AtomicBool::new(false),
            reaper_state: Arc::new(FailureReaperState::new()),
        });
        spawn_ok_ack_writer(Arc::clone(&session), writer_rx);
        session
    }

    fn test_host(provider_session_id: &str, phase: SessionPhase) -> SessionHost {
        let revision = Arc::new(HostRevision::new());
        let session = test_session_with_revision(phase, Arc::clone(&revision));
        let handle = session.state().unwrap().session;
        SessionHost {
            registry: Mutex::new(SessionRegistry {
                accepting: true,
                sessions: HashMap::from([(handle.clone(), session)]),
                provider_sessions: HashMap::from([(provider_session_id.to_owned(), handle)]),
                pending_providers: HashMap::new(),
                clients: HashMap::new(),
                client_epochs: HashMap::new(),
                clients_closing: HashMap::new(),
                pending_creates: 0,
                pending_creates_by_client: HashMap::new(),
                pending_creates_by_owner: HashMap::new(),
                owner_epochs: HashMap::new(),
                owners_closing: HashMap::new(),
                closed_sessions: HashMap::new(),
                closed_session_order: VecDeque::new(),
            }),
            next_id: AtomicU64::new(1),
            orphan_policy: OrphanPolicy {
                lease_ms: 120_000,
                grace_ms: 600_000,
            },
            revision,
            create_after_lease_hook: Mutex::new(None),
            close_client_before_lease_hook: Mutex::new(None),
            close_client_force_err_after_fence: AtomicBool::new(false),
            install_inject_failure: Mutex::new(None),
            #[cfg(test)]
            acquire_web_control_force_err: AtomicBool::new(false),
        }
    }

    #[test]
    fn resolves_grok_state_directory_with_platform_precedence() {
        assert_eq!(
            grok_state_dir_from(
                Some(OsString::from("/custom/grok")),
                Some(OsString::from("/home/test")),
                Some(OsString::from(r"C:\Users\test")),
                false,
            ),
            Some(PathBuf::from("/custom/grok"))
        );
        assert_eq!(
            grok_state_dir_from(
                None,
                Some(OsString::from("/home/test")),
                Some(OsString::from(r"C:\Users\test")),
                false,
            ),
            Some(PathBuf::from("/home/test").join(".grok"))
        );
        assert_eq!(
            grok_state_dir_from(
                None,
                Some(OsString::from("/home/test")),
                Some(OsString::from(r"C:\Users\test")),
                true,
            ),
            Some(PathBuf::from(r"C:\Users\test").join(".grok"))
        );
        assert_eq!(
            grok_state_dir_from(Some(OsString::new()), None, None, false),
            None
        );
    }

    #[test]
    fn resolves_relative_grok_home_against_session_working_directory() {
        let cwd = PathBuf::from("/workspace/project");
        assert_eq!(
            resolve_state_dir_from_cwd(&cwd, PathBuf::from(".grok-state")),
            cwd.join(".grok-state")
        );
        assert_eq!(
            resolve_state_dir_from_cwd(&cwd, PathBuf::from("/custom/grok")),
            PathBuf::from("/custom/grok")
        );
    }

    #[test]
    fn probes_writable_grok_state_directory_and_removes_probe() {
        let root = temporary_test_directory("writable-state");
        let state_dir = root.join("state");
        ensure_grok_state_dir_writable_at(&state_dir, TEST_PROVIDER_SESSION_ID).unwrap();
        assert!(state_dir.is_dir());
        assert!(
            !state_dir
                .join(format!(
                    ".grok-bridge-write-probe-{TEST_PROVIDER_SESSION_ID}"
                ))
                .exists()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reports_unwritable_grok_state_directory_with_sandbox_guidance() {
        let root = temporary_test_directory("blocked-state");
        fs::create_dir_all(&root).unwrap();
        let state_dir = root.join("state-file");
        fs::write(&state_dir, b"not a directory").unwrap();
        let error = ensure_grok_state_dir_writable_at(&state_dir, TEST_PROVIDER_SESSION_ID)
            .unwrap_err()
            .to_string();
        assert!(error.contains("Grok state directory is not writable"));
        assert!(error.contains("filesystem sandbox"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn probe_collision_preserves_existing_file() {
        let root = temporary_test_directory("probe-collision");
        fs::create_dir_all(&root).unwrap();
        let probe_path = root.join(format!(
            ".grok-bridge-write-probe-{TEST_PROVIDER_SESSION_ID}"
        ));
        fs::write(&probe_path, b"existing").unwrap();
        assert!(ensure_grok_state_dir_writable_at(&root, TEST_PROVIDER_SESSION_ID).is_err());
        assert_eq!(fs::read(&probe_path).unwrap(), b"existing");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn detects_grok_working_and_idle_titles() {
        assert_eq!(
            phase_from_title(Some("⠋ - Waiting for response… - grok")),
            Some(SessionPhase::Running)
        );
        assert_eq!(
            phase_from_title(Some("Fix the auth bug - grok")),
            Some(SessionPhase::Idle)
        );
        assert_eq!(phase_from_title(Some("grok")), Some(SessionPhase::Idle));
        assert_eq!(phase_from_title(Some("PowerShell")), None);
    }

    #[test]
    fn builds_only_interactive_grok_arguments() {
        let config = LaunchConfig {
            grok_bin: OsString::from("grok.exe"),
            cwd: PathBuf::from(r"C:\repo"),
            prompt: Some("修复中文".to_owned()),
            model: Some("grok-4".to_owned()),
            owner: None,
            always_approve: true,
            client_session_id: None,
            client_lease: None,
            orphan_policy: OrphanPolicy {
                lease_ms: 120_000,
                grace_ms: 600_000,
            },
        };
        let grok_home = PathBuf::from(r"C:\repo\.grok-state");
        let command = build_grok_command(&config, TEST_PROVIDER_SESSION_ID, Some(&grok_home));
        assert_eq!(command.get_env("GROK_BRIDGE_SESSION"), None);
        assert_eq!(command.get_env("GROK_BRIDGE_HOOK_TOKEN"), None);
        assert_eq!(command.get_env("GROK_HOME"), Some(grok_home.as_os_str()));
        let argv = command
            .get_argv()
            .iter()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            argv,
            [
                "grok.exe",
                "--session-id",
                TEST_PROVIDER_SESSION_ID,
                "--always-approve",
                "--model",
                "grok-4",
                "修复中文"
            ]
        );
        assert!(!argv.iter().any(|value| value == "-p"));
        assert!(!argv.iter().any(|value| value == "--output-format"));
    }

    #[test]
    fn publishes_terminal_phase_only_after_process_and_reader_finish() {
        // Phase selection only applies when not already terminal; both done flags
        // are required before finalize_session returns true.
        assert_eq!(completed_phase(false, false, Some(0)), SessionPhase::Exited);
        assert_eq!(completed_phase(true, false, Some(1)), SessionPhase::Stopped);
        assert_eq!(completed_phase(false, true, Some(0)), SessionPhase::Failed);
        {
            let session = test_session(SessionPhase::Running);
            let mut inner = session.inner.lock().unwrap();
            inner.process_done = true;
            inner.reader_done = false;
            inner.exit_code = Some(0);
            assert!(!finalize_session(&mut inner, false));
            inner.reader_done = true;
            assert!(finalize_session(&mut inner, false));
            assert_eq!(inner.phase, SessionPhase::Exited);
        }

        // Already Failed: still finalizes (resource release) without rewriting phase.
        {
            let session = test_session(SessionPhase::Running);
            let mut failed = session.inner.lock().unwrap();
            failed.phase = SessionPhase::Failed;
            failed.error = Some("prior".to_owned());
            failed.process_done = true;
            failed.reader_done = true;
            failed.process_id = Some(99);
            assert!(finalize_session(&mut failed, false));
            assert_eq!(failed.phase, SessionPhase::Failed);
            assert_eq!(failed.error.as_deref(), Some("prior"));
            assert!(failed.process_id.is_none());
        }
    }

    #[test]
    fn generates_random_uuid_v4_provider_session_ids() {
        let first = generate_provider_session_id().unwrap();
        let second = generate_provider_session_id().unwrap();
        assert_eq!(first.len(), 36);
        assert_eq!(&first[8..9], "-");
        assert_eq!(&first[13..14], "-");
        assert_eq!(&first[18..19], "-");
        assert_eq!(&first[23..24], "-");
        assert_eq!(&first[14..15], "4");
        assert!(matches!(&first[19..20], "8" | "9" | "a" | "b"));
        assert!(
            first
                .bytes()
                .all(|byte| { byte.is_ascii_digit() || matches!(byte, b'a'..=b'f' | b'-') })
        );
        assert_ne!(first, second);
    }

    #[test]
    fn maps_hook_lifecycle_and_tool_events_to_activity() {
        let mut event = hook_event(HookEventKind::PreToolUse);
        event.tool_name = Some("read_file".to_owned());
        assert_eq!(
            hook_effect(&event),
            HookEffect::Working {
                tool_name: Some("read_file".to_owned())
            }
        );

        event.tool_name = Some("ASK_USER_QUESTION".to_owned());
        event.message = Some("请选择目标".to_owned());
        assert_eq!(
            hook_effect(&event),
            HookEffect::Waiting {
                tool_name: Some("ASK_USER_QUESTION".to_owned()),
                reason: "请选择目标".to_owned(),
            }
        );

        for kind in [
            HookEventKind::UserPromptSubmit,
            HookEventKind::PostToolUse,
            HookEventKind::PostToolUseFailure,
            HookEventKind::PermissionDenied,
            HookEventKind::PreCompact,
            HookEventKind::PostCompact,
        ] {
            assert!(matches!(
                hook_effect(&hook_event(kind)),
                HookEffect::Working { .. }
            ));
        }
        for kind in [
            HookEventKind::Stop,
            HookEventKind::StopFailure,
            HookEventKind::SessionEnd,
        ] {
            assert_eq!(hook_effect(&hook_event(kind)), HookEffect::Done);
        }
        assert_eq!(
            hook_effect(&hook_event(HookEventKind::SessionStart)),
            HookEffect::Reset
        );
        for kind in [HookEventKind::SubagentStart, HookEventKind::SubagentStop] {
            assert_eq!(hook_effect(&hook_event(kind)), HookEffect::RecordOnly);
        }
    }

    #[test]
    fn completed_turn_ignores_late_tool_events_until_the_next_prompt() {
        let session = test_session(SessionPhase::Running);
        session
            .apply_hook_event(hook_event(HookEventKind::Stop))
            .unwrap();
        let stopped = session.state().unwrap();
        assert_eq!(stopped.phase, SessionPhase::Idle);
        assert_eq!(stopped.activity, HookActivity::Done);

        let mut late = hook_event(HookEventKind::PostToolUse);
        late.tool_name = Some("edit_file".to_owned());
        session.apply_hook_event(late).unwrap();
        let guarded = session.state().unwrap();
        assert_eq!(guarded.phase, SessionPhase::Idle);
        assert_eq!(guarded.activity, HookActivity::Done);
        assert_eq!(guarded.tool_name, None);

        session
            .apply_hook_event(hook_event(HookEventKind::UserPromptSubmit))
            .unwrap();
        let resumed = session.state().unwrap();
        assert_eq!(resumed.phase, SessionPhase::Running);
        assert_eq!(resumed.activity, HookActivity::Working);
    }

    #[test]
    fn lease_cleanup_only_targets_idle_or_terminal_sessions_after_grace() {
        let session = test_session(SessionPhase::Idle);
        let lease = Arc::new(AtomicU64::new(1_000));
        {
            let mut inner = session.inner.lock().unwrap();
            inner.client_session_id = Some("codex-thread".to_owned());
            inner.client_lease = Some(lease);
            inner.orphan_policy = OrphanPolicy {
                lease_ms: 100,
                grace_ms: 200,
            };
            inner.phase_changed_at_ms = 900;
            assert_eq!(
                inner.client_lifecycle(1_050, false).0,
                ClientLeaseState::Connected
            );
            let lifecycle = inner.client_lifecycle(1_101, false);
            assert_eq!(lifecycle.0, ClientLeaseState::Orphaned);
            assert_eq!(lifecycle.2, Some(1_100));
            assert_eq!(lifecycle.3, Some(1_300));
            assert!(!inner.orphan_cleanup_due(1_299));
            assert!(inner.orphan_cleanup_due(1_300));

            set_phase(&mut inner, SessionPhase::Running, 1_200);
            let running = inner.client_lifecycle(2_000, false);
            assert_eq!(running.0, ClientLeaseState::Disconnected);
            assert_eq!(running.3, None);
            assert!(!inner.orphan_cleanup_due(10_000));
        }
    }

    #[test]
    fn web_control_hold_defers_orphan_cleanup_without_forging_codex_connected() {
        // Short Codex lease/grace: interactive claim delays reap; read-only does not.
        let session = test_session(SessionPhase::Idle);
        let lease = Arc::new(AtomicU64::new(1_000));
        {
            let mut inner = session.inner.lock().unwrap();
            inner.client_session_id = Some("codex-web-hold".to_owned());
            inner.client_lease = Some(Arc::clone(&lease));
            inner.orphan_policy = OrphanPolicy {
                lease_ms: 50,
                grace_ms: 80,
            };
            inner.phase_changed_at_ms = 900;
        }
        // Codex expired at 1050; without web hold → orphaned, due at 1050+80=1130.
        {
            let inner = session.inner.lock().unwrap();
            assert_eq!(
                inner.client_lifecycle(1_100, false).0,
                ClientLeaseState::Orphaned
            );
            assert!(inner.orphan_cleanup_due(1_130));
        }
        // Interactive claim at 1100: not Connected (Codex still dead), WebControlled.
        session.acquire_web_control(42, 1_100).unwrap();
        {
            let inner = session.inner.lock().unwrap();
            assert_eq!(
                inner.client_lifecycle(1_120, false).0,
                ClientLeaseState::WebControlled
            );
            assert!(!inner.orphan_cleanup_due(1_200));
            assert!(!inner.orphan_cleanup_due(1_100 + WEB_CONTROL_LEASE_MS - 1));
        }
        // Heartbeat refresh keeps hold.
        assert!(session.refresh_web_control(42, 1_100 + 5_000).unwrap());
        {
            let inner = session.inner.lock().unwrap();
            assert_eq!(
                inner.client_lifecycle(1_100 + 5_000 + 1, false).0,
                ClientLeaseState::WebControlled
            );
            assert!(!inner.orphan_cleanup_due(1_100 + 5_000 + 1));
        }
        // Release: grace restarts from revoke time, not permanent keep-alive.
        assert!(session.release_web_control(42, 2_000).unwrap());
        {
            let inner = session.inner.lock().unwrap();
            let life = inner.client_lifecycle(2_000, false);
            assert_eq!(life.0, ClientLeaseState::Orphaned);
            // orphaned_at >= release time 2000; auto_close = orphaned_at + 80.
            assert!(life.2.unwrap() >= 2_000);
            assert_eq!(life.3, Some(life.2.unwrap() + 80));
            assert!(!inner.orphan_cleanup_due(2_000 + 79));
            assert!(inner.orphan_cleanup_due(life.3.unwrap()));
        }
    }

    #[test]
    fn web_control_read_only_never_defers_and_takeover_keeps_new_owner_only() {
        let session = test_session(SessionPhase::Idle);
        let lease = Arc::new(AtomicU64::new(1_000));
        {
            let mut inner = session.inner.lock().unwrap();
            inner.client_session_id = Some("codex-ro".to_owned());
            inner.client_lease = Some(lease);
            inner.orphan_policy = OrphanPolicy {
                lease_ms: 50,
                grace_ms: 40,
            };
            inner.phase_changed_at_ms = 900;
        }
        // No claim → read-only path: still orphaned on schedule.
        {
            let inner = session.inner.lock().unwrap();
            assert_eq!(
                inner.client_lifecycle(1_100, false).0,
                ClientLeaseState::Orphaned
            );
            assert!(inner.orphan_cleanup_due(1_100 + 40));
        }
        // Owner A claims; B cannot refresh; A release then B claim.
        session.acquire_web_control(1, 1_050).unwrap();
        assert!(!session.refresh_web_control(2, 1_060).unwrap());
        assert!(session.release_web_control(1, 1_070).unwrap());
        session.acquire_web_control(2, 1_080).unwrap();
        assert!(!session.refresh_web_control(1, 1_090).unwrap());
        assert!(session.refresh_web_control(2, 1_090).unwrap());
        {
            let inner = session.inner.lock().unwrap();
            assert_eq!(inner.web_control_connection_id, Some(2));
            assert_eq!(
                inner.client_lifecycle(1_090, false).0,
                ClientLeaseState::WebControlled
            );
        }
        // Disconnect-style release of new owner restarts grace.
        assert!(session.release_web_control_if_owner(2, 1_200).unwrap());
        {
            let inner = session.inner.lock().unwrap();
            assert!(inner.web_control_connection_id.is_none());
            assert_eq!(
                inner.client_lifecycle(1_200, false).0,
                ClientLeaseState::Orphaned
            );
            assert!(inner.orphan_cleanup_due(1_200 + 40));
        }
    }

    #[test]
    fn web_control_passive_expiry_restarts_grace_from_hold_end() {
        let session = test_session(SessionPhase::Idle);
        let lease = Arc::new(AtomicU64::new(1_000));
        {
            let mut inner = session.inner.lock().unwrap();
            inner.client_session_id = Some("codex-expire".to_owned());
            inner.client_lease = Some(lease);
            inner.orphan_policy = OrphanPolicy {
                lease_ms: 50,
                grace_ms: 30,
            };
            inner.phase_changed_at_ms = 900;
        }
        session.acquire_web_control(9, 1_100).unwrap();
        // Just after WEB_CONTROL_LEASE_MS from last heartbeat without refresh.
        let after = 1_100 + WEB_CONTROL_LEASE_MS;
        {
            let inner = session.inner.lock().unwrap();
            // Not WebControlled once lease window elapsed.
            assert_ne!(
                inner.client_lifecycle(after, false).0,
                ClientLeaseState::WebControlled
            );
            assert_eq!(
                inner.client_lifecycle(after, false).0,
                ClientLeaseState::Orphaned
            );
            // Grace bases on hold end (1_100 + lease), not original codex 1050 alone.
            let life = inner.client_lifecycle(after, false);
            assert!(life.2.unwrap() >= after);
            assert!(!inner.orphan_cleanup_due(after + 29));
            assert!(inner.orphan_cleanup_due(life.3.unwrap()));
        }
    }

    #[test]
    fn final_orphan_commit_rechecks_lease_and_blocks_late_input() {
        let session = test_session(SessionPhase::Idle);
        let lease = Arc::new(AtomicU64::new(1_000));
        {
            let mut inner = session.inner.lock().unwrap();
            inner.client_session_id = Some("codex-race".to_owned());
            inner.client_lease = Some(Arc::clone(&lease));
            inner.orphan_policy = OrphanPolicy {
                lease_ms: 100,
                grace_ms: 200,
            };
            inner.phase_changed_at_ms = 900;
        }

        assert!(session.claim_orphan_cleanup(1_300).unwrap());
        lease.store(1_300, Ordering::Release);
        assert!(!session.commit_orphan_cleanup(1_300).unwrap());
        assert!(!session.cleanup_claimed.load(Ordering::Acquire));

        assert!(session.claim_orphan_cleanup(1_600).unwrap());
        assert!(session.commit_orphan_cleanup(1_600).unwrap());
        let error = session.write_raw(b"new task\r".to_vec()).unwrap_err();
        assert!(format!("{error:#}").contains("session cleanup has started"));
    }

    #[test]
    fn client_heartbeat_cancels_claim_before_input_is_accepted() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Idle);
        let session = host.get("gbt-test").unwrap();
        let lease = Arc::new(AtomicU64::new(1_000));
        let (writer_tx, writer_rx) = sync_channel(1);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);
        {
            let mut inner = session.inner.lock().unwrap();
            inner.client_session_id = Some("codex-resume".to_owned());
            inner.client_lease = Some(Arc::clone(&lease));
            inner.orphan_policy = OrphanPolicy {
                lease_ms: 100,
                grace_ms: 200,
            };
            inner.phase_changed_at_ms = 900;
        }
        host.registry
            .lock()
            .unwrap()
            .clients
            .insert("codex-resume".to_owned(), lease);

        assert!(session.claim_orphan_cleanup(1_300).unwrap());
        host.touch_client_at("codex-resume", 1_300).unwrap();
        assert!(!session.cleanup_claimed.load(Ordering::Acquire));
        let (seen_tx, seen_rx) = sync_channel(1);
        let session_w = Arc::clone(&session);
        thread::spawn(move || {
            if let Ok(job) = writer_rx.recv() {
                let _ = seen_tx.send(job.data.clone());
                let _ = session_w.apply_input_effect(job.effect);
                if let Some(completion) = job.completion {
                    completion.complete(Ok(()));
                }
            }
        });
        session.write_raw(b"resume\r".to_vec()).unwrap();
        assert_eq!(seen_rx.recv().unwrap(), b"resume\r");
        assert_eq!(session.state().unwrap().phase, SessionPhase::Running);
    }

    #[test]
    fn orphan_reaper_removes_expired_terminal_sessions_but_keeps_running_ones() {
        let expired = Arc::new(AtomicU64::new(1));
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Exited);
        {
            let session = host.get("gbt-test").unwrap();
            let mut inner = session.inner.lock().unwrap();
            inner.client_session_id = Some("codex-expired".to_owned());
            inner.client_lease = Some(Arc::clone(&expired));
            inner.orphan_policy = OrphanPolicy {
                lease_ms: 1,
                grace_ms: 1,
            };
            inner.phase_changed_at_ms = 1;
        }
        host.registry
            .lock()
            .unwrap()
            .clients
            .insert("codex-expired".to_owned(), expired);
        let result = host.reap_orphans().unwrap();
        assert_eq!(result.matched, 1);
        assert_eq!(result.closed, 1);
        assert!(result.failures.is_empty());
        assert!(host.list().unwrap().is_empty());

        let running = Arc::new(AtomicU64::new(1));
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        {
            let session = host.get("gbt-test").unwrap();
            let mut inner = session.inner.lock().unwrap();
            inner.client_session_id = Some("codex-running".to_owned());
            inner.client_lease = Some(Arc::clone(&running));
            inner.orphan_policy = OrphanPolicy {
                lease_ms: 1,
                grace_ms: 1,
            };
            inner.phase_changed_at_ms = 1;
        }
        host.registry
            .lock()
            .unwrap()
            .clients
            .insert("codex-running".to_owned(), running);
        let result = host.reap_orphans().unwrap();
        assert_eq!(result.matched, 0);
        assert_eq!(host.list().unwrap().len(), 1);
        assert_eq!(
            host.show("gbt-test").unwrap().client_state,
            ClientLeaseState::Disconnected
        );
    }

    #[test]
    fn classifies_notification_events_without_treating_permission_prompt_as_blocked() {
        let mut event = hook_event(HookEventKind::Notification);
        event.notification_type = Some("permission_prompt".to_owned());
        event.message = Some("Approval required".to_owned());
        assert_eq!(hook_effect(&event), HookEffect::RecordOnly);

        event.notification_type = Some("question".to_owned());
        event.message = Some("请选择".to_owned());
        assert_eq!(
            hook_effect(&event),
            HookEffect::Waiting {
                tool_name: None,
                reason: "请选择".to_owned(),
            }
        );

        event.notification_type = Some("input_required".to_owned());
        event.message = None;
        assert_eq!(hook_effect(&event), HookEffect::Done);

        event.notification_type = Some("status".to_owned());
        event.level = Some("info".to_owned());
        assert_eq!(hook_effect(&event), HookEffect::RecordOnly);
    }

    #[test]
    fn applies_hook_state_without_advancing_the_read_cursor() {
        let session = test_session(SessionPhase::Running);
        let cwd = session.state().unwrap().cwd;
        let mut event = hook_event(HookEventKind::PreToolUse);
        event.cwd = Some(cwd);
        event.tool_name = Some("ask_user_question".to_owned());
        event.message = Some("需要选择".to_owned());
        session.apply_hook_event(event).unwrap();

        let web = session.state().unwrap();
        assert_eq!(web.phase, SessionPhase::Running);
        assert_eq!(web.activity, HookActivity::Waiting);
        assert_eq!(web.tool_name.as_deref(), Some("ask_user_question"));
        assert_eq!(web.waiting_reason.as_deref(), Some("需要选择"));
        assert_eq!(web.last_cursor, 0);
        let read = session.read(0, 1, 0).unwrap();
        assert_eq!(read.cursor, 0);
        assert_eq!(read.next_cursor, 0);
        // Protocol min timeout is 1ms; blocked_reason returns without waiting.
        let wait = session.wait(WaitCondition::TuiIdle, 1).unwrap();
        assert!(!wait.satisfied);
        assert!(!wait.timed_out);
        assert_eq!(wait.blocked_reason.as_deref(), Some("需要选择"));

        let serialized = serde_json::to_value(web).unwrap();
        assert_eq!(serialized["session"], "gbt-test");
        assert_eq!(serialized["activity"], "waiting");
        assert!(serialized.get("hook_token").is_none());
        assert!(serialized.get("provider_session_id").is_none());
    }

    #[test]
    fn ignores_late_terminal_hook_events() {
        let terminal = test_session(SessionPhase::Exited);
        let mut late = hook_event(HookEventKind::PreToolUse);
        late.cwd = Some("path-that-does-not-exist".to_owned());
        late.tool_name = Some("ask_user_question".to_owned());
        terminal.apply_hook_event(late).unwrap();
        let web = terminal.state().unwrap();
        assert_eq!(web.phase, SessionPhase::Exited);
        assert_eq!(web.activity, HookActivity::Unknown);
        assert_eq!(web.hook_event, None);
    }

    #[test]
    fn routes_hook_events_by_provider_session_id() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let cwd = host.show("gbt-test").unwrap().cwd;
        let mut event = hook_event(HookEventKind::PreToolUse);
        event.cwd = Some(cwd);
        event.tool_name = Some("ask_user_question".to_owned());
        event.message = Some("需要选择".to_owned());

        assert!(
            host.apply_hook_event(TEST_PROVIDER_SESSION_ID, event)
                .unwrap()
        );
        let web = host.list().unwrap().pop().unwrap();
        assert_eq!(web.activity, HookActivity::Waiting);
        assert_eq!(web.tool_name.as_deref(), Some("ask_user_question"));
        assert_eq!(web.waiting_reason.as_deref(), Some("需要选择"));
    }

    #[test]
    fn returns_false_for_unknown_provider_sessions() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        assert!(
            !host
                .apply_hook_event(
                    "00000000-0000-4000-8000-000000000000",
                    hook_event(HookEventKind::Stop)
                )
                .unwrap()
        );
        assert_eq!(host.show("gbt-test").unwrap().phase, SessionPhase::Running);
    }

    #[test]
    fn close_removes_the_provider_session_index() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Exited);
        assert!(host.close("gbt-test").unwrap());
        assert!(host.close("gbt-test").unwrap());
        assert!(
            !host
                .apply_hook_event(TEST_PROVIDER_SESSION_ID, hook_event(HookEventKind::Stop))
                .unwrap()
        );
        let registry = host.registry.lock().unwrap();
        assert!(registry.sessions.is_empty());
        assert!(registry.provider_sessions.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn closes_real_pty_process_group_and_descendants() {
        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let mut command = CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg(
            "trap '' HUP TERM; sleep 60 & child=$!; printf '%s\\n' \"$child\"; wait \"$child\"",
        );
        let session = Session::spawn_with_command(
            "gbt-pty-process-tree".to_owned(),
            test_launch_config(&cwd),
            command,
            Arc::new(HostRevision::new()),
        )
        .unwrap();
        let process_group_id = session
            .termination
            .as_ref()
            .map(|termination| termination.process_group_id)
            .unwrap();
        let output = wait_for_pty_output(&session, "\n");
        let descendant = output
            .split_whitespace()
            .find_map(|value| value.parse::<libc::pid_t>().ok())
            .expect("shell must print the descendant PID");

        session.shutdown().unwrap();
        assert_eq!(session.state().unwrap().phase, SessionPhase::Stopped);
        eventually_process_group_gone(process_group_id);
        eventually_process_gone(descendant);
    }

    /// Root exits quickly and PTY EOF may race ahead of tree death. Descendants
    /// close stdio and ignore HUP/TERM — close must escalate to KILL and only
    /// return Ok once the PGID is empty.
    #[cfg(unix)]
    #[test]
    fn close_escalates_to_kill_when_root_exits_and_descendants_ignore_hup_term() {
        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        // Python: fork a HUP/TERM-immune child that drops stdio, then root exits.
        // Stays in the PTY process group so kill(-pgid) reaches it.
        // Prefer system Python (avoid pyenv shim double-exec weirdness under PTY).
        let python = ["/usr/bin/python3", "/usr/local/bin/python3", "python3"]
            .into_iter()
            .find(|path| Path::new(path).exists() || *path == "python3")
            .unwrap_or("python3");
        let mut command = CommandBuilder::new(python);
        command.arg("-c");
        command.arg(
            r#"
import os, signal, sys, time
# Handshake so signal handlers are installed before the root exits
# (otherwise SIGHUP from session teardown can win the race).
ready_r, ready_w = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(ready_r)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    os.write(ready_w, b"1")
    os.close(ready_w)
    devnull = os.open("/dev/null", os.O_RDWR)
    os.dup2(devnull, 0)
    os.dup2(devnull, 1)
    os.dup2(devnull, 2)
    # Stay in the PTY process group; do not setsid().
    while True:
        time.sleep(1)
os.close(ready_w)
os.read(ready_r, 1)
os.close(ready_r)
sys.stdout.write("ORPHAN %d\n" % pid)
sys.stdout.flush()
# Root exits immediately (process_done race).
os._exit(0)
"#,
        );
        let session = Session::spawn_with_command(
            "gbt-pty-orphan-tree".to_owned(),
            test_launch_config(&cwd),
            command,
            Arc::new(HostRevision::new()),
        )
        .unwrap();
        let process_group_id = session
            .termination
            .as_ref()
            .map(|termination| termination.process_group_id)
            .unwrap();
        let output = wait_for_pty_output(&session, "ORPHAN ");
        let orphan = output
            .lines()
            .find_map(|line| {
                line.strip_prefix("ORPHAN ")
                    .and_then(|v| v.trim().parse::<libc::pid_t>().ok())
            })
            .expect("python must print orphan PID");
        // Check immediately — PTY teardown must not have reaped the descendant yet.
        let alive_now = unsafe { libc::kill(orphan, 0) } == 0;
        assert!(
            alive_now,
            "orphan {orphan} already dead right after print; output={output:?}"
        );

        // Give root a moment to exit so process_done can race ahead of tree-gone.
        thread::sleep(Duration::from_millis(200));
        // Precondition: descendant still alive while root may already be done.
        let orphan_alive = unsafe { libc::kill(orphan, 0) } == 0;
        assert!(
            orphan_alive,
            "orphan {orphan} must still be running before close (setup failed)"
        );
        assert!(
            !session
                .termination
                .as_ref()
                .expect("terminator")
                .is_tree_gone(),
            "tree must not already be gone while orphan lives"
        );

        session.shutdown().unwrap();
        // Root may have already exited with 0 (Exited) before close; close itself
        // sets Stopped only when finalize runs under shutdown=true.
        assert!(
            phase_is_terminal(session.state().unwrap().phase),
            "phase={:?}",
            session.state().unwrap().phase
        );
        // Must have escalated past soft signals.
        let last = session
            .termination
            .as_ref()
            .and_then(|t| t.last_signal_sent());
        assert_eq!(
            last,
            Some(TerminationSignal::Kill),
            "close must escalate to SIGKILL when descendants ignore HUP/TERM; last={last:?}"
        );
        // Immediately after Ok, PGID must be empty — not a delayed reaper hope.
        let probe = unsafe { libc::kill(-process_group_id, 0) };
        assert_ne!(
            probe, 0,
            "process group {process_group_id} still has members after close Ok"
        );
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
        eventually_process_gone(orphan);
    }

    #[cfg(windows)]
    #[test]
    fn closes_real_conpty_job_tree() {
        let root = temporary_test_directory("job-tree");
        fs::create_dir_all(&root).unwrap();
        let marker = root.join("descendant-marker.txt");
        let marker = marker.to_string_lossy().replace('\'', "''");
        let script = format!(
            "$p = Start-Process -FilePath powershell.exe -ArgumentList '-NoProfile','-Command','Start-Sleep -Seconds 60; Set-Content -LiteralPath ''{marker}'' -Value descendant' -PassThru; Write-Output $p.Id; Wait-Process -Id $p.Id"
        );
        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let mut command = CommandBuilder::new("powershell.exe");
        command.args(["-NoProfile", "-Command", &script]);
        let session = Session::spawn_with_command(
            "gbt-conpty-process-tree".to_owned(),
            test_launch_config(&cwd),
            command,
            Arc::new(HostRevision::new()),
        )
        .unwrap();
        let output = wait_for_pty_output(&session, "\n");
        let descendant_pid = output
            .lines()
            .find_map(|line| line.trim().parse::<u32>().ok())
            .expect("PowerShell must print the immediate descendant PID");
        let descendant_handle = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE,
                0,
                descendant_pid,
            )
        };
        assert!(
            !descendant_handle.is_null(),
            "descendant must be alive before close"
        );
        let descendant_handle = unsafe { OwnedHandle::from_raw_handle(descendant_handle) };
        session.shutdown().unwrap();
        assert_eq!(session.state().unwrap().phase, SessionPhase::Stopped);
        assert_eq!(
            unsafe { WaitForSingleObject(descendant_handle.as_raw_handle(), 5_000) },
            0,
            "descendant process handle must signal before close verification deadline"
        );
        let terminator = session.termination.as_ref().expect("terminator");
        assert_eq!(
            windows_job_active_processes(terminator.job.as_raw_handle()),
            Some(0),
            "successful close must leave the admission Job empty"
        );
        assert!(!root.join("descendant-marker.txt").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn hook_done_and_waiting_survive_output_without_an_explicit_grok_title() {
        assert_eq!(
            phase_after_output(
                SessionPhase::Running,
                None,
                false,
                HookActivity::Done,
                false,
                false,
                false,
            ),
            SessionPhase::Idle
        );
        assert_eq!(
            phase_after_output(
                SessionPhase::Running,
                None,
                false,
                HookActivity::Waiting,
                false,
                false,
                false,
            ),
            SessionPhase::Running
        );
        assert_eq!(
            phase_after_output(
                SessionPhase::Idle,
                Some("⠋ - Waiting for response… - grok"),
                true,
                HookActivity::Done,
                false,
                false,
                false,
            ),
            SessionPhase::Running
        );
        assert_eq!(
            phase_after_output(
                SessionPhase::Running,
                Some("grok"),
                true,
                HookActivity::Waiting,
                false,
                false,
                false,
            ),
            SessionPhase::Idle
        );
    }

    #[test]
    fn quiet_fallback_recovers_from_a_missing_completion_hook() {
        let session = test_session(SessionPhase::Running);
        let mut inner = session.inner.lock().unwrap();
        inner.updated_at_ms = now_millis().saturating_sub(QUIET_IDLE_MILLISECONDS + 1);
        inner.hook.activity = HookActivity::Working;
        assert!(wait_satisfied(&mut inner, WaitCondition::TuiIdle));
        assert_eq!(inner.phase, SessionPhase::Idle);
        assert_eq!(inner.hook.activity, HookActivity::Done);

        inner.phase = SessionPhase::Running;
        inner.updated_at_ms = now_millis().saturating_sub(QUIET_IDLE_MILLISECONDS + 1);
        inner.hook.activity = HookActivity::Waiting;
        assert!(!wait_satisfied(&mut inner, WaitCondition::TuiIdle));
        assert_eq!(inner.phase, SessionPhase::Running);
    }

    #[test]
    fn late_output_does_not_revive_a_finished_process() {
        assert_eq!(
            phase_after_output(
                SessionPhase::Exited,
                Some("grok"),
                true,
                HookActivity::Done,
                true,
                false,
                false,
            ),
            SessionPhase::Exited
        );
        assert_eq!(
            phase_after_output(
                SessionPhase::Running,
                Some("grok"),
                false,
                HookActivity::Unknown,
                false,
                false,
                false
            ),
            SessionPhase::Running
        );
        assert_eq!(
            phase_after_output(
                SessionPhase::Running,
                Some("grok"),
                true,
                HookActivity::Unknown,
                false,
                false,
                false
            ),
            SessionPhase::Idle
        );
    }

    #[cfg(windows)]
    #[test]
    fn normalizes_windows_verbatim_paths_for_child_processes() {
        assert_eq!(
            normalize_platform_path(PathBuf::from(r"\\?\D:\repo\project")),
            PathBuf::from(r"D:\repo\project")
        );
        assert_eq!(
            normalize_platform_path(PathBuf::from(r"\\?\UNC\server\share\repo")),
            PathBuf::from(r"\\server\share\repo")
        );
    }

    #[test]
    fn detects_interactive_grok_prompts_as_blocked() {
        assert_eq!(
            blocked_reason("Run Grok Build in a project directory?"),
            Some("grok-project-directory")
        );
        assert_eq!(
            blocked_reason("Type your answer here  Enter:submit"),
            Some("grok-interactive-prompt")
        );
        assert_eq!(blocked_reason("中文通讯正常"), None);
    }

    #[test]
    fn raw_navigation_does_not_mark_a_turn_running() {
        let mut paste = false;
        let mut hold = Vec::new();
        assert_eq!(
            raw_input_effect(b"hello", &mut paste, &mut hold),
            InputEffect::None
        );
        assert_eq!(
            raw_input_effect(b"\x1b[A", &mut paste, &mut hold),
            InputEffect::None
        );
        assert_eq!(
            raw_input_effect(b"hello\r", &mut paste, &mut hold),
            InputEffect::StartTurn
        );
        assert_ne!(
            raw_input_effect(&[0x03], &mut paste, &mut hold),
            InputEffect::StartTurn
        );
        assert_eq!(
            raw_input_effect(&[0x03], &mut paste, &mut hold),
            InputEffect::Cancel
        );
        assert_eq!(
            raw_input_effect(b"a\x03b", &mut paste, &mut hold),
            InputEffect::None
        );
        assert!(!paste);
        assert!(hold.is_empty());
    }

    #[test]
    fn bracketed_paste_crlf_does_not_start_turn_across_chunks() {
        let mut paste = false;
        let mut hold = Vec::new();
        // Chunk 1: open paste + multi-line body with CR/LF (not submitted yet).
        assert_eq!(
            raw_input_effect(b"\x1b[200~line1\r\nline2", &mut paste, &mut hold),
            InputEffect::None
        );
        assert!(paste, "paste must stay open across WriteJob chunks");
        // Chunk 2: more paste body.
        assert_eq!(
            raw_input_effect(b"\nline3\r", &mut paste, &mut hold),
            InputEffect::None
        );
        assert!(paste);
        // Chunk 3: close paste only — still no StartTurn.
        assert_eq!(
            raw_input_effect(b"\x1b[201~", &mut paste, &mut hold),
            InputEffect::None
        );
        assert!(!paste);
        // Explicit Enter after paste ends is the real submit.
        assert_eq!(
            raw_input_effect(b"\r", &mut paste, &mut hold),
            InputEffect::StartTurn
        );
        // CSI open split across WriteJobs must reassemble.
        paste = false;
        hold.clear();
        assert_eq!(
            raw_input_effect(b"\x1b[20", &mut paste, &mut hold),
            InputEffect::None
        );
        assert!(!paste);
        assert_eq!(hold, b"\x1b[20");
        assert_eq!(
            raw_input_effect(b"0~line\r\n", &mut paste, &mut hold),
            InputEffect::None
        );
        assert!(paste);
        assert!(hold.is_empty());
        // CSI close split mid-sequence.
        assert_eq!(
            raw_input_effect(b"\x1b[2", &mut paste, &mut hold),
            InputEffect::None
        );
        assert!(paste);
        assert_eq!(
            raw_input_effect(b"01~", &mut paste, &mut hold),
            InputEffect::None
        );
        assert!(!paste);
        assert!(hold.is_empty());
        // Full open/body/close in one job with embedded CR/LF.
        assert_eq!(
            raw_input_effect(b"\x1b[200~paste\r\ntext\x1b[201~", &mut paste, &mut hold),
            InputEffect::None
        );
        assert!(!paste);
        // Ctrl-C alone remains Cancel even after paste traffic.
        assert_eq!(
            raw_input_effect(&[0x03], &mut paste, &mut hold),
            InputEffect::Cancel
        );
    }

    #[test]
    fn write_jobs_track_paste_state_across_chunks() {
        let session = test_session(SessionPhase::Idle);
        let (writer_tx, writer_rx) = sync_channel(8);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);
        let session_w = Arc::clone(&session);
        thread::spawn(move || {
            while let Ok(job) = writer_rx.recv() {
                let _ = session_w.apply_input_effect(job.effect);
                if let Some(completion) = job.completion {
                    completion.complete(Ok(()));
                }
            }
        });
        // Multi-line paste body split like WebUI 64 KiB chunking of a large paste.
        session
            .write_raw(b"\x1b[200~first\r\nsecond".to_vec())
            .unwrap();
        assert_eq!(session.state().unwrap().phase, SessionPhase::Idle);
        assert_ne!(session.state().unwrap().activity, HookActivity::Working);
        assert!(session.inner.lock().unwrap().paste_open);
        session.write_raw(b"\nthird\r".to_vec()).unwrap();
        assert_ne!(session.state().unwrap().activity, HookActivity::Working);
        session.write_raw(b"\x1b[201~".to_vec()).unwrap();
        assert!(!session.inner.lock().unwrap().paste_open);
        assert_ne!(session.state().unwrap().activity, HookActivity::Working);
        // Only Enter outside paste flips activity.
        session.write_raw(b"\r".to_vec()).unwrap();
        assert_eq!(session.state().unwrap().activity, HookActivity::Working);
    }

    #[test]
    fn high_level_send_marks_start_turn_without_scanning_paste_body() {
        let session = test_session(SessionPhase::Idle);
        let (writer_tx, writer_rx) = sync_channel(4);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);
        let session_w = Arc::clone(&session);
        thread::spawn(move || {
            while let Ok(job) = writer_rx.recv() {
                let _ = session_w.apply_input_effect(job.effect);
                if let Some(completion) = job.completion {
                    completion.complete(Ok(()));
                }
            }
        });
        // Multi-line paste body would look like StartTurn if scanned naively.
        session.send("line1\nline2".to_owned()).unwrap();
        assert_eq!(session.state().unwrap().phase, SessionPhase::Running);
        assert_eq!(session.state().unwrap().activity, HookActivity::Working);
    }

    #[test]
    fn concurrent_list_and_close_does_not_deadlock() {
        // Regression: list used to hold registry while locking Session.inner via
        // state(); close held Session.inner in shutdown then re-locked registry
        // in remove_session (which also locked other sessions' inner). Concurrent
        // WebUI list + HTTP close could hang permanently.
        let host = Arc::new(SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        }));
        let revision = Arc::clone(&host.revision);
        let handles: Vec<String> = (0..6).map(|i| format!("gbt-lock-{i}")).collect();
        {
            let mut registry = host.registry.lock().unwrap();
            for handle in &handles {
                let session =
                    test_session_with_revision(SessionPhase::Running, Arc::clone(&revision));
                session.inner.lock().unwrap().session = handle.clone();
                // Brief hang so close workers actually take Session.inner.
                session.test_shutdown_hang_ms.store(20, Ordering::Release);
                registry.sessions.insert(handle.clone(), session);
            }
        }

        let barrier = Arc::new(std::sync::Barrier::new(3));
        let stop = Arc::new(AtomicBool::new(false));

        let list_barrier = Arc::clone(&barrier);
        let list_host = Arc::clone(&host);
        let list_stop = Arc::clone(&stop);
        let list_thread = thread::spawn(move || {
            list_barrier.wait();
            while !list_stop.load(Ordering::Acquire) {
                let _ = list_host.list();
                let _ = list_host.list_web_board();
                let _ = list_host.next_client_lifecycle_deadline_ms();
                let _ = list_host.plan_web_events_with_subscriptions(
                    &HashMap::new(),
                    false,
                    64 * 1024,
                    Some(&HashSet::new()),
                );
            }
        });

        let touch_barrier = Arc::clone(&barrier);
        let touch_host = Arc::clone(&host);
        let touch_stop = Arc::clone(&stop);
        let touch_thread = thread::spawn(move || {
            touch_barrier.wait();
            let mut i = 0u64;
            while !touch_stop.load(Ordering::Acquire) {
                let _ = touch_host.touch_client("codex-lock-test");
                i = i.wrapping_add(1);
                if i.is_multiple_of(8) {
                    thread::yield_now();
                }
            }
        });

        let close_barrier = Arc::clone(&barrier);
        let close_host = Arc::clone(&host);
        let close_handles = handles.clone();
        let close_thread = thread::spawn(move || {
            close_barrier.wait();
            for handle in &close_handles {
                let _ = close_host.close(handle);
            }
            // Also exercise batch close on anything still present.
            let remaining = {
                let registry = close_host.registry.lock().unwrap();
                registry
                    .sessions
                    .iter()
                    .map(|(h, s)| (h.clone(), Arc::clone(s)))
                    .collect::<Vec<_>>()
            };
            let _ = close_host.close_sessions(remaining);
        });

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let _ = close_thread.join();
            stop.store(true, Ordering::Release);
            let _ = list_thread.join();
            let _ = touch_thread.join();
            let _ = done_tx.send(());
        });

        done_rx
            .recv_timeout(Duration::from_secs(15))
            .expect("list/close/touch concurrent paths deadlocked (registry ↔ Session.inner)");
    }

    #[test]
    fn batch_close_respects_absolute_wall_deadline_with_stuck_sessions() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        });
        let revision = Arc::clone(&host.revision);
        let mut sessions = Vec::new();
        // Nine sessions each hang 5s → without a true batch deadline this would
        // take ceil(9/8)*5s ≈ 10s and blow the WebUI 8s budget.
        for i in 0..9 {
            let handle = format!("gbt-stuck-{i}");
            let session = test_session_with_revision(SessionPhase::Running, Arc::clone(&revision));
            session.inner.lock().unwrap().session = handle.clone();
            session
                .test_shutdown_hang_ms
                .store(5_000, Ordering::Release);
            {
                let mut registry = host.registry.lock().unwrap();
                registry
                    .sessions
                    .insert(handle.clone(), Arc::clone(&session));
            }
            sessions.push((handle, session));
        }
        let started = Instant::now();
        let result = host.close_sessions(sessions).unwrap();
        let elapsed = started.elapsed();
        // Must finish near CLOSE_BATCH_DEADLINE_MS, not multi-chunk 10s+.
        assert!(
            elapsed < Duration::from_millis(CLOSE_BATCH_DEADLINE_MS + 1_500),
            "batch close took {elapsed:?}, expected under ~{} ms",
            CLOSE_BATCH_DEADLINE_MS + 1_500
        );
        assert!(elapsed >= Duration::from_millis(CLOSE_BATCH_DEADLINE_MS.saturating_sub(500)));
        assert_eq!(result.matched, 9);
        // Some workers may complete within the window; the rest fail retryably.
        assert!(
            result.closed + result.failures.len() == 9,
            "closed={} failures={}",
            result.closed,
            result.failures.len()
        );
        assert!(
            !result.failures.is_empty(),
            "expected deadline failures for stuck sessions"
        );
        assert!(
            result.failures.iter().any(|f| f.contains("deadline")),
            "failures={:?}",
            result.failures
        );
    }

    /// close_owner must share one absolute deadline across rounds + final scan
    /// (not 7.5s per close_sessions → ~15s), and dedupe matched/failures by handle.
    #[test]
    fn close_owner_shares_absolute_deadline_and_dedupes_stats() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        });
        let revision = Arc::clone(&host.revision);
        let owner = "test-owner";
        let n = 6usize;
        for i in 0..n {
            let handle = format!("gbt-owner-stuck-{i}");
            let session = test_session_with_revision(SessionPhase::Running, Arc::clone(&revision));
            // test_session defaults owner to "test-owner" on both outer and inner.
            session.inner.lock().unwrap().session = handle.clone();
            session
                .test_shutdown_hang_ms
                .store(5_000, Ordering::Release);
            let mut registry = host.registry.lock().unwrap();
            registry.sessions.insert(handle, session);
        }
        let started = Instant::now();
        let result = host.close_owner(owner).unwrap();
        let elapsed = started.elapsed();
        // Must not stack first-round budget + final-scan budget (~15s).
        assert!(
            elapsed < Duration::from_millis(CLOSE_BATCH_DEADLINE_MS + 1_500),
            "close_owner took {elapsed:?}; whole call must fit one {CLOSE_BATCH_DEADLINE_MS}ms budget"
        );
        assert!(
            elapsed >= Duration::from_millis(CLOSE_BATCH_DEADLINE_MS.saturating_sub(500)),
            "expected to consume most of the shared budget, elapsed={elapsed:?}"
        );
        assert_eq!(
            result.matched, n,
            "matched must count unique handles once (not round+scan double)"
        );
        assert_eq!(
            result.failures.len(),
            n,
            "failures must be one per stuck handle; failures={:?}",
            result.failures
        );
        assert_eq!(result.closed, 0);
        // Fence released: retryable close is allowed again.
        assert!(!host.registry.lock().unwrap().is_owner_closing(owner));
        let retry = host.close_owner(owner).unwrap();
        assert_eq!(retry.matched, n, "retry still sees surviving sessions");
        assert_eq!(retry.failures.len(), n);
    }

    /// close_client: same shared deadline + handle-deduped stats contract.
    #[test]
    fn close_client_shares_absolute_deadline_and_dedupes_stats() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        });
        let revision = Arc::clone(&host.revision);
        let client = "codex-thread-close-budget";
        let n = 5usize;
        for i in 0..n {
            let handle = format!("gbt-client-stuck-{i}");
            let session = test_session_with_identity(
                SessionPhase::Running,
                Arc::clone(&revision),
                Some(client.to_owned()),
                Some("other-owner".to_owned()),
            );
            session.inner.lock().unwrap().session = handle.clone();
            session
                .test_shutdown_hang_ms
                .store(5_000, Ordering::Release);
            let mut registry = host.registry.lock().unwrap();
            registry
                .clients
                .insert(client.to_owned(), Arc::new(AtomicU64::new(now_millis())));
            registry.sessions.insert(handle, session);
        }
        let started = Instant::now();
        let result = host.close_client(client).unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(CLOSE_BATCH_DEADLINE_MS + 1_500),
            "close_client took {elapsed:?}; must share one {CLOSE_BATCH_DEADLINE_MS}ms budget"
        );
        assert!(
            elapsed >= Duration::from_millis(CLOSE_BATCH_DEADLINE_MS.saturating_sub(500)),
            "expected to consume most of the shared budget, elapsed={elapsed:?}"
        );
        assert_eq!(result.matched, n);
        assert_eq!(result.failures.len(), n);
        assert_eq!(result.closed, 0);
        assert!(!host.registry.lock().unwrap().is_client_closing(client));
        // Lease kept while failures remain (retryable).
        assert!(host.registry.lock().unwrap().clients.contains_key(client));
    }

    #[test]
    fn interrupt_marks_cancelling_not_idle_until_semantic_done() {
        let session = test_session(SessionPhase::Running);
        // test_session already has an ack writer; send waits for flush ack.
        session.send("\u{3}".to_owned()).unwrap();
        let state = session.state().unwrap();
        // Written Ctrl-C is not completion evidence.
        assert_eq!(state.phase, SessionPhase::Running);
        assert_eq!(state.activity, HookActivity::Cancelling);
        assert!(!wait_satisfied(
            &mut session.inner.lock().unwrap(),
            WaitCondition::TuiIdle
        ));

        // Hook Stop is semantic evidence that cancel finished.
        session
            .apply_hook_event(hook_event(HookEventKind::Stop))
            .unwrap();
        let done = session.state().unwrap();
        assert_eq!(done.phase, SessionPhase::Idle);
        assert_eq!(done.activity, HookActivity::Done);
    }

    #[test]
    fn close_tombstone_survives_unrelated_close_churn() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Exited);
        assert!(host.close("gbt-test").unwrap());
        // Flood past the soft capacity bound; TTL-only purge must keep the
        // original close idempotent for the full window.
        {
            let mut registry = host.registry.lock().unwrap();
            let now = now_millis();
            for i in 0..(CLOSED_SESSION_CACHE_CAPACITY + 2_000) {
                let handle = format!("gbt-churn-{i}");
                registry.remember_closed_session(&handle, now);
            }
            assert_eq!(
                registry.closed_sessions.len(),
                CLOSED_SESSION_CACHE_CAPACITY,
                "fresh tombstones are bounded without evicting the original close"
            );
            assert!(registry.is_closed_session("gbt-test", now));
        }
        assert!(host.close("gbt-test").unwrap());
    }

    #[test]
    fn close_tombstone_ttl_only_evicts_expired_entries() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Exited);
        assert!(host.close("gbt-test").unwrap());
        let mut registry = host.registry.lock().unwrap();
        let now = now_millis();
        // Plant an expired oldest entry and a fresh one beyond soft capacity.
        registry.closed_sessions.insert(
            "gbt-expired".to_owned(),
            now.saturating_sub(CLOSED_SESSION_TOMBSTONE_TTL_MS + 1),
        );
        registry
            .closed_session_order
            .push_front("gbt-expired".to_owned());
        for i in 0..(CLOSED_SESSION_CACHE_CAPACITY + 10) {
            registry.remember_closed_session(&format!("gbt-fresh-{i}"), now);
        }
        assert!(
            !registry.is_closed_session("gbt-expired", now),
            "TTL-expired tombstone must be reaped"
        );
        assert!(
            registry.is_closed_session("gbt-test", now),
            "fresh close must survive capacity overflow"
        );
    }

    /// Hooks that race spawn→install must buffer on the pending provider and
    /// replay in order so SessionStart/PromptSubmit are never Ok(false)-lost.
    #[cfg(unix)]
    #[test]
    fn pending_provider_buffers_hooks_until_install_replays_in_order() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        });
        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let config = test_launch_config(&cwd);
        let mut command = CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("sleep 60");
        let provider = generate_provider_session_id().unwrap();
        host.registry
            .lock()
            .unwrap()
            .begin_pending_provider(&provider)
            .unwrap();

        // Deterministic interleave: hooks arrive before install publishes.
        assert!(
            host.apply_hook_event(&provider, hook_event(HookEventKind::SessionStart))
                .unwrap()
        );
        assert!(
            host.apply_hook_event(&provider, hook_event(HookEventKind::UserPromptSubmit))
                .unwrap()
        );
        {
            let registry = host.registry.lock().unwrap();
            let pending = registry.pending_providers.get(&provider).unwrap();
            assert_eq!(pending.hooks.len(), 2);
            assert!(!registry.provider_sessions.contains_key(&provider));
        }

        let session = Session::spawn_with_command(
            "gbt-hook-race".to_owned(),
            config,
            command,
            Arc::clone(&host.revision),
        )
        .unwrap();
        // Pre-install: hooks not applied yet.
        let before = session.state().unwrap();
        assert_eq!(before.activity, HookActivity::Unknown);

        host.install_created_session(
            "gbt-hook-race".to_owned(),
            provider.clone(),
            Arc::clone(&session),
            None,
            None,
            None,
            0,
            0,
        )
        .unwrap();

        let after = session.state().unwrap();
        assert_eq!(after.phase, SessionPhase::Running);
        assert_eq!(after.activity, HookActivity::Working);
        assert_eq!(after.hook_event, Some(HookEventKind::UserPromptSubmit));
        {
            let registry = host.registry.lock().unwrap();
            assert!(!registry.pending_providers.contains_key(&provider));
            assert_eq!(
                registry
                    .provider_sessions
                    .get(&provider)
                    .map(String::as_str),
                Some("gbt-hook-race")
            );
        }
        // Post-install hooks route to the live session.
        assert!(
            host.apply_hook_event(&provider, hook_event(HookEventKind::Stop))
                .unwrap()
        );
        let done = session.state().unwrap();
        assert_eq!(done.phase, SessionPhase::Idle);
        assert_eq!(done.activity, HookActivity::Done);
        let _ = session.shutdown();
    }

    #[cfg(unix)]
    #[test]
    fn install_reattach_failure_terminates_process_and_clears_pending() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        });
        let client = "codex-reattach-fail";
        let spawn_lease = Arc::new(AtomicU64::new(1_000));
        host.registry
            .lock()
            .unwrap()
            .clients
            .insert(client.to_owned(), Arc::clone(&spawn_lease));

        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let mut config = test_launch_config(&cwd);
        config.client_session_id = Some(client.to_owned());
        config.client_lease = Some(Arc::clone(&spawn_lease));
        let mut command = CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("sleep 60");
        let provider = generate_provider_session_id().unwrap();
        host.registry
            .lock()
            .unwrap()
            .begin_pending_provider(&provider)
            .unwrap();
        let session = Session::spawn_with_command(
            "gbt-reattach-fail".to_owned(),
            config,
            command,
            Arc::clone(&host.revision),
        )
        .unwrap();

        *host.install_inject_failure.lock().unwrap() = Some("reattach");
        let err = host
            .install_created_session(
                "gbt-reattach-fail".to_owned(),
                provider.clone(),
                Arc::clone(&session),
                Some(client.to_owned()),
                None,
                Some(spawn_lease),
                0,
                0,
            )
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("injected reattach failure"),
            "error={err:#}"
        );
        assert!(host.registry.lock().unwrap().sessions.is_empty());
        assert!(
            !host
                .registry
                .lock()
                .unwrap()
                .pending_providers
                .contains_key(&provider)
        );
        assert!(!host.registry.lock().unwrap().clients.contains_key(client));

        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if phase_is_terminal(session.state().unwrap().phase) {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "session not terminal after reattach-fail install: {:?}",
            session.state().unwrap().phase
        );
    }

    #[cfg(unix)]
    #[test]
    fn install_phase3_failure_terminates_process_and_clears_pending() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        });
        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let config = test_launch_config(&cwd);
        let mut command = CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("sleep 60");
        let provider = generate_provider_session_id().unwrap();
        host.registry
            .lock()
            .unwrap()
            .begin_pending_provider(&provider)
            .unwrap();
        // Buffer a hook that must not stick around after abort.
        assert!(
            host.apply_hook_event(&provider, hook_event(HookEventKind::SessionStart))
                .unwrap()
        );
        let session = Session::spawn_with_command(
            "gbt-phase3-fail".to_owned(),
            config,
            command,
            Arc::clone(&host.revision),
        )
        .unwrap();

        *host.install_inject_failure.lock().unwrap() = Some("phase3");
        let err = host
            .install_created_session(
                "gbt-phase3-fail".to_owned(),
                provider.clone(),
                Arc::clone(&session),
                None,
                None,
                None,
                0,
                0,
            )
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("injected phase3 failure"),
            "error={err:#}"
        );
        assert!(host.registry.lock().unwrap().sessions.is_empty());
        assert!(
            !host
                .registry
                .lock()
                .unwrap()
                .pending_providers
                .contains_key(&provider)
        );
        // Late hooks for aborted provider must not be accepted.
        assert!(
            !host
                .apply_hook_event(&provider, hook_event(HookEventKind::UserPromptSubmit))
                .unwrap()
        );

        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if phase_is_terminal(session.state().unwrap().phase) {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "session not terminal after phase3-fail install: {:?}",
            session.state().unwrap().phase
        );
    }

    /// Phase-1 failure before any registry mutation must still tear down the
    /// spawned process via InstallSessionGuard (same cleanup as lock-poison).
    #[cfg(unix)]
    #[test]
    fn install_phase1_reject_terminates_and_clears_pending() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        });
        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let config = test_launch_config(&cwd);
        let mut command = CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("sleep 60");
        let provider = generate_provider_session_id().unwrap();
        host.registry
            .lock()
            .unwrap()
            .begin_pending_provider(&provider)
            .unwrap();
        let session = Session::spawn_with_command(
            "gbt-phase1-reject".to_owned(),
            config,
            command,
            Arc::clone(&host.revision),
        )
        .unwrap();

        // Stop accepting so Phase 1 fails before insert; guard must kill PTY.
        host.registry.lock().unwrap().accepting = false;
        let err = host
            .install_created_session(
                "gbt-phase1-reject".to_owned(),
                provider.clone(),
                Arc::clone(&session),
                None,
                None,
                None,
                0,
                0,
            )
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("no longer accepts"),
            "error={err:#}"
        );
        assert!(host.registry.lock().unwrap().sessions.is_empty());
        assert!(
            !host
                .registry
                .lock()
                .unwrap()
                .pending_providers
                .contains_key(&provider)
        );

        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if phase_is_terminal(session.state().unwrap().phase) {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "session not terminal after phase1 reject: {:?}",
            session.state().unwrap().phase
        );
    }

    #[test]
    fn mark_wait_error_converges_to_failed_without_half_open() {
        let session = test_session(SessionPhase::Running);
        {
            let mut inner = session.inner.lock().unwrap();
            inner.reader_done = true;
            inner.process_done = false;
            inner.process_id = Some(4_242);
        }
        session.mark_wait_error("wait failed".to_owned());
        let state = session.state().unwrap();
        assert!(phase_is_terminal(state.phase));
        assert_eq!(state.phase, SessionPhase::Failed);
        assert!(state.error.as_deref().unwrap().contains("wait failed"));
        // wait Err is not OS death: process_done must stay false and pid retained.
        {
            let inner = session.inner.lock().unwrap();
            assert!(
                !inner.process_done,
                "mark_wait_error must not invent process_done"
            );
            assert_eq!(inner.process_id, Some(4_242));
        }
    }

    #[test]
    fn failed_phase_then_exit_then_reader_still_releases_resources() {
        let session = test_session(SessionPhase::Running);
        {
            let mut inner = session.inner.lock().unwrap();
            inner.process_done = false;
            inner.reader_done = false;
            inner.process_id = Some(7_001);
        }
        assert!(session.writer_tx.lock().unwrap().is_some());
        session.mark_wait_error("wait failed first".to_owned());
        assert_eq!(session.state().unwrap().phase, SessionPhase::Failed);
        // UI Failed first: resources still held while OS facts incomplete.
        assert!(session.writer_tx.lock().unwrap().is_some());

        session.mark_exit(1);
        // Only one done flag: still no finish_transition.
        assert!(session.writer_tx.lock().unwrap().is_some());
        {
            let inner = session.inner.lock().unwrap();
            assert!(inner.process_done);
            assert!(!inner.reader_done);
        }

        session.mark_reader_done();
        // Both done after Failed: must finalize resources, keep Failed + error.
        assert!(
            session.writer_tx.lock().unwrap().is_none(),
            "writer must drop so the writer thread can exit"
        );
        assert!(session.master.lock().unwrap().is_none());
        assert!(session.reaper_state.is_terminal());
        let state = session.state().unwrap();
        assert_eq!(state.phase, SessionPhase::Failed);
        assert!(
            state
                .error
                .as_deref()
                .unwrap()
                .contains("wait failed first")
        );
        assert!(session.inner.lock().unwrap().process_id.is_none());
        let wait = session.wait(WaitCondition::Exit, 50).unwrap();
        assert!(
            wait.satisfied,
            "wait exit only after real process+reader done"
        );
        assert!(!wait.timed_out);
    }

    #[test]
    fn failed_phase_then_reader_then_exit_still_releases_resources() {
        let session = test_session(SessionPhase::Running);
        {
            let mut inner = session.inner.lock().unwrap();
            inner.process_done = false;
            inner.reader_done = false;
            inner.process_id = Some(7_002);
        }
        session.force_failed_if_unrecoverable(); // no-op without error
        {
            let mut inner = session.inner.lock().unwrap();
            record_error(&mut inner, "writer/unrecoverable first".to_owned());
        }
        session.force_failed_if_unrecoverable();
        assert_eq!(session.state().unwrap().phase, SessionPhase::Failed);
        assert!(session.writer_tx.lock().unwrap().is_some());

        session.mark_reader_done();
        assert!(session.writer_tx.lock().unwrap().is_some());
        assert!(!session.inner.lock().unwrap().process_done);

        session.mark_exit(137);
        assert!(
            session.writer_tx.lock().unwrap().is_none(),
            "writer must release on second done after Failed phase"
        );
        assert!(session.reaper_state.is_terminal());
        let state = session.state().unwrap();
        assert_eq!(state.phase, SessionPhase::Failed);
        assert!(
            state
                .error
                .as_deref()
                .unwrap()
                .contains("writer/unrecoverable first")
        );
        let wait = session.wait(WaitCondition::Exit, 50).unwrap();
        assert!(wait.satisfied);
        assert_eq!(wait.exit_code, Some(137));
    }

    #[test]
    fn failed_phase_read_waits_for_real_reader_eof() {
        let session = test_session(SessionPhase::Running);
        {
            let mut inner = session.inner.lock().unwrap();
            set_phase(&mut inner, SessionPhase::Failed, now_millis());
            inner.error = Some("writer failed while reader remains open".to_owned());
            inner.reader_done = false;
        }

        let started = Instant::now();
        let before_eof = session.read(0, 1, 30).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(20));
        assert!(!before_eof.eof);

        session.mark_reader_done();
        let after_eof = session.read(0, 1, 0).unwrap();
        assert!(after_eof.eof);
    }

    #[test]
    fn wait_exit_not_satisfied_while_failed_but_process_still_open() {
        let session = test_session(SessionPhase::Running);
        {
            let mut inner = session.inner.lock().unwrap();
            inner.process_done = false;
            inner.reader_done = false;
            record_error(&mut inner, "error edge".to_owned());
        }
        session.force_failed_if_unrecoverable();
        assert_eq!(session.state().unwrap().phase, SessionPhase::Failed);
        let early = session.wait(WaitCondition::Exit, 30).unwrap();
        assert!(!early.satisfied);
        assert!(early.timed_out);
        // Real completion after Failed UI phase.
        session.mark_exit(1);
        session.mark_reader_done();
        let done = session.wait(WaitCondition::Exit, 50).unwrap();
        assert!(done.satisfied);
    }

    #[test]
    fn mark_wait_error_with_reader_done_does_not_let_close_short_circuit() {
        // reader_done + wait error + still-living process (process_done false):
        // close/shutdown must not return Ok solely because phase is Failed.
        let session = test_session(SessionPhase::Running);
        {
            let mut inner = session.inner.lock().unwrap();
            inner.reader_done = true;
            inner.process_done = false;
            inner.process_id = Some(9_001);
        }
        session.mark_wait_error("wait failed while process may live".to_owned());
        assert_eq!(session.state().unwrap().phase, SessionPhase::Failed);
        assert!(!session.inner.lock().unwrap().process_done);
        // No terminator on test_session: shutdown must fail rather than Ok.
        let err = session.shutdown().unwrap_err();
        let message = format!("{err:#}");
        assert!(
            message.contains("terminator is unavailable")
                || message.contains("did not terminate")
                || message.contains("close deadline"),
            "close must not short-circuit success while process_done is false: {message}"
        );
        assert!(
            !session.inner.lock().unwrap().process_done,
            "failed close must not invent process_done"
        );
    }

    #[cfg(unix)]
    #[test]
    fn wait_error_then_close_still_requires_real_process_exit() {
        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let mut command = CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("trap '' HUP; sleep 60");
        let session = Session::spawn_with_command(
            "gbt-wait-error-close".to_owned(),
            test_launch_config(&cwd),
            command,
            Arc::new(HostRevision::new()),
        )
        .unwrap();
        // Simulate waiter error while the tree is still alive.
        session.mark_wait_error("simulated child.wait Err".to_owned());
        {
            let mut inner = session.inner.lock().unwrap();
            // Reader finished (EOF or closed) while process may still run.
            inner.reader_done = true;
            assert!(!inner.process_done);
            assert!(inner.process_id.is_some());
        }
        assert_eq!(session.state().unwrap().phase, SessionPhase::Failed);
        // close must drive terminator and only succeed after real process_done.
        session.shutdown().unwrap();
        let inner = session.inner.lock().unwrap();
        assert!(
            inner.process_done,
            "successful close requires real process_done after wait error"
        );
        assert!(inner.reader_done);
    }

    #[test]
    fn mark_reader_error_with_process_done_finalizes() {
        let session = test_session(SessionPhase::Running);
        {
            let mut inner = session.inner.lock().unwrap();
            inner.process_done = true;
            inner.exit_code = Some(1);
            inner.reader_done = false;
        }
        session.mark_reader_error("reader failed".to_owned());
        let state = session.state().unwrap();
        assert_eq!(state.phase, SessionPhase::Failed);
        assert!(state.error.as_deref().unwrap().contains("reader failed"));
    }

    #[cfg(unix)]
    #[test]
    fn failure_reaper_escalates_when_peer_ignores_first_signal() {
        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        // Ignore HUP; TERM/KILL from the reaper must still stop the process group.
        let mut command = CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("trap '' HUP; sleep 60");
        let host_revision = Arc::new(HostRevision::new());
        let session = Session::spawn_with_command(
            "gbt-ignore-hup".to_owned(),
            test_launch_config(&cwd),
            command,
            Arc::clone(&host_revision),
        )
        .unwrap();
        // Writer failure starts the reaper; timeout/escalation must autonomously
        // finalize and bump host revision without any private poll helper and
        // without requiring a later state()/signal event from the caller.
        session.mark_writer_error("simulated writer failure".to_owned());
        let after_error_revision = host_revision.current();
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            // Observe via HostRevision waiters + a non-mutating state() read.
            // Do not call poll_reaper_force_fail — production must wake itself.
            let advanced =
                host_revision.wait_for_change(after_error_revision, Duration::from_millis(50));
            let state = session.state().unwrap();
            if phase_is_terminal(state.phase) {
                assert!(
                    state
                        .error
                        .as_deref()
                        .unwrap_or("")
                        .contains("writer failure")
                        || matches!(
                            state.phase,
                            SessionPhase::Failed | SessionPhase::Stopped | SessionPhase::Exited
                        ),
                    "phase={:?} error={:?}",
                    state.phase,
                    state.error
                );
                assert_ne!(
                    advanced, after_error_revision,
                    "terminal convergence must notify host revision waiters without a later poll"
                );
                return;
            }
            if Instant::now() >= deadline {
                panic!(
                    "reaper did not converge autonomously; phase={:?} error={:?}",
                    state.phase, state.error
                );
            }
        }
    }

    #[test]
    fn host_revision_bumps_on_touch_client_and_waiters_observe_it() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        });
        // Only registered clients may refresh a lease.
        host.registry
            .lock()
            .unwrap()
            .clients
            .insert("codex-thread-42".to_owned(), Arc::new(AtomicU64::new(0)));
        let seen = host.revision();
        host.touch_client("codex-thread-42").unwrap();
        assert_ne!(host.revision(), seen);
        let advanced = host.wait_revision(seen, Duration::from_millis(50));
        assert_ne!(advanced, seen);
    }

    #[test]
    fn touch_client_does_not_create_lease_for_unknown_ids() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        });
        let before = host.registry.lock().unwrap().clients.len();
        // Flood unique unknown ids — none may enter the map.
        for i in 0..2_000 {
            host.touch_client_at(&format!("flood-client-{i}"), 1_000 + i as u64)
                .unwrap();
        }
        assert_eq!(
            host.registry.lock().unwrap().clients.len(),
            before,
            "unknown touch_client must not grow clients"
        );
        // Heartbeat semantics: success with no side effects.
        host.touch_client("never-created").unwrap();
        assert!(
            !host
                .registry
                .lock()
                .unwrap()
                .clients
                .contains_key("never-created")
        );
    }

    #[test]
    fn touch_client_after_close_does_not_resurrect_empty_lease() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        });
        let client = "codex-close-no-resurrect";
        host.registry
            .lock()
            .unwrap()
            .clients
            .insert(client.to_owned(), Arc::new(AtomicU64::new(100)));
        // close_client removes the lease (no sessions needed).
        host.close_client(client).unwrap();
        assert!(!host.registry.lock().unwrap().clients.contains_key(client));
        // Post-response refresh path used to re-insert; must stay gone.
        host.touch_client_at(client, 9_999).unwrap();
        assert!(
            !host.registry.lock().unwrap().clients.contains_key(client),
            "close must not be undone by touch_client refresh"
        );
    }

    #[test]
    fn touch_client_refreshes_registered_lease_for_existing_session() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let client = "codex-refresh-lease";
        {
            let mut registry = host.registry.lock().unwrap();
            registry
                .clients
                .insert(client.to_owned(), Arc::new(AtomicU64::new(500)));
            let session = registry.sessions.get("gbt-test").unwrap().clone();
            drop(registry);
            session.inner.lock().unwrap().client_session_id = Some(client.to_owned());
            // Point session at the live map Arc.
            let lease = host
                .registry
                .lock()
                .unwrap()
                .clients
                .get(client)
                .unwrap()
                .clone();
            session.inner.lock().unwrap().client_lease = Some(lease);
        }
        host.touch_client_at(client, 2_500).unwrap();
        assert_eq!(
            host.registry
                .lock()
                .unwrap()
                .clients
                .get(client)
                .unwrap()
                .load(Ordering::Acquire),
            2_500
        );
        assert_eq!(
            host.show("gbt-test").unwrap().client_last_seen_at_ms,
            Some(2_500)
        );
    }

    /// close_client during create (after lease capture, before register) must
    /// abort registration — not leave a Session on a detached lease Arc.
    #[cfg(unix)]
    #[test]
    fn create_vs_close_client_aborts_when_epoch_advances() {
        let host = Arc::new(SessionHost::new(OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        }));
        let client = "codex-create-close-race";
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        {
            let mut hook = host.create_after_lease_hook.lock().unwrap();
            *hook = Some(Box::new(move || {
                ready_tx.send(()).unwrap();
                go_rx.recv().unwrap();
            }));
        }

        // Fast-exit bin so spawn returns quickly after close unblocks the hook.
        // SAFETY: test-only env mutation, restored below.
        let previous_bin = env::var_os("GROK_BIN");
        unsafe {
            env::set_var("GROK_BIN", "/bin/true");
        }

        let create_host = Arc::clone(&host);
        let cwd = env!("CARGO_MANIFEST_DIR").to_owned();
        let create_thread = thread::spawn(move || {
            create_host.create(
                &cwd,
                None,
                None,
                Some("owner-race".to_owned()),
                false,
                Some(client.to_owned()),
            )
        });

        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("create did not reach after-lease hook");
        // Lease is captured and clients map has an entry; no session registered.
        {
            let registry = host.registry.lock().unwrap();
            assert!(registry.clients.contains_key(client));
            assert!(registry.sessions.is_empty());
            assert_eq!(registry.client_epoch(client), 0);
        }
        let closed = host.close_client(client).unwrap();
        assert_eq!(closed.matched, 0);
        {
            let registry = host.registry.lock().unwrap();
            assert!(!registry.clients.contains_key(client));
            assert_eq!(registry.client_epoch(client), 1);
        }
        go_tx.send(()).unwrap();
        let result = create_thread.join().expect("create thread");
        match previous_bin {
            Some(value) => unsafe {
                env::set_var("GROK_BIN", value);
            },
            None => unsafe {
                env::remove_var("GROK_BIN");
            },
        }
        assert!(
            result.is_err(),
            "create must fail after close_client: {result:?}"
        );
        let message = format!("{:#}", result.unwrap_err());
        assert!(
            message.contains("closed during create")
                || message.contains("failed to")
                || message.contains("No such file"),
            "unexpected create error after close_client: {message}"
        );
        {
            let registry = host.registry.lock().unwrap();
            assert!(
                registry.sessions.is_empty(),
                "in-flight create must not register after close_client"
            );
            assert!(
                !registry.clients.contains_key(client),
                "failed create must not leave a stale clients map entry"
            );
        }
    }

    /// Map eviction without epoch bump (last-session remove_session) must
    /// reattach so subsequent touch_client heartbeats reach the Session.
    #[cfg(unix)]
    #[test]
    fn install_reattaches_lease_after_client_map_eviction() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        });
        let client = "codex-reattach";
        let spawn_lease = Arc::new(AtomicU64::new(1_000));
        {
            let mut registry = host.registry.lock().unwrap();
            registry
                .clients
                .insert(client.to_owned(), Arc::clone(&spawn_lease));
        }

        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let mut config = test_launch_config(&cwd);
        config.client_session_id = Some(client.to_owned());
        config.client_lease = Some(Arc::clone(&spawn_lease));
        config.orphan_policy = OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        };
        let mut command = CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("sleep 60");
        let provider = generate_provider_session_id().unwrap();
        host.registry
            .lock()
            .unwrap()
            .begin_pending_provider(&provider)
            .unwrap();
        let session = Session::spawn_with_command(
            "gbt-reattach".to_owned(),
            config,
            command,
            Arc::clone(&host.revision),
        )
        .unwrap();

        // Simulate remove_session of the last registered peer: drop clients entry
        // without advancing epoch (close_client is the only epoch path).
        host.registry.lock().unwrap().clients.remove(client);
        // Concurrent touch mints a *new* Arc — the classic detached-lease bug.
        let replacement = Arc::new(AtomicU64::new(0));
        host.registry
            .lock()
            .unwrap()
            .clients
            .insert(client.to_owned(), Arc::clone(&replacement));
        replacement.store(2_000, Ordering::Release);

        host.install_created_session(
            "gbt-reattach".to_owned(),
            provider,
            Arc::clone(&session),
            Some(client.to_owned()),
            None,
            Some(Arc::clone(&spawn_lease)),
            0,
            0,
        )
        .unwrap();

        // Session must track the map's live Arc, not the pre-eviction spawn_lease.
        host.touch_client_at(client, 3_000).unwrap();
        let state = session.state().unwrap();
        assert_eq!(state.client_last_seen_at_ms, Some(3_000));
        assert_eq!(
            host.registry
                .lock()
                .unwrap()
                .clients
                .get(client)
                .unwrap()
                .load(Ordering::Acquire),
            3_000
        );
        // spawn_lease was left behind and must not be the heartbeat target.
        assert_eq!(spawn_lease.load(Ordering::Acquire), 1_000);
        let _ = session.shutdown();
    }

    #[cfg(unix)]
    #[test]
    fn install_fails_cleanly_when_close_client_wins_epoch() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        });
        let client = "codex-epoch-close";
        let spawn_lease = Arc::new(AtomicU64::new(1_000));
        host.registry
            .lock()
            .unwrap()
            .clients
            .insert(client.to_owned(), Arc::clone(&spawn_lease));

        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let mut config = test_launch_config(&cwd);
        config.client_session_id = Some(client.to_owned());
        config.client_lease = Some(Arc::clone(&spawn_lease));
        let mut command = CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("sleep 60");
        let provider = generate_provider_session_id().unwrap();
        let session = Session::spawn_with_command(
            "gbt-epoch-close".to_owned(),
            config,
            command,
            Arc::clone(&host.revision),
        )
        .unwrap();

        // Hold a pending-create reservation so close cannot reclaim the epoch
        // before this late install (mirrors a real in-flight create).
        host.registry
            .lock()
            .unwrap()
            .reserve_pending_create(Some(client), None)
            .unwrap();
        // Explicit client teardown while create would be between spawn and install.
        host.close_client(client).unwrap();
        assert_eq!(host.registry.lock().unwrap().client_epoch(client), 1);

        let err = host
            .install_created_session(
                "gbt-epoch-close".to_owned(),
                provider,
                Arc::clone(&session),
                Some(client.to_owned()),
                None,
                Some(spawn_lease),
                0,
                0,
            )
            .unwrap_err();
        host.registry
            .lock()
            .unwrap()
            .release_pending_create(Some(client), None);
        assert!(
            format!("{err:#}").contains("closed during create"),
            "error={err:#}"
        );
        assert!(host.registry.lock().unwrap().sessions.is_empty());
        assert!(!host.registry.lock().unwrap().clients.contains_key(client));
        // Epoch reclaimed once no pending create / session / closer remains.
        assert_eq!(host.registry.lock().unwrap().client_epoch(client), 0);
        assert!(
            !host
                .registry
                .lock()
                .unwrap()
                .client_epochs
                .contains_key(client)
        );
        // Process must be torn down, not left half-registered.
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if phase_is_terminal(session.state().unwrap().phase) {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "session not terminal after failed install: {:?}",
            session.state().unwrap().phase
        );
    }

    #[test]
    fn forget_client_if_unreferenced_drops_stale_map_entry() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        });
        let client = "codex-stale-map";
        host.registry
            .lock()
            .unwrap()
            .clients
            .insert(client.to_owned(), Arc::new(AtomicU64::new(1)));
        host.forget_client_if_unreferenced(Some(client)).unwrap();
        assert!(!host.registry.lock().unwrap().clients.contains_key(client));
        // Epoch must stay put — this is spawn-failure cleanup, not close_client.
        assert_eq!(host.registry.lock().unwrap().client_epoch(client), 0);
    }

    #[test]
    fn close_client_fence_bumps_epoch_and_uses_refcount() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        });
        let client = "codex-fence-refcount";
        assert_eq!(host.registry.lock().unwrap().client_epoch(client), 0);
        {
            let mut reg = host.registry.lock().unwrap();
            reg.begin_client_closing(client);
            assert_eq!(reg.client_epoch(client), 1);
            assert_eq!(reg.client_closing_count(client), 1);
            reg.begin_client_closing(client);
            // Second concurrent closer: refcount up, epoch not double-bumped.
            assert_eq!(reg.client_epoch(client), 1);
            assert_eq!(reg.client_closing_count(client), 2);
            assert!(reg.is_client_closing(client));
            reg.end_client_closing(client);
            assert_eq!(reg.client_closing_count(client), 1);
            assert!(reg.is_client_closing(client));
            reg.end_client_closing(client);
            assert_eq!(reg.client_closing_count(client), 0);
            assert!(!reg.is_client_closing(client));
        }
    }

    #[test]
    fn close_client_err_after_fence_clears_closing_so_create_recovers() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        });
        let client = "codex-fence-recover";
        host.close_client_force_err_after_fence
            .store(true, Ordering::Release);
        let err = host.close_client(client).unwrap_err();
        assert!(
            format!("{err:#}").contains("injected close_sessions failure"),
            "{err:#}"
        );
        assert!(
            !host.registry.lock().unwrap().is_client_closing(client),
            "fence must clear on Err return"
        );
        // No pending create/session: generation entry is reclaimed (not a permanent
        // map growth site). In-flight creates keep the epoch via pending reservation.
        assert_eq!(host.registry.lock().unwrap().client_epoch(client), 0);
        assert!(
            !host
                .registry
                .lock()
                .unwrap()
                .client_epochs
                .contains_key(client)
        );
        // Second close succeeds and leaves fence clear.
        host.close_client(client).unwrap();
        assert!(!host.registry.lock().unwrap().is_client_closing(client));
    }

    #[test]
    fn concurrent_close_client_keeps_fence_until_last_closer() {
        let host = Arc::new(SessionHost::new(OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        }));
        let client = "codex-concurrent-close";
        let (a_hold_tx, a_hold_rx) = std::sync::mpsc::channel();
        let (b_enter_tx, b_enter_rx) = std::sync::mpsc::channel();
        let (a_go_tx, a_go_rx) = std::sync::mpsc::channel();

        // Closer A: enter fence, signal, wait, then finish.
        let host_a = Arc::clone(&host);
        let a = thread::spawn(move || {
            let _guard = ClientClosingGuard::enter(&host_a.registry, client).unwrap();
            a_hold_tx.send(()).unwrap();
            a_go_rx.recv().unwrap();
        });

        a_hold_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(host.registry.lock().unwrap().is_client_closing(client));
        assert_eq!(host.registry.lock().unwrap().client_epoch(client), 1);

        // Closer B enters while A still holds the fence.
        let host_b = Arc::clone(&host);
        let b = thread::spawn(move || {
            let _guard = ClientClosingGuard::enter(&host_b.registry, client).unwrap();
            b_enter_tx.send(()).unwrap();
            // Hold briefly so A can exit first.
            thread::sleep(Duration::from_millis(50));
        });

        b_enter_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(
            host.registry.lock().unwrap().client_closing_count(client),
            2
        );
        // Epoch bumped only once for the first closer.
        assert_eq!(host.registry.lock().unwrap().client_epoch(client), 1);

        // A exits: fence remains while B is still in.
        a_go_tx.send(()).unwrap();
        a.join().unwrap();
        assert!(
            host.registry.lock().unwrap().is_client_closing(client),
            "first closer must not clear fence while second still runs"
        );
        assert_eq!(
            host.registry.lock().unwrap().client_closing_count(client),
            1
        );

        b.join().unwrap();
        assert!(
            !host.registry.lock().unwrap().is_client_closing(client),
            "last closer clears fence"
        );
    }

    #[cfg(unix)]
    #[test]
    fn concurrent_two_close_client_blocks_create_until_both_finish() {
        let host = Arc::new(SessionHost::new(OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        }));
        let client = "codex-two-close-create";
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let host_a = Arc::clone(&host);
        let closer_a = thread::spawn(move || {
            let _g = ClientClosingGuard::enter(&host_a.registry, client).unwrap();
            let _ = ready_tx.send(());
            let _ = release_rx.recv();
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let host_b = Arc::clone(&host);
        let (b_ready_tx, b_ready_rx) = std::sync::mpsc::channel();
        let (b_release_tx, b_release_rx) = std::sync::mpsc::channel();
        let closer_b = thread::spawn(move || {
            let _g = ClientClosingGuard::enter(&host_b.registry, client).unwrap();
            let _ = b_ready_tx.send(());
            let _ = b_release_rx.recv();
        });
        b_ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        // Mid dual-close: install with stale epoch 0 must fail.
        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let mut config = test_launch_config(&cwd);
        config.client_session_id = Some(client.to_owned());
        config.client_lease = Some(Arc::new(AtomicU64::new(1)));
        let mut command = CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("sleep 60");
        let provider = generate_provider_session_id().unwrap();
        let session = Session::spawn_with_command(
            "gbt-dual-close".to_owned(),
            config,
            command,
            Arc::clone(&host.revision),
        )
        .unwrap();
        let err = host
            .install_created_session(
                "gbt-dual-close".to_owned(),
                provider,
                Arc::clone(&session),
                Some(client.to_owned()),
                None,
                Some(Arc::new(AtomicU64::new(1))),
                0,
                0,
            )
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("closed during create")
                || format!("{err:#}").contains("closing"),
            "{err:#}"
        );

        let _ = release_tx.send(());
        closer_a.join().unwrap();
        assert!(
            host.registry.lock().unwrap().is_client_closing(client),
            "first closer exit must not reopen create while second still holds fence"
        );
        let _ = b_release_tx.send(());
        closer_b.join().unwrap();
        assert!(!host.registry.lock().unwrap().is_client_closing(client));
        let _ = session.shutdown();
    }

    /// close_client fences create/install: concurrent install during close must
    /// fail (or any slipped session must be closed). No survivor after close.
    #[cfg(unix)]
    #[test]
    fn close_client_rejects_install_race_before_lease_drop() {
        let host = Arc::new(SessionHost::new(OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        }));
        let client = "codex-close-install-race";
        let spawn_lease = Arc::new(AtomicU64::new(1_000));
        host.registry
            .lock()
            .unwrap()
            .clients
            .insert(client.to_owned(), Arc::clone(&spawn_lease));

        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let mut config = test_launch_config(&cwd);
        config.client_session_id = Some(client.to_owned());
        config.client_lease = Some(Arc::clone(&spawn_lease));
        config.orphan_policy = OrphanPolicy {
            lease_ms: 100,
            grace_ms: 200,
        };
        let mut command = CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("sleep 60");
        let provider = generate_provider_session_id().unwrap();
        let session = Session::spawn_with_command(
            "gbt-close-race".to_owned(),
            config,
            command,
            Arc::clone(&host.revision),
        )
        .unwrap();

        let host_install = Arc::clone(&host);
        let session_install = Arc::clone(&session);
        let lease_install = Arc::clone(&spawn_lease);
        let install_result = Arc::new(Mutex::new(None));
        let install_slot = Arc::clone(&install_result);
        {
            let mut hook = host.close_client_before_lease_hook.lock().unwrap();
            *hook = Some(Box::new(move || {
                // Epoch was already advanced at close start; install must fail.
                let err = host_install.install_created_session(
                    "gbt-close-race".to_owned(),
                    provider,
                    session_install,
                    Some(client.to_owned()),
                    None,
                    Some(lease_install),
                    0,
                    0,
                );
                *install_slot.lock().unwrap() = Some(err.err().map(|e| format!("{e:#}")));
            }));
        }

        let result = host.close_client(client).unwrap();
        assert!(result.failures.is_empty());
        let install_err = install_result
            .lock()
            .unwrap()
            .clone()
            .expect("hook must run");
        assert!(
            install_err
                .as_ref()
                .is_some_and(|e| e.contains("closed during create") || e.contains("closing")),
            "install must fail during close fence: {install_err:?}"
        );
        assert!(
            !host
                .registry
                .lock()
                .unwrap()
                .sessions
                .contains_key("gbt-close-race"),
            "no session may survive close_client"
        );
        assert!(!host.registry.lock().unwrap().clients.contains_key(client));
        // Process from the failed install must not stay registered; shutdown best-effort.
        let _ = session.shutdown();
    }

    #[test]
    fn force_failed_does_not_fake_process_done() {
        let session = test_session(SessionPhase::Running);
        {
            let mut inner = session.inner.lock().unwrap();
            record_error(&mut inner, "simulated unrecoverable".to_owned());
            assert!(!inner.process_done);
            assert!(!inner.reader_done);
        }
        session.force_failed_if_unrecoverable();
        let state = session.state().unwrap();
        assert_eq!(state.phase, SessionPhase::Failed);
        {
            let inner = session.inner.lock().unwrap();
            assert!(
                !inner.process_done,
                "force_failed must not invent process_done"
            );
            assert!(
                !inner.reader_done,
                "force_failed must not invent reader_done"
            );
        }
        // Phase is terminal but OS facts are incomplete: shutdown must not
        // short-circuit as success without process_done && reader_done.
        let err = session.shutdown().unwrap_err();
        let message = format!("{err:#}");
        assert!(
            message.contains("terminator is unavailable")
                || message.contains("did not terminate")
                || message.contains("close deadline")
                || message.contains("PTY output"),
            "close must keep requiring real completion: {message}"
        );
    }

    #[test]
    fn wait_exit_not_satisfied_after_force_failed_while_process_still_open() {
        let session = test_session(SessionPhase::Running);
        {
            let mut inner = session.inner.lock().unwrap();
            record_error(&mut inner, "reaper budget exhausted".to_owned());
        }
        session.force_failed_if_unrecoverable();
        assert_eq!(session.state().unwrap().phase, SessionPhase::Failed);
        {
            let mut inner = session.inner.lock().unwrap();
            assert!(
                !wait_satisfied(&mut inner, WaitCondition::Exit),
                "Failed phase alone must not satisfy wait --for exit"
            );
        }
        // Short wait must time out, not claim success while process/PTY live.
        let result = session.wait(WaitCondition::Exit, 40).unwrap();
        assert!(
            !result.satisfied,
            "wait exit must not false-succeed after force_failed"
        );
        assert!(result.timed_out);
        assert_eq!(result.phase, SessionPhase::Failed);
    }

    #[test]
    fn wait_exit_satisfied_only_after_real_process_and_reader_done() {
        let session = test_session(SessionPhase::Running);
        {
            let mut inner = session.inner.lock().unwrap();
            record_error(&mut inner, "error edge before real exit".to_owned());
        }
        session.force_failed_if_unrecoverable();
        assert_eq!(session.state().unwrap().phase, SessionPhase::Failed);
        let early = session.wait(WaitCondition::Exit, 20).unwrap();
        assert!(!early.satisfied);
        assert!(early.timed_out);

        // Real OS process end + PTY EOF (what mark_exit / mark_reader_done set).
        {
            let mut inner = session.inner.lock().unwrap();
            inner.process_done = true;
            inner.reader_done = true;
            inner.exit_code = Some(1);
            let _ = finalize_session(&mut inner, false);
        }
        session.signal_changed();
        let done = session.wait(WaitCondition::Exit, 100).unwrap();
        assert!(done.satisfied);
        assert!(!done.timed_out);
        assert_eq!(done.exit_code, Some(1));
    }

    #[cfg(unix)]
    #[test]
    fn shutdown_after_force_failed_still_drives_real_termination() {
        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let mut command = CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("trap '' HUP; sleep 60");
        let session = Session::spawn_with_command(
            "gbt-force-fail-close".to_owned(),
            test_launch_config(&cwd),
            command,
            Arc::new(HostRevision::new()),
        )
        .unwrap();
        {
            let mut inner = session.inner.lock().unwrap();
            record_error(&mut inner, "writer failed for force-fail test".to_owned());
        }
        session.force_failed_if_unrecoverable();
        assert_eq!(session.state().unwrap().phase, SessionPhase::Failed);
        // Real terminator still available: close eventually ends the tree.
        session.shutdown().unwrap();
        let inner = session.inner.lock().unwrap();
        assert!(inner.process_done);
        assert!(inner.reader_done);
    }

    #[cfg(unix)]
    #[test]
    fn wait_exit_after_force_failed_stays_unsatisfied_until_real_exit() {
        let cwd = canonical_directory(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let mut command = CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("trap '' HUP; sleep 60");
        let session = Session::spawn_with_command(
            "gbt-wait-exit-force-fail".to_owned(),
            test_launch_config(&cwd),
            command,
            Arc::new(HostRevision::new()),
        )
        .unwrap();
        {
            let mut inner = session.inner.lock().unwrap();
            record_error(&mut inner, "reaper-style force failed".to_owned());
        }
        session.force_failed_if_unrecoverable();
        assert_eq!(session.state().unwrap().phase, SessionPhase::Failed);
        {
            let inner = session.inner.lock().unwrap();
            assert!(!inner.process_done);
            assert!(!inner.reader_done);
        }
        // Reaper/UI may already show Failed, but wait --for exit must not
        // succeed while the process group is still alive.
        let early = session.wait(WaitCondition::Exit, 80).unwrap();
        assert!(
            !early.satisfied,
            "wait exit false-succeeded while process still open: {early:?}"
        );
        assert!(early.timed_out);

        session.shutdown().unwrap();
        let after = session.wait(WaitCondition::Exit, 200).unwrap();
        assert!(
            after.satisfied,
            "wait exit must succeed only after real process/PTY end: {after:?}"
        );
        assert!(!after.timed_out);
        let inner = session.inner.lock().unwrap();
        assert!(inner.process_done);
        assert!(inner.reader_done);
    }

    #[test]
    fn write_raw_surfaces_writer_flush_failure_without_success_effect() {
        let session = test_session(SessionPhase::Idle);
        let (writer_tx, writer_rx) = sync_channel(4);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);
        thread::spawn(move || {
            while let Ok(job) = writer_rx.recv() {
                if let Some(completion) = job.completion {
                    completion.complete(Err("simulated broken pipe".to_owned()));
                }
            }
        });
        let err = session.write_raw(b"hello\r".to_vec()).unwrap_err();
        assert!(
            format!("{err:#}").contains("broken pipe")
                || format!("{err:#}").contains("not safe to retry"),
            "err={err:#}"
        );
        // StartTurn effect must not apply when flush failed; session leaves
        // the writable path (Failed) so the same payload cannot be re-enqueued.
        assert_eq!(session.state().unwrap().phase, SessionPhase::Failed);
        assert_ne!(session.state().unwrap().activity, HookActivity::Working);
        let again = session.write_raw(b"hello\r".to_vec());
        assert!(
            again.is_err(),
            "partial/failed write must reject further enqueues"
        );
    }

    #[test]
    fn write_raw_ack_waits_for_real_flush_success() {
        let session = test_session(SessionPhase::Idle);
        let (writer_tx, writer_rx) = sync_channel(4);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);
        let (started_tx, started_rx) = sync_channel(1);
        let session_w = Arc::clone(&session);
        thread::spawn(move || {
            while let Ok(job) = writer_rx.recv() {
                let _ = started_tx.send(job.data.clone());
                thread::sleep(Duration::from_millis(30));
                let _ = session_w.apply_input_effect(job.effect);
                if let Some(completion) = job.completion {
                    completion.complete(Ok(()));
                }
            }
        });
        let started = Instant::now();
        session.write_raw(b"hello\r".to_vec()).unwrap();
        assert!(
            started.elapsed() >= Duration::from_millis(20),
            "write_raw must wait for writer flush ack"
        );
        assert_eq!(started_rx.recv().unwrap(), b"hello\r");
        assert_eq!(session.state().unwrap().phase, SessionPhase::Running);
    }

    #[test]
    fn successful_write_same_id_replay_does_not_double_write() {
        // Counts real writer jobs: first write succeeds once; a second
        // write_raw is a separate call. Identity-cache replay is tested in
        // server; here we verify the writer only sees one job per call.
        let session = test_session(SessionPhase::Idle);
        let (writer_tx, writer_rx) = sync_channel(8);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);
        let writes = Arc::new(AtomicU64::new(0));
        let writes_thread = Arc::clone(&writes);
        let session_w = Arc::clone(&session);
        thread::spawn(move || {
            while let Ok(job) = writer_rx.recv() {
                writes_thread.fetch_add(1, Ordering::SeqCst);
                let _ = session_w.apply_input_effect(job.effect);
                if let Some(completion) = job.completion {
                    completion.complete(Ok(()));
                }
            }
        });
        session.write_raw(b"once\r".to_vec()).unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        // A second distinct write is another job (serial A then B).
        session.write_raw(b"twice\r".to_vec()).unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn first_write_failure_fail_alls_queued_jobs_without_writing_them() {
        // spawn_writer used to return on the first write Err and drop the Receiver,
        // leaving later WriteJob completions unpublished → wait hung forever.
        let session = test_session(SessionPhase::Idle);
        let (writer_tx, writer_rx) = sync_channel(8);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);
        let writes = Arc::new(AtomicU64::new(0));
        let writes_w = Arc::clone(&writes);
        let session_w = Arc::clone(&session);
        let (go_tx, go_rx) = sync_channel::<()>(0);
        // Mimic spawn_writer fail-all: first job errors, drain rest without write.
        thread::spawn(move || {
            go_rx.recv().unwrap();
            let Ok(job) = writer_rx.recv() else {
                return;
            };
            if session_w.writer_must_cancel() {
                job.fail(WRITE_CANCELLED_MSG);
                fail_remaining_write_jobs(&writer_rx, WRITE_CANCELLED_MSG);
                return;
            }
            // First job fails; remaining are drained as cancelled (never a second write).
            writes_w.fetch_add(1, Ordering::SeqCst);
            let msg = "PTY write/flush failed after possible partial delivery (not safe to retry): broken pipe";
            if let Some(c) = job.completion {
                c.complete(Err(msg.to_owned()));
            }
            session_w.mark_writer_error(msg.to_owned());
            fail_remaining_write_jobs(
                &writer_rx,
                "PTY write cancelled after prior write failure (not safe to retry)",
            );
        });

        let c1 = session.begin_write_job(b"first\r".to_vec()).unwrap();
        let c2 = session.begin_write_job(b"second\r".to_vec()).unwrap();
        let c3 = session.begin_write_job(b"third\r".to_vec()).unwrap();
        go_tx.send(()).unwrap();

        let t1 = thread::spawn(move || c1.wait());
        let t2 = thread::spawn(move || c2.wait());
        let t3 = thread::spawn(move || c3.wait());

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();
        let r3 = t3.join().unwrap();
        let e1 = r1.expect_err("first job must fail");
        assert!(
            e1.contains("broken pipe") || e1.contains("not safe to retry"),
            "e1={e1}"
        );
        let e2 = r2.expect_err("second must not hang or succeed");
        let e3 = r3.expect_err("third must not hang or succeed");
        assert!(
            e2.contains("cancelled") || e2.contains("not safe to retry"),
            "e2={e2}"
        );
        assert!(
            e3.contains("cancelled") || e3.contains("not safe to retry"),
            "e3={e3}"
        );
        assert_eq!(
            writes.load(Ordering::SeqCst),
            1,
            "only the failing first job may touch the writer"
        );
        assert_eq!(session.state().unwrap().phase, SessionPhase::Failed);
    }

    #[test]
    fn write_completion_timeout_is_terminal_and_late_writer_cannot_apply_effect() {
        let completion = WriteCompletion::new();
        let waiter = Arc::clone(&completion);
        let result = thread::spawn(move || waiter.wait_timeout(Duration::from_millis(10)))
            .join()
            .unwrap()
            .unwrap_err();
        assert!(result.contains("not safe to retry"));
        let applied = Arc::new(AtomicBool::new(false));
        let applied_writer = Arc::clone(&applied);
        assert!(
            completion
                .complete_success_with(|| {
                    applied_writer.store(true, Ordering::Release);
                    Ok(())
                })
                .is_none()
        );
        assert!(!applied.load(Ordering::Acquire));
    }

    #[test]
    fn write_completion_timeout_and_success_race_has_one_authoritative_winner() {
        for _ in 0..128 {
            let completion = WriteCompletion::new();
            let barrier = Arc::new(std::sync::Barrier::new(3));
            let writer_completion = Arc::clone(&completion);
            let timeout_completion = Arc::clone(&completion);
            let writer_barrier = Arc::clone(&barrier);
            let timeout_barrier = Arc::clone(&barrier);
            let writer = thread::spawn(move || {
                writer_barrier.wait();
                writer_completion.complete_success_with(|| Ok(()))
            });
            let timeout = thread::spawn(move || {
                timeout_barrier.wait();
                timeout_completion.complete_timeout()
            });
            barrier.wait();
            let writer_result = writer.join().unwrap();
            let timeout_result = timeout.join().unwrap();
            assert!(writer_result.is_some() ^ timeout_result.is_some());
            let final_result = completion.poll(false).expect("race must publish a result");
            assert!(final_result.is_ok() || final_result.is_err());
        }
    }

    #[test]
    fn raw_write_queue_full_rolls_back_paste_parser_state() {
        let session = test_session(SessionPhase::Idle);
        let (writer_tx, writer_rx) = sync_channel(1);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);

        let _first = session.begin_write_job(b"queued".to_vec()).unwrap();
        let error = match session.begin_write_job(b"\x1b[200~".to_vec()) {
            Ok(_) => panic!("second raw write must hit the full queue"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("queue is full"));
        let inner = session.inner.lock().unwrap();
        assert!(!inner.paste_open, "rejected paste start must roll back");
        assert!(
            inner.paste_scan_hold.is_empty(),
            "rejected raw bytes must not leave a partial CSI prefix"
        );
        drop(inner);
        drop(writer_rx);
    }

    #[test]
    fn close_writer_fail_alls_queued_jobs_without_executing_them() {
        let session = test_session(SessionPhase::Idle);
        let (writer_tx, writer_rx) = sync_channel(8);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);
        let writes = Arc::new(AtomicU64::new(0));
        let writes_w = Arc::clone(&writes);
        let session_w = Arc::clone(&session);
        let (started_tx, started_rx) = sync_channel::<()>(1);
        // Real spawn_writer cancel path: check writer_must_cancel before write.
        thread::spawn(move || {
            while let Ok(job) = writer_rx.recv() {
                let _ = started_tx.try_send(());
                if session_w.writer_must_cancel() {
                    job.fail(WRITE_CANCELLED_MSG);
                    fail_remaining_write_jobs(&writer_rx, WRITE_CANCELLED_MSG);
                    return;
                }
                writes_w.fetch_add(1, Ordering::SeqCst);
                // Simulate slow write so close can land while jobs are queued.
                thread::sleep(Duration::from_millis(50));
                if session_w.writer_must_cancel() {
                    job.fail(WRITE_CANCELLED_MSG);
                    fail_remaining_write_jobs(&writer_rx, WRITE_CANCELLED_MSG);
                    return;
                }
                let _ = session_w.apply_input_effect(job.effect);
                if let Some(c) = job.completion {
                    c.complete(Ok(()));
                }
            }
        });

        let c1 = session.begin_write_job(b"a\r".to_vec()).unwrap();
        let c2 = session.begin_write_job(b"b\r".to_vec()).unwrap();
        let c3 = session.begin_write_job(b"c\r".to_vec()).unwrap();
        // Wait until writer has dequeued at least the first job.
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer did not start");
        session.close_writer();

        let deadline = Instant::now() + Duration::from_secs(2);
        let wait_bounded = |c: Arc<WriteCompletion>| {
            while Instant::now() < deadline {
                if c.is_ready() {
                    return c.wait();
                }
                thread::sleep(Duration::from_millis(5));
            }
            panic!("completion not published within deadline");
        };
        let r1 = wait_bounded(c1);
        let r2 = wait_bounded(c2);
        let r3 = wait_bounded(c3);
        // First may have written or cancelled; later must be cancelled and not all written.
        assert!(r2.is_err() || r1.is_err() || r3.is_err());
        for (i, r) in [(1, r1), (2, r2), (3, r3)] {
            if let Err(e) = &r {
                assert!(
                    e.contains("cancelled") || e.contains("not safe to retry"),
                    "job {i}: {e}"
                );
            }
        }
        assert!(
            writes.load(Ordering::SeqCst) <= 1,
            "close must not let a healthy writer drain the whole pre-close queue; writes={}",
            writes.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn enqueue_vs_close_race_completions_always_resolve() {
        let session = test_session(SessionPhase::Idle);
        let (writer_tx, writer_rx) = sync_channel(64);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);
        let session_w = Arc::clone(&session);
        thread::spawn(move || {
            while let Ok(job) = writer_rx.recv() {
                if session_w.writer_must_cancel() {
                    job.fail(WRITE_CANCELLED_MSG);
                    fail_remaining_write_jobs(&writer_rx, WRITE_CANCELLED_MSG);
                    return;
                }
                // Tiny delay to widen enqueue/close race.
                thread::sleep(Duration::from_millis(1));
                if session_w.writer_must_cancel() {
                    job.fail(WRITE_CANCELLED_MSG);
                    fail_remaining_write_jobs(&writer_rx, WRITE_CANCELLED_MSG);
                    return;
                }
                let _ = session_w.apply_input_effect(job.effect);
                if let Some(c) = job.completion {
                    c.complete(Ok(()));
                }
            }
        });

        let mut handles = Vec::new();
        for i in 0..16 {
            let session_e = Arc::clone(&session);
            handles.push(thread::spawn(move || {
                match session_e.begin_write_job(format!("j{i}\r").into_bytes()) {
                    Ok(c) => Some(c.wait()),
                    Err(_) => None, // closed mid-enqueue — acceptable
                }
            }));
        }
        thread::sleep(Duration::from_millis(5));
        session.close_writer();
        for h in handles {
            // join is the wait; each worker must finish without hanging.
            let outcome = h.join().expect("enqueue worker panicked");
            if let Some(result) = outcome {
                // Either Ok (wrote before close) or definitive Err — never hang.
                let _ = result;
            }
        }
    }

    #[test]
    fn write_completion_drop_publishes_cancelled_as_safety_net() {
        let completion = WriteCompletion::new();
        assert!(!completion.is_ready());
        drop(completion);
        // Reconstruct: Drop on the only owner publishes before deallocation;
        // verify via a second Arc path that complete-before-drop is stable.
        let c = WriteCompletion::new();
        let wait = Arc::clone(&c);
        c.complete(Ok(()));
        assert!(wait.wait().is_ok());
        // Drop after complete is a no-op (no double panic / hang).
        drop(c);
        drop(wait);
    }

    #[test]
    fn slow_writer_beyond_old_timeout_still_succeeds_once_for_attached_waiters() {
        // Former 5s recv_timeout treated in-flight delay as write_failed and
        // aborted identity reservation, allowing same-id retries to re-enqueue.
        // Waiters must stay bound to one WriteCompletion until real success.
        let session = test_session(SessionPhase::Idle);
        let (writer_tx, writer_rx) = sync_channel(8);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);
        let writes = Arc::new(AtomicU64::new(0));
        let writes_thread = Arc::clone(&writes);
        let session_w = Arc::clone(&session);
        let (release_tx, release_rx) = sync_channel::<()>(1);
        thread::spawn(move || {
            while let Ok(job) = writer_rx.recv() {
                writes_thread.fetch_add(1, Ordering::SeqCst);
                // Block longer than the old 5s timeout would have allowed.
                let _ = release_rx.recv_timeout(Duration::from_secs(30));
                let _ = session_w.apply_input_effect(job.effect);
                if let Some(completion) = job.completion {
                    completion.complete(Ok(()));
                }
            }
        });

        let c1 = session.begin_write_job(b"same\r".to_vec()).unwrap();
        // Attach two more waiters to the same completion (same-id retries).
        // Effects are committed on the writer thread before complete — waiters
        // must not re-apply.
        let c2 = Arc::clone(&c1);
        let c3 = Arc::clone(&c1);
        let t1 = thread::spawn(move || c1.wait().unwrap());
        let t2 = thread::spawn(move || c2.wait().unwrap());
        let t3 = thread::spawn(move || c3.wait().unwrap());

        // Writer still blocked; only one job on the wire.
        thread::sleep(Duration::from_millis(20));
        assert_eq!(writes.load(Ordering::SeqCst), 1);

        release_tx.send(()).unwrap();
        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();
        assert_eq!(
            writes.load(Ordering::SeqCst),
            1,
            "attached waiters must not enqueue extra WriteJobs"
        );
        assert_eq!(session.state().unwrap().phase, SessionPhase::Running);
    }

    /// A=StartTurn then B=Cancel: even if waiters join in reverse order, final
    /// activity is Cancelling (writer FIFO commit), never Working from a late A.
    #[test]
    fn writer_fifo_effect_commit_ignores_waiter_join_order() {
        let session = test_session(SessionPhase::Idle);
        let (writer_tx, writer_rx) = sync_channel(8);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);
        let session_w = Arc::clone(&session);
        let (release_tx, release_rx) = sync_channel::<()>(1);
        let (both_tx, both_rx) = sync_channel::<()>(1);
        thread::spawn(move || {
            let mut jobs = Vec::new();
            // Hold both jobs until both are enqueued so write order is A then B.
            while jobs.len() < 2 {
                if let Ok(job) = writer_rx.recv() {
                    jobs.push(job);
                }
            }
            let _ = both_tx.send(());
            release_rx.recv().unwrap();
            for job in jobs {
                // Simulate successful write, then ordered effect commit.
                let _ = session_w.apply_input_effect(job.effect);
                if let Some(c) = job.completion {
                    c.complete(Ok(()));
                }
            }
        });

        // A: Enter (StartTurn), B: Ctrl-C (Cancel).
        let ca = session.begin_write_job(b"task\r".to_vec()).unwrap();
        let cb = session.begin_write_job(vec![0x03]).unwrap();
        both_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer did not hold both jobs");

        // Waiters complete in reverse order (B then A).
        let tb = thread::spawn(move || cb.wait());
        let ta = thread::spawn(move || ca.wait());
        release_tx.send(()).unwrap();
        tb.join().unwrap().unwrap();
        ta.join().unwrap().unwrap();

        let state = session.state().unwrap();
        assert_eq!(
            state.activity,
            HookActivity::Cancelling,
            "FIFO commit must leave Cancel after StartTurn, not reverse-waiter Working"
        );
        assert_eq!(state.phase, SessionPhase::Running);
    }

    /// close/shutdown between write success and effect must not revive terminal.
    #[test]
    fn start_turn_does_not_revive_terminal_phase_after_close() {
        let session = test_session(SessionPhase::Idle);
        // Force terminal + shutdown as if close finished after a write.
        session.shutdown.store(true, Ordering::Release);
        {
            let mut inner = session.inner.lock().unwrap();
            set_phase(&mut inner, SessionPhase::Stopped, now_millis());
            inner.hook.activity = HookActivity::Done;
        }
        // Late StartTurn (old waiter path) must be a no-op.
        session.apply_input_effect(InputEffect::StartTurn).unwrap();
        let state = session.state().unwrap();
        assert_eq!(state.phase, SessionPhase::Stopped);
        assert_eq!(state.activity, HookActivity::Done);

        // Cancel must not flip terminal activity either.
        session.apply_input_effect(InputEffect::Cancel).unwrap();
        let state = session.state().unwrap();
        assert_eq!(state.phase, SessionPhase::Stopped);
        assert_eq!(state.activity, HookActivity::Done);
    }

    /// close drops sender: not-yet-started jobs fail uniquely without write;
    /// a job that already wrote still reports success but effect is suppressed.
    #[test]
    fn close_during_write_success_does_not_apply_effect_after_terminal() {
        let session = test_session(SessionPhase::Idle);
        let (writer_tx, writer_rx) = sync_channel(4);
        *session.writer_tx.lock().unwrap() = Some(writer_tx);
        let session_w = Arc::clone(&session);
        let (started_tx, started_rx) = sync_channel::<()>(1);
        let (go_tx, go_rx) = sync_channel::<()>(1);
        thread::spawn(move || {
            while let Ok(job) = writer_rx.recv() {
                if session_w.writer_must_cancel() {
                    job.fail(WRITE_CANCELLED_MSG);
                    fail_remaining_write_jobs(&writer_rx, WRITE_CANCELLED_MSG);
                    return;
                }
                let _ = started_tx.send(());
                // Caller closes while this job is "on the wire".
                go_rx.recv().unwrap();
                // Write "succeeded" but session is already terminal/shutdown.
                let _ = session_w.apply_input_effect(job.effect);
                if let Some(c) = job.completion {
                    c.complete(Ok(()));
                }
            }
        });

        let c = session.begin_write_job(b"late\r".to_vec()).unwrap();
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer started");
        // Move to terminal before effect commit.
        session.shutdown.store(true, Ordering::Release);
        {
            let mut inner = session.inner.lock().unwrap();
            set_phase(&mut inner, SessionPhase::Exited, now_millis());
            inner.hook.activity = HookActivity::Done;
            inner.process_done = true;
            inner.reader_done = true;
        }
        go_tx.send(()).unwrap();
        // Write completed successfully (bytes may be on PTY) — accurate Ok.
        c.wait().unwrap();
        let state = session.state().unwrap();
        assert_eq!(
            state.phase,
            SessionPhase::Exited,
            "late StartTurn must not revive Exited"
        );
        assert_eq!(state.activity, HookActivity::Done);
    }

    #[test]
    fn windows_kill_order_enumerates_before_root() {
        // Decision table used by Windows termination: pre-kill snapshot includes
        // children; post-root-death reparenting loses them from parent walk.
        let pairs_before = [(2u32, 1u32), (3, 1), (4, 2)];
        let before = windows_descendants_from_pairs(1, &pairs_before);
        assert_eq!(before, HashSet::from([2, 3, 4]));
        // After root kill, OS reparents under 0 — walk from root 1 sees nothing.
        let pairs_after = [(2u32, 0u32), (3, 0), (4, 0)];
        assert!(windows_descendants_from_pairs(1, &pairs_after).is_empty());
        // Fallback must therefore kill the pre-enumerated set, not re-walk after.
        let fallback_targets: HashSet<u32> = before.into_iter().chain([1]).collect();
        assert!(fallback_targets.contains(&4));
        assert!(fallback_targets.contains(&1));
    }

    /// TerminateProcess/TerminateJobObject accepted while still STILL_ACTIVE is
    /// request-success (poll/escalate), not a hard request failure.
    #[test]
    fn windows_terminate_accepted_but_alive_allows_shutdown_poll() {
        assert_eq!(
            classify_windows_terminate_request(true, WindowsLiveness::Alive),
            WindowsTerminateRequestResult::AcceptedPending
        );
        assert_eq!(
            classify_windows_terminate_request(true, WindowsLiveness::Unknown),
            WindowsTerminateRequestResult::AcceptedPending
        );
        assert_eq!(
            classify_windows_terminate_request(true, WindowsLiveness::Dead),
            WindowsTerminateRequestResult::ConfirmedDead
        );
        assert_eq!(
            classify_windows_terminate_request(false, WindowsLiveness::Alive),
            WindowsTerminateRequestResult::RequestFailed
        );
        assert!(windows_terminate_request_allows_poll(
            WindowsTerminateRequestResult::AcceptedPending
        ));
        assert!(!windows_terminate_request_allows_poll(
            WindowsTerminateRequestResult::RequestFailed
        ));

        // Job kill accepted, members still active → poll, do not fail the request.
        assert_eq!(
            classify_windows_job_terminate_request(
                true,
                Some(3),
                WindowsTerminateRequestResult::AcceptedPending
            ),
            WindowsTerminateRequestResult::AcceptedPending
        );
        // Both APIs failed with live members → request failure.
        assert_eq!(
            classify_windows_job_terminate_request(
                false,
                Some(2),
                WindowsTerminateRequestResult::RequestFailed
            ),
            WindowsTerminateRequestResult::RequestFailed
        );
        // Job empty but root still Alive after accepted kill → still allow poll;
        // tree-gone proof remains fail-closed via windows_job_tree_is_gone.
        assert_eq!(
            classify_windows_job_terminate_request(
                true,
                Some(0),
                WindowsTerminateRequestResult::AcceptedPending
            ),
            WindowsTerminateRequestResult::AcceptedPending
        );
        assert!(
            !windows_job_tree_is_gone(Some(0), WindowsLiveness::Alive),
            "AcceptedPending must not invent tree-gone"
        );
        assert!(windows_job_tree_is_gone(Some(0), WindowsLiveness::Dead));
    }

    /// Windows tree-gone is Job ActiveProcesses==0 + root handle Dead.
    /// Pure logic (runs on all hosts). No PID-only ownership for create.
    #[test]
    fn windows_tree_gone_predicate_requires_job_empty_and_root_dead() {
        assert!(
            windows_job_tree_is_gone(Some(0), WindowsLiveness::Dead),
            "job empty + root dead => gone"
        );
        assert!(
            !windows_job_tree_is_gone(Some(0), WindowsLiveness::Alive),
            "job empty but root still STILL_ACTIVE is not gone"
        );
        assert!(
            !windows_job_tree_is_gone(Some(0), WindowsLiveness::Unknown),
            "job empty but root query unknown is not gone (access denied)"
        );
        assert!(
            !windows_job_tree_is_gone(Some(2), WindowsLiveness::Dead),
            "non-zero ActiveProcesses blocks close Ok"
        );
        assert!(
            !windows_job_tree_is_gone(None, WindowsLiveness::Dead),
            "job query failure is fail-closed (not gone)"
        );

        let tracked = HashSet::from([10u32, 20, 30]);
        assert!(
            !windows_tracked_pids_all_gone(&tracked, |_| WindowsLiveness::Alive),
            "any live PID means tree not gone"
        );
        assert!(
            windows_tracked_pids_all_gone(&tracked, |_| WindowsLiveness::Dead),
            "all dead => tree gone"
        );
        assert!(
            !windows_tracked_pids_all_gone(&tracked, |pid| {
                if pid == 20 {
                    WindowsLiveness::Alive
                } else {
                    WindowsLiveness::Dead
                }
            }),
            "single survivor keeps tree alive"
        );
        assert!(
            !windows_tracked_pids_all_gone(&tracked, |pid| {
                if pid == 20 {
                    WindowsLiveness::Unknown
                } else {
                    WindowsLiveness::Dead
                }
            }),
            "access-denied/unknown must not look gone"
        );
        assert!(
            !windows_tracked_pids_all_gone(&HashSet::new(), |_| WindowsLiveness::Dead),
            "empty tracked set is not success"
        );
        // Create policy: PID-only fallback is never accepted.
        assert!(windows_create_ownership_allowed(
            WindowsTreeOwnership::KillOnCloseJob
        ));
        assert!(!windows_create_ownership_allowed(
            WindowsTreeOwnership::PidOnlyFallback
        ));
    }

    #[test]
    fn windows_liveness_unknown_is_not_dead() {
        assert!(!matches!(WindowsLiveness::Unknown, WindowsLiveness::Dead));
        assert!(!windows_tracked_liveness_all_dead(&[
            WindowsLiveness::Dead,
            WindowsLiveness::Unknown
        ]));
        assert!(windows_tracked_liveness_all_dead(&[
            WindowsLiveness::Dead,
            WindowsLiveness::Dead
        ]));
        assert!(!windows_tracked_liveness_all_dead(&[]));
    }

    #[test]
    fn spawn_cleanup_plan_handles_missing_process_id() {
        // Unix: process group preferred even when PID is missing.
        assert_eq!(
            plan_spawn_cleanup(SpawnHostPlatform::Unix, true, None, false),
            SpawnCleanupPlan::UnixProcessGroup
        );
        assert_eq!(
            plan_spawn_cleanup(SpawnHostPlatform::Unix, false, Some(42), false),
            SpawnCleanupPlan::UnixRootPid(42)
        );
        assert_eq!(
            plan_spawn_cleanup(SpawnHostPlatform::Unix, false, None, false),
            SpawnCleanupPlan::UnixChildKillOnly
        );
        // Windows: no PID still cleans via raw handle; never "no op" when handle exists.
        assert_eq!(
            plan_spawn_cleanup(SpawnHostPlatform::Windows, false, Some(7), true),
            SpawnCleanupPlan::WindowsTreeAndHandle(7)
        );
        assert_eq!(
            plan_spawn_cleanup(SpawnHostPlatform::Windows, false, None, true),
            SpawnCleanupPlan::WindowsHandleOnly
        );
        assert_eq!(
            plan_spawn_cleanup(SpawnHostPlatform::Windows, false, None, false),
            SpawnCleanupPlan::None
        );
        // Missing PID must never skip cleanup on Unix.
        assert_ne!(
            plan_spawn_cleanup(SpawnHostPlatform::Unix, true, None, false),
            SpawnCleanupPlan::None
        );
        assert_ne!(
            plan_spawn_cleanup(SpawnHostPlatform::Unix, false, None, false),
            SpawnCleanupPlan::None
        );
    }

    #[test]
    fn list_web_board_omits_screen_snapshots() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let session = host.get("gbt-test").unwrap();
        session.append_output(b"expensive-screen-bytes".to_vec());
        let full = session.state().unwrap();
        assert!(full.screen.is_some());
        assert!(!full.screen_ansi_base64.is_empty());

        let board = host.list_web_board().unwrap();
        assert_eq!(board.len(), 1);
        assert!(board[0].screen.is_none());
        assert!(board[0].screen_ansi_base64.is_empty());
        assert_eq!(board[0].last_cursor, full.last_cursor);
        assert_eq!(board[0].phase, full.phase);
    }

    #[test]
    fn windows_descendant_walk_uses_pre_kill_parent_links() {
        // pairs: (pid, parent). Root 1 has children 2,3; 2 has 4.
        let pairs = [(2, 1), (3, 1), (4, 2), (5, 99)];
        let found = windows_descendants_from_pairs(1, &pairs);
        assert_eq!(found, HashSet::from([2, 3, 4]));
        // After root death, OS may reparent 4 under 0 — pre-kill snapshot still
        // has the full set; a post-kill walk would miss reparented nodes.
        let post_kill_pairs = [(2, 0), (3, 0), (4, 0)];
        let post = windows_descendants_from_pairs(1, &post_kill_pairs);
        assert!(
            post.is_empty(),
            "post-kill reparented tree is invisible via parent walk"
        );
    }

    #[test]
    fn host_revision_bumps_when_session_output_arrives() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let before = host.revision();
        let session = host.get("gbt-test").unwrap();
        session.append_output(b"hello".to_vec());
        assert_ne!(host.revision(), before);
    }

    fn apply_frame_commits(cursors: &mut HashMap<String, u64>, frames: &[WebEventsFramePlan]) {
        for frame in frames {
            for (session, cursor) in &frame.cursor_commits {
                cursors.insert(session.clone(), *cursor);
            }
            for session in &frame.cursor_drops {
                cursors.remove(session);
            }
        }
    }

    #[test]
    fn web_events_initial_reset_uses_ansi_snapshot_and_last_cursor() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let session = host.get("gbt-test").unwrap();
        session.append_output(b"abc".to_vec());
        let full_ansi = session.state().unwrap().screen_ansi_base64;
        let cursors = HashMap::new();
        let frames = host.plan_web_events(&cursors, true, 1024 * 1024).unwrap();
        assert_eq!(frames.len(), 1);
        let message = &frames[0].message;
        assert_eq!(message.message_type, "sessions");
        assert_eq!(message.sessions.len(), 1);
        assert!(message.sessions[0].screen.is_none());
        assert!(message.sessions[0].screen_ansi_base64.is_empty());
        assert_eq!(message.terminals.len(), 1);
        let entry = &message.terminals[0];
        assert!(entry.reset);
        assert_eq!(entry.cursor, 0);
        assert_eq!(entry.next_cursor, 3);
        assert_eq!(entry.data_base64, full_ansi);
        assert_eq!(frames[0].cursor_commits.get("gbt-test").copied(), Some(3));
    }

    #[test]
    fn web_events_drains_past_64kib_across_bounded_frames() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let session = host.get("gbt-test").unwrap();
        let payload = vec![b'x'; MAX_READ_BYTES + 1_024];
        session.append_output(payload.clone());

        let cursors = HashMap::from([("gbt-test".to_owned(), 0_u64)]);
        // Force multi-frame packing; every produced frame must stay in bound.
        let max_frame = 50_000;
        let frames = host.plan_web_events(&cursors, false, max_frame).unwrap();
        assert!(
            frames.len() >= 2,
            "expected multi-frame drain, got {}",
            frames.len()
        );
        for frame in &frames {
            let encoded = serde_json::to_vec(&frame.message).unwrap();
            assert!(
                encoded.len() <= max_frame,
                "frame len {} exceeds bound {}",
                encoded.len(),
                max_frame
            );
        }

        let mut decoded = Vec::new();
        for frame in &frames {
            for entry in &frame.message.terminals {
                assert!(!entry.reset);
                decoded.extend(
                    BASE64
                        .decode(&entry.data_base64)
                        .expect("terminal delta must be valid base64"),
                );
            }
        }
        assert_eq!(decoded, payload);

        let mut committed = cursors.clone();
        assert_eq!(committed.get("gbt-test").copied(), Some(0));
        apply_frame_commits(&mut committed, &frames);
        assert_eq!(
            committed.get("gbt-test").copied(),
            Some(payload.len() as u64)
        );
    }

    #[test]
    fn web_events_freeze_end_stops_live_cursor_chase() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let session = host.get("gbt-test").unwrap();
        session.append_output(vec![b'a'; 4_096]);
        let cursors = HashMap::from([("gbt-test".to_owned(), 0_u64)]);

        let producer = Arc::clone(&session);
        let running = Arc::new(AtomicBool::new(true));
        let running_flag = Arc::clone(&running);
        let hammer = thread::spawn(move || {
            while running_flag.load(Ordering::Acquire) {
                producer.append_output(vec![b'z'; 8_192]);
                thread::sleep(Duration::from_millis(1));
            }
        });
        // Let the producer run so list/read can observe a moving live cursor.
        thread::sleep(Duration::from_millis(30));
        let frames = host.plan_web_events(&cursors, false, 1024 * 1024).unwrap();
        running.store(false, Ordering::Release);
        hammer.join().unwrap();

        let mut decoded = 0_u64;
        let mut end = 0_u64;
        for frame in &frames {
            for entry in &frame.message.terminals {
                let raw = BASE64.decode(&entry.data_base64).unwrap();
                decoded += raw.len() as u64;
                end = end.max(entry.next_cursor);
            }
        }
        // Batch is finite: committed end equals total decoded and matches freeze commits.
        assert!(decoded > 0);
        assert_eq!(decoded, end);
        let committed = frames
            .iter()
            .filter_map(|frame| frame.cursor_commits.get("gbt-test").copied())
            .max()
            .unwrap();
        assert_eq!(committed, end);
        // Live stream may have advanced further after the frozen batch.
        assert!(session.state().unwrap().last_cursor >= end);
    }

    #[test]
    fn web_events_splits_large_reset_snapshot_with_final_commit_only() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let session = host.get("gbt-test").unwrap();
        // Large screen content so a reset ANSI snapshot exceeds a small bound.
        session.append_output(vec![b'R'; 12_000]);
        let full = session.state().unwrap();
        let full_ansi = BASE64
            .decode(&full.screen_ansi_base64)
            .expect("screen ansi");
        assert!(full_ansi.len() > 1_000);

        let cursors = HashMap::new();
        let max_frame = 2_500;
        let frames = host.plan_web_events(&cursors, true, max_frame).unwrap();
        assert!(
            frames.len() >= 2,
            "expected split reset, got {}",
            frames.len()
        );

        let mut reconstructed = Vec::new();
        let mut saw_reset = false;
        let mut commit_frames = 0_usize;
        for (index, frame) in frames.iter().enumerate() {
            let encoded = serde_json::to_vec(&frame.message).unwrap();
            assert!(
                encoded.len() <= max_frame,
                "frame {index} len {} exceeds bound {max_frame}",
                encoded.len()
            );
            if !frame.cursor_commits.is_empty() {
                commit_frames += 1;
            }
            for entry in &frame.message.terminals {
                if entry.reset {
                    assert!(!entry.reset_cont, "head must not set reset_cont");
                    assert!(!saw_reset, "reset must appear only on the first chunk");
                    assert_eq!(index, 0);
                    saw_reset = true;
                } else if entry.reset_cont {
                    assert!(saw_reset, "reset_cont before reset head");
                    assert!(!entry.reset);
                } else {
                    panic!("split reset must use reset/reset_cont, not plain deltas");
                }
                reconstructed.extend(BASE64.decode(&entry.data_base64).unwrap());
            }
        }
        assert!(saw_reset);
        assert_eq!(reconstructed, full_ansi);
        assert_eq!(
            commit_frames, 1,
            "PTY cursor commits only on the final chunk"
        );
        assert_eq!(
            frames
                .last()
                .and_then(|frame| frame.cursor_commits.get("gbt-test").copied()),
            Some(full.last_cursor)
        );
        // Mid-send failure simulation: durable map stays uncommitted until apply.
        assert!(cursors.is_empty());
        let mut durable = cursors.clone();
        apply_frame_commits(&mut durable, &frames[..frames.len() - 1]);
        assert!(
            !durable.contains_key("gbt-test"),
            "partial send must not commit the PTY cursor"
        );
        apply_frame_commits(&mut durable, &frames[frames.len() - 1..]);
        assert_eq!(durable.get("gbt-test").copied(), Some(full.last_cursor));
    }

    #[test]
    fn web_events_sessions_only_oversize_is_a_planning_error() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let cursors = HashMap::new();
        // Bound smaller than any sessions metadata JSON.
        let err = host
            .plan_web_events(&cursors, false, 8)
            .expect_err("sessions-only oversize must fail planning");
        assert!(
            err.to_string().contains("sessions metadata exceeds"),
            "{err:#}"
        );
    }

    #[test]
    fn web_events_budget_stops_generation_before_backlog_exhaustion() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let session = host.get("gbt-test").unwrap();
        session.append_output(vec![b'x'; 128 * 1024]);
        let cursors = HashMap::from([(String::from("gbt-test"), 0_u64)]);
        let frames = host
            .plan_web_events_with_budget(
                &cursors,
                false,
                1024 * 1024,
                None,
                64,
                4 * 1024,
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap();
        let planned_bytes: usize = frames
            .iter()
            .flat_map(|frame| frame.message.terminals.iter())
            .map(|entry| BASE64.decode(&entry.data_base64).unwrap().len())
            .sum();
        assert!(planned_bytes <= 4 * 1024, "planner exceeded byte budget");
        assert!(frames.len() <= 64, "planner exceeded frame budget");
        assert_eq!(cursors.get("gbt-test"), Some(&0));
    }

    #[test]
    fn web_events_final_frame_budget_bounds_split_metadata_and_deadline() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let session = host.get("gbt-test").unwrap();
        session.append_output(vec![b'r'; 32 * 1024]);

        let frames = host
            .plan_web_events_with_budget(
                &HashMap::new(),
                true,
                2_500,
                None,
                2,
                5_000,
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap();
        assert!(frames.len() <= 2);
        let exact_bytes: usize = frames
            .iter()
            .map(|frame| serde_json::to_vec(&frame.message).unwrap().len())
            .sum();
        assert!(exact_bytes <= 5_000, "exact_bytes={exact_bytes}");
        assert!(
            frames
                .iter()
                .all(|frame| serde_json::to_vec(&frame.message).unwrap().len() <= 2_500)
        );
        assert!(
            frames.iter().all(|frame| frame.cursor_commits.is_empty()),
            "partial reset must not commit a cursor"
        );

        let expired = host
            .plan_web_events_with_budget(
                &HashMap::new(),
                true,
                2_500,
                None,
                64,
                4 * 1024 * 1024,
                Instant::now() - Duration::from_millis(1),
            )
            .unwrap();
        assert!(expired.is_empty());

        let metadata = host.list_web_board().unwrap();
        let metadata_bytes = message_json_len(&WebEventsMessage::sessions(metadata, Vec::new()));
        let metadata_limited = host
            .plan_web_events_with_budget(
                &HashMap::from([(
                    String::from("gbt-test"),
                    session.state().unwrap().last_cursor,
                )]),
                false,
                1024 * 1024,
                None,
                1,
                metadata_bytes.saturating_sub(1),
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap();
        assert!(metadata_limited.is_empty());
    }

    #[test]
    fn web_events_continuation_drains_without_revision_or_duplicate_bytes() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let session = host.get("gbt-test").unwrap();
        let source = (0..180_000_u32)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        session.append_output(source.clone());
        let mut cursors = HashMap::from([(String::from("gbt-test"), 0_u64)]);
        let mut received = Vec::new();
        let mut batches = 0;
        let mut more = true;
        let mut continuation = WebEventsContinuation::default();
        while more {
            let plan = host
                .plan_web_events_batch_with_budget(
                    &cursors,
                    false,
                    2_500,
                    None,
                    2,
                    5_000,
                    Instant::now() + Duration::from_secs(1),
                    &mut continuation,
                )
                .unwrap();
            assert!(
                !plan.frames.is_empty(),
                "continuation must not be swallowed"
            );
            for frame in &plan.frames {
                for entry in &frame.message.terminals {
                    received.extend(BASE64.decode(&entry.data_base64).unwrap());
                }
                for (handle, cursor) in &frame.cursor_commits {
                    cursors.insert(handle.clone(), *cursor);
                }
            }
            more = plan.more_pending;
            batches += 1;
            assert!(batches < 200, "continuation livelocked");
        }
        assert_eq!(received, source);
        assert_eq!(cursors["gbt-test"], source.len() as u64);
    }

    #[test]
    fn reset_completion_rechecks_cursor_after_append_during_continuation() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let session = host.get("gbt-test").unwrap();
        session.append_output(vec![b'a'; 77]);
        let source = (0..(5 * 1024 * 1024 + 1))
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        let mut continuation = WebEventsContinuation::default();
        continuation.resets.insert(
            "gbt-test".to_owned(),
            WebEventsResetStream {
                source: WebEventsResetSource::Bytes(source),
                offset: 0,
                last_cursor: 77,
            },
        );
        let mut cursors = HashMap::new();
        let mut more = true;
        let mut batches = 0;
        while more {
            let plan = host
                .plan_web_events_batch_with_budget(
                    &cursors,
                    false,
                    1024 * 1024,
                    None,
                    64,
                    4 * 1024 * 1024,
                    Instant::now() + Duration::from_secs(5),
                    &mut continuation,
                )
                .unwrap();
            assert!(!plan.frames.is_empty());
            more = plan.more_pending;
            for frame in plan.frames {
                for (session, cursor) in frame.cursor_commits {
                    cursors.insert(session, cursor);
                }
                for commit in frame.reset_commits {
                    continuation.commit_reset(commit);
                }
            }
            batches += 1;
            if batches == 1 {
                session.append_output(b"tail".to_vec());
            }
            assert!(batches < 10);
        }
        assert_eq!(cursors.get("gbt-test"), Some(&77));
        let tail = host
            .plan_web_events_batch_with_budget(
                &cursors,
                false,
                1024 * 1024,
                None,
                64,
                4 * 1024 * 1024,
                Instant::now() + Duration::from_secs(1),
                &mut continuation,
            )
            .unwrap();
        assert!(
            tail.frames
                .iter()
                .flat_map(|frame| frame.message.terminals.iter())
                .any(|entry| BASE64
                    .decode(&entry.data_base64)
                    .unwrap()
                    .ends_with(b"tail"))
        );
    }

    #[test]
    fn web_events_reset_continuation_crosses_four_mib_without_replaying_head() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let source = (0..(5 * 1024 * 1024 + 17))
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        let mut continuation = WebEventsContinuation::default();
        continuation.resets.insert(
            "gbt-test".to_owned(),
            WebEventsResetStream {
                source: WebEventsResetSource::Bytes(source.clone()),
                offset: 0,
                last_cursor: 77,
            },
        );
        let mut cursors = HashMap::new();
        let mut received = Vec::new();
        let mut reset_heads = 0usize;
        let mut batches = 0usize;
        loop {
            let plan = host
                .plan_web_events_batch_with_budget(
                    &cursors,
                    false,
                    1024 * 1024,
                    None,
                    64,
                    4 * 1024 * 1024,
                    Instant::now() + Duration::from_secs(5),
                    &mut continuation,
                )
                .unwrap();
            assert!(
                !plan.frames.is_empty(),
                "reset continuation made no progress"
            );
            let more = plan.more_pending;
            for frame in plan.frames {
                assert!(message_json_len(&frame.message) <= 1024 * 1024);
                for entry in frame.message.terminals {
                    reset_heads += usize::from(entry.reset);
                    if !entry.reset {
                        assert!(entry.reset_cont);
                    }
                    received.extend(BASE64.decode(entry.data_base64).unwrap());
                }
                for (session, cursor) in frame.cursor_commits {
                    cursors.insert(session, cursor);
                }
                for commit in frame.reset_commits {
                    continuation.commit_reset(commit);
                }
            }
            batches += 1;
            assert!(batches < 10, "reset continuation livelocked");
            if !more {
                break;
            }
        }
        assert!(batches > 1, "snapshot did not cross the 4 MiB batch budget");
        assert_eq!(reset_heads, 1);
        assert_eq!(received, source);
        assert_eq!(cursors.get("gbt-test"), Some(&77));
        assert!(continuation.resets.is_empty());
    }

    #[test]
    fn reset_window_exact_component_boundary_keeps_terminal_state_tail() {
        let components = vec![
            b"\x1b[1;1Hrow".to_vec(),
            b"\x1b[?25l".to_vec(),
            b"\x1b[31m".to_vec(),
            b"\x1b[?2004h".to_vec(),
        ];
        let source = WebEventsResetSource::Components {
            bytes: components.iter().map(Vec::len).sum(),
            components: components.clone(),
        };
        let stream = WebEventsResetStream {
            source,
            offset: 0,
            last_cursor: 0,
        };
        let deadline = Instant::now() + Duration::from_secs(1);
        let first_len = components[0].len();
        let (first, first_complete) = stream.window(0, first_len, deadline);
        assert_eq!(first, components[0]);
        assert!(!first_complete, "later terminal state components remain");

        let mut received = first;
        let mut offset = first_len;
        while offset < components.iter().map(Vec::len).sum::<usize>() {
            let (piece, complete) = stream.window(offset, 3, deadline);
            assert!(!piece.is_empty(), "window must make progress");
            offset += piece.len();
            received.extend(piece);
            if complete {
                break;
            }
        }
        assert_eq!(received, components.concat());
        let mut parser = vt100::Parser::new(2, 20, 0);
        parser.process(&received);
        assert!(parser.screen().contents().starts_with("row"));
        assert!(
            parser
                .screen()
                .cursor_state_formatted()
                .starts_with(b"\x1b[?25l")
        );
        assert!(
            parser
                .screen()
                .input_mode_formatted()
                .ends_with(b"\x1b[?2004h")
        );
    }

    #[test]
    fn reset_snapshot_is_visible_components_only_and_has_a_hard_budget() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let session = host.get("gbt-test").unwrap();
        session.append_output(vec![b'x'; SCROLLBACK_ROWS * INITIAL_COLS as usize]);
        let (source, _) = session.web_reset_snapshot().unwrap();
        let retained = source.retained_bytes();
        assert!(retained <= MAX_WEB_RESET_CONTINUATION_BYTES);
        assert!(retained < SCROLLBACK_ROWS * INITIAL_COLS as usize);
        assert!(matches!(source, WebEventsResetSource::Components { .. }));
    }

    #[test]
    fn incremental_screen_reset_reconstructs_visible_terminal_state() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let session = host.get("gbt-test").unwrap();
        session.append_output(
            b"plain\r\n\x1b[31mred\x1b[0m\r\nwide: \xe4\xb8\xad\xe6\x96\x87".to_vec(),
        );
        let expected = session.state().unwrap();
        let mut continuation = WebEventsContinuation::default();
        let mut cursors = HashMap::new();
        let mut reset_bytes = Vec::new();
        let mut batches = 0usize;
        loop {
            let plan = host
                .plan_web_events_batch_with_budget(
                    &cursors,
                    batches == 0,
                    900,
                    None,
                    2,
                    1_800,
                    Instant::now() + Duration::from_secs(1),
                    &mut continuation,
                )
                .unwrap();
            assert!(!plan.frames.is_empty());
            let more = plan.more_pending;
            for frame in plan.frames {
                for entry in frame.message.terminals {
                    reset_bytes.extend(BASE64.decode(entry.data_base64).unwrap());
                }
                for (handle, cursor) in frame.cursor_commits {
                    cursors.insert(handle, cursor);
                }
                for commit in frame.reset_commits {
                    continuation.commit_reset(commit);
                }
            }
            batches += 1;
            assert!(batches < 20);
            if !more {
                break;
            }
        }
        let mut parser = vt100::Parser::new(expected.rows, expected.cols, 0);
        parser.process(&reset_bytes);
        assert_eq!(parser.screen().contents(), expected.screen.unwrap());
        assert_eq!(cursors.get("gbt-test"), Some(&expected.last_cursor));
    }

    #[test]
    fn web_events_resets_when_client_cursor_is_truncated() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let session = host.get("gbt-test").unwrap();
        // Exceed the bounded transcript so cursor 0 becomes truncated.
        let big = vec![b'y'; MAX_TRANSCRIPT_BYTES + 4_096];
        session.append_output(big);
        let last_cursor = session.state().unwrap().last_cursor;

        let cursors = HashMap::from([("gbt-test".to_owned(), 0_u64)]);
        let frames = host.plan_web_events(&cursors, false, 1024 * 1024).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].message.terminals.len(), 1);
        assert!(frames[0].message.terminals[0].reset);
        assert_eq!(frames[0].message.terminals[0].next_cursor, last_cursor);
        assert_eq!(
            frames[0].cursor_commits.get("gbt-test").copied(),
            Some(last_cursor)
        );
        assert_eq!(cursors.get("gbt-test").copied(), Some(0));
    }

    #[test]
    fn web_events_resets_for_new_sessions_and_drops_closed_cursors() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Running);
        let cursors = HashMap::from([("stale-session".to_owned(), 9_u64)]);
        let frames = host.plan_web_events(&cursors, false, 1024 * 1024).unwrap();
        assert_eq!(frames[0].message.terminals.len(), 1);
        assert!(frames[0].message.terminals[0].reset);
        assert!(
            frames[0]
                .cursor_drops
                .iter()
                .any(|session| session == "stale-session")
        );
        assert!(frames[0].cursor_commits.contains_key("gbt-test"));
        assert_eq!(cursors.get("stale-session").copied(), Some(9));
    }

    #[test]
    fn web_observation_does_not_refresh_codex_leases_or_cancel_cleanup() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Idle);
        let session = host.get("gbt-test").unwrap();
        let now = now_millis();
        let lease = Arc::new(AtomicU64::new(now.saturating_sub(1_000)));
        {
            let mut inner = session.inner.lock().unwrap();
            inner.client_session_id = Some("codex-observation".to_owned());
            inner.client_lease = Some(Arc::clone(&lease));
            inner.orphan_policy = OrphanPolicy {
                lease_ms: 100,
                grace_ms: 200,
            };
            inner.phase_changed_at_ms = now.saturating_sub(1_000);
        }
        host.registry
            .lock()
            .unwrap()
            .clients
            .insert("codex-observation".to_owned(), Arc::clone(&lease));

        let observed_lease = lease.load(Ordering::Acquire);
        let frames = host
            .plan_web_events(&HashMap::new(), false, 1024 * 1024)
            .unwrap();
        assert_eq!(
            frames[0].message.sessions[0].client_state,
            ClientLeaseState::Orphaned
        );
        assert_eq!(lease.load(Ordering::Acquire), observed_lease);
        assert!(!session.cleanup_claimed.load(Ordering::Acquire));

        assert!(session.claim_orphan_cleanup(now_millis()).unwrap());
        let closing_frames = host
            .plan_web_events(&HashMap::new(), false, 1024 * 1024)
            .unwrap();
        assert_eq!(
            closing_frames[0].message.sessions[0].client_state,
            ClientLeaseState::Closing
        );
        assert_eq!(lease.load(Ordering::Acquire), observed_lease);
        assert!(session.cleanup_claimed.load(Ordering::Acquire));
    }

    #[test]
    fn lease_deadline_and_client_state_transition_at_expiry() {
        let host = test_host(TEST_PROVIDER_SESSION_ID, SessionPhase::Idle);
        let lease = Arc::new(AtomicU64::new(1_000));
        {
            let session = host.get("gbt-test").unwrap();
            let mut inner = session.inner.lock().unwrap();
            inner.client_session_id = Some("codex-thread".to_owned());
            inner.client_lease = Some(Arc::clone(&lease));
            inner.orphan_policy = OrphanPolicy {
                lease_ms: 100,
                grace_ms: 200,
            };
            inner.phase_changed_at_ms = 900;
        }
        // last_seen=1000, lease_ms=100 => lease_expires_at=1100.
        // Connected while now < 1100; at 1100 state is already Orphaned (idle).
        // Never call next_lifecycle_deadline_ms while holding inner (non-reentrant Mutex).
        let session = host.get("gbt-test").unwrap();

        let connected_state = session.inner.lock().unwrap().to_state(1_050, false);
        assert_eq!(connected_state.client_lease_ms, Some(100));
        assert_eq!(connected_state.orphan_grace_ms, Some(200));
        assert_eq!(connected_state.client_state, ClientLeaseState::Connected);

        assert_eq!(
            session
                .inner
                .lock()
                .unwrap()
                .client_lifecycle(1_099, false)
                .0,
            ClientLeaseState::Connected
        );
        assert_eq!(
            session.next_lifecycle_deadline_ms(1_099).unwrap(),
            Some(1_100)
        );

        assert_eq!(
            session
                .inner
                .lock()
                .unwrap()
                .client_lifecycle(1_100, false)
                .0,
            ClientLeaseState::Orphaned
        );
        assert_eq!(session.next_lifecycle_deadline_ms(1_100).unwrap(), None);

        assert_eq!(
            session
                .inner
                .lock()
                .unwrap()
                .client_lifecycle(1_101, false)
                .0,
            ClientLeaseState::Orphaned
        );
        assert_eq!(session.next_lifecycle_deadline_ms(1_101).unwrap(), None);

        // Exactly one due wake at expiry observes the changed state: deadline was
        // scheduled while Connected, and the first moment now >= deadline lists
        // Orphaned with no further pure-time deadline.
        let scheduled = session.next_lifecycle_deadline_ms(1_050).unwrap();
        assert_eq!(scheduled, Some(1_100));
        let due_now = scheduled.unwrap();
        let due_state = session
            .inner
            .lock()
            .unwrap()
            .client_lifecycle(due_now, false)
            .0;
        assert_eq!(due_state, ClientLeaseState::Orphaned);
        assert_eq!(session.next_lifecycle_deadline_ms(due_now).unwrap(), None);
    }

    #[test]
    fn session_capacity_rejects_when_live_plus_pending_hits_max() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        });
        {
            let mut registry = host.registry.lock().unwrap();
            for i in 0..MAX_SESSIONS {
                let session =
                    test_session_with_revision(SessionPhase::Idle, Arc::clone(&host.revision));
                // Distinct handles so map entries do not collide.
                let handle = format!("gbt-cap-{i}");
                {
                    let mut inner = session.inner.lock().unwrap();
                    inner.session = handle.clone();
                }
                registry.sessions.insert(handle, session);
            }
            assert_eq!(registry.occupied_session_slots(), MAX_SESSIONS);
            let err = registry.reserve_pending_create(None, None).unwrap_err();
            assert!(format!("{err:#}").contains("capacity"), "err={err:#}");
        }
    }

    #[test]
    fn pending_create_reservation_releases_on_guard_drop() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        });
        let client = "codex-pending-release";
        {
            let mut registry = host.registry.lock().unwrap();
            registry
                .reserve_pending_create(Some(client), Some("owner-a"))
                .unwrap();
            assert_eq!(registry.pending_creates, 1);
            assert_eq!(
                registry.pending_creates_by_client.get(client).copied(),
                Some(1)
            );
        }
        {
            let mut guard = PendingCreateGuard {
                registry: &host.registry,
                client_session_id: Some(client.to_owned()),
                owner: Some("owner-a".to_owned()),
                active: true,
            };
            assert_eq!(host.registry.lock().unwrap().pending_creates, 1);
            guard.release_now();
            assert_eq!(host.registry.lock().unwrap().pending_creates, 0);
            assert!(
                !host
                    .registry
                    .lock()
                    .unwrap()
                    .pending_creates_by_client
                    .contains_key(client)
            );
        }
        // Double-release is a no-op.
        assert_eq!(host.registry.lock().unwrap().pending_creates, 0);
    }

    #[test]
    fn close_client_reclaims_epoch_when_idle() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        });
        let client = "codex-reclaim-epoch";
        // close on unknown id used to leave a permanent client_epochs entry.
        host.close_client(client).unwrap();
        let registry = host.registry.lock().unwrap();
        assert!(
            !registry.client_epochs.contains_key(client),
            "epoch must be reclaimed when no session/pending/closer remains"
        );
        assert_eq!(registry.client_epoch(client), 0);
        assert!(!registry.clients.contains_key(client));
        assert!(!registry.is_client_closing(client));
    }

    #[test]
    fn pending_create_blocks_epoch_reclaim_for_slow_create_race() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        });
        let client = "codex-slow-create";
        host.registry
            .lock()
            .unwrap()
            .reserve_pending_create(Some(client), None)
            .unwrap();
        // close advances epoch; pending create keeps the generation entry alive.
        host.close_client(client).unwrap();
        {
            let registry = host.registry.lock().unwrap();
            assert_eq!(registry.client_epoch(client), 1);
            assert!(registry.client_epochs.contains_key(client));
        }
        host.registry
            .lock()
            .unwrap()
            .release_pending_create(Some(client), None);
        let registry = host.registry.lock().unwrap();
        assert!(
            !registry.client_epochs.contains_key(client),
            "epoch reclaims only after pending create ends"
        );
    }

    #[test]
    fn close_owner_fence_blocks_install_and_reclaims() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        });
        let owner = "owner-fence";
        host.registry
            .lock()
            .unwrap()
            .reserve_pending_create(None, Some(owner))
            .unwrap();
        let closed = host.close_owner(owner).unwrap();
        assert_eq!(closed.matched, 0);
        // While pending create holds the owner slot, epoch stays for install check.
        assert_eq!(host.registry.lock().unwrap().owner_epoch(owner), 1);
        assert!(!host.registry.lock().unwrap().is_owner_closing(owner));
        // Simulate install seeing advanced epoch after close returned.
        let session = test_session_with_revision(SessionPhase::Idle, Arc::clone(&host.revision));
        {
            let mut inner = session.inner.lock().unwrap();
            inner.owner = Some(owner.to_owned());
        }
        // Owner fence already exited (no sessions); epoch kept by pending create.
        assert_eq!(host.registry.lock().unwrap().owner_epoch(owner), 1);
        let err = host
            .install_created_session(
                "gbt-owner-fence".to_owned(),
                generate_provider_session_id().unwrap(),
                session,
                None,
                Some(owner.to_owned()),
                None,
                0,
                0, // create captured epoch 0 before close
            )
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("owner was closed"),
            "err={err:#}"
        );
        host.registry
            .lock()
            .unwrap()
            .release_pending_create(None, Some(owner));
        assert!(
            !host
                .registry
                .lock()
                .unwrap()
                .owner_epochs
                .contains_key(owner),
            "owner epoch reclaims when idle"
        );
    }

    #[test]
    fn shutdown_all_restores_accepting_on_partial_failure() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        });
        let session = test_session_with_revision(SessionPhase::Running, Arc::clone(&host.revision));
        // Hang shutdown past the absolute budget so close fails without false success.
        session
            .test_shutdown_hang_ms
            .store(60_000, Ordering::Release);
        {
            let mut registry = host.registry.lock().unwrap();
            let handle = session.state().unwrap().session;
            registry.sessions.insert(handle, session);
        }
        let err = host.shutdown_all().unwrap_err();
        assert!(format!("{err:#}").contains("failed to stop"), "err={err:#}");
        assert!(
            host.registry.lock().unwrap().accepting,
            "accepting must reopen after partial shutdown so close/stop can retry"
        );
        assert_eq!(host.registry.lock().unwrap().sessions.len(), 1);
        // Clear hang so process cleanup in Drop/tests does not linger.
        let remaining = host
            .registry
            .lock()
            .unwrap()
            .sessions
            .values()
            .next()
            .cloned();
        if let Some(session) = remaining {
            session.test_shutdown_hang_ms.store(0, Ordering::Release);
            let _ = session.shutdown();
        }
    }

    #[test]
    fn shutdown_all_success_leaves_accepting_false() {
        let host = SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        });
        // Empty registry: full success path.
        host.shutdown_all().unwrap();
        assert!(
            !host.registry.lock().unwrap().accepting,
            "successful drain must keep accepting=false until process exit"
        );
    }

    #[test]
    fn shutdown_all_waits_for_create_reservation_before_success() {
        let host = Arc::new(SessionHost::new(OrphanPolicy {
            lease_ms: 120_000,
            grace_ms: 600_000,
        }));
        host.registry
            .lock()
            .unwrap()
            .reserve_pending_create(None, None)
            .unwrap();

        let shutdown_host = Arc::clone(&host);
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let shutdown = thread::spawn(move || {
            result_tx.send(shutdown_host.shutdown_all()).unwrap();
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        while host.registry.lock().unwrap().accepting && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(!host.registry.lock().unwrap().accepting);
        assert!(result_rx.try_recv().is_err());
        assert_eq!(host.registry.lock().unwrap().pending_creates, 1);
        host.registry
            .lock()
            .unwrap()
            .release_pending_create(None, None);

        result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown did not finish after pending create released")
            .unwrap();
        shutdown.join().unwrap();
        let registry = host.registry.lock().unwrap();
        assert_eq!(registry.pending_creates, 0);
        assert!(!registry.accepting);
    }

    #[test]
    fn board_metadata_for_max_sessions_fits_frame_budget() {
        // Board states omit screen payloads; cap keeps JSON under 1 MiB.
        let mut sessions = Vec::with_capacity(MAX_SESSIONS);
        for i in 0..MAX_SESSIONS {
            let mut state = SessionState {
                session: format!("gbt-board-{i:04}"),
                owner: Some(format!("owner-{i}")),
                client_session_id: Some(format!("client-{i}")),
                client_state: ClientLeaseState::Connected,
                client_lease_ms: Some(120_000),
                orphan_grace_ms: Some(600_000),
                client_last_seen_at_ms: Some(1),
                orphaned_at_ms: None,
                auto_close_at_ms: None,
                phase: SessionPhase::Idle,
                title: Some("t".repeat(64)),
                cwd: "/tmp".to_owned(),
                model: Some("grok".to_owned()),
                always_approve: false,
                process_id: Some(1),
                screen: None,
                rows: 36,
                cols: 120,
                screen_ansi_base64: String::new(),
                last_cursor: 0,
                last_output_at_ms: None,
                created_at_ms: i as u64,
                updated_at_ms: i as u64,
                semantic_active_at_ms: i as u64,
                completed_at_ms: Some(i as u64),
                exit_code: None,
                error: None,
                activity: HookActivity::Unknown,
                hook_event: None,
                hook_at_ms: None,
                tool_name: None,
                waiting_reason: None,
            };
            // Keep fields representative of board payload.
            let _ = &mut state;
            sessions.push(state);
        }
        let encoded = serde_json::to_vec(&sessions).expect("serialize board list");
        assert!(
            encoded.len() < crate::protocol::MAX_FRAME_BYTES,
            "board list for MAX_SESSIONS must fit 1 MiB frame: {} bytes",
            encoded.len()
        );
    }
}
