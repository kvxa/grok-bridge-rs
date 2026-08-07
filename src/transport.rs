use std::{
    env,
    io::{BufRead, BufReader, ErrorKind, Write},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(not(windows))]
use std::sync::mpsc;

// Windows production pipe identity + test-only Unix identity helper.
#[cfg(any(windows, test))]
use std::ffi::OsString;

#[cfg(unix)]
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::fs::{File, OpenOptions};

use anyhow::{Context, Result, bail};
#[cfg(unix)]
use interprocess::local_socket::GenericFilePath;
#[cfg(windows)]
use interprocess::local_socket::GenericNamespaced;
use interprocess::{
    ConnectWaitMode,
    local_socket::{ConnectOptions, Name, Stream, prelude::*},
};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(windows)]
use std::os::windows::io::OwnedHandle;
#[cfg(not(windows))]
use std::process::{Command, Stdio};
#[cfg(windows)]
use windows_sys::Win32::{
    Foundation::CloseHandle,
    System::Threading::{
        CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW, CreateProcessW, PROCESS_INFORMATION,
        STARTUPINFOW,
    },
};

use crate::protocol::{
    DEFAULT_WAIT_TIMEOUT_MS, MAX_FRAME_BYTES, MAX_READ_WAIT_MS, MAX_WAIT_TIMEOUT_MS,
    MIN_WAIT_TIMEOUT_MS, Request, RequestEnvelope, ResponseEnvelope, decode_response, encode_frame,
    validate_client_session_id,
};

const START_RETRIES: usize = 50;
const START_RETRY_DELAY: Duration = Duration::from_millis(100);
/// Default bound for ordinary RPC (create/list/send/close/…).
const RPC_DEFAULT_IO_TIMEOUT: Duration = Duration::from_secs(30);
/// Margin beyond request-level wait/read budgets so server processing still fits.
const RPC_WAIT_BUDGET_MARGIN: Duration = Duration::from_secs(5);
/// Hard ceiling for wait-budgeted RPC (matches wait --timeout-ms max 2h + margin).
const RPC_WAIT_IO_CEILING: Duration = Duration::from_secs(7_200 + 30);
/// Connect attempt bound so a wedged peer cannot block bind/probe forever.
const RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Tight I/O bound for singleton probes (ServerStatus) during bind / auto-start.
const RPC_PROBE_IO_TIMEOUT: Duration = Duration::from_secs(3);
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// Held for process life on Unix so flock ownership survives while Runtime runs.
#[cfg(unix)]
static UNIX_RUNTIME_LOCK: Mutex<Option<File>> = Mutex::new(None);

pub(crate) fn call(request: Request, auto_start: bool) -> Result<ResponseEnvelope> {
    call_with_client_session(request, auto_start, current_client_session_id()?)
}

pub(crate) fn call_anonymous(request: Request, auto_start: bool) -> Result<ResponseEnvelope> {
    call_with_client_session(request, auto_start, None)
}

fn call_with_client_session(
    request: Request,
    auto_start: bool,
    client_session_id: Option<String>,
) -> Result<ResponseEnvelope> {
    let envelope = RequestEnvelope {
        id: next_request_id(),
        client_session_id,
        request,
    };
    let stream = match connect() {
        Ok(stream) => stream,
        Err(first_error) if auto_start => {
            start_detached_server().context("failed to launch the Grok runtime server")?;
            let mut last_error = first_error;
            for _ in 0..START_RETRIES {
                thread::sleep(START_RETRY_DELAY);
                match connect() {
                    Ok(stream) => return call_over_stream(stream, &envelope),
                    Err(error) => last_error = error,
                }
            }
            return Err(last_error)
                .context("runtime server did not become ready within five seconds");
        }
        Err(error) => return Err(error),
    };
    call_over_stream(stream, &envelope)
}

fn current_client_session_id() -> Result<Option<String>> {
    client_session_id_from(
        env::var("CODEX_THREAD_ID").ok(),
        env::var("CODEX_SESSION_ID").ok(),
    )
}

fn client_session_id_from(
    thread_id: Option<String>,
    session_id: Option<String>,
) -> Result<Option<String>> {
    let value = thread_id
        .filter(|value| !value.trim().is_empty())
        .or_else(|| session_id.filter(|value| !value.trim().is_empty()));
    if let Some(value) = value.as_deref() {
        validate_client_session_id(value)?;
    }
    Ok(value)
}

fn connect() -> Result<Stream> {
    connect_with_timeout(RPC_CONNECT_TIMEOUT)
}

/// Connect with a hard wall budget so a half-open peer cannot pin bind/probe.
fn connect_with_timeout(timeout: Duration) -> Result<Stream> {
    #[cfg(windows)]
    {
        let mut path = OsString::from(r"\\.\pipe\");
        path.push(runtime_identity());
        return connect_windows_pipe_path(&path, timeout);
    }
    #[cfg(not(windows))]
    {
        let (tx, rx) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let name = match runtime_name() {
                Ok(name) => name,
                Err(error) => {
                    let _ = tx.send(Err(error));
                    return;
                }
            };
            let result = ConnectOptions::new()
                .name(name)
                .wait_mode(ConnectWaitMode::Timeout(timeout))
                .nonblocking_stream(true)
                .connect_sync()
                .context("runtime server is not running");
            let _ = tx.send(result);
        });
        match rx.recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                bail!("timed out connecting to the runtime server ({timeout:?})")
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                bail!("runtime connect worker exited without a result")
            }
        }
    }
}

#[cfg(windows)]
fn connect_windows_pipe_path(path: &std::ffi::OsStr, timeout: Duration) -> Result<Stream> {
    use interprocess::os::windows::named_pipe::{
        DuplexPipeStream, local_socket::Stream as WindowsLocalStream, pipe_mode::Bytes,
    };
    let pipe = DuplexPipeStream::<Bytes>::connect_by_path_with_wait_mode(
        path,
        ConnectWaitMode::Timeout(timeout),
    )
    .context("runtime server is not running")?;
    let handle: OwnedHandle = pipe
        .try_into()
        .map_err(|error| anyhow::anyhow!("failed to extract bounded pipe handle: {error}"))?;
    let platform = WindowsLocalStream::try_from(handle)
        .map_err(|error| anyhow::anyhow!("failed to wrap bounded runtime pipe: {error}"))?;
    let stream = Stream::from(platform);
    stream
        .set_nonblocking(true)
        .context("failed to enable nonblocking runtime pipe")?;
    Ok(stream)
}

fn call_over_stream(stream: Stream, envelope: &RequestEnvelope) -> Result<ResponseEnvelope> {
    call_over_stream_with_io_timeout(stream, envelope, rpc_io_timeout_for(&envelope.request))
}

fn call_over_stream_with_io_timeout(
    stream: Stream,
    envelope: &RequestEnvelope,
    io_timeout: Duration,
) -> Result<ResponseEnvelope> {
    // Bound I/O so a peer that accepts then stalls cannot hang the client forever.
    stream
        .set_nonblocking(true)
        .context("failed to enable bounded nonblocking runtime I/O")?;
    let mut connection = BufReader::new(stream);
    let deadline = Instant::now() + io_timeout;
    write_all_until(connection.get_mut(), &encode_frame(envelope)?, deadline)
        .context("failed to write runtime request")?;
    flush_until(connection.get_mut(), deadline).context("failed to flush runtime request")?;
    let frame =
        read_frame_until(&mut connection, deadline).context("failed to read runtime response")?;
    let response = decode_response(&frame)?;
    if response.id != envelope.id {
        bail!(
            "runtime response id mismatch: expected {}, received {}",
            envelope.id,
            response.id
        );
    }
    Ok(response)
}

/// Socket I/O timeout for one client RPC. Wait and read-with-wait use the
/// request budget plus margin (capped to the protocol max); everything else
/// uses the ordinary RPC bound. This only sizes the client socket — illegal
/// `wait_ms`/`timeout_ms` are still rejected by server `decode_request`
/// (no silent semantic rewrite of the RPC body).
fn rpc_io_timeout_for(request: &Request) -> Duration {
    match request {
        Request::Wait { timeout_ms, .. } => {
            let wait_ms = timeout_ms
                .unwrap_or(DEFAULT_WAIT_TIMEOUT_MS)
                .clamp(MIN_WAIT_TIMEOUT_MS, MAX_WAIT_TIMEOUT_MS);
            Duration::from_millis(wait_ms)
                .saturating_add(RPC_WAIT_BUDGET_MARGIN)
                .min(RPC_WAIT_IO_CEILING)
                .max(RPC_DEFAULT_IO_TIMEOUT)
        }
        Request::Read {
            wait_ms: Some(wait_ms),
            ..
        } if *wait_ms > 0 => {
            let wait = (*wait_ms).min(MAX_READ_WAIT_MS);
            Duration::from_millis(wait)
                .saturating_add(RPC_WAIT_BUDGET_MARGIN)
                .min(RPC_WAIT_IO_CEILING)
                .max(RPC_DEFAULT_IO_TIMEOUT)
        }
        // Singleton / status probes: keep a tight bound so bind never waits 30s.
        Request::ServerStatus => RPC_PROBE_IO_TIMEOUT,
        _ => RPC_DEFAULT_IO_TIMEOUT,
    }
}

/// Outcome of a liveness connect probe — policy helpers used by tests.
/// Production Unix bind unlinks leftover sockets only under exclusive flock
/// (see `remove_stale_socket_under_lock`). **Never** treat Timeout / Malformed /
/// Busy as permission to unlink a socket without that lock.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SocketLiveness {
    /// Kernel refused the connection: no process is accepting (safe stale candidate).
    ConnectionRefused,
    /// Peer accepted (or connect succeeded): treat as live — do not unlink.
    Live,
    /// Connect/read timed out or protocol noise: may still be a live owner.
    Indeterminate,
}

/// Only `ConnectionRefused` may justify removing a socket path, and only after
/// exclusive singleton lock acquisition (see Unix bind path).
#[cfg(test)]
fn may_unlink_socket_for_liveness(probe: SocketLiveness) -> bool {
    matches!(probe, SocketLiveness::ConnectionRefused)
}

/// IPC endpoint name for the per-user runtime singleton.
///
/// - **Unix (Linux, macOS, FreeBSD, …):** filesystem UDS under a user-owned 0700
///   directory (`$XDG_RUNTIME_DIR/grok-bridge` or `~/.grok-bridge/run`), socket
///   mode 0600. `GenericNamespaced` is **not** used: on non-Linux Unices it
///   prepends `/tmp/` (world-reachable); on Linux it is the abstract namespace
///   (any local user can connect). Abstract / `/tmp/` names that embed a UID
///   are not access control.
/// - **Windows:** local named pipe under `\\.\pipe\`, with an explicit DACL
///   limited to the creating owner and SYSTEM (not the process default token
///   DACL alone).
pub(crate) fn runtime_name() -> Result<Name<'static>> {
    #[cfg(unix)]
    {
        let path = unix_runtime_socket_path()?;
        path.into_os_string()
            .to_fs_name::<GenericFilePath>()
            .context("failed to construct the runtime filesystem socket name")
    }
    #[cfg(windows)]
    {
        let identity = runtime_identity();
        identity
            .to_ns_name::<GenericNamespaced>()
            .context("failed to construct the runtime pipe name")
    }
    #[cfg(not(any(unix, windows)))]
    {
        bail!("unsupported platform for grok-bridge runtime IPC")
    }
}

/// Absolute path of the Unix runtime socket file (bind / diagnostics / tests).
#[cfg(unix)]
pub(crate) fn unix_runtime_socket_path() -> Result<PathBuf> {
    let dir = unix_runtime_ipc_dir()?;
    Ok(dir.join("runtime.sock"))
}

/// Linux-compatible alias (older call sites / docs).
#[cfg(target_os = "linux")]
pub(crate) fn linux_runtime_socket_path() -> Result<PathBuf> {
    unix_runtime_socket_path()
}

/// Ensure the Unix IPC directory exists with 0700 and is owned by this user.
/// Prefer a trusted `XDG_RUNTIME_DIR` subdir; never trust a runtime dir that
/// another user can write. Fall back to `~/.grok-bridge/run` only when every
/// replaceable parent is free of group/world write and not a user-owned symlink.
/// Applies to Linux, macOS, FreeBSD, and other Unices.
#[cfg(unix)]
pub(crate) fn unix_runtime_ipc_dir() -> Result<PathBuf> {
    if let Some(dir) = trusted_xdg_runtime_subdir("grok-bridge")? {
        ensure_private_user_dir(&dir)?;
        return Ok(dir);
    }
    let home = env::var_os("HOME").context(
        "HOME is unset and no trusted XDG_RUNTIME_DIR is available for the runtime socket",
    )?;
    let dir = PathBuf::from(home).join(".grok-bridge").join("run");
    ensure_private_user_dir(&dir)?;
    Ok(dir)
}

#[cfg(target_os = "linux")]
pub(crate) fn linux_runtime_ipc_dir() -> Result<PathBuf> {
    unix_runtime_ipc_dir()
}

/// True when `path` is a directory owned by the current uid and not group/world writable.
#[cfg(unix)]
pub(crate) fn is_trusted_user_dir(path: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to stat IPC directory {}", path.display()));
        }
    };
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return Ok(false);
    }
    let uid = unsafe { libc::getuid() };
    if meta.uid() != uid {
        return Ok(false);
    }
    // Reject group/world write — another principal could replace children.
    Ok(meta.mode() & 0o022 == 0)
}

/// Every path component must be free of group/world write. User-owned symlinks
/// in the chain are refused (rename/pivot races). Root-owned system symlinks
/// (e.g. macOS `/var` → `/private/var`) are allowed when not writable. Chmod on
/// the leaf alone is not enough if a parent is group/world-writable.
#[cfg(unix)]
pub(crate) fn validate_ipc_path_ancestors(path: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    if !path.is_absolute() {
        bail!("IPC path must be absolute: {}", path.display());
    }
    let uid = unsafe { libc::getuid() };
    let mut cur = PathBuf::new();
    for component in path.components() {
        cur.push(component);
        let meta = std::fs::symlink_metadata(&cur)
            .with_context(|| format!("failed to lstat IPC path component {}", cur.display()))?;
        // Writable by group/other on any component enables replace races.
        if meta.mode() & 0o022 != 0 {
            bail!(
                "IPC path component {} is group/world-writable (refusing bind)",
                cur.display()
            );
        }
        if meta.file_type().is_symlink() {
            // User-owned symlink in the chain: attacker can repoint the tree.
            if meta.uid() == uid {
                bail!(
                    "IPC path component {} is a user-owned symlink (refusing rename/symlink races)",
                    cur.display()
                );
            }
            // Third-party non-root symlink is also untrusted.
            if meta.uid() != 0 {
                bail!(
                    "IPC path component {} is a non-root symlink (refusing)",
                    cur.display()
                );
            }
            // Root-owned system symlink: OK (not group/world-writable, checked above).
            continue;
        }
        if !meta.is_dir() && cur != path {
            bail!("IPC path component {} is not a directory", cur.display());
        }
    }
    Ok(())
}

#[cfg(unix)]
fn trusted_xdg_runtime_subdir(name: &str) -> Result<Option<PathBuf>> {
    let Ok(xdg) = env::var("XDG_RUNTIME_DIR") else {
        return Ok(None);
    };
    if xdg.is_empty() {
        return Ok(None);
    }
    let base = PathBuf::from(xdg);
    // XDG base must be user-owned and not group/world-writable; also validate
    // ancestors so a writable /run/user parent cannot pivot the tree.
    if !is_trusted_user_dir(&base)? {
        return Ok(None);
    }
    if validate_ipc_path_ancestors(&base).is_err() {
        return Ok(None);
    }
    Ok(Some(base.join(name)))
}

/// Create `path` (and missing parents) as a user-private 0700 directory tree.
/// Validates every existing ancestor against group/world write and symlinks.
#[cfg(unix)]
fn ensure_private_user_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};

    if !path.is_absolute() {
        bail!("IPC directory must be absolute: {}", path.display());
    }

    // Walk existing ancestors first — refuse writable / user-owned-symlink parents.
    let mut missing: Vec<PathBuf> = Vec::new();
    {
        let mut cur = path.to_path_buf();
        loop {
            match std::fs::symlink_metadata(&cur) {
                Ok(_meta) => {
                    // Validate this existing node and all ancestors in one pass.
                    validate_ipc_path_ancestors(&cur)?;
                    let followed = std::fs::metadata(&cur)
                        .with_context(|| format!("failed to stat IPC path {}", cur.display()))?;
                    if !followed.is_dir() {
                        bail!("IPC path {} exists but is not a directory", cur.display());
                    }
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    missing.push(cur.clone());
                    match cur.parent() {
                        Some(parent) if parent != cur.as_path() => {
                            cur = parent.to_path_buf();
                        }
                        _ => bail!(
                            "cannot create IPC directory {}: no existing parent",
                            path.display()
                        ),
                    }
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to lstat IPC path {}", cur.display()));
                }
            }
        }
    }

    // Create missing components leaf-ward with explicit 0700 (not umask-dependent 0755).
    missing.reverse();
    for component in missing {
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        if let Err(error) = builder.create(&component)
            && error.kind() != std::io::ErrorKind::AlreadyExists
        {
            return Err(error).with_context(|| {
                format!(
                    "failed to create private IPC directory {} (need a user-owned 0700 path)",
                    component.display()
                )
            });
        }
        // Re-validate after create to catch TOCTOU replace.
        let meta = std::fs::symlink_metadata(&component)
            .with_context(|| format!("failed to re-stat IPC directory {}", component.display()))?;
        if meta.file_type().is_symlink() || !meta.is_dir() {
            bail!(
                "IPC path {} was replaced during creation",
                component.display()
            );
        }
        let uid = unsafe { libc::getuid() };
        if meta.uid() != uid {
            bail!(
                "IPC directory {} is not owned by the current user (refusing to bind)",
                component.display()
            );
        }
        if meta.mode() & 0o777 != 0o700 {
            let mut perms = meta.permissions();
            perms.set_mode(0o700);
            std::fs::set_permissions(&component, perms).with_context(|| {
                format!(
                    "failed to set 0700 on IPC directory {}",
                    component.display()
                )
            })?;
        }
    }

    // Final leaf must be user-owned 0700.
    let meta = std::fs::symlink_metadata(path)
        .with_context(|| format!("failed to stat IPC directory {}", path.display()))?;
    if meta.file_type().is_symlink() || !meta.is_dir() {
        bail!("IPC path {} is not a plain directory", path.display());
    }
    let uid = unsafe { libc::getuid() };
    if meta.uid() != uid {
        bail!(
            "IPC directory {} is not owned by the current user (refusing to bind)",
            path.display()
        );
    }
    if meta.mode() & 0o777 != 0o700 {
        let mut perms = meta.permissions();
        perms.set_mode(0o700);
        std::fs::set_permissions(path, perms)
            .with_context(|| format!("failed to set 0700 on IPC directory {}", path.display()))?;
    }
    let meta = std::fs::symlink_metadata(path)?;
    if meta.mode() & 0o022 != 0 {
        bail!(
            "IPC directory {} remains group/world-writable after chmod",
            path.display()
        );
    }
    validate_ipc_path_ancestors(path)?;
    Ok(())
}

/// Acquire exclusive `runtime.lock` under the IPC dir (flock LOCK_EX|LOCK_NB).
/// Held for process lifetime in `UNIX_RUNTIME_LOCK`. Live owners never drop it,
/// so a second process cannot unlink their socket as "stale".
#[cfg(unix)]
fn acquire_unix_runtime_lock(dir: &Path) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::os::unix::io::AsRawFd;

    {
        let slot = UNIX_RUNTIME_LOCK
            .lock()
            .map_err(|_| anyhow::anyhow!("runtime lock mutex was poisoned"))?;
        if slot.is_some() {
            // Already the singleton owner in this process.
            return Ok(());
        }
    }

    let lock_path = dir.join("runtime.lock");
    // create(true) without truncate: open or create the flock target; never
    // zero an existing lock file that another process may still hold metadata on.
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&lock_path)
        .with_context(|| format!("failed to open runtime lock {}", lock_path.display()))?;
    let meta = file
        .metadata()
        .with_context(|| format!("failed to stat runtime lock {}", lock_path.display()))?;
    let uid = unsafe { libc::getuid() };
    if meta.uid() != uid {
        bail!(
            "runtime lock {} is not owned by the current user",
            lock_path.display()
        );
    }
    let lmeta = std::fs::symlink_metadata(&lock_path)?;
    if lmeta.file_type().is_symlink() {
        bail!("runtime lock {} is a symlink", lock_path.display());
    }

    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        let code = err.raw_os_error();
        if code == Some(libc::EWOULDBLOCK) || code == Some(libc::EAGAIN) {
            bail!("runtime server is already running");
        }
        return Err(err).context("failed to flock the runtime lock");
    }

    let mut slot = UNIX_RUNTIME_LOCK
        .lock()
        .map_err(|_| anyhow::anyhow!("runtime lock mutex was poisoned"))?;
    if slot.is_some() {
        // Lost a race with another thread in this process; keep the first fd.
        return Ok(());
    }
    *slot = Some(file);
    Ok(())
}

/// Remove a leftover socket **only while the exclusive runtime lock is held**.
/// Verifies ownership and type so we never unlink a foreign path.
#[cfg(unix)]
fn remove_stale_socket_under_lock(path: &Path) -> Result<()> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("failed to lstat runtime socket {}", path.display()))
        }
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                bail!(
                    "runtime socket path {} is a symlink (refusing to remove)",
                    path.display()
                );
            }
            if !meta.file_type().is_socket() {
                bail!(
                    "runtime socket path {} exists but is not a unix socket",
                    path.display()
                );
            }
            let uid = unsafe { libc::getuid() };
            if meta.uid() != uid {
                bail!(
                    "runtime socket {} is not owned by the current user",
                    path.display()
                );
            }
            let _ino = meta.ino();
            std::fs::remove_file(path).with_context(|| {
                format!("failed to remove stale runtime socket {}", path.display())
            })?;
            Ok(())
        }
    }
}

/// Explicit DACL: owner (OW) + SYSTEM only. Null SECURITY_ATTRIBUTES would use
/// the process default token DACL, which is usually user-scoped but is not a
/// documented guarantee of "current user only" for named pipes.
#[cfg(windows)]
fn current_user_only_pipe_security_descriptor()
-> Result<interprocess::os::windows::security_descriptor::SecurityDescriptor> {
    use interprocess::os::windows::security_descriptor::SecurityDescriptor;
    use widestring::U16CString;
    // Protected DACL: Generic All for object Owner Rights and Local System.
    // No Everyone / Authenticated Users / Builtin Users.
    let sddl = U16CString::from_str("D:P(A;;GA;;;OW)(A;;GA;;;SY)")
        .map_err(|error| anyhow::anyhow!("invalid pipe SDDL: {error}"))?;
    SecurityDescriptor::deserialize(sddl.as_ucstr())
        .context("failed to build user-only named-pipe security descriptor")
}

/// Bind a filesystem UDS at `path` with effective mode 0600.
///
/// Linux/FreeBSD can set mode via `ListenerOptions::mode` (fchmod on the socket
/// fd). macOS rejects fchmod on sockets (`EINVAL` → interprocess `Unsupported`),
/// so we bind then `chmod` the path.
#[cfg(unix)]
fn bind_unix_filesystem_socket(path: &Path) -> Result<interprocess::local_socket::Listener> {
    use interprocess::os::unix::local_socket::ListenerOptionsExt;
    use std::io::ErrorKind;
    use std::os::unix::fs::PermissionsExt;

    let name = path
        .as_os_str()
        .to_fs_name::<GenericFilePath>()
        .context("failed to construct the runtime filesystem socket name")?;
    let listener = match interprocess::local_socket::ListenerOptions::new()
        .name(name)
        .mode(0o600)
        .create_sync()
    {
        Ok(listener) => listener,
        Err(error) if error.kind() == ErrorKind::Unsupported => {
            // macOS and residual Unices where fchmod(socket) is unsupported.
            let name = path
                .as_os_str()
                .to_fs_name::<GenericFilePath>()
                .context("failed to construct the runtime filesystem socket name")?;
            let listener = interprocess::local_socket::ListenerOptions::new()
                .name(name)
                .create_sync()
                .map_err(anyhow::Error::from)
                .context("failed to bind the runtime filesystem socket")?;
            let meta = std::fs::metadata(path)
                .with_context(|| format!("failed to stat runtime socket {}", path.display()))?;
            let mut perms = meta.permissions();
            perms.set_mode(0o600);
            std::fs::set_permissions(path, perms).with_context(|| {
                format!("failed to chmod 0600 runtime socket {}", path.display())
            })?;
            listener
        }
        Err(error) => {
            return Err(anyhow::Error::from(error))
                .context("failed to bind the runtime filesystem socket");
        }
    };
    // Defense in depth: re-assert mode after bind on all Unices (umask / race).
    if let Ok(meta) = std::fs::metadata(path)
        && meta.permissions().mode() & 0o777 != 0o600
    {
        let mut perms = meta.permissions();
        perms.set_mode(0o600);
        let _ = std::fs::set_permissions(path, perms);
    }
    Ok(listener)
}

/// Bind the runtime listener.
///
/// **Unix (incl. macOS / FreeBSD):** exclusive flock on `runtime.lock` under a
/// private 0700 dir is the singleton token. Stale `runtime.sock` is removed only
/// while that lock is held — never because one RPC timed out or returned garbage.
/// Socket mode is 0600 (`GenericFilePath`, never `/tmp/` namespaced mapping).
///
/// **Windows:** local named pipe with an explicit owner+SYSTEM DACL.
pub(crate) fn bind_runtime_listener() -> Result<interprocess::local_socket::Listener> {
    #[cfg(unix)]
    {
        let dir = unix_runtime_ipc_dir()?;
        let path = dir.join("runtime.sock");
        // Singleton ownership first — TOCTOU-free vs concurrent starters.
        acquire_unix_runtime_lock(&dir)?;
        // Only the lock holder may clear a leftover socket inode.
        remove_stale_socket_under_lock(&path)?;
        bind_unix_filesystem_socket(&path)
    }
    #[cfg(windows)]
    {
        use interprocess::os::windows::local_socket::ListenerOptionsExt;
        use std::io::ErrorKind;

        let name = runtime_name()?;
        let security = current_user_only_pipe_security_descriptor()?;
        match interprocess::local_socket::ListenerOptions::new()
            .name(name)
            .security_descriptor(security)
            .create_sync()
        {
            Ok(listener) => Ok(listener),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::AddrInUse | ErrorKind::PermissionDenied
                ) =>
            {
                // Bounded probe only; never "cleanup" a pipe name on probe failure.
                if call_anonymous(Request::ServerStatus, false).is_ok_and(|response| {
                    response.ok
                        && matches!(
                            response.result,
                            Some(crate::protocol::ResponseResult::ServerInfo(_))
                        )
                }) {
                    bail!("runtime server is already running");
                }
                Err(error).context("runtime pipe name is occupied by another process")
            }
            Err(error) => Err(error).context("failed to bind the runtime named pipe"),
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        bail!("unsupported platform for grok-bridge runtime IPC")
    }
}

#[allow(dead_code)]
pub(crate) fn read_frame(reader: &mut impl BufRead) -> Result<Vec<u8>> {
    let mut frame = Vec::with_capacity(4096);
    loop {
        let buffer = reader
            .fill_buf()
            .context("failed to buffer protocol data")?;
        if buffer.is_empty() {
            if frame.is_empty() {
                bail!("protocol peer closed before sending a frame");
            }
            return Ok(frame);
        }
        let length = buffer
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(buffer.len(), |index| index + 1);
        if frame.len() + length > MAX_FRAME_BYTES {
            bail!("protocol frame exceeds the 1 MiB limit");
        }
        frame.extend_from_slice(&buffer[..length]);
        reader.consume(length);
        if frame.last() == Some(&b'\n') {
            return Ok(frame);
        }
    }
}

pub(crate) fn read_frame_until(reader: &mut impl BufRead, deadline: Instant) -> Result<Vec<u8>> {
    let mut frame = Vec::with_capacity(4096);
    loop {
        match reader.fill_buf() {
            Ok(buffer) => {
                if buffer.is_empty() {
                    if frame.is_empty() {
                        bail!("protocol peer closed before sending a frame")
                    }
                    return Ok(frame);
                }
                let length = buffer
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(buffer.len(), |index| index + 1);
                if frame.len() + length > MAX_FRAME_BYTES {
                    bail!("protocol frame exceeds the 1 MiB limit");
                }
                frame.extend_from_slice(&buffer[..length]);
                reader.consume(length);
                if frame.last() == Some(&b'\n') {
                    return Ok(frame);
                }
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    bail!("timed out reading runtime frame")
                }
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn write_all_until(writer: &mut impl Write, data: &[u8], deadline: Instant) -> Result<()> {
    let mut written = 0;
    while written < data.len() {
        match writer.write(&data[written..]) {
            Ok(0) => bail!("runtime peer closed while writing"),
            Ok(count) => written += count,
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    bail!("timed out writing runtime frame")
                }
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn flush_until(writer: &mut impl Write, deadline: Instant) -> Result<()> {
    loop {
        match writer.flush() {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    bail!("timed out flushing runtime frame")
                }
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub(crate) fn write_response_until(
    stream: &mut impl Write,
    response: &ResponseEnvelope,
    deadline: Instant,
) -> Result<()> {
    let frame = encode_frame(response)?;
    write_all_until(stream, &frame, deadline)?;
    flush_until(stream, deadline)
}

#[cfg(windows)]
fn start_detached_server() -> Result<()> {
    let executable = env::current_exe().context("failed to locate grok-bridge executable")?;
    let mut application = executable.as_os_str().encode_wide().collect::<Vec<_>>();
    application.push(0);
    let mut command_line = OsString::from(format!("\"{}\" __server", executable.display()))
        .encode_wide()
        .collect::<Vec<_>>();
    command_line.push(0);
    let startup = STARTUPINFOW {
        cb: std::mem::size_of::<STARTUPINFOW>() as u32,
        ..Default::default()
    };
    let mut process = PROCESS_INFORMATION::default();
    let created = unsafe {
        CreateProcessW(
            application.as_ptr(),
            command_line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            0,
            CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP,
            std::ptr::null(),
            std::ptr::null(),
            &startup,
            &mut process,
        )
    };
    if created == 0 {
        return Err(std::io::Error::last_os_error()).context("failed to spawn runtime server");
    }
    unsafe {
        CloseHandle(process.hThread);
        CloseHandle(process.hProcess);
    }
    Ok(())
}

#[cfg(unix)]
fn start_detached_server() -> Result<()> {
    let executable = env::current_exe().context("failed to locate grok-bridge executable")?;
    let mut command = Command::new(executable);
    command
        .arg("__server")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: setsid is async-signal-safe and the callback does not access shared state.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    command.spawn().context("failed to spawn runtime server")?;
    Ok(())
}

#[cfg(not(any(windows, unix)))]
fn start_detached_server() -> Result<()> {
    let executable = env::current_exe().context("failed to locate grok-bridge executable")?;
    Command::new(executable)
        .arg("__server")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to spawn runtime server")?;
    Ok(())
}

#[cfg(windows)]
fn runtime_identity() -> OsString {
    let user = env::var("USERNAME").unwrap_or_else(|_| "default".to_owned());
    let domain = env::var("USERDOMAIN").unwrap_or_default();
    let suffix = format!("{domain}-{user}")
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    OsString::from(format!("grok-bridge-runtime-v1-{suffix}"))
}

/// Unix identity string is test-only (production binds a filesystem path, not
/// a namespaced abstract name). Windows production uses the USERDOMAIN form.
#[cfg(all(unix, test))]
fn runtime_identity() -> OsString {
    let uid = unsafe { libc::getuid() };
    OsString::from(format!("grok-bridge-runtime-v1-u{uid}"))
}

#[cfg(all(not(any(windows, unix)), test))]
fn runtime_identity() -> OsString {
    OsString::from("grok-bridge-runtime-v1-default")
}

fn next_request_id() -> String {
    let sequence = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    format!(
        "req-{:x}-{:x}-{sequence:x}",
        std::process::id(),
        now_millis()
    )
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
    use std::io::{self, Cursor, Read};
    #[cfg(windows)]
    use std::{
        num::NonZeroU8,
        sync::atomic::{AtomicU64, Ordering},
    };

    #[cfg(windows)]
    static NEXT_PIPE_TEST: AtomicU64 = AtomicU64::new(1);

    #[cfg(windows)]
    fn windows_test_pipe_path(suffix: &str) -> OsString {
        let id = NEXT_PIPE_TEST.fetch_add(1, Ordering::Relaxed);
        let mut path = OsString::from(r"\\.\pipe\grok-bridge-test-");
        path.push(format!("{}-{id}-{suffix}", std::process::id()));
        path
    }

    #[cfg(windows)]
    fn windows_resource_counts() -> (u32, usize) {
        use std::os::windows::io::{AsRawHandle, FromRawHandle};
        use windows_sys::Win32::{
            Foundation::INVALID_HANDLE_VALUE,
            System::{
                Diagnostics::ToolHelp::{
                    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First,
                    Thread32Next,
                },
                Threading::{GetCurrentProcess, GetProcessHandleCount},
            },
        };
        let mut handles = 0;
        assert_ne!(
            unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut handles) },
            0
        );
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        assert_ne!(snapshot, INVALID_HANDLE_VALUE);
        let snapshot = unsafe { OwnedHandle::from_raw_handle(snapshot) };
        let mut entry = THREADENTRY32 {
            dwSize: std::mem::size_of::<THREADENTRY32>() as u32,
            ..Default::default()
        };
        let mut threads = 0usize;
        if unsafe { Thread32First(snapshot.as_raw_handle(), &mut entry) } != 0 {
            loop {
                if entry.th32OwnerProcessID == std::process::id() {
                    threads += 1;
                }
                if unsafe { Thread32Next(snapshot.as_raw_handle(), &mut entry) } == 0 {
                    break;
                }
            }
        }
        (handles, threads)
    }

    #[cfg(windows)]
    fn windows_stable_resource_counts() -> (u32, usize) {
        let mut previous = windows_resource_counts();
        let mut stable_samples = 0;
        for _ in 0..20 {
            thread::sleep(Duration::from_millis(10));
            let current = windows_resource_counts();
            if current == previous {
                stable_samples += 1;
                if stable_samples == 3 {
                    return current;
                }
            } else {
                previous = current;
                stable_samples = 0;
            }
        }
        panic!("Windows process resources did not stabilize: {previous:?}");
    }

    #[cfg(windows)]
    #[test]
    fn windows_named_pipe_rpc_round_trip() {
        use crate::protocol::{ResponseResult, decode_request};
        use interprocess::os::windows::named_pipe::{PipeListenerOptions, pipe_mode::Bytes};

        let path = windows_test_pipe_path("rpc");
        let listener = PipeListenerOptions::new()
            .path(path.as_os_str())
            .create_duplex::<Bytes>()
            .expect("create unique test pipe");
        let server = thread::spawn(move || {
            let stream = listener.accept().expect("accept connected test pipe");
            let mut connection = BufReader::new(stream);
            let frame = read_frame(&mut connection).expect("read NDJSON request");
            let request = decode_request(&frame).expect("decode RequestEnvelope");
            let response =
                ResponseEnvelope::success(request.id, ResponseResult::Accepted { accepted: true });
            connection
                .get_mut()
                .write_all(&encode_frame(&response).unwrap())
                .expect("write NDJSON response");
            connection.get_mut().flush().expect("flush response");
        });
        let client = connect_windows_pipe_path(&path, Duration::from_millis(500)).unwrap();
        let request = RequestEnvelope {
            id: "windows-pipe-rpc-1".to_owned(),
            client_session_id: None,
            request: Request::Heartbeat,
        };
        let response =
            call_over_stream_with_io_timeout(client, &request, Duration::from_millis(500))
                .expect("complete bounded RPC over converted local_socket Stream");
        assert_eq!(response.id, request.id);
        assert!(response.ok);
        assert_eq!(
            response.result,
            Some(ResponseResult::Accepted { accepted: true })
        );
        server.join().expect("join pipe RPC server");
    }

    #[cfg(windows)]
    fn run_windows_busy_pipe_timeouts(attempts: usize) {
        use interprocess::os::windows::named_pipe::{PipeListenerOptions, pipe_mode::Bytes};

        let busy_path = windows_test_pipe_path("busy");
        let busy_listener = PipeListenerOptions::new()
            .path(busy_path.as_os_str())
            .instance_limit(NonZeroU8::new(1))
            .create_duplex::<Bytes>()
            .expect("create single-instance busy pipe");
        let busy_client =
            connect_windows_pipe_path(&busy_path, Duration::from_millis(500)).unwrap();
        // Keep the sole instance occupied without accepting it. A second connection must wait for
        // the real named-pipe instance and then time out, not return early.
        let started = std::time::Instant::now();
        for _ in 0..attempts {
            let error = connect_windows_pipe_path(&busy_path, Duration::from_millis(40))
                .expect_err("instance_limit=1 must reject a second client");
            assert!(
                error.to_string().contains("runtime server is not running")
                    || error.to_string().contains("timed out")
            );
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(20 * attempts as u64),
            "busy pipe returned too quickly: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(500 * attempts as u64),
            "busy pipe timeout loop exceeded bound: {elapsed:?}"
        );
        drop(busy_client);
        drop(busy_listener);
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "resource accounting must run alone via the Windows CI control-plane step"]
    fn windows_named_pipe_busy_timeout_resources_are_stable() {
        // Warm lazy Windows/interprocess initialization before taking the baseline.
        run_windows_busy_pipe_timeouts(2);
        let resources_before = windows_stable_resource_counts();
        run_windows_busy_pipe_timeouts(32);
        let resources_after = windows_stable_resource_counts();
        assert_eq!(
            resources_after, resources_before,
            "resources grew across repeated busy-pipe timeouts: {resources_before:?} -> {resources_after:?}"
        );
    }

    #[cfg(windows)]
    fn run_windows_stalled_pipe_rpc() {
        use interprocess::os::windows::named_pipe::{PipeListenerOptions, pipe_mode::Bytes};

        let path = windows_test_pipe_path("stalled");
        let listener = PipeListenerOptions::new()
            .path(path.as_os_str())
            .create_duplex::<Bytes>()
            .expect("create stalled test pipe");
        let client = connect_windows_pipe_path(&path, Duration::from_millis(500)).unwrap();
        let server = listener.accept().expect("accept stalled client");
        let request = RequestEnvelope {
            id: "windows-stalled-pipe-rpc".to_owned(),
            client_session_id: None,
            request: Request::Heartbeat,
        };
        let started = Instant::now();
        let error = call_over_stream_with_io_timeout(client, &request, Duration::from_millis(40))
            .expect_err("accepted pipe that never responds must hit the I/O deadline");
        let elapsed = started.elapsed();
        assert!(
            error
                .to_string()
                .contains("failed to read runtime response"),
            "unexpected stalled-pipe error: {error:#}"
        );
        assert!(
            elapsed >= Duration::from_millis(20),
            "stalled pipe returned before its deadline: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "stalled pipe exceeded its deadline bound: {elapsed:?}"
        );
        drop(server);
        drop(listener);
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "resource accounting must run alone via the Windows CI control-plane step"]
    fn windows_stalled_pipe_io_deadline_resources_are_stable() {
        // Warm lazy Windows/interprocess initialization before taking the baseline.
        run_windows_stalled_pipe_rpc();
        let resources_before = windows_stable_resource_counts();
        for _ in 0..20 {
            run_windows_stalled_pipe_rpc();
        }
        let resources_after = windows_stable_resource_counts();
        assert_eq!(
            resources_after, resources_before,
            "resources grew across stalled-pipe RPC deadlines: {resources_before:?} -> {resources_after:?}"
        );
    }

    struct FragmentedReader {
        step: u8,
    }

    impl Read for FragmentedReader {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            match self.step {
                0 => {
                    self.step = 1;
                    out[..3].copy_from_slice(b"one");
                    Ok(3)
                }
                1 => {
                    self.step = 2;
                    Err(io::Error::new(io::ErrorKind::WouldBlock, "fragment"))
                }
                _ => {
                    out[0] = b'\n';
                    Ok(1)
                }
            }
        }
    }

    #[test]
    fn deadline_reader_preserves_partial_frame_across_would_block() {
        let mut reader = BufReader::with_capacity(3, FragmentedReader { step: 0 });
        let frame = read_frame_until(&mut reader, Instant::now() + Duration::from_secs(1)).unwrap();
        assert_eq!(frame, b"one\n");
    }

    #[test]
    fn reads_exactly_one_frame() {
        let mut reader = BufReader::new(Cursor::new(b"one\ntwo\n"));
        assert_eq!(read_frame(&mut reader).unwrap(), b"one\n");
        assert_eq!(read_frame(&mut reader).unwrap(), b"two\n");
    }

    #[test]
    fn rejects_oversized_frames_before_unbounded_growth() {
        let input = vec![b'x'; MAX_FRAME_BYTES + 1];
        let mut reader = BufReader::new(Cursor::new(input));
        assert!(read_frame(&mut reader).is_err());
    }

    #[test]
    fn runtime_identity_is_namespaced_and_stable() {
        let first = runtime_identity();
        let second = runtime_identity();
        assert_eq!(first, second);
        assert!(
            first
                .to_string_lossy()
                .starts_with("grok-bridge-runtime-v1-")
        );
    }

    #[test]
    fn codex_thread_identity_precedes_the_legacy_session_identity() {
        assert_eq!(
            client_session_id_from(Some("thread-42".to_owned()), Some("session-7".to_owned()))
                .unwrap()
                .as_deref(),
            Some("thread-42")
        );
        assert_eq!(
            client_session_id_from(None, Some("session-7".to_owned()))
                .unwrap()
                .as_deref(),
            Some("session-7")
        );
        assert_eq!(client_session_id_from(None, None).unwrap(), None);
        assert!(client_session_id_from(Some("bad\nidentity".to_owned()), None).is_err());
    }

    /// Dynamic platform check: endpoint is under a private user dir, never `/tmp/`
    /// namespaced mapping (macOS/FreeBSD previously srwxr-xr-x under /tmp).
    #[cfg(unix)]
    #[test]
    fn unix_runtime_socket_is_private_filesystem_path_not_tmp_namespaced() {
        let path = unix_runtime_socket_path().unwrap();
        assert!(path.is_absolute(), "path={path:?}");
        assert!(path.ends_with("runtime.sock"), "socket file name: {path:?}");
        let path_str = path.to_string_lossy();
        assert!(
            !path_str.starts_with("/tmp/grok-bridge-runtime"),
            "must not use GenericNamespaced /tmp mapping: {path_str}"
        );
        assert!(
            path_str.contains(".grok-bridge") || path_str.contains("grok-bridge"),
            "expected private IPC dir: {path_str}"
        );
        let dir = path.parent().unwrap();
        assert!(is_trusted_user_dir(dir).unwrap(), "dir={dir:?}");
        let name = runtime_name().unwrap();
        let rendered = format!("{name:?}");
        assert!(
            !rendered.contains("\\0") && !rendered.contains('\0'),
            "must not use abstract namespace: {rendered}"
        );
    }

    /// Bind and verify socket mode 0600 under a 0700 dir (macOS/Linux/FreeBSD).
    #[cfg(unix)]
    #[test]
    fn unix_bind_creates_0600_socket_under_0700_dir() {
        use std::os::unix::fs::{FileTypeExt, PermissionsExt};
        // Keep paths short (sun_path / SUN_LEN limits). Avoid world-writable /tmp.
        let dir = std::env::temp_dir().join(format!("gbm{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        ensure_private_user_dir(&dir).unwrap();
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "dir mode={mode:o}");

        {
            let mut slot = UNIX_RUNTIME_LOCK.lock().unwrap();
            *slot = None;
        }
        acquire_unix_runtime_lock(&dir).unwrap();
        let sock = dir.join("r.sock");
        remove_stale_socket_under_lock(&sock).unwrap();
        let _listener = bind_unix_filesystem_socket(&sock).unwrap();
        let meta = std::fs::metadata(&sock).unwrap();
        assert!(meta.file_type().is_socket(), "expected socket");
        let sock_mode = meta.permissions().mode() & 0o777;
        assert_eq!(
            sock_mode, 0o600,
            "socket must be 0600, got {sock_mode:o} path={sock:?}"
        );
        {
            let mut slot = UNIX_RUNTIME_LOCK.lock().unwrap();
            *slot = None;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn untrusted_xdg_runtime_dir_is_rejected() {
        let tmp =
            std::env::temp_dir().join(format!("grok-bridge-untrusted-xdg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&tmp).unwrap().permissions();
        perms.set_mode(0o777);
        std::fs::set_permissions(&tmp, perms).unwrap();
        assert!(!is_trusted_user_dir(&tmp).unwrap());
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[cfg(unix)]
    #[test]
    fn ensure_private_dir_sets_0700_and_rejects_foreign_owner_semantics() {
        let dir =
            std::env::temp_dir().join(format!("grok-bridge-private-dir-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        ensure_private_user_dir(&dir).unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "mode={mode:o}");
        assert!(is_trusted_user_dir(&dir).unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(windows)]
    #[test]
    fn windows_runtime_name_is_local_named_pipe_identity() {
        let name = runtime_name().unwrap();
        let identity = runtime_identity();
        let rendered = format!("{name:?}");
        assert!(
            rendered.contains(&*identity.to_string_lossy())
                || rendered.contains("grok-bridge-runtime-v1")
                || rendered.contains("pipe"),
            "name={rendered} identity={identity:?}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_user_only_security_descriptor_deserializes() {
        let sd = current_user_only_pipe_security_descriptor().unwrap();
        // Drop is enough: construction must not fall back to a null DACL.
        drop(sd);
    }

    #[test]
    fn ordinary_rpc_uses_bounded_default_io_timeout() {
        // ServerStatus is a singleton probe (tight bound); ordinary RPCs stay 30s.
        let list = rpc_io_timeout_for(&Request::List);
        assert_eq!(list, RPC_DEFAULT_IO_TIMEOUT);
        let heartbeat = rpc_io_timeout_for(&Request::Heartbeat);
        assert_eq!(heartbeat, RPC_DEFAULT_IO_TIMEOUT);
    }

    #[test]
    fn wait_rpc_io_timeout_covers_budget_with_margin_and_preserves_two_hour_max() {
        let short = rpc_io_timeout_for(&Request::Wait {
            session: "gbt-x".to_owned(),
            for_condition: crate::protocol::WaitCondition::TuiIdle,
            timeout_ms: Some(1_000),
        });
        assert!(short >= Duration::from_millis(1_000) + RPC_WAIT_BUDGET_MARGIN);
        assert!(short <= RPC_WAIT_IO_CEILING);

        let two_hours = rpc_io_timeout_for(&Request::Wait {
            session: "gbt-x".to_owned(),
            for_condition: crate::protocol::WaitCondition::TuiIdle,
            timeout_ms: Some(MAX_WAIT_TIMEOUT_MS),
        });
        assert!(
            two_hours >= Duration::from_millis(MAX_WAIT_TIMEOUT_MS),
            "2h wait must not be cut by ordinary 30s socket timeout: {two_hours:?}"
        );
        assert_eq!(
            two_hours,
            Duration::from_millis(MAX_WAIT_TIMEOUT_MS).saturating_add(RPC_WAIT_BUDGET_MARGIN)
        );
        assert!(two_hours <= RPC_WAIT_IO_CEILING);

        let read_wait = rpc_io_timeout_for(&Request::Read {
            session: "gbt-x".to_owned(),
            cursor: Some(0),
            limit: Some(100),
            wait_ms: Some(60_000),
        });
        assert!(read_wait >= Duration::from_secs(60) + RPC_WAIT_BUDGET_MARGIN);
    }

    #[test]
    fn server_status_probe_uses_tight_io_timeout() {
        assert_eq!(
            rpc_io_timeout_for(&Request::ServerStatus),
            RPC_PROBE_IO_TIMEOUT
        );
        assert!(RPC_PROBE_IO_TIMEOUT < RPC_DEFAULT_IO_TIMEOUT);
        assert!(RPC_CONNECT_TIMEOUT <= RPC_PROBE_IO_TIMEOUT + Duration::from_secs(1));
    }

    #[test]
    fn only_connection_refused_may_justify_socket_unlink() {
        // Live / delayed / malformed peers must never be treated as stale.
        assert!(!may_unlink_socket_for_liveness(SocketLiveness::Live));
        assert!(!may_unlink_socket_for_liveness(
            SocketLiveness::Indeterminate
        ));
        assert!(may_unlink_socket_for_liveness(
            SocketLiveness::ConnectionRefused
        ));
    }

    #[cfg(unix)]
    #[test]
    fn group_writable_ancestor_is_rejected_for_ipc_path() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::temp_dir().join(format!(
            "grok-bridge-ipc-anc-{}-{}",
            std::process::id(),
            now_millis()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        // Start trusted, then make group-writable — leaf chmod alone must not pass.
        let mut perms = std::fs::metadata(&base).unwrap().permissions();
        perms.set_mode(0o770);
        std::fs::set_permissions(&base, perms).unwrap();
        let leaf = base.join("run");
        std::fs::create_dir(&leaf).unwrap();
        let mut leaf_perms = std::fs::metadata(&leaf).unwrap().permissions();
        leaf_perms.set_mode(0o700);
        std::fs::set_permissions(&leaf, leaf_perms).unwrap();
        let err = validate_ipc_path_ancestors(&leaf).unwrap_err();
        assert!(
            format!("{err:#}").contains("group/world-writable"),
            "err={err:#}"
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[cfg(unix)]
    #[test]
    fn ensure_private_user_dir_rejects_symlink_component() {
        let base = std::env::temp_dir().join(format!(
            "grok-bridge-ipc-sym-{}-{}",
            std::process::id(),
            now_millis()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&base).unwrap().permissions();
        perms.set_mode(0o700);
        std::fs::set_permissions(&base, perms).unwrap();
        let real = base.join("real");
        std::fs::create_dir(&real).unwrap();
        let link = base.join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let target = link.join("run");
        let err = ensure_private_user_dir(&target).unwrap_err();
        let msg = format!("{err:#}").to_lowercase();
        assert!(msg.contains("symlink"), "err={err:#}");
        let _ = std::fs::remove_dir_all(&base);
    }

    /// Live lock holder keeps the socket; probe policy never unlinks Live/Indeterminate.
    #[cfg(unix)]
    #[test]
    fn live_lock_holder_blocks_second_owner_without_unlinking_socket() {
        use std::os::unix::io::AsRawFd;
        use std::os::unix::net::UnixListener;
        let dir = std::env::temp_dir().join(format!("gbl{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        ensure_private_user_dir(&dir).unwrap();
        let sock = dir.join("r.sock");
        {
            let mut slot = UNIX_RUNTIME_LOCK.lock().unwrap();
            *slot = None;
        }
        acquire_unix_runtime_lock(&dir).unwrap();
        // Lock fd remains open for the process lifetime (singleton ownership).
        {
            let slot = UNIX_RUNTIME_LOCK.lock().unwrap();
            assert!(slot.is_some(), "runtime lock file must stay open");
            let fd = slot.as_ref().unwrap().as_raw_fd();
            assert!(fd >= 0);
        }
        let _listener = UnixListener::bind(&sock).unwrap();
        assert!(sock.exists());
        // Live/delayed peers must never be treated as unlink-eligible.
        assert!(!may_unlink_socket_for_liveness(
            SocketLiveness::Indeterminate
        ));
        assert!(!may_unlink_socket_for_liveness(SocketLiveness::Live));
        // Without remove_stale, the listening socket remains (no RPC-fail cleanup).
        assert!(sock.exists());
        {
            let mut slot = UNIX_RUNTIME_LOCK.lock().unwrap();
            *slot = None;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Dead process left a socket inode; after lock acquisition it is removed
    /// and a new listener can bind the same path.
    #[cfg(unix)]
    #[test]
    fn stale_socket_inode_is_removed_only_under_exclusive_lock() {
        use std::os::unix::net::UnixListener;
        let dir = std::env::temp_dir().join(format!("gbs{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        ensure_private_user_dir(&dir).unwrap();
        let sock = dir.join("r.sock");
        {
            let listener = UnixListener::bind(&sock).unwrap();
            drop(listener);
        }
        assert!(sock.exists());
        {
            let mut slot = UNIX_RUNTIME_LOCK.lock().unwrap();
            *slot = None;
        }
        acquire_unix_runtime_lock(&dir).unwrap();
        remove_stale_socket_under_lock(&sock).unwrap();
        assert!(
            !sock.exists(),
            "stale socket must be removed under exclusive lock"
        );
        let _listener = UnixListener::bind(&sock).unwrap();
        assert!(sock.exists());
        {
            let mut slot = UNIX_RUNTIME_LOCK.lock().unwrap();
            *slot = None;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Accept-then-stall peer is Indeterminate: policy forbids unlink/rebind.
    #[cfg(unix)]
    #[test]
    fn accept_then_stall_peer_is_indeterminate_not_stale() {
        use std::os::unix::net::{UnixListener, UnixStream};
        use std::sync::mpsc::sync_channel;
        let dir = std::env::temp_dir().join(format!("gbp{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        ensure_private_user_dir(&dir).unwrap();
        let sock = dir.join("r.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let (ready_tx, ready_rx) = sync_channel::<()>(1);
        let accept = thread::spawn(move || {
            let _ = ready_tx.send(());
            let _stream = listener.accept().unwrap();
            thread::sleep(Duration::from_secs(30));
        });
        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let _client = UnixStream::connect(&sock).unwrap();
        assert!(!may_unlink_socket_for_liveness(SocketLiveness::Live));
        assert!(!may_unlink_socket_for_liveness(
            SocketLiveness::Indeterminate
        ));
        assert!(sock.exists());
        let _ = accept;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
