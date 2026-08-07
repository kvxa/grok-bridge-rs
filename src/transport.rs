#[cfg(unix)]
use std::io;
use std::{
    env,
    ffi::OsString,
    io::{BufRead, BufReader, ErrorKind, Write},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
#[cfg(not(unix))]
use interprocess::local_socket::GenericNamespaced;
#[cfg(any(unix, test))]
use interprocess::local_socket::ListenerOptions;
#[cfg(unix)]
use interprocess::local_socket::{GenericFilePath, Listener};
use interprocess::local_socket::{Name, Stream, prelude::*};
#[cfg(unix)]
use std::fs::{File, OpenOptions};
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(unix)]
use std::path::{Path, PathBuf};
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
    MAX_FRAME_BYTES, Request, RequestEnvelope, ResponseEnvelope, decode_response, encode_frame,
    validate_client_session_id,
};

const START_RETRIES: usize = 50;
const START_RETRY_DELAY: Duration = Duration::from_millis(100);
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// Server-side deadline for receiving a complete request frame from an IPC
/// client. A connected-but-silent or trickling client never pins a handler
/// thread beyond this.
pub(crate) const IPC_FRAME_READ_DEADLINE: Duration = Duration::from_secs(30);
/// Deadline for any single frame write (client request or server response).
/// Frames are at most 1 MiB and written in one burst, so a peer that stops
/// draining surfaces an error within this window.
pub(crate) const IPC_WRITE_DEADLINE: Duration = Duration::from_secs(30);
#[cfg(unix)]
const RUNTIME_STARTUP_LOCK_TIMEOUT: Duration = Duration::from_secs(10);
/// Poll backoff bounds while an IPC peer is idle, so a waiting thread sleeps
/// instead of spinning yet still notices data promptly.
const IPC_POLL_MIN: Duration = Duration::from_millis(1);
const IPC_POLL_MAX: Duration = Duration::from_millis(50);
/// Extra headroom the client grants the server beyond the request's own
/// documented timeout (`read` wait and `wait` timeout are bounded server-side
/// in session.rs) so legitimate long operations are never cut short.
const IPC_RESPONSE_SLACK: Duration = Duration::from_secs(60);

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
    let name = runtime_name()?;
    Stream::connect(name).context("runtime server is not running")
}

fn call_over_stream(stream: Stream, envelope: &RequestEnvelope) -> Result<ResponseEnvelope> {
    // Non-blocking I/O lets both sides enforce I/O deadlines on every platform
    // (Windows named pipes do not support native socket timeouts).
    stream
        .set_nonblocking(true)
        .context("failed to enable non-blocking runtime I/O")?;
    let response_deadline = Instant::now() + response_deadline_for(&envelope.request);
    let mut connection = BufReader::new(stream);
    write_frame_all(
        connection.get_mut(),
        &encode_frame(envelope)?,
        IPC_WRITE_DEADLINE,
    )
    .context("failed to write runtime request")?;
    let frame = read_frame(&mut connection, Some(response_deadline))
        .context("failed to read runtime response")?;
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

/// Response read deadline mirroring the server's per-operation timeouts plus
/// slack. `Session::read` clamps its wait to 300 s and `Session::wait` clamps
/// its timeout to 7_200_000 ms (session.rs); a request that uses the full
/// documented timeout must not be cut short by an unrelated default deadline.
fn response_deadline_for(request: &Request) -> Duration {
    let base_ms = match request {
        Request::Read { wait_ms, .. } => wait_ms.unwrap_or(0).min(300_000),
        Request::Wait { timeout_ms, .. } => timeout_ms.unwrap_or(300_000).min(7_200_000),
        _ => 0,
    };
    Duration::from_millis(base_ms) + IPC_RESPONSE_SLACK
}

/// The Runtime IPC name as understood by the local socket API. On Unix this is
/// a filesystem socket path inside the private owner-only runtime directory; on
/// Windows (and other platforms) it remains a namespaced named pipe. Server and
/// client both compute the name through this function, so the two sides always
/// agree within the same environment.
#[cfg(unix)]
pub(crate) fn runtime_name() -> Result<Name<'static>> {
    runtime_socket_path()?
        .to_fs_name::<GenericFilePath>()
        .context("failed to construct the runtime socket path")
}

#[cfg(not(unix))]
pub(crate) fn runtime_name() -> Result<Name<'static>> {
    let identity = runtime_identity();
    identity
        .to_ns_name::<GenericNamespaced>()
        .context("failed to construct the runtime pipe name")
}

/// On Unix, the private per-user directory that holds the Runtime IPC socket.
/// A configured XDG base is used only when it is an absolute, owner-only real
/// directory owned by this uid. Invalid values fall back to an absolute temp
/// base; the private child remains uid-namespaced and is secured to 0700.
#[cfg(unix)]
pub(crate) fn runtime_dir() -> Result<PathBuf> {
    runtime_dir_from(
        env::var_os("XDG_RUNTIME_DIR").filter(|value| !value.is_empty()),
        env::temp_dir(),
        current_uid(),
    )
}

#[cfg(unix)]
fn runtime_dir_from(xdg: Option<OsString>, temp: PathBuf, expected_uid: u32) -> Result<PathBuf> {
    let base = xdg
        .map(PathBuf::from)
        .and_then(|path| trusted_runtime_base(&path, expected_uid, true))
        .or_else(|| trusted_runtime_base(&temp, expected_uid, false))
        .or_else(|| trusted_runtime_base(Path::new("/tmp"), expected_uid, false))
        .context("no trusted absolute runtime base is available")?;
    Ok(base.join(runtime_identity()))
}

#[cfg(all(unix, test))]
fn trusted_xdg_runtime_base(path: &Path, expected_uid: u32) -> bool {
    trusted_runtime_base(path, expected_uid, true).is_some()
}

/// Resolve an existing base and verify that no directory in its canonical
/// ancestor chain can be replaced by another user. Root and the current uid
/// are trusted owners. A group/world-writable directory is accepted only with
/// the sticky bit, which prevents peers from renaming another owner's entry.
#[cfg(unix)]
fn trusted_runtime_base(
    path: &Path,
    expected_uid: u32,
    require_owner_only: bool,
) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let original = std::fs::symlink_metadata(path).ok()?;
    if require_owner_only && (original.file_type().is_symlink() || !original.is_dir()) {
        return None;
    }
    if !require_owner_only && !original.is_dir() && !original.file_type().is_symlink() {
        return None;
    }
    if require_owner_only
        && (original.uid() != expected_uid || original.permissions().mode() & 0o077 != 0)
    {
        return None;
    }
    let canonical = std::fs::canonicalize(path).ok()?;
    if !canonical.is_absolute() {
        return None;
    }
    if !trusted_ancestor_chain(path, expected_uid, true)
        || !trusted_ancestor_chain(&canonical, expected_uid, false)
    {
        return None;
    }
    Some(canonical)
}

#[cfg(unix)]
fn trusted_ancestor_chain(path: &Path, expected_uid: u32, allow_symlinks: bool) -> bool {
    for ancestor in path.ancestors() {
        let Ok(meta) = std::fs::symlink_metadata(ancestor) else {
            return false;
        };
        if meta.file_type().is_symlink() {
            if !allow_symlinks || (meta.uid() != 0 && meta.uid() != expected_uid) {
                return false;
            }
            continue;
        }
        if !meta.is_dir() {
            return false;
        }
        if meta.uid() != 0 && meta.uid() != expected_uid {
            return false;
        }
        let mode = meta.permissions().mode();
        if mode & 0o022 != 0 && mode & 0o1000 == 0 {
            return false;
        }
    }
    true
}

/// On Unix, the socket file lives inside the private runtime directory.
#[cfg(unix)]
pub(crate) fn runtime_socket_path() -> Result<PathBuf> {
    Ok(runtime_dir()?.join("runtime.sock"))
}

/// Read one NDJSON frame, bounded by `MAX_FRAME_BYTES` and an optional read
/// deadline.
///
/// With `deadline` set, the peer is expected to keep the stream non-blocking:
/// idle reads poll with a short backoff and the call fails once the deadline
/// passes, so a silent or trickling peer can never pin the calling thread
/// indefinitely. With `deadline = None` the read blocks like a plain
/// `fill_buf` loop and preserves the historical behavior.
pub(crate) fn read_frame(reader: &mut impl BufRead, deadline: Option<Instant>) -> Result<Vec<u8>> {
    let mut frame = Vec::with_capacity(4096);
    let mut poll_delay = IPC_POLL_MIN;
    loop {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            bail!(
                "protocol frame read timed out; the peer did not complete the frame within the I/O deadline"
            );
        }
        let buffer = match reader.fill_buf() {
            Ok(buffer) => {
                poll_delay = IPC_POLL_MIN;
                buffer
            }
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                if deadline.is_none() {
                    return Err(error).context("failed to buffer protocol data");
                }
                thread::sleep(poll_delay);
                poll_delay = (poll_delay * 2).min(IPC_POLL_MAX);
                continue;
            }
            Err(error) => return Err(error).context("failed to buffer protocol data"),
        };
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

/// Write `data` to the stream within `deadline`, polling with backoff while
/// the peer's receive buffer is full. Non-blocking peers that stop draining
/// produce a bounded write error instead of blocking the caller forever.
///
/// Windows named pipes in nonblocking (PIPE_NOWAIT) mode report a full buffer
/// as `Ok(0)`: a successful zero-byte completion, not a peer close. Such a
/// zero-byte write is retried under the same bounded backoff and deadline.
/// On every other platform `Ok(0)` still means the peer closed the stream.
///
/// There is deliberately no trailing `flush()`: these frames are written
/// directly to the underlying OS handle (never through a userspace
/// `BufWriter`), so there is no buffered data to flush, and a flush on a
/// Windows named pipe (`FlushFileBuffers`) can block until the peer reads —
/// exactly the unbounded wait this function exists to avoid.
fn write_frame_all(stream: &mut impl Write, mut data: &[u8], deadline: Duration) -> Result<()> {
    let deadline = Instant::now() + deadline;
    let mut poll_delay = IPC_POLL_MIN;
    while !data.is_empty() {
        if Instant::now() >= deadline {
            bail!(
                "protocol frame write timed out; the peer did not drain the data within the I/O deadline"
            );
        }
        let stalled = match stream.write(data) {
            #[cfg(windows)]
            Ok(0) => true,
            #[cfg(not(windows))]
            Ok(0) => bail!("protocol peer closed while receiving the frame"),
            Ok(written) => {
                data = &data[written..];
                poll_delay = IPC_POLL_MIN;
                false
            }
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                true
            }
            Err(error) => return Err(error.into()),
        };
        if stalled {
            thread::sleep(poll_delay);
            poll_delay = (poll_delay * 2).min(IPC_POLL_MAX);
        }
    }
    Ok(())
}

pub(crate) fn write_response(stream: &mut impl Write, response: &ResponseEnvelope) -> Result<()> {
    write_frame_all(stream, &encode_frame(response)?, IPC_WRITE_DEADLINE)
        .context("failed to write runtime response")
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

#[cfg(unix)]
fn runtime_identity() -> OsString {
    let uid = unsafe { libc::getuid() };
    OsString::from(format!("grok-bridge-runtime-v1-u{uid}"))
}

#[cfg(not(any(windows, unix)))]
fn runtime_identity() -> OsString {
    OsString::from("grok-bridge-runtime-v1-default")
}

/// Owner-only mode for the Runtime IPC directory on Unix.
#[cfg(unix)]
pub(crate) const RUNTIME_DIR_MODE: u32 = 0o700;
/// Owner-only mode for the Runtime IPC socket on Unix. Unix sockets authorize
/// connection attempts with the write bits of the socket file, so 0600 admits
/// only the owner.
#[cfg(unix)]
pub(crate) const RUNTIME_SOCKET_MODE: u32 = 0o600;

#[cfg(unix)]
fn current_uid() -> u32 {
    unsafe { libc::getuid() }
}

/// Holds the advisory startup lock while one Runtime binds, probes, removes a
/// stale socket, and retries the bind. Closing the file releases the lock.
#[cfg(unix)]
pub(crate) struct RuntimeStartupLock {
    _file: File,
}

#[cfg(unix)]
pub(crate) fn acquire_runtime_startup_lock() -> Result<RuntimeStartupLock> {
    let dir = ensure_runtime_dir()?;
    acquire_runtime_startup_lock_at(&dir, RUNTIME_STARTUP_LOCK_TIMEOUT)
}

#[cfg(unix)]
fn acquire_runtime_startup_lock_at(dir: &Path, timeout: Duration) -> Result<RuntimeStartupLock> {
    let path = dir.join("runtime.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(RUNTIME_SOCKET_MODE)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&path)
        .with_context(|| format!("failed to open the Runtime startup lock {path:?}"))?;
    let meta = file
        .metadata()
        .with_context(|| format!("failed to inspect the Runtime startup lock {path:?}"))?;
    if !meta.is_file() || meta.uid() != current_uid() {
        bail!(
            "Runtime startup lock {path:?} is not a regular file owned by uid {}",
            current_uid()
        );
    }
    file.set_permissions(std::fs::Permissions::from_mode(RUNTIME_SOCKET_MODE))
        .with_context(|| format!("failed to secure the Runtime startup lock {path:?}"))?;

    let deadline = Instant::now() + timeout;
    loop {
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(RuntimeStartupLock { _file: file });
        }
        let error = io::Error::last_os_error();
        let raw_error = error.raw_os_error();
        if raw_error != Some(libc::EWOULDBLOCK) && raw_error != Some(libc::EAGAIN) {
            return Err(error)
                .with_context(|| format!("failed to lock Runtime startup file {path:?}"));
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for the Runtime startup lock {path:?}");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// Create or verify the private Runtime IPC directory. The directory must be a
/// real directory owned by the current user; a symlink, another file type, or a
/// foreign owner is refused with a diagnosable error instead of being touched.
/// An owned directory is then tightened to owner-only (0700) and the resulting
/// mode is verified on disk.
#[cfg(unix)]
pub(crate) fn ensure_runtime_dir() -> Result<PathBuf> {
    let dir = runtime_dir()?;
    ensure_private_dir(&dir, current_uid())
        .with_context(|| format!("failed to secure the runtime IPC directory {dir:?}"))?;
    Ok(dir)
}

/// Verify that `path` is a directory owned by `expected_uid` and make it
/// owner-only. Uses `symlink_metadata` (lstat) so a symlink at the path is
/// rejected instead of followed. `expected_uid` is a parameter so tests can
/// exercise the ownership check without impersonating another user.
#[cfg(unix)]
fn ensure_private_dir(path: &Path, expected_uid: u32) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                bail!(
                    "{path:?} is a symlink; refusing to use or modify it — remove the link manually"
                );
            }
            if !meta.is_dir() {
                bail!("{path:?} is {:?}, not a directory", meta.file_type());
            }
            if meta.uid() != expected_uid {
                bail!(
                    "{path:?} is owned by uid {} (current user uid {}); refusing to use it",
                    meta.uid(),
                    expected_uid
                );
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let mut builder = std::fs::DirBuilder::new();
            builder.mode(RUNTIME_DIR_MODE);
            builder
                .create(path)
                .with_context(|| format!("failed to create the runtime IPC directory {path:?}"))?;
        }
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {path:?}"));
        }
    }
    set_owner_only_mode(path)
}

/// Apply mode 0700 to a directory already verified as ours and confirm the mode
/// on disk. Chmod by path is safe here: only the owner of a directory can
/// change its permissions, so no other actor can widen them in between.
#[cfg(unix)]
fn set_owner_only_mode(path: &Path) -> Result<()> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(RUNTIME_DIR_MODE))
        .with_context(|| format!("failed to set owner-only permissions on {path:?}"))?;
    let mode = std::fs::symlink_metadata(path)
        .with_context(|| format!("failed to re-inspect {path:?} after chmod"))?
        .permissions()
        .mode()
        & 0o777;
    if mode != RUNTIME_DIR_MODE {
        bail!("{path:?} has mode {mode:o} after chmod; expected owner-only {RUNTIME_DIR_MODE:o}");
    }
    Ok(())
}

/// Whether the path currently holds a stale socket that may be removed.
/// Returns `Ok(true)` when the path is a socket file owned by `expected_uid`,
/// and `Ok(false)` when nothing is there. A symlink, any other file type, or a
/// foreign owner is refused with a diagnosable error; the path is never
/// followed or removed in those cases.
#[cfg(unix)]
fn stale_socket_safe_to_remove(path: &Path, expected_uid: u32) -> Result<bool> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).with_context(|| format!("failed to inspect {path:?}")),
    };
    if meta.file_type().is_symlink() {
        bail!("{path:?} is a symlink; refusing to remove it — delete the link manually and re-run");
    }
    if !meta.file_type().is_socket() {
        bail!(
            "{path:?} is {:?}, not a Unix domain socket; refusing to remove it",
            meta.file_type()
        );
    }
    if meta.uid() != expected_uid {
        bail!(
            "{path:?} is owned by uid {} (current user uid {}); refusing to remove it",
            meta.uid(),
            expected_uid
        );
    }
    Ok(true)
}

/// Remove a stale Runtime socket, but only when it is provably our own socket
/// file: never a symlink, never another file type, never a foreign owner.
/// `Ok(false)` means the path was already gone. Unsafe paths surface as
/// diagnosable errors instead of being deleted.
#[cfg(unix)]
fn remove_stale_socket_at(path: &Path) -> Result<bool> {
    if !stale_socket_safe_to_remove(path, current_uid())? {
        return Ok(false);
    }
    std::fs::remove_file(path)
        .with_context(|| format!("failed to remove the stale runtime socket {path:?}"))?;
    Ok(true)
}

/// Remove the stale socket at the current Runtime IPC path, if one exists and
/// is safe to remove. Called after a bind failure when no live Runtime
/// answered, so a crashed server's leftover socket can be reclaimed.
#[cfg(unix)]
pub(crate) fn remove_stale_runtime_socket(_lock: &RuntimeStartupLock) -> Result<bool> {
    remove_stale_socket_at(&runtime_socket_path()?)
}

/// Apply owner-only (0600) permissions to a freshly bound Runtime socket and
/// verify its on-disk state: a socket file owned by the current user with
/// exactly mode 0600. Chmod by path is safe because the socket lives inside the
/// 0700 owner-only runtime directory, so no other user can replace the file
/// between our syscalls.
#[cfg(unix)]
pub(crate) fn verify_runtime_socket(path: &Path) -> Result<()> {
    let meta = std::fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect the runtime socket {path:?}"))?;
    if meta.file_type().is_symlink() {
        bail!("the runtime socket {path:?} is a symlink; refusing to serve it");
    }
    if !meta.file_type().is_socket() {
        bail!(
            "the runtime socket {path:?} is {:?}, not a Unix domain socket",
            meta.file_type()
        );
    }
    if meta.uid() != current_uid() {
        bail!(
            "the runtime socket {path:?} is owned by uid {} (current user uid {}); refusing to serve it",
            meta.uid(),
            current_uid()
        );
    }
    let mode = meta.permissions().mode() & 0o777;
    if mode != RUNTIME_SOCKET_MODE {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(RUNTIME_SOCKET_MODE))
            .with_context(|| {
                format!("failed to set owner-only permissions on the runtime socket {path:?}")
            })?;
        let mode = std::fs::symlink_metadata(path)
            .with_context(|| {
                format!("failed to re-inspect the runtime socket {path:?} after chmod")
            })?
            .permissions()
            .mode()
            & 0o777;
        if mode != RUNTIME_SOCKET_MODE {
            bail!(
                "the runtime socket {path:?} has mode {mode:o} after chmod; expected owner-only {RUNTIME_SOCKET_MODE:o}"
            );
        }
    }
    Ok(())
}

/// Bind a Unix filesystem local socket listener with owner-only permissions.
/// The socket mode is set through interprocess (`fchmod` before `bind`, immune
/// to umask races) when the platform supports it; otherwise the listener is
/// bound first and secured by path afterwards — safe because the socket is
/// created inside the 0700 owner-only runtime directory, which no other user
/// can traverse or modify. The resulting on-disk mode is always verified.
#[cfg(unix)]
pub(crate) fn bind_listener_at(path: &Path) -> io::Result<Listener> {
    use interprocess::os::unix::local_socket::ListenerOptionsExt;
    let name = path.to_fs_name::<GenericFilePath>()?;
    let secure = |listener: Listener| -> io::Result<Listener> {
        verify_runtime_socket(path).map_err(|error| io::Error::other(format!("{error:#}")))?;
        Ok(listener)
    };
    match ListenerOptions::new()
        .name(name.clone())
        .mode(RUNTIME_SOCKET_MODE as libc::mode_t)
        .create_sync()
    {
        Ok(listener) => secure(listener),
        Err(error) if error.kind() == ErrorKind::Unsupported => {
            // The platform cannot fchmod sockets (e.g. macOS): bind first, then
            // apply the mode by path and verify.
            let listener = ListenerOptions::new().name(name).create_sync()?;
            secure(listener)
        }
        Err(error) => Err(error),
    }
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
    use crate::protocol::WaitCondition;
    use std::io::Cursor;

    #[test]
    fn reads_exactly_one_frame() {
        let mut reader = BufReader::new(Cursor::new(b"one\ntwo\n"));
        assert_eq!(read_frame(&mut reader, None).unwrap(), b"one\n");
        assert_eq!(read_frame(&mut reader, None).unwrap(), b"two\n");
    }

    #[test]
    fn rejects_oversized_frames_before_unbounded_growth() {
        let input = vec![b'x'; MAX_FRAME_BYTES + 1];
        let mut reader = BufReader::new(Cursor::new(input));
        assert!(read_frame(&mut reader, None).is_err());
    }

    #[test]
    fn read_frame_with_a_fresh_deadline_accepts_prompt_frames() {
        let mut reader = BufReader::new(Cursor::new(b"one\n"));
        let deadline = Instant::now() + Duration::from_secs(60);
        assert_eq!(read_frame(&mut reader, Some(deadline)).unwrap(), b"one\n");
    }

    #[test]
    fn read_frame_with_an_expired_deadline_fails_before_reading() {
        let mut reader = BufReader::new(Cursor::new(b"one\n"));
        let expired = Instant::now() - Duration::from_secs(1);
        let error = read_frame(&mut reader, Some(expired)).unwrap_err();
        assert!(error.to_string().contains("timed out"), "{error:#}");
    }

    #[test]
    fn response_deadline_accommodates_documented_long_waits() {
        // The server clamps wait timeouts to 7_200_000 ms and read waits to
        // 300 s; the client deadline must never undercut either.
        let wait = Request::Wait {
            session: "s".to_owned(),
            for_condition: WaitCondition::TuiIdle,
            timeout_ms: Some(7_200_000),
        };
        assert!(
            response_deadline_for(&wait) >= Duration::from_millis(7_200_000) + IPC_RESPONSE_SLACK
        );

        let read = Request::Read {
            session: "s".to_owned(),
            cursor: Some(0),
            limit: Some(1024),
            wait_ms: Some(300_000),
        };
        assert!(
            response_deadline_for(&read) >= Duration::from_millis(300_000) + IPC_RESPONSE_SLACK
        );

        // Ordinary requests still get a bounded default rather than forever.
        let status = Request::ServerStatus;
        assert_eq!(response_deadline_for(&status), IPC_RESPONSE_SLACK);
        assert!(response_deadline_for(&status) < Duration::from_secs(120));
    }

    #[test]
    fn write_response_round_trips_an_envelope() {
        let response = ResponseEnvelope::failure("r1", "boom", "kaput");
        let mut sink = Cursor::new(Vec::new());
        write_response(&mut sink, &response).unwrap();
        let frame = sink.into_inner();
        assert_eq!(decode_response(&frame).unwrap(), response);
    }

    #[test]
    fn read_frame_with_deadline_times_out_for_a_silent_peer() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        // The peer never sends; the non-blocking read + deadline must give up
        // instead of pinning the thread forever.
        server.set_nonblocking(true).unwrap();
        let mut reader = BufReader::new(server);
        let started = Instant::now();
        let error = read_frame(
            &mut reader,
            Some(Instant::now() + Duration::from_millis(100)),
        )
        .unwrap_err();
        assert!(error.to_string().contains("timed out"), "{error:#}");
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[test]
    fn write_frame_all_times_out_for_a_peer_that_stops_draining() {
        // Exercise the Runtime's real IPC channel type (AF_UNIX socket on Unix,
        // named pipe on Windows) on a throwaway name — never the user's live
        // runtime path. Its pipe buffers are small and fixed on every platform,
        // so a peer that stops reading genuinely fills the channel — unlike a
        // TCP pair, where delayed ACKs and autotuned buffers keep freeing
        // headroom and can let a small frame slip through, hiding the write
        // deadline.
        #[cfg(unix)]
        let name = {
            let dir = std::env::temp_dir().join(format!(
                "grok-bridge-write-deadline-test-{}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            dir.join("test.sock")
                .to_fs_name::<GenericFilePath>()
                .unwrap()
        };
        #[cfg(windows)]
        let name = runtime_name().unwrap();
        let listener = ListenerOptions::new()
            .name(name.clone())
            .create_sync()
            .unwrap();
        let _client = Stream::connect(name).unwrap();
        let mut server = listener.accept().unwrap();
        // The peer never reads. Fill the pipe until a non-blocking write
        // signals backpressure: a WouldBlock/TimedOut error on either
        // platform, or Ok(0) on Windows PIPE_NOWAIT pipes, which report a
        // successful zero-byte write when the buffer is full. Unix pipes
        // instead accept bytes until the buffer is genuinely full and only
        // then start returning WouldBlock, so `filled` may stay 0 on Windows
        // but is expected to be large on Unix.
        server.set_nonblocking(true).unwrap();
        let filler = vec![0x55u8; 64 * 1024];
        let mut filled = 0usize;
        let pipe_full = loop {
            match server.write(&filler) {
                // Windows named pipes in nonblocking (PIPE_NOWAIT) mode return
                // Ok(0) when the write could not make progress (the pipe
                // buffer is full), with no implication that the peer closed;
                // WriteFileEx reports a successful zero-byte completion. That
                // is the platform's backpressure signal. On Unix, Ok(0) still
                // means the peer half-closed the connection and must not be
                // mistaken for a full buffer.
                #[cfg(windows)]
                Ok(0) => break true,
                #[cfg(not(windows))]
                Ok(0) => panic!("IPC peer closed while filling the send buffer"),
                Ok(written) => filled += written,
                Err(error)
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
                {
                    break true;
                }
                Err(error) => panic!("unexpected fill error: {error}"),
            }
            if filled > 16 * 1024 * 1024 {
                panic!("send buffer never blocked; cannot exercise the write deadline");
            }
        };
        assert!(
            pipe_full,
            "fill loop ended without a backpressure signal (WouldBlock/TimedOut, or Ok(0) on Windows) after {filled} bytes"
        );

        // A full-size frame can never fit in the small filled pipe, so the
        // stalled peer must surface as a bounded timeout instead of a
        // completed write.
        let started = Instant::now();
        let data = vec![0x66u8; MAX_FRAME_BYTES];
        let error = write_frame_all(&mut server, &data, Duration::from_millis(100)).unwrap_err();
        assert!(error.to_string().contains("timed out"), "{error:#}");
        assert!(started.elapsed() < Duration::from_secs(3));
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
}

/// Unix-only tests for owner-only Runtime IPC permissions and safe stale-socket
/// cleanup. Every test works on an explicit throwaway temp directory, never on
/// the user's real runtime path.
#[cfg(all(unix, test))]
mod unix_ipc_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    /// A unique throwaway temp directory, removed on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "grok-bridge-ipc-test-{label}-{}-{}",
                std::process::id(),
                NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Binds a real Unix domain socket at `path`, leaving the socket file in
    /// place when the listener is dropped.
    fn bind_test_socket(path: &Path) -> std::os::unix::net::UnixListener {
        std::os::unix::net::UnixListener::bind(path).unwrap()
    }

    #[test]
    fn fresh_dir_is_created_owner_only() {
        let temp = TempDir::new("dir-fresh");
        let dir = temp.path().join("runtime");
        ensure_private_dir(&dir, current_uid()).unwrap();
        let meta = std::fs::symlink_metadata(&dir).unwrap();
        assert!(meta.is_dir());
        assert_eq!(meta.uid(), current_uid());
        assert_eq!(meta.permissions().mode() & 0o777, RUNTIME_DIR_MODE);
    }

    #[test]
    fn existing_owner_dir_is_tightened_to_owner_only() {
        let temp = TempDir::new("dir-tighten");
        let dir = temp.path().join("runtime");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        ensure_private_dir(&dir, current_uid()).unwrap();
        let mode = std::fs::symlink_metadata(&dir)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, RUNTIME_DIR_MODE);
    }

    #[test]
    fn symlink_dir_is_rejected_without_being_followed() {
        let temp = TempDir::new("dir-link");
        let real = temp.path().join("real");
        std::fs::create_dir_all(&real).unwrap();
        let link = temp.path().join("runtime");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let error = ensure_private_dir(&link, current_uid()).unwrap_err();
        assert!(error.to_string().contains("symlink"), "{error:#}");
        // The link and its target must be untouched.
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(real.is_dir());
    }

    #[test]
    fn regular_file_dir_is_rejected_without_deletion() {
        let temp = TempDir::new("dir-file");
        let file = temp.path().join("runtime");
        std::fs::write(&file, b"not a dir").unwrap();
        let error = ensure_private_dir(&file, current_uid()).unwrap_err();
        assert!(error.to_string().contains("not a directory"), "{error:#}");
        assert_eq!(std::fs::read(&file).unwrap(), b"not a dir");
    }

    #[test]
    fn foreign_owner_dir_is_rejected() {
        let temp = TempDir::new("dir-owner");
        let dir = temp.path().join("runtime");
        std::fs::create_dir_all(&dir).unwrap();
        // Exercise the ownership check with a uid that is not ours; the real
        // path stays untouched and unrejected.
        let error = ensure_private_dir(&dir, current_uid() + 1).unwrap_err();
        assert!(error.to_string().contains("uid"), "{error:#}");
        assert!(dir.is_dir());
    }

    #[test]
    fn runtime_dir_uses_only_absolute_owner_only_xdg_bases() {
        let temp = TempDir::new("xdg-base");
        let trusted = temp.path().join("trusted");
        std::fs::create_dir_all(&trusted).unwrap();
        std::fs::set_permissions(&trusted, std::fs::Permissions::from_mode(0o700)).unwrap();
        let fallback = temp.path().join("fallback");
        std::fs::create_dir_all(&fallback).unwrap();
        std::fs::set_permissions(&fallback, std::fs::Permissions::from_mode(0o700)).unwrap();
        let trusted_canonical = std::fs::canonicalize(&trusted).unwrap();
        let fallback_canonical = std::fs::canonicalize(&fallback).unwrap();

        assert!(trusted_xdg_runtime_base(&trusted, current_uid()));
        assert_eq!(
            runtime_dir_from(
                Some(trusted.clone().into_os_string()),
                fallback.clone(),
                current_uid(),
            )
            .unwrap(),
            trusted_canonical.join(runtime_identity())
        );

        assert_eq!(
            runtime_dir_from(
                Some(OsString::from("relative/runtime")),
                fallback.clone(),
                current_uid(),
            )
            .unwrap(),
            fallback_canonical.join(runtime_identity())
        );
        assert!(!trusted_xdg_runtime_base(&trusted, current_uid() + 1));

        std::fs::set_permissions(&trusted, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(!trusted_xdg_runtime_base(&trusted, current_uid()));
        assert_eq!(
            runtime_dir_from(
                Some(trusted.into_os_string()),
                fallback.clone(),
                current_uid(),
            )
            .unwrap(),
            fallback_canonical.join(runtime_identity())
        );
    }

    #[test]
    fn runtime_base_rejects_replaceable_parent_and_accepts_sticky_parent() {
        let temp = TempDir::new("runtime-parent");
        let fallback = temp.path().join("fallback");
        std::fs::create_dir_all(&fallback).unwrap();
        std::fs::set_permissions(&fallback, std::fs::Permissions::from_mode(0o700)).unwrap();

        let shared = temp.path().join("shared");
        let candidate = shared.join("candidate");
        std::fs::create_dir_all(&candidate).unwrap();
        std::fs::set_permissions(&candidate, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(!trusted_xdg_runtime_base(&candidate, current_uid()));
        assert_eq!(
            runtime_dir_from(
                Some(candidate.clone().into_os_string()),
                fallback.clone(),
                current_uid(),
            )
            .unwrap(),
            std::fs::canonicalize(&fallback)
                .unwrap()
                .join(runtime_identity())
        );

        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o1777)).unwrap();
        assert!(trusted_xdg_runtime_base(&candidate, current_uid()));
        assert_eq!(
            runtime_dir_from(
                Some(candidate.clone().into_os_string()),
                fallback,
                current_uid(),
            )
            .unwrap(),
            std::fs::canonicalize(candidate)
                .unwrap()
                .join(runtime_identity())
        );
    }

    #[test]
    fn runtime_startup_lock_serializes_concurrent_starters() {
        let temp = TempDir::new("startup-lock");
        let dir = temp.path().join("runtime");
        ensure_private_dir(&dir, current_uid()).unwrap();
        let first = acquire_runtime_startup_lock_at(&dir, Duration::from_secs(1)).unwrap();
        let acquired = Arc::new(AtomicBool::new(false));
        let acquired_in_thread = Arc::clone(&acquired);
        let thread_dir = dir.clone();
        let waiter = std::thread::spawn(move || {
            let second =
                acquire_runtime_startup_lock_at(&thread_dir, Duration::from_secs(2)).unwrap();
            acquired_in_thread.store(true, Ordering::Release);
            drop(second);
        });
        std::thread::sleep(Duration::from_millis(50));
        assert!(!acquired.load(Ordering::Acquire));
        drop(first);
        waiter.join().unwrap();
        assert!(acquired.load(Ordering::Acquire));
    }

    #[test]
    fn stale_socket_symlink_is_refused_without_being_followed() {
        let temp = TempDir::new("sock-link");
        let target = temp.path().join("target");
        std::fs::write(&target, b"x").unwrap();
        let link = temp.path().join("runtime.sock");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let error = stale_socket_safe_to_remove(&link, current_uid()).unwrap_err();
        assert!(error.to_string().contains("symlink"), "{error:#}");
        // The symlink itself is never followed or removed.
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"x");
    }

    #[test]
    fn stale_socket_wrong_file_type_is_refused_without_deletion() {
        let temp = TempDir::new("sock-file");
        let file = temp.path().join("runtime.sock");
        std::fs::write(&file, b"keep me").unwrap();
        let error = stale_socket_safe_to_remove(&file, current_uid()).unwrap_err();
        assert!(
            error.to_string().contains("not a Unix domain socket"),
            "{error:#}"
        );
        assert_eq!(std::fs::read(&file).unwrap(), b"keep me");
    }

    #[test]
    fn stale_socket_foreign_owner_is_refused() {
        let temp = TempDir::new("sock-owner");
        let socket = temp.path().join("runtime.sock");
        let listener = bind_test_socket(&socket);
        drop(listener);
        let error = stale_socket_safe_to_remove(&socket, current_uid() + 1).unwrap_err();
        assert!(error.to_string().contains("uid"), "{error:#}");
        assert!(
            std::fs::symlink_metadata(&socket)
                .unwrap()
                .file_type()
                .is_socket()
        );
    }

    #[test]
    fn owned_stale_socket_is_removed() {
        let temp = TempDir::new("sock-remove");
        let socket = temp.path().join("runtime.sock");
        let listener = bind_test_socket(&socket);
        drop(listener);
        assert!(socket.exists());
        assert!(remove_stale_socket_at(&socket).unwrap());
        assert!(!socket.exists());
    }

    #[test]
    fn missing_stale_socket_is_not_an_error() {
        let temp = TempDir::new("sock-missing");
        let socket = temp.path().join("runtime.sock");
        assert!(!remove_stale_socket_at(&socket).unwrap());
    }

    #[test]
    fn bound_socket_is_owner_only_socket_for_current_user() {
        let temp = TempDir::new("sock-mode");
        let path = temp.path().join("runtime.sock");
        let _listener = bind_listener_at(&path).unwrap();
        let meta = std::fs::symlink_metadata(&path).unwrap();
        assert!(meta.file_type().is_socket());
        assert_eq!(meta.uid(), current_uid());
        assert_eq!(meta.permissions().mode() & 0o777, RUNTIME_SOCKET_MODE);
    }

    #[test]
    fn occupied_socket_bind_fails_with_addr_in_use() {
        let temp = TempDir::new("sock-occupied");
        let path = temp.path().join("runtime.sock");
        let _first = bind_listener_at(&path).unwrap();
        let error = bind_listener_at(&path).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::AddrInUse);
    }

    #[test]
    fn runtime_socket_path_lives_inside_the_runtime_dir() {
        assert_eq!(
            runtime_socket_path().unwrap(),
            runtime_dir().unwrap().join("runtime.sock")
        );
    }
}
