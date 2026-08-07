# Local Windows Job admission patch

This directory vendors the crates.io `portable-pty` 0.9.0 source, package
checksum `b4a596a2b3d2752d94f51fac2d4a96737b8705dddd311a32b9af47211f08671e`,
under its original MIT license.

The local patch adds an `Arc<OwnedHandle>` Windows Job Object handle to
`CommandBuilder` and supplies it through `PROC_THREAD_ATTRIBUTE_JOB_LIST`
alongside the ConPTY attribute. The Arc keeps the handle alive through the
synchronous `CreateProcessW` call; the session retains another Arc for close
and wait. This makes Job membership part of process creation, before the new
process can execute and create descendants. Non-Windows behavior is unchanged.

The patch changes `cmdbuilder.rs`, `win/procthreadattr.rs`, and
`win/psuedocon.rs`. It requires Windows 10 / Windows Server 2016 or newer,
where `PROC_THREAD_ATTRIBUTE_JOB_LIST` is available, within the same supported
range as ConPTY. Native process-tree tests
must run on the repository's existing Windows x64 and ARM CI jobs.

Remove the patch after upstream exposes equivalent pre-spawn Job admission.
