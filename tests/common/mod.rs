// Shared by several test binaries, each of which uses a different part of it.
#![allow(dead_code)]

use std::ffi::OsStr;
use std::process::Command;

pub fn tempdir() -> tempdir::TempDir {
    tempdir::TempDir::new("chdb-rust").expect("failed to create temp dir")
}

/// A [`Command`] whose stdio redirection survives static linking.
///
/// A statically linked artifact resolves `posix_spawn` and the whole
/// `posix_spawn_file_actions_*` family against `libchdb.a`, which carries
/// ClickHouse's glibc-compatibility shims. That `posix_spawn` deliberately does
/// not walk file actions — its own source says "callers that need file actions
/// must avoid this stub or fall back to fork+exec" — so the standard library
/// records a dup2 action, calls it, gets no error back, and the child quietly
/// inherits this process's stdout. Measured against the published archive on
/// linux/aarch64: captured output came back empty while the child's answer
/// appeared on the parent's stdout.
///
/// An empty `pre_exec` closure is what makes the standard library skip the
/// `posix_spawn` fast path, which is the fallback that source recommends. The
/// fork/exec path calls `dup2` directly, and that symbol the archive does not
/// define.
pub fn command(program: impl AsRef<OsStr>) -> Command {
    let mut command = Command::new(program);

    // SAFETY: the closure allocates nothing, takes no locks and only returns
    // Ok, which is all that is allowed between fork and exec.
    #[cfg(unix)]
    unsafe {
        use std::os::unix::process::CommandExt as _;
        command.pre_exec(|| Ok(()));
    }

    command
}
