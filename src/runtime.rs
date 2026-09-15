//! Process-wide engine lifecycle.
//!
//! chDB installs signal handlers and starts threads for the whole process, not
//! for a connection. Both facts are invisible from the connection API and both
//! matter to a host that has its own signal handling or its own teardown
//! sequence, so the controls for them live here.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::bindings;
use crate::error::{Error, Result};
use crate::registry;

/// Choose whether chDB installs process-wide signal handlers.
///
/// May be called at any time, not just before the first connection. Passing
/// `false` sets a process-wide disable flag *and* immediately resets any
/// handlers already installed (chDB's own `chdb_set_signal_handlers_enabled`
/// calls `chdb_reset_signal_handlers` internally when disabling). The flag is
/// consulted whenever the engine installs handlers — at connect for this
/// crate's [`Connection`](crate::connection::Connection) API, and at the
/// start of every query on the one-shot cmdline entry — so a host with an
/// existing connection can still opt out and have it stick.
///
/// Calling this before the first connection is still the cleanest way to
/// ensure the handlers are never installed at all, since it removes the brief
/// window where a query could run before the preference is set. But it is no
/// longer required: this call is infallible and works after connections are
/// already open.
///
/// # Examples
///
/// ```no_run
/// chdb_rust::runtime::signal_handlers(false);
/// let conn = chdb_rust::connection::Connection::open_in_memory()?;
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
pub fn signal_handlers(enabled: bool) {
    // Wraps chdb_set_signal_handlers_enabled. Sets the process-wide disable
    // flag consulted at handler install; disabling also resets any handlers
    // already installed, as an immediate effect of this call.
    unsafe { bindings::chdb_set_signal_handlers_enabled(i32::from(enabled)) };
}

/// Restore every signal handler chDB installed to `SIG_DFL`.
///
/// Does not set the disable flag. Handlers are installed once at connect, not
/// at the start of every [`Connection::query`](crate::connection::Connection::query),
/// so a later query on that connection leaves the disposition at `SIG_DFL`. A
/// later connect may install them again. For an opt-out that also covers a
/// later connect, use [`signal_handlers`]`(false)`, which sets the flag
/// consulted at install as well as resetting.
///
/// # Examples
///
/// ```no_run
/// let conn = chdb_rust::connection::Connection::open_in_memory()?;
/// let _ = conn.query("SELECT 1", chdb_rust::format::OutputFormat::TabSeparated)?;
/// chdb_rust::runtime::reset_signal_handlers();
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
pub fn reset_signal_handlers() {
    // Wraps chdb_reset_signal_handlers. Safe at any time; resets disposition
    // only, does not touch the disable flag.
    unsafe { bindings::chdb_reset_signal_handlers() };
}

/// Set once a shutdown has actually succeeded.
///
/// A refusal because connections are still open leaves the engine usable, and
/// this flag correctly stays false for that case — that logic is not changed
/// here. But per `chdb.h`'s own wording on `chdb_shutdown`, once it starts,
/// "the library is closed for business for the rest of the process, whether
/// or not it manages to stop every thread". So a refusal because some thread
/// could not be stopped still closes the engine, yet leaves this flag unset:
/// [`ensure_running`] then can't see it, and a later `open()` surfaces the
/// engine's own `ConnectionFailed` instead of the more informative
/// [`Error::EngineShutDown`] that flag exists to produce.
static SHUT_DOWN: AtomicBool = AtomicBool::new(false);

/// Stop the engine, joining every thread chDB started.
///
/// Call this before a host teardown sequence of its own — global destructors, a
/// finalizing language runtime, a sanitizer exit handler — which would
/// otherwise race the engine's still-running threads. A process that simply
/// exits does not need it.
///
/// This is one-way. Once it succeeds, no further connection can be opened in
/// this process. Calling it again is harmless.
///
/// # Errors
///
/// Returns [`Error::ConnectionsStillOpen`] if any connection is open, and
/// [`Error::Unknown`] if the engine could not stop every thread. In the
/// latter case `chdb.h` documents the engine as closed for the process
/// regardless — "whether or not it manages to stop every thread" — so this
/// error means the engine is already shut down even though it is reported as
/// a failure; the crate's internal shut-down flag is not set for it (see
/// `SHUT_DOWN`), so a subsequent `open()` will surface `ConnectionFailed`
/// rather than [`Error::EngineShutDown`].
///
/// # Examples
///
/// ```no_run
/// let conn = chdb_rust::connection::Connection::open_in_memory()?;
/// drop(conn);
/// chdb_rust::runtime::shutdown()?;
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
pub fn shutdown() -> Result<()> {
    if SHUT_DOWN.load(Ordering::SeqCst) {
        return Ok(());
    }

    // This count is a snapshot: a connect or drop can race it before
    // chdb_shutdown below runs. That window only sharpens the error message,
    // it cannot corrupt state — chdb.h (1078-1080) guarantees chdb_shutdown is
    // safe to call concurrently with chdb_connect because callers are
    // serialized, so a racing connect either gets in first and is counted, or
    // arrives later and is refused. Worst case here is a less specific error
    // (falling through to chdb_shutdown's own refusal) rather than this
    // check's friendlier `ConnectionsStillOpen`.
    let count = registry::refs();
    if count != 0 {
        return Err(Error::ConnectionsStillOpen { count });
    }

    // Wraps chdb_shutdown. Refuses while the engine holds a connection, which
    // the check above should already have caught; a refusal here means some
    // other crate in this process holds one.
    let state = unsafe { bindings::chdb_shutdown() };
    if state != bindings::chdb_state_CHDBSuccess {
        return Err(Error::Unknown);
    }

    SHUT_DOWN.store(true, Ordering::SeqCst);
    Ok(())
}

/// Whether a new connection may still be opened.
pub(crate) fn ensure_running() -> Result<()> {
    if SHUT_DOWN.load(Ordering::SeqCst) {
        return Err(Error::EngineShutDown);
    }
    Ok(())
}
