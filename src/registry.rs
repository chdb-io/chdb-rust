//! Which data path the engine is bound to, and how many handles hold it.
//!
//! chDB embeds one engine per process, serving one storage path. Any number of
//! connections to that path may be open at once; a different path is refused
//! until they are all closed. The engine reports a refusal as a null connection
//! and nothing more, so the reason is kept here instead.
//!
//! Connections opened against libchdb by another crate in the same process are
//! not visible here, and the engine would refuse those the same way.

use std::sync::{Mutex, OnceLock};

use crate::error::{Error, Result};

/// What the engine binds when no path is given, which is why an in-memory
/// connection and an on-disk one cannot be open together.
pub(crate) const IN_MEMORY: &str = ":memory:";

#[derive(Default)]
struct Registry {
    /// Resolved identity of the live engine, or `None` when nothing is open.
    active: Option<String>,
    /// How many handles are holding `active`.
    refs: usize,
}

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Registry::default()))
}

/// A held reference to the live engine. Releasing it lets a later connection
/// bind a different path.
#[derive(Debug)]
pub(crate) struct Slot {
    key: String,
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut registry = match registry().lock() {
            Ok(registry) => registry,
            // Refusing to decrement on a poisoned lock would strand the
            // engine for the rest of the process.
            Err(poisoned) => poisoned.into_inner(),
        };
        registry.refs = registry.refs.saturating_sub(1);
        if registry.refs == 0 {
            registry.active = None;
        }
        debug_assert!(
            registry.refs == 0 || registry.active.as_deref() == Some(self.key.as_str()),
            "the registry holds {:?} while a slot for {:?} was released",
            registry.active,
            self.key
        );
    }
}

/// Claims a reference to the engine on `key`, or explains why it cannot.
pub(crate) fn acquire(key: String) -> Result<Slot> {
    let mut registry = match registry().lock() {
        Ok(registry) => registry,
        Err(poisoned) => poisoned.into_inner(),
    };

    match registry.active.as_deref() {
        Some(active) if active != key => {
            return Err(Error::PathConflict {
                active: active.to_string(),
                requested: key,
            })
        }
        Some(_) => {}
        None => registry.active = Some(key.clone()),
    }

    registry.refs += 1;
    Ok(Slot { key })
}

/// The path the engine is currently bound to, or `None` when it is not running.
pub(crate) fn active_path() -> Option<String> {
    match registry().lock() {
        Ok(registry) => registry.active.clone(),
        Err(poisoned) => poisoned.into_inner().active.clone(),
    }
}

/// How many handles are holding the live engine.
pub(crate) fn refs() -> usize {
    match registry().lock() {
        Ok(registry) => registry.refs,
        Err(poisoned) => poisoned.into_inner().refs,
    }
}

/// The engine path named by a connection's arguments.
pub(crate) fn key_from_args(args: &[&str]) -> String {
    args.iter()
        .find_map(|arg| arg.strip_prefix("--path="))
        .map_or_else(|| IN_MEMORY.to_string(), resolve_key)
}

/// The identity two handles are compared by: the `--path=` value itself.
///
/// Not normalized, because the engine compares these strings as given: an
/// identical string attaches, the same directory with a trailing slash is
/// refused. Treating more as equivalent than the engine does would wave a
/// handle through for it to reject. Spellings converge earlier, in
/// [`SessionBuilder`].
///
/// [`SessionBuilder`]: crate::session::SessionBuilder
pub(crate) fn resolve_key(path: &str) -> String {
    if path.is_empty() {
        return IN_MEMORY.to_string();
    }
    path.to_string()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn no_path_argument_means_in_memory() {
        assert_eq!(key_from_args(&[]), IN_MEMORY);
        assert_eq!(key_from_args(&["--log-level=debug"]), IN_MEMORY);
        assert_eq!(key_from_args(&["--path=:memory:"]), IN_MEMORY);
    }

    #[test]
    fn the_path_argument_is_found_among_others() {
        let key = key_from_args(&["--log-level=debug", "--path=/tmp", "--max_threads=2"]);
        assert_eq!(key, resolve_key("/tmp"));
    }

    #[test]
    fn two_spellings_of_one_directory_are_two_keys() {
        assert_ne!(resolve_key("/tmp"), resolve_key("/tmp/"));
        assert_ne!(resolve_key("db"), resolve_key("./db"));
        assert_ne!(resolve_key("file:/tmp"), resolve_key("/tmp"));
    }

    #[test]
    fn an_identical_spelling_is_one_key() {
        assert_eq!(resolve_key("/tmp/db"), resolve_key("/tmp/db"));
    }
}
