//! The bundled C++ runtime must not capture symbols used by the host runtime.
//!
//! The runtime cases in `runtime_faces.rs` check the effect in a linked binary;
//! this checks the archive's symbols directly.
//!
//! `libchdb.so` / `.dylib` gate their exports at link time, with a version
//! script on Linux and an exported-symbols list on macOS, which is why dynamic
//! linking never had this problem. An archive has no link step of its own: every
//! symbol it carries reaches the final artifact and takes part in resolution. So
//! for a static build the gate must be baked into the archive. Older archives
//! hide the bundled runtime's definitions, while newer ones prelink and localize
//! them on Linux or rename them on macOS.
//!
//! Two reasons to check the cause and not only the effect. It holds on Linux,
//! where the visibility can regress with no symptom to observe, so no runtime
//! case would notice. And it needs no elevated deployment target, no linker and
//! no process: it reads the archive this build is about to link.
//!
//! Not a reimplementation for its own sake — chdb-core runs the same assertion
//! over its own build output (`chdb/build/check_static_lib_hermetic.sh`, gate
//! 2). That one guards the release; this one guards what actually arrived.
//!
//! Measured across older engines: v26.7.0 shows 2365 externally visible
//! definitions among 58 runtime members, and v26.7.2-rc.2/v26.7.3 show 0.

mod common;

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

/// The archive members holding the bundled runtime, by the naming chdb-core's
/// `create_static_libchdb.py` gives them.
const RUNTIME_MEMBER_PREFIXES: [&str; 3] = ["libcxx__", "libcxxabi__", "libunwind__"];

/// How many offending symbols to print before giving up on the reader.
const OFFENDERS_SHOWN: usize = 20;

#[test]
fn bundled_runtime_symbols_cannot_bind_host() {
    let artifact = Path::new(env!("CHDB_LINKED_ARTIFACT"));
    assert!(
        !artifact.as_os_str().is_empty(),
        "the build script did not record which artifact it linked"
    );

    if artifact.extension().is_none_or(|kind| kind != "a") {
        println!(
            "not applicable: {} is not an archive. A dynamic library gates its \
             exports at its own link step, so there is nothing here to check.",
            artifact.display()
        );
        return;
    }

    let members = runtime_members(artifact);
    if members.is_empty() {
        // New Linux archives prelink all objects into one member and localize
        // everything outside the C API. The old per-runtime-member check no
        // longer describes that layout; verify its exported surface here and
        // exercise the host unwinder in runtime_faces.
        let all_members = run("ar", &[OsStr::new("t"), artifact.as_os_str()], None);
        let names: Vec<_> = all_members.lines().collect();
        assert!(
            cfg!(target_os = "linux")
                && names.len() == 1
                && names[0].starts_with("libchdb")
                && names[0].ends_with(".o"),
            "unknown static archive layout: {names:?}"
        );
        let (defined, visible) = symbols(&[artifact.to_path_buf()]);
        assert!(defined > 0 && visible.iter().any(|name| name == "chdb_connect"));
        assert!(
            visible.len() <= 100
                && !visible.iter().any(|name| {
                    name.starts_with("_Unwind_")
                        || name.starts_with("unw_")
                        || name.starts_with("__cxa_")
                }),
            "prelinked archive exports unexpected runtime symbols: {visible:?}"
        );
        return;
    }

    let scratch = common::tempdir();
    let objects = extract(artifact, &members, scratch.path());
    assert_eq!(
        objects.len(),
        members.len(),
        "extracted {} of {} runtime members",
        objects.len(),
        members.len()
    );

    let (defined, visible) = symbols(&objects);

    println!(
        "{}: {} runtime members, {} definitions, {} externally visible",
        artifact.display(),
        members.len(),
        defined,
        visible.len()
    );

    if !visible.is_empty() {
        for symbol in visible.iter().take(OFFENDERS_SHOWN) {
            println!("  {symbol}");
        }
        if visible.len() > OFFENDERS_SHOWN {
            println!("  ... and {} more", visible.len() - OFFENDERS_SHOWN);
        }
    }

    // The old layout hides these definitions; the new macOS layout renames
    // every one so a host can never resolve a runtime reference to it.
    assert!(
        visible.is_empty()
            || (cfg!(target_os = "macos") && visible.iter().all(|s| s.ends_with(".chdb"))),
        "{} of {} definitions in the bundled runtime are visible from outside the \
         archive under an unrenamed name; a static build can then bind a host \
         runtime call to the bundled C++ runtime",
        visible.len(),
        defined
    );
}

/// Runs `program`, requiring it to succeed, and returns its stdout.
fn run(program: &str, args: &[&OsStr], cwd: Option<&Path>) -> String {
    let mut command = common::command(program);
    command.args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }

    let output = command
        .output()
        .unwrap_or_else(|e| panic!("cannot run {program}: {e}"));
    assert!(
        output.status.success(),
        "{program} exited with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );

    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn runtime_members(artifact: &Path) -> Vec<String> {
    run("ar", &[OsStr::new("t"), artifact.as_os_str()], None)
        .lines()
        .map(str::trim)
        .filter(|member| {
            RUNTIME_MEMBER_PREFIXES
                .iter()
                .any(|prefix| member.starts_with(prefix))
        })
        .map(str::to_owned)
        .collect()
}

/// Extracts `members` into `into`, returning the object files that appeared.
fn extract(artifact: &Path, members: &[String], into: &Path) -> Vec<PathBuf> {
    // `ar x` writes into the working directory, so the archive has to be named
    // absolutely from there.
    let artifact = artifact
        .canonicalize()
        .unwrap_or_else(|e| panic!("cannot resolve {}: {e}", artifact.display()));

    let mut args: Vec<&OsStr> = vec![OsStr::new("x"), artifact.as_os_str()];
    args.extend(members.iter().map(|member| OsStr::new(member.as_str())));
    run("ar", &args, Some(into));

    members
        .iter()
        .map(|member| into.join(member))
        .filter(|path| path.exists())
        .collect()
}

/// The distinct definitions across `objects`, and which of them are visible
/// from outside their object.
fn symbols(objects: &[PathBuf]) -> (usize, Vec<String>) {
    let args: Vec<&OsStr> = objects.iter().map(|path| path.as_os_str()).collect();

    if cfg!(target_os = "macos") {
        // Only the -m listing spells out "private external". Plain -g shows
        // hidden symbols as well and would report a gated archive as wide open.
        let mut nm = vec![
            OsStr::new("-m"),
            OsStr::new("-g"),
            OsStr::new("--defined-only"),
        ];
        nm.extend_from_slice(&args);
        let listing = run("nm", &nm, None);
        let mut defined = Vec::new();
        let mut visible = Vec::new();
        for line in listing.lines() {
            let Some(symbol) = line.split_whitespace().next_back() else {
                continue;
            };
            let Some(symbol) = symbol.strip_prefix('_') else {
                continue;
            };
            defined.push(symbol.to_owned());
            if !line.contains("private external") {
                visible.push(symbol.to_owned());
            }
        }
        counted(defined, visible)
    } else {
        let mut readelf = vec![OsStr::new("-sW")];
        readelf.extend_from_slice(&args);
        let listing = run("readelf", &readelf, None);
        let mut defined = Vec::new();
        let mut visible = Vec::new();
        for line in listing.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            // Num: Value Size Type Bind Vis Ndx Name
            if fields.len() < 8 || !fields[0].ends_with(':') {
                continue;
            }
            let (bind, vis, ndx, name) = (fields[4], fields[5], fields[6], fields[7]);
            if ndx == "UND" || !matches!(bind, "GLOBAL" | "WEAK") {
                continue;
            }
            let name = name.split('@').next().unwrap_or_default();
            if name.is_empty() {
                continue;
            }
            defined.push(name.to_owned());
            if matches!(vis, "DEFAULT" | "PROTECTED") {
                visible.push(name.to_owned());
            }
        }
        counted(defined, visible)
    }
}

/// Both counted as distinct names, so the two numbers in a failure message are
/// in the same unit.
fn counted(mut defined: Vec<String>, mut visible: Vec<String>) -> (usize, Vec<String>) {
    defined.sort_unstable();
    defined.dedup();
    visible.sort_unstable();
    visible.dedup();
    (defined.len(), visible)
}
