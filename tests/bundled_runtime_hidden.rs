//! The bundled C++ runtime must not be visible from outside the archive.
//!
//! This is the cause the runtime cases in `runtime_faces.rs` observe the effect
//! of, checked directly and without linking anything.
//!
//! `libchdb.so` / `.dylib` gate their exports at link time, with a version
//! script on Linux and an exported-symbols list on macOS, which is why dynamic
//! linking never had this problem. An archive has no link step of its own: every
//! symbol it carries reaches the final artifact and takes part in resolution. So
//! for a static build the gate has to be compiled into the objects, and this
//! asserts that it was — that the three bundled runtime targets have no
//! externally visible definitions at all.
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
//! The archive arrives in one of two layouts, and the check follows it:
//!
//! - Separate runtime members, `libcxx__*`, `libcxxabi__*` and `libunwind__*`,
//!   compiled hidden. Those members must have no externally visible definitions.
//!   This is the macOS archive, and the Linux one before chdb-core#261.
//! - Prelinked, on Linux since chdb-core#261. Hidden turned out not to be
//!   enough: a hidden definition in an archive member still satisfies a
//!   reference from any other object in the consumer's link, which is how
//!   Rust's std ended up calling the bundled libunwind (chdb-rust#53). So the
//!   archive is now one relocatable object, made by `ld -r`, that keeps only the
//!   C API global. There are no runtime members left to name, so the runtime is
//!   looked for inside that object instead, and none of it may be global at any
//!   visibility.
//!
//! Measured across the engines this crate has been pinned to: v26.7.0 shows
//! 2365 externally visible definitions among 58 runtime members, and
//! v26.7.2-rc.2 and v26.7.3 show 0 among the same 58. v26.11.0-rc.1 is the first
//! pin whose Linux archive is prelinked: one member, `libchdb.o`, whose 68 global
//! definitions are all C API and none of them the runtime's, on x86_64 and
//! aarch64 alike. Its macOS archive keeps the member layout, with 0 visible
//! among 59. v26.11.0-rc.2 measures the same on all three.

mod common;

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

/// The archive members holding the bundled runtime, by the naming chdb-core's
/// `create_static_libchdb.py` gives them.
const RUNTIME_MEMBER_PREFIXES: [&str; 3] = ["libcxx__", "libcxxabi__", "libunwind__"];

/// One definition from each bundled runtime — libc++, libc++abi, libunwind — by
/// which a prelinked archive shows it still carries the runtime. Without them a
/// clean result could equally mean the runtime is gone, or that the symbol table
/// was stripped, and the test would have quietly stopped testing anything.
const RUNTIME_ANCHORS: [&str; 3] = [
    "_ZNSt3__15mutex4lockEv",
    "__cxa_throw",
    "_Unwind_RaiseException",
];

/// How many offending symbols to print before giving up on the reader.
const OFFENDERS_SHOWN: usize = 20;

#[test]
fn bundled_runtime_objects_export_nothing() {
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
    // Only the Linux archive is prelinked; the macOS one keeps its members and
    // is hardened by renaming instead, so there the old check still applies.
    if members.is_empty() && cfg!(target_os = "linux") {
        prelinked_runtime_is_local(artifact);
        return;
    }
    // Zero would mean the naming changed and this test had quietly stopped
    // testing anything, which is worse than a failure.
    assert!(
        !members.is_empty(),
        "no {RUNTIME_MEMBER_PREFIXES:?} members among the archive's members: either the \
         bundled runtime is no longer a separate set of objects, or chdb-core renamed them"
    );

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

    // Asserted at zero rather than "disjoint from this host's system runtime".
    // These targets are built to have no externally visible definitions at all,
    // and a symbol that merely happens to be absent from the running OS version
    // would otherwise slip through.
    assert!(
        visible.is_empty(),
        "{} of {} definitions in the bundled runtime are visible from outside the \
         archive; a static build can then bind a call made inside the engine to \
         another copy of the C++ runtime",
        visible.len(),
        defined
    );
}

/// The prelinked layout: the whole engine as one relocatable object, with the
/// runtime inside it.
///
/// Every binding that takes part in resolution counts — `GLOBAL`, `WEAK` and
/// `UNIQUE` — at every visibility, since hidden is exactly what failed to keep
/// the runtime out of the consumer's link. Only `LOCAL` is out of reach.
fn prelinked_runtime_is_local(artifact: &Path) {
    let listing = run("readelf", &[OsStr::new("-sW"), artifact.as_os_str()], None);

    let mut anchors = Vec::new();
    let mut globals = Vec::new();
    for line in listing.lines() {
        // Millions of lines, nearly all of them LOCAL definitions that matter
        // only when they are an anchor. A substring test turns those away before
        // the line is split, which in a debug build is most of the cost.
        let global_binding = [" GLOBAL ", " WEAK ", " UNIQUE "]
            .iter()
            .any(|bind| line.contains(bind));
        if !global_binding && !RUNTIME_ANCHORS.iter().any(|anchor| line.ends_with(anchor)) {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        // Num: Value Size Type Bind Vis Ndx Name. Ndx and Name are read from the
        // end, because aarch64 can put a [VARIANT_PCS] marker after Vis. The
        // numeric index keeps the column header line out.
        if fields.len() < 8
            || fields[0]
                .strip_suffix(':')
                .is_none_or(|index| index.parse::<u64>().is_err())
        {
            continue;
        }
        let bind = fields[4];
        let (ndx, name) = (fields[fields.len() - 2], fields[fields.len() - 1]);
        if ndx == "UND" {
            continue;
        }
        let name = name.split('@').next().unwrap_or_default();
        if name.is_empty() {
            continue;
        }
        if RUNTIME_ANCHORS.contains(&name) {
            anchors.push(name.to_owned());
        }
        if matches!(bind, "GLOBAL" | "WEAK" | "UNIQUE") {
            globals.push(name.to_owned());
        }
    }

    for names in [&mut anchors, &mut globals] {
        names.sort_unstable();
        names.dedup();
    }
    let runtime: Vec<&String> = globals
        .iter()
        .filter(|name| in_runtime_namespace(name))
        .collect();

    println!(
        "{}: prelinked, runtime anchors {:?}, {} global definitions, {} of them in the \
         runtime's namespace",
        artifact.display(),
        anchors,
        globals.len(),
        runtime.len()
    );
    for symbol in runtime.iter().take(OFFENDERS_SHOWN) {
        println!("  {symbol}");
    }
    if runtime.len() > OFFENDERS_SHOWN {
        println!("  ... and {} more", runtime.len() - OFFENDERS_SHOWN);
    }

    assert_eq!(
        anchors.len(),
        RUNTIME_ANCHORS.len(),
        "the archive has no {RUNTIME_MEMBER_PREFIXES:?} members and defines only {anchors:?} \
         of {RUNTIME_ANCHORS:?}: either it no longer bundles the runtime, or its symbol \
         table was stripped, and there is nothing left here to check"
    );
    // The C API has to be global for anything to link at all, so none means the
    // listing was not understood rather than that the archive is clean.
    assert!(
        !globals.is_empty(),
        "readelf reported no global definitions in {}",
        artifact.display()
    );
    assert!(
        runtime.is_empty(),
        "{} of {} global definitions in the prelinked archive belong to the C++ runtime; a \
         static build then binds the consumer's own runtime references to the engine's copy",
        runtime.len(),
        globals.len()
    );
}

/// Whether `name` has the shape of a C++ runtime symbol.
///
/// Everything libc++ defines is mangled, `_Z…`. libc++abi's entry points are
/// `__cxa_*`, `__gxx_personality_v0` and `__dynamic_cast`; libunwind's are
/// `_Unwind_*`, `__unw_*` and `__register_frame`. All of those are reserved
/// identifiers, beginning with an underscore. The one exception is libunwind's
/// public `unw_*` API. No C API name has either shape, so this needs no list of
/// it to tell the two apart.
fn in_runtime_namespace(name: &str) -> bool {
    name.starts_with('_') || name.starts_with("unw_")
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
