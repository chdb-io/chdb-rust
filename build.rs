use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The engine this crate is developed against, used when no libchdb is already
/// present. Keep it on its own line and in step with CHDB_ENGINE_PIN in
/// update_libchdb.sh: the release check greps for both when it proposes a bump.
const CHDB_ENGINE_PIN: &str = "v26.7.2-rc.2";

fn main() {
    println!("cargo::rustc-check-cfg=cfg(direct_arrow_insert)");
    println!("cargo::rustc-check-cfg=cfg(has_chdb_version)");

    if env::var("DOCS_RS").is_ok() {
        // docs.rs links no engine, but the version accessors still have to
        // compile, and they read their provenance out of the environment.
        publish_engine_provenance(None, "none: docs.rs build");
        return;
    }

    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = PathBuf::from(&out_dir);
    let libchdb_info = find_libchdb_or_download(&out_path);
    match libchdb_info {
        Ok(engine) => {
            publish_engine_provenance(engine.version.as_deref(), &engine.source);
            setup_link_paths(&engine.lib_path, &engine.header_path, &out_path);
            generate_bindings(&engine.header_path, &out_path);
        }
        Err(e) => {
            eprintln!("Failed to find or download libchdb: {e}");
            // cargo:warning is what a plain `cargo build` shows; the build
            // script's own stderr only appears under --verbose or on the second
            // run. The reason has to go here or the user sees nothing but
            // "failed to run custom build command".
            println!("cargo:warning=Failed to find libchdb: {e}");
            println!("cargo:warning=Install one with './update_libchdb.sh --local' or '--global', or set CHDB_LIB_DIR (plus CHDB_INCLUDE_DIR when the header is not next to the library)");
            std::process::exit(1);
        }
    }
}

/// The engine this build resolved, and how.
struct Engine {
    /// The artifact rustc is asked to link.
    lib_path: PathBuf,
    /// The header bindgen reads. It can come from `CHDB_INCLUDE_DIR` and so does
    /// not necessarily sit next to `lib_path`.
    header_path: PathBuf,
    /// The chdb-core release, without the leading `v`, when this build fetched it
    /// and therefore knows which one it is. `None` for a library the build was
    /// handed rather than downloaded.
    version: Option<String>,
    /// Where the library came from, for `chdb_rust::version::ENGINE_SOURCE`.
    source: String,
}

/// Hands the resolved provenance to the crate as two `env!`-readable values.
///
/// The expected version is deliberately empty rather than the pin whenever the
/// build did not fetch the engine itself. A constant that reports the pin while
/// the artifact came from somewhere else is worse than no constant at all: it
/// reads as a fact about the linked library and is not one.
fn publish_engine_provenance(version: Option<&str>, source: &str) {
    println!(
        "cargo:rustc-env=CHDB_EXPECTED_ENGINE_VERSION={}",
        version.unwrap_or_default()
    );
    println!("cargo:rustc-env=CHDB_ENGINE_SOURCE={source}");
}

/// Whether the enabled features ask rustc to link the engine statically.
///
/// Every place that has to name a file on disk goes through this and
/// [`lib_file_names`]: discovery, the download's chmod, the link directive and the
/// rerun-if-changed line all have to agree about which artifact is in play.
/// Disagreeing is what made `--features static` fail on any machine that already
/// had a `libchdb.so` — discovery found the dynamic library, skipped the download,
/// and rustc was then asked for a static one that had never been fetched.
fn is_static() -> bool {
    env::var_os("CARGO_FEATURE_STATIC").is_some()
}

/// The names the linked artifact can have, in preference order.
///
/// chdb-core ships the dynamic library as `libchdb.so` on macOS as well, so that
/// name comes first there too; `.dylib` is accepted only on macOS, because a
/// locally built or renamed copy is a reasonable thing to point `CHDB_LIB_DIR`
/// at and Apple's linker will find it. Elsewhere `-lchdb` reaches a linker that
/// does not search that name, so accepting it would turn "no engine here" into a
/// missing-library link error further along.
///
/// Keyed on the target rather than the host: which linker sees `-lchdb` is a
/// property of what is being built, not of what is building it.
fn lib_file_names() -> &'static [&'static str] {
    if is_static() {
        &["libchdb.a"]
    } else if target_platform().0 == "macos" {
        &["libchdb.so", "libchdb.dylib"]
    } else {
        &["libchdb.so"]
    }
}

/// The artifact for the current linkage in `dir`, if there is one.
fn find_lib_in(dir: &Path) -> Option<PathBuf> {
    lib_file_names()
        .iter()
        .map(|name| dir.join(name))
        .find(|path| path.exists())
}

/// A directory named by an environment variable. Empty counts as unset, matching
/// how `${VAR:-default}` behaves in update_libchdb.sh.
///
/// `var_os` rather than `var`: a path is bytes and need not be UTF-8, and
/// `env::var` reports a non-UTF-8 value as an error, which would read here as
/// "unset" — the build would then quietly resolve some other engine than the one
/// it was pointed at, which is the outcome this hatch exists to rule out.
fn env_dir(var: &str) -> Option<PathBuf> {
    env::var_os(var)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// Resolves the engine to build against.
///
/// In order: whatever `CHDB_LIB_DIR` / `CHDB_INCLUDE_DIR` name, then a copy
/// already on the machine, then a download of the pinned engine.
fn find_libchdb_or_download(out_dir: &Path) -> Result<Engine, Box<dyn std::error::Error>> {
    // Both are read here rather than inside the branches below, so the build
    // re-runs when either changes even on the paths that ignore them.
    println!("cargo:rerun-if-env-changed=CHDB_LIB_DIR");
    println!("cargo:rerun-if-env-changed=CHDB_INCLUDE_DIR");
    let header_override = header_from_env()?;

    if let Some(lib_dir) = env_dir("CHDB_LIB_DIR") {
        return libchdb_from_lib_dir(&lib_dir, header_override);
    }

    if let Some((lib_path, header_path)) = find_existing_libchdb() {
        let source = format!("local: {}", lib_path.display());
        return Ok(Engine {
            lib_path,
            header_path: header_override.unwrap_or(header_path),
            version: None,
            source,
        });
    }

    println!("cargo:warning=libchdb not found locally, fetching it");
    let (engine_dir, version) = fetch_engine(out_dir)?;

    let lib_path = find_lib_in(&engine_dir).ok_or_else(|| {
        format!(
            "the archive contains none of {:?}, which the {} linkage needs",
            lib_file_names(),
            linkage_name()
        )
    })?;
    let header_path = header_override.unwrap_or_else(|| engine_dir.join("chdb.h"));

    if !header_path.exists() {
        return Err(format!("no chdb.h in {}", engine_dir.display()).into());
    }

    Ok(Engine {
        source: format!("download: chdb-core {version}"),
        // The tag is the only place a `v` belongs. Everything downstream compares
        // versions by plain string equality, which needs one spelling.
        version: Some(version.trim_start_matches('v').to_string()),
        lib_path,
        header_path,
    })
}

fn linkage_name() -> &'static str {
    if is_static() {
        "static"
    } else {
        "dynamic"
    }
}

/// The header named by `CHDB_INCLUDE_DIR`, if it is set.
///
/// Separate from `CHDB_LIB_DIR` because a chdb-core build tree does not keep the
/// two together: `chdb/build/build_static_lib.sh` leaves `libchdb.a` at the
/// repository root while the header stays at `programs/local/chdb.h`. Requiring
/// them to be adjacent would mean copying a gigabyte-sized archive around to
/// build against one.
fn header_from_env() -> Result<Option<PathBuf>, Box<dyn std::error::Error>> {
    let Some(include_dir) = env_dir("CHDB_INCLUDE_DIR") else {
        return Ok(None);
    };
    let header_path = include_dir.join("chdb.h");
    if !header_path.exists() {
        return Err(format!(
            "CHDB_INCLUDE_DIR is {} but there is no chdb.h in it",
            include_dir.display()
        )
        .into());
    }
    Ok(Some(header_path))
}

/// The engine under `CHDB_LIB_DIR`.
///
/// A directory that is set but unusable is an error, never a fall-through to the
/// download. Quietly fetching the pinned engine instead would produce a green
/// build that tested a different engine than the one asked for — the failure this
/// hatch exists to let people investigate is exactly the kind that a substituted
/// artifact hides.
fn libchdb_from_lib_dir(
    lib_dir: &Path,
    header_override: Option<PathBuf>,
) -> Result<Engine, Box<dyn std::error::Error>> {
    let lib_path = find_lib_in(lib_dir).ok_or_else(|| {
        format!(
            "CHDB_LIB_DIR is {} but it contains none of {:?}, which the {} linkage needs",
            lib_dir.display(),
            lib_file_names(),
            linkage_name()
        )
    })?;

    let header_path = match header_override {
        Some(header_path) => header_path,
        None => {
            let adjacent = lib_dir.join("chdb.h");
            if !adjacent.exists() {
                return Err(format!(
                    "found {} but no chdb.h next to it; set CHDB_INCLUDE_DIR to the directory \
                     holding the matching header (a chdb-core build tree keeps it in \
                     programs/local)",
                    lib_path.display()
                )
                .into());
            }
            adjacent
        }
    };

    println!(
        "cargo:warning=using libchdb from CHDB_LIB_DIR: {} (header {})",
        lib_path.display(),
        header_path.display()
    );
    Ok(Engine {
        source: format!("CHDB_LIB_DIR: {}", lib_path.display()),
        version: None,
        lib_path,
        header_path,
    })
}

fn find_existing_libchdb() -> Option<(PathBuf, PathBuf)> {
    if let Some(lib_path) = find_lib_in(Path::new(".")) {
        if Path::new("./chdb.h").exists() {
            return Some((lib_path, PathBuf::from("./chdb.h")));
        }
    }

    // Check system installation
    let system_lib_path = Path::new("/usr/local/lib");
    let system_header_path = Path::new("/usr/local/include/chdb.h");

    if system_header_path.exists() {
        if let Some(lib_path) = find_lib_in(system_lib_path) {
            return Some((lib_path, system_header_path.to_path_buf()));
        }
    }

    None
}

/// The chdb-core tag to download, `v` and all.
///
/// CHDB_ENGINE_VERSION overrides the pin, so a build can be pointed at one
/// specific engine. Empty counts as unset, matching what `${VAR:-default}` in
/// update_libchdb.sh does — otherwise an empty value here builds a URL with no
/// version segment while the shell script quietly uses the pin.
fn engine_tag_to_download() -> String {
    println!("cargo:rerun-if-env-changed=CHDB_ENGINE_VERSION");
    env::var("CHDB_ENGINE_VERSION")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| CHDB_ENGINE_PIN.to_string())
}

/// Written last inside a cache entry, so an entry is complete or absent and
/// never half-unpacked.
const CACHE_STAMP: &str = ".chdb-engine-complete";

/// Where fetched engines are kept, outside `target/`.
///
/// An engine depends on the release tag and the platform and changes rarely, but
/// `OUT_DIR` is handed out per profile and per feature combination: switch
/// profile or enable one more feature and it is fetched again, and `cargo clean`
/// discards every copy. Measured in this repository before the cache existed:
/// 4.9 GB under target/, seven copies of the same two engines.
///
/// `CHDB_ENGINE_CACHE_DIR` moves it. Deleting the directory clears it — nothing
/// here evicts, because an entry is only ever added under a key it does not
/// already have.
fn engine_cache_root() -> Option<PathBuf> {
    println!("cargo:rerun-if-env-changed=CHDB_ENGINE_CACHE_DIR");
    if let Some(dir) = env_dir("CHDB_ENGINE_CACHE_DIR") {
        return Some(dir);
    }

    // Two paths are not worth a build dependency: build dependencies are
    // compiled before anything else in the graph.
    let home = env_dir("HOME")?;
    Some(if cfg!(target_os = "macos") {
        home.join("Library/Caches/chdb-rust")
    } else {
        env_dir("XDG_CACHE_HOME")
            .unwrap_or_else(|| home.join(".cache"))
            .join("chdb-rust")
    })
}

/// The unpacked engine directory, plus the tag it holds.
///
/// The shared cache when one is usable, `OUT_DIR` when it is not: an unwritable
/// cache directory should cost a build time, not break it.
fn fetch_engine(out_dir: &Path) -> Result<(PathBuf, String), Box<dyn std::error::Error>> {
    let tag = engine_tag_to_download();
    let asset = get_platform_string()?;

    if let Some(root) = engine_cache_root() {
        match cached_engine(&tag, &asset, &root) {
            Ok(dir) => return Ok((dir, tag)),
            Err(e) => {
                println!("cargo:warning=engine cache at {} unusable ({e}); unpacking into OUT_DIR instead", root.display());
            }
        }
    }

    download_and_unpack(&tag, &asset, out_dir)?;
    Ok((out_dir.to_path_buf(), tag))
}

/// The cache entry for this tag and asset, filling it if it is not there.
///
/// The key is the release tag and the asset name, which together are the version,
/// the platform and the linkage — everything the downloaded bytes depend on.
///
/// Filling goes through a sibling directory and one rename, so an interrupted
/// build leaves nothing a later build can mistake for an engine. A concurrent
/// build that gets there first wins: its copy came from the same URL.
fn cached_engine(
    tag: &str,
    asset: &str,
    cache_root: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let key = asset.trim_end_matches(".tar.gz");
    let entry = cache_root.join(tag).join(key);

    if entry.join(CACHE_STAMP).exists() {
        println!(
            "cargo:warning=using the cached engine at {}",
            entry.display()
        );
        return Ok(entry);
    }

    let parent = cache_root.join(tag);
    let staging = parent.join(format!("{key}.incomplete.{}", std::process::id()));
    fs::create_dir_all(&parent)?;
    let _ = fs::remove_dir_all(&staging);
    fs::create_dir_all(&staging)?;

    if let Err(e) = fill_cache_entry(tag, asset, &staging) {
        // A failed fetch of a few hundred megabytes should not leave a few
        // hundred megabytes behind. The next build would not mistake it for an
        // engine — there is no stamp in it — but it would never clean it up
        // either.
        let _ = fs::remove_dir_all(&staging);
        let _ = fs::remove_dir(&parent);
        return Err(e);
    }

    match fs::rename(&staging, &entry) {
        Ok(()) => {}
        Err(_) if entry.join(CACHE_STAMP).exists() => {
            let _ = fs::remove_dir_all(&staging);
        }
        Err(e) => {
            let _ = fs::remove_dir_all(&staging);
            return Err(e.into());
        }
    }

    println!("cargo:warning=cached the engine at {}", entry.display());
    Ok(entry)
}

/// Unpacks the engine into a staging directory and stamps it complete.
fn fill_cache_entry(
    tag: &str,
    asset: &str,
    staging: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    download_and_unpack(tag, asset, staging)?;
    fs::write(staging.join(CACHE_STAMP), tag)?;
    Ok(())
}

/// Downloads `asset` from release `tag` and unpacks it into `dest`.
fn download_and_unpack(
    tag: &str,
    asset: &str,
    dest: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("https://github.com/chdb-io/chdb-core/releases/download/{tag}/{asset}");

    println!("cargo:warning=Downloading libchdb from: {url}");

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()?;

    let mut response = client.get(&url).send()?;

    if !response.status().is_success() {
        return Err(format!("Download failed with status: {}", response.status()).into());
    }

    let temp_archive = dest.join("libchdb.tar.gz");
    let mut archive_file = fs::File::create(&temp_archive)?;
    response.copy_to(&mut archive_file)?;

    println!("cargo:warning=Unpacking libchdb...");
    let file = fs::File::open(&temp_archive)?;
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(file));
    archive.unpack(dest)?;
    fs::remove_file(&temp_archive)?;

    if cfg!(unix) {
        if let Some(lib_path) = find_lib_in(dest) {
            let _ = Command::new("chmod")
                .args(["+x", lib_path.to_str().unwrap()])
                .output();
        }
    }

    Ok(())
}

fn target_platform() -> (String, String) {
    let os = env::var("CARGO_CFG_TARGET_OS").expect("Failed to get target OS");
    let arch = env::var("CARGO_CFG_TARGET_ARCH").expect("Failed to get target architecture");
    (os, arch)
}

fn get_platform_string() -> Result<String, &'static str> {
    let ext = if is_static() {
        "-static.tar.gz"
    } else {
        ".tar.gz"
    };

    let (os, arch) = target_platform();
    match (os.as_str(), arch.as_str()) {
        ("linux", "x86_64") => Ok(format!("linux-x86_64-libchdb{}", ext)),
        ("linux", "aarch64") => Ok(format!("linux-aarch64-libchdb{}", ext)),
        ("macos", "x86_64") => Ok(format!("macos-x86_64-libchdb{}", ext)),
        ("macos", "aarch64") => Ok(format!("macos-arm64-libchdb{}", ext)),
        _ => Err("Unsupported platform"),
    }
}

/// Whether the linked artifact exports `symbol`.
///
/// `nm -D` asks for an ELF dynamic symbol table: it answers only for a dynamic
/// library on a platform that has one. Measured on macOS, Apple's nm rejects the
/// flag outright for both `libchdb.so` and `libchdb.a`, so this is false there
/// whatever the artifact contains. Only `direct_arrow_insert` consults it, and
/// that cfg is gated first on a header declaration no released header carries.
fn lib_exports_symbol(lib_path: &Path, symbol: &str) -> bool {
    if !lib_path.exists() {
        return false;
    }
    let output = Command::new("nm")
        .args(["-D", lib_path.to_str().unwrap_or_default()])
        .output();
    match output {
        Ok(o) if o.status.success() => {
            let stdout = String::from_utf8_lossy(&o.stdout);
            stdout.contains(symbol)
        }
        _ => false,
    }
}

/// Whether the header mentions `symbol`.
///
/// Coarse on purpose: what matters is whether bindgen will have produced a
/// declaration to call, and a header that names the symbol at all is a header
/// bindgen saw it in.
fn header_declares(header_path: &Path, symbol: &str) -> bool {
    fs::read_to_string(header_path)
        .map(|contents| contents.contains(symbol))
        .unwrap_or(false)
}

/// A directory holding nothing but the archive, so no dynamic library can be
/// picked in its place.
///
/// `cargo:rustc-link-lib=static=chdb` reaches the linker as a plain `-lchdb`
/// whenever the crate is compiled directly rather than consumed as an rlib —
/// `cargo test --lib` is the case in this repository. Apple's linker then
/// searches the `-L` directories and takes a `libchdb.so` found alongside the
/// archive, which is how `--features static` produced a 1.5 MB binary with a
/// dynamic dependency on libchdb: statically linked according to the feature
/// flag, dynamically linked according to `otool -L`. Any directory that can hold
/// both artifacts is ambiguous, and a chdb-core build tree holds both.
///
/// A symlink rather than a copy: the archive is around a gigabyte.
fn isolate_archive(lib_path: &Path, out_dir: &Path) -> Option<PathBuf> {
    let dir = out_dir.join("static-link");
    fs::create_dir_all(&dir).ok()?;
    let link = dir.join("libchdb.a");
    let target = lib_path.canonicalize().ok()?;

    if fs::read_link(&link).is_ok_and(|existing| existing == target) {
        return Some(dir);
    }
    let _ = fs::remove_file(&link);

    #[cfg(unix)]
    std::os::unix::fs::symlink(&target, &link).ok()?;
    #[cfg(not(unix))]
    fs::copy(&target, &link).ok()?;

    Some(dir)
}

fn setup_link_paths(lib_path: &Path, header_path: &Path, out_dir: &Path) {
    let lib_dir = lib_path.parent().unwrap_or(Path::new("."));

    if is_static() {
        // Only the isolated directory: ./ and /usr/local/lib are exactly the
        // places a stray libchdb.so lives, and here they would shadow the
        // archive rather than help find it.
        let search_dir =
            isolate_archive(lib_path, out_dir).unwrap_or_else(|| lib_dir.to_path_buf());
        println!("cargo:rustc-link-search=native={}", search_dir.display());
        println!("cargo:rustc-link-lib=static=chdb");
        match target_platform().0.as_str() {
            "linux" => {
                println!("cargo:rustc-link-lib=stdc++");
            }
            "macos" => {
                // https://github.com/chdb-io/chdb-core/blob/10e35571d0fa9c863d590cc9e1f00ca927ae908d/chdb/build/build_static_lib.sh#L104-L107
                println!("cargo:rustc-link-lib=c++");
                println!("cargo:rustc-link-lib=iconv");
                println!("cargo:rustc-link-lib=framework=CoreFoundation");
                println!("cargo:rustc-link-lib=framework=Security");
            }
            _ => {}
        }
    } else {
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
        println!("cargo:rustc-link-search=native=./");
        println!("cargo:rustc-link-search=native=/usr/local/lib");
        println!("cargo:rustc-link-lib=chdb");
    }

    if header_declares(header_path, "chdb_insert_arrow_array")
        && lib_exports_symbol(lib_path, "chdb_insert_arrow_array")
    {
        println!("cargo:rustc-cfg=direct_arrow_insert");
    }

    // chdb_version() arrived in chdb-core v26.7.0. Older engines have no way to
    // report their own release, and calling a symbol they do not export would
    // not link, so the accessor is compiled out instead.
    if header_declares(header_path, "chdb_version") {
        println!("cargo:rustc-cfg=has_chdb_version");
    }

    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={}", header_path.display());
    // The artifact that is actually linked, rather than a fixed libchdb.so. When
    // building against a local chdb-core tree the engine is rebuilt underneath a
    // build script that has no other reason to re-run, and without this line the
    // tests keep exercising the previous engine while reporting on the new one.
    println!("cargo:rerun-if-changed={}", lib_path.display());
}

fn generate_bindings(header_path: &Path, out_dir: &Path) {
    let header_path = header_path
        .canonicalize()
        .unwrap_or_else(|_| header_path.to_path_buf());
    let wrapper_content = format!("#include \"{}\"", header_path.display());
    let temp_wrapper = out_dir.join("temp_wrapper.h");
    if fs::read_to_string(&temp_wrapper)
        .map(|s| s != wrapper_content)
        .unwrap_or(true)
    {
        fs::write(&temp_wrapper, wrapper_content).expect("Failed to write temp wrapper");
    }
    let bindings = bindgen::Builder::default()
        .header(temp_wrapper.to_str().unwrap())
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");
    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("Couldn't write bindings to OUT_DIR!");
}
