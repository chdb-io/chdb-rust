use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The engine this crate is developed against, used when no libchdb is already
/// present. Keep it on its own line and in step with CHDB_ENGINE_PIN in
/// update_libchdb.sh: the release check greps for both when it proposes a bump.
const CHDB_ENGINE_PIN: &str = "v26.7.1-rc.1";

fn main() {
    println!("cargo::rustc-check-cfg=cfg(direct_arrow_insert)");

    if env::var("DOCS_RS").is_ok() {
        return;
    }

    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = PathBuf::from(&out_dir);
    let libchdb_info = find_libchdb_or_download(&out_path);
    match libchdb_info {
        Ok((lib_dir, header_path)) => {
            setup_link_paths(&lib_dir, &header_path);
            generate_bindings(&header_path, &out_path);
        }
        Err(e) => {
            eprintln!("Failed to find or download libchdb: {e}");
            println!("cargo:warning=Failed to find libchdb. Please install manually using './update_libchdb.sh --local' or '--global'");
            std::process::exit(1);
        }
    }
}

fn find_libchdb_or_download(
    out_dir: &Path,
) -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
    if let Some((lib_dir, header_path)) = find_existing_libchdb() {
        return Ok((lib_dir, header_path));
    }

    println!("cargo:warning=libchdb not found locally, attempting to download...");
    download_libchdb_to_out_dir(out_dir)?;
    let lib_dir = out_dir.to_path_buf();
    let header_path = out_dir.join("chdb.h");

    if !header_path.exists() {
        return Err("Header file not found after download".into());
    }

    Ok((lib_dir, header_path))
}

fn find_existing_libchdb() -> Option<(PathBuf, PathBuf)> {
    if Path::new("./libchdb.so").exists() && Path::new("./chdb.h").exists() {
        return Some((PathBuf::from("."), PathBuf::from("./chdb.h")));
    }

    // Check system installation
    let system_lib_path = Path::new("/usr/local/lib");
    let system_header_path = Path::new("/usr/local/include/chdb.h");

    if system_header_path.exists()
        && (system_lib_path.join("libchdb.so").exists()
            || system_lib_path.join("libchdb.dylib").exists())
    {
        return Some((
            system_lib_path.to_path_buf(),
            system_header_path.to_path_buf(),
        ));
    }

    None
}

fn download_libchdb_to_out_dir(out_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let platform = get_platform_string()?;
    // CHDB_ENGINE_VERSION overrides the pin, so a build can be pointed at one
    // specific engine. Empty counts as unset, matching what `${VAR:-default}` in
    // update_libchdb.sh does — otherwise an empty value here builds a URL with
    // no version segment while the shell script quietly uses the pin.
    println!("cargo:rerun-if-env-changed=CHDB_ENGINE_VERSION");
    let version = env::var("CHDB_ENGINE_VERSION")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| CHDB_ENGINE_PIN.to_string());
    let url =
        format!("https://github.com/chdb-io/chdb-core/releases/download/{version}/{platform}");

    println!("cargo:warning=Downloading libchdb from: {url}");

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()?;

    let mut response = client.get(&url).send()?;

    if !response.status().is_success() {
        return Err(format!("Download failed with status: {}", response.status()).into());
    }

    let temp_archive = out_dir.join("libchdb.tar.gz");
    let mut dest = fs::File::create(&temp_archive)?;
    response.copy_to(&mut dest)?;

    println!("cargo:warning=Unpacking libchdb...");
    let file = fs::File::open(&temp_archive)?;
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(file));
    archive.unpack(out_dir)?;
    fs::remove_file(&temp_archive)?;

    if cfg!(unix) {
        let lib_path = if std::env::var("CARGO_FEATURE_STATIC").is_ok() {
            out_dir.join("libchdb.a")
        } else {
            out_dir.join("libchdb.so")
        };

        if lib_path.exists() {
            let _ = Command::new("chmod")
                .args(["+x", lib_path.to_str().unwrap()])
                .output();
        }
    }

    println!("cargo:warning=libchdb downloaded successfully to OUT_DIR");
    Ok(())
}

fn target_platform() -> (String, String) {
    let os = env::var("CARGO_CFG_TARGET_OS").expect("Failed to get target OS");
    let arch = env::var("CARGO_CFG_TARGET_ARCH").expect("Failed to get target architecture");
    (os, arch)
}

fn get_platform_string() -> Result<String, &'static str> {
    // Check if the static feature is enabled to decide which filename to download
    let is_static = std::env::var("CARGO_FEATURE_STATIC").is_ok();
    let ext = if is_static {
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

fn lib_exports_symbol(lib_dir: &Path, symbol: &str) -> bool {
    let lib_path = lib_dir.join("libchdb.so");
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

fn header_declares_direct_insert(header_path: &Path) -> bool {
    fs::read_to_string(header_path)
        .map(|s| s.contains("chdb_insert_arrow_array"))
        .unwrap_or(false)
}

fn setup_link_paths(lib_dir: &Path, header_path: &Path) {
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-search=native=./");
    println!("cargo:rustc-link-search=native=/usr/local/lib");

    let is_static = std::env::var("CARGO_FEATURE_STATIC").is_ok();

    if is_static {
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
        println!("cargo:rustc-link-lib=chdb");
    }

    if header_declares_direct_insert(header_path)
        && lib_exports_symbol(lib_dir, "chdb_insert_arrow_array")
    {
        println!("cargo:rustc-cfg=direct_arrow_insert");
    }

    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={}", header_path.display());
    if lib_dir.join("libchdb.so").exists() {
        println!(
            "cargo:rerun-if-changed={}",
            lib_dir.join("libchdb.so").display()
        );
    }
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
