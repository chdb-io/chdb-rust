use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = PathBuf::from(&out_dir);
    let libchdb_info = find_libchdb_or_download(&out_path);
    match libchdb_info {
        Ok((lib_dir, header_path)) => {
            setup_link_paths(&lib_dir);
            generate_bindings(&header_path, &out_path);
        }
        Err(e) => {
            eprintln!("Failed to find or download libchdb: {}", e);
            println!("cargo:warning=Failed to find libchdb. Please install manually using './update_libchdb.sh --local' or '--global'");
            std::process::exit(1);
        }
    }
}

fn find_libchdb_or_download(out_dir: &Path) -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
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

    if system_header_path.exists() {
        if system_lib_path.join("libchdb.so").exists() || 
           system_lib_path.join("libchdb.dylib").exists() {
            return Some((system_lib_path.to_path_buf(), system_header_path.to_path_buf()));
        }
    }

    None
}

fn download_libchdb_to_out_dir(out_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let platform = get_platform_string()?;
    let version = "v3.7.2";
    let url = format!(
        "https://github.com/chdb-io/chdb/releases/download/{}/{}",
        version, platform
    );
    println!("cargo:warning=Downloading libchdb from: {}", url);
    let response = reqwest::blocking::get(&url)?;
    let content = response.bytes()?;
    let temp_archive = out_dir.join("libchdb.tar.gz");
    fs::write(&temp_archive, content)?;
    let file = fs::File::open(&temp_archive)?;
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(file));
    archive.unpack(out_dir)?;
    fs::remove_file(&temp_archive)?;
    if cfg!(unix) {
        let lib_path = out_dir.join("libchdb.so");
        if lib_path.exists() {
            let _ = Command::new("chmod")
                .args(&["+x", lib_path.to_str().unwrap()])
                .output();
        }
    }
    println!("cargo:warning=libchdb downloaded successfully to OUT_DIR");
    Ok(())
}

fn get_platform_string() -> Result<String, &'static str> {
    let os = env::consts::OS;
    let arch = env::consts::ARCH;
    match (os, arch) {
        ("linux", "x86_64") => Ok("linux-x86_64-libchdb.tar.gz".to_string()),
        ("linux", "aarch64") => Ok("linux-aarch64-libchdb.tar.gz".to_string()),
        ("macos", "x86_64") => Ok("macos-x86_64-libchdb.tar.gz".to_string()),
        ("macos", "aarch64") => Ok("macos-arm64-libchdb.tar.gz".to_string()),
        _ => Err("Unsupported platform"),
    }
}

fn setup_link_paths(lib_dir: &Path) {
    println!("cargo:rustc-link-search={}", lib_dir.display());
    println!("cargo:rustc-link-search=./");
    println!("cargo:rustc-link-search=/usr/local/lib");
    println!("cargo:rustc-link-lib=chdb");
    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-changed=build.rs");
}

fn generate_bindings(header_path: &Path, out_dir: &Path) {
    let wrapper_content = format!("#include \"{}\"", header_path.display());
    let temp_wrapper = out_dir.join("temp_wrapper.h");
    fs::write(&temp_wrapper, wrapper_content).expect("Failed to write temp wrapper");
    let bindings = bindgen::Builder::default()
        .header(temp_wrapper.to_str().unwrap())
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");
    let src_path = PathBuf::from("./src/");
    bindings
        .write_to_file(src_path.join("bindings.rs"))
        .expect("Couldn't write bindings to src!");
    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("Couldn't write bindings to OUT_DIR!");
}
