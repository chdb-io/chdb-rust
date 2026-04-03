pub fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("chdb-rust")
        .tempdir()
        .expect("failed to create temp dir")
}
