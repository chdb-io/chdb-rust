pub fn tempdir() -> tempdir::TempDir {
    tempdir::TempDir::new("chdb-rust").expect("failed to create temp dir")
}
