<img src="https://avatars.githubusercontent.com/u/132536224" width=130 />

[![Rust](https://github.com/chdb-io/chdb-rust/actions/workflows/rust.yml/badge.svg)](https://github.com/chdb-io/chdb-rust/actions/workflows/rust.yml)

# chdb-rust <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/d/d5/Rust_programming_language_black_logo.svg/1024px-Rust_programming_language_black_logo.svg.png" height=20 />

Experimental [chDB](https://github.com/chdb-io/chdb) FFI bindings for Rust

## Status

- Experimental, unstable, subject to changes
- Automatically downloads and manages [`libchdb`](https://github.com/chdb-io/chdb) dependencies during build

## Usage

The library automatically downloads the required `libchdb` binary during the build process.

### Supported platforms:
- Linux x86_64
- Linux aarch64  
- macOS x86_64
- macOS arm64 (Apple Silicon)

### Manual Installation (Optional)

If you prefer to install `libchdb` manually, you can:

Install it system-wide:
```bash
./update_libchdb.sh --global
```

Or use it in a local directory:
```bash
./update_libchdb.sh --local
```

### Build

```bash
RUST_BACKTRACE=full cargo build --verbose

```

### Run tests

`cargo test`

### Examples

See `tests` directory.
