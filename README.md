<img src="https://avatars.githubusercontent.com/u/132536224" width=130 />

[![Rust](https://github.com/chdb-io/chdb-rust/actions/workflows/rust.yml/badge.svg)](https://github.com/chdb-io/chdb-rust/actions/workflows/rust.yml)

# chdb-rust <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/d/d5/Rust_programming_language_black_logo.svg/1024px-Rust_programming_language_black_logo.svg.png" height=20 />
Experimental [chDB](https://github.com/chdb-io/chdb) FFI bindings for Rust
### Status

- Experimental, unstable, subject to changes
- Requires [`libchdb`](https://github.com/chdb-io/chdb) on the system

#### Build binding
```bash
./update_libchdb.sh
RUST_BACKTRACE=full cargo build --verbose
```
