<img src="https://avatars.githubusercontent.com/u/132536224" width=130 />

[![Rust](https://github.com/chdb-io/chdb-rust/actions/workflows/rust.yml/badge.svg)](https://github.com/chdb-io/chdb-rust/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/chdb-rust.svg)](https://crates.io/crates/chdb-rust)
[![docs.rs](https://docs.rs/chdb-rust/badge.svg)](https://docs.rs/chdb-rust)

# chdb-rust <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/d/d5/Rust_programming_language_black_logo.svg/1024px-Rust_programming_language_black_logo.svg.png" height=20 />

Experimental [chDB](https://github.com/chdb-io/chdb) FFI bindings for Rust.

## Documentation

**[Full API Documentation](https://docs.rs/crate/chdb-rust/latest)** - Complete Rust API reference on docs.rs

## Status

**Experimental** - This library is currently experimental, unstable, and subject to changes.

The library automatically downloads and manages [`libchdb`](https://github.com/chdb-io/chdb) dependencies during the build process.

## Quick Start

Add `chdb-rust` to your `Cargo.toml`:

```toml
[dependencies]
chdb-rust = "1.1.0"
```

The library will automatically download the required `libchdb` binary during the build process.

## Supported Platforms

- **Linux**: x86_64, aarch64
- **macOS**: x86_64, arm64 (Apple Silicon)

## Building

### Standard Build

```bash
cargo build
```

### Verbose Build (for debugging)

```bash
RUST_BACKTRACE=full cargo build --verbose
```

### Manual Installation (Optional)

If you prefer to install `libchdb` manually instead of automatic download:

**System-wide installation:**
```bash
./update_libchdb.sh --global
```

**Local directory installation:**
```bash
./update_libchdb.sh --local
```

## Testing

Run the test suite:

```bash
cargo test -- --test-threads=1
```

## Examples

- **Runnable examples**: See the [examples/](examples/) directory
  ```bash
  cargo run --example <name>
  ```
- **Detailed documentation**: See [docs/examples.md](docs/examples.md) for comprehensive examples and explanations
- **Test examples**: See [tests/](tests/) directory for additional usage examples

## Contributing

We welcome contributions! Here's how you can help:

### Getting Started

1. **Fork the repository** and clone your fork
   ```bash
   git clone https://github.com/YOUR_USERNAME/chdb-rust.git
   cd chdb-rust
   ```

2. **Create a branch** for your changes
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   ```

3. **Make your changes** and ensure they work
   - Run tests: `cargo test`
   - Check formatting: `cargo fmt --check`
   - Run clippy: `cargo clippy`

4. **Commit your changes** with clear, descriptive commit messages
   ```bash
   git commit -m "Add feature: description of what you did"
   ```

5. **Push to your fork** and open a Pull Request
   ```bash
   git push origin feature/your-feature-name
   ```

### Development Guidelines

- **Code Style**: Follow Rust conventions and run `cargo fmt` before committing
- **Testing**: Add tests for new features and ensure all existing tests pass
- **Documentation**: Update relevant documentation for user-facing changes
- **Commit Messages**: Write clear, descriptive commit messages
- **Pull Requests**: 
  - Provide a clear description of your changes
  - Reference any related issues
  - Ensure CI checks pass

### Reporting Issues

Found a bug or have a feature request? Please open an issue on GitHub with:
- A clear description of the problem or feature
- Steps to reproduce (for bugs)
- Expected vs actual behavior
- Your environment (OS, Rust version, etc.)

### Questions?

Feel free to open a discussion or issue if you have questions about contributing!
