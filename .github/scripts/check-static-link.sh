#!/bin/sh
#
# Asserts that a test binary carries the engine rather than loading it.
#
# `--features static` states an intention; it does not establish an outcome.
# `cargo:rustc-link-lib=static=chdb` reaches the linker as a plain `-lchdb` for
# any target that compiles the crate directly instead of consuming its rlib, and
# a linker offered both artifacts can take the dynamic one. That produced a test
# binary of 1.5 MB with a dynamic dependency on libchdb — statically linked
# according to the feature flag, dynamically linked according to the loader —
# and it passed the whole suite, because the machine happened to have the
# library. So the static job checks the property instead of trusting the flag.
#
# Two checks per binary:
#
#   1. no dynamic dependency on libchdb
#   2. the C API is not imported - it is either defined here or not referenced
#
# The first can pass on its own for a binary that finds libchdb through a
# mechanism the check does not read, such as a dlopen. The second can pass on
# its own for a binary that also carries a dynamic reference. Together they say
# the engine is in this file and is not being loaded from anywhere else.
#
# "Not imported" rather than "defined here", because a test that never calls the
# engine is legitimately linked without it: the linker drops what nothing
# references. What must never happen is an undefined reference, which is exactly
# how the engine would be arriving dynamically.
#
# Usage: check-static-link.sh <binary>...

set -eu

if [ "$#" -eq 0 ]; then
	echo "usage: $0 <binary>..." >&2
	exit 2
fi

case "$(uname -s)" in
Darwin) platform=macos ;;
Linux) platform=linux ;;
*)
	echo "unsupported platform $(uname -s)" >&2
	exit 2
	;;
esac

# One function from the C API that every test reaches, so neither -dead_strip nor
# --gc-sections can have dropped it. The macOS toolchain prefixes symbols with an
# underscore.
symbol=chdb_connect

failed=0

for bin in "$@"; do
	echo "== $bin"

	if [ ! -f "$bin" ]; then
		echo "   FAIL: no such file"
		failed=1
		continue
	fi

	case "$platform" in
	macos) dynamic=$(otool -L "$bin" | tail -n +2 | grep -i libchdb || true) ;;
	linux) dynamic=$(objdump -p "$bin" | grep NEEDED | grep -i libchdb || true) ;;
	esac

	if [ -n "$dynamic" ]; then
		echo "   FAIL: loads libchdb at run time"
		printf '%s\n' "$dynamic" | sed 's/^/     /'
		failed=1
	else
		echo "   ok: no dynamic libchdb dependency"
	fi

	symbols=$(nm "$bin" 2>/dev/null | grep -E "[[:space:]]_?${symbol}\$" || true)
	if printf '%s\n' "$symbols" | grep -Eq "[[:space:]]U[[:space:]]_?${symbol}\$"; then
		echo "   FAIL: ${symbol} is imported, so the engine is arriving from elsewhere"
		printf '%s\n' "$symbols" | sed 's/^/     /'
		failed=1
	elif printf '%s\n' "$symbols" | grep -Eq "[[:space:]][Tt][[:space:]]_?${symbol}\$"; then
		echo "   ok: ${symbol} is defined here"
	else
		echo "   ok: ${symbol} is not referenced, so nothing was linked for it"
	fi

	echo "   size: $(wc -c <"$bin" | tr -d ' ') bytes"
done

if [ "$failed" -ne 0 ]; then
	echo "the engine is not statically linked into every test binary" >&2
	exit 1
fi

echo "every test binary carries the engine"
