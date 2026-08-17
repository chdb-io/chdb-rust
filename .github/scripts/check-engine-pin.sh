#!/usr/bin/env bash
#
# The chdb-core release this crate builds against is written in three places, and
# they have to say the same thing:
#
#   build.rs             CHDB_ENGINE_PIN   what a build actually downloads
#   update_libchdb.sh    CHDB_ENGINE_PIN   what the script installs
#   Cargo.toml           [package.metadata.chdb] engine   what a user is told
#
# The first two disagreeing means the engine a build links against is not the one
# the script put there, which surfaces as missing symbols or as behaviour that
# does not match the version anyone thinks they are running. The third disagreeing
# is quieter and worse: the crate says it carries one ClickHouse and carries
# another, and a crates.io version cannot be replaced — only yanked — so it says
# the wrong thing permanently.
#
# Only vX.Y.Z and vX.Y.Z-rc.N are accepted. Those are the two shapes chdb-core
# tags and the two the other bindings can parse, so a pin outside them is either a
# typo or a tag nothing downstream can consume.

set -euo pipefail

cd "$(dirname "$0")/../.."

fail() {
	echo "::error::$*" >&2
	exit 1
}

shell_pin=$(sed -n 's/^CHDB_ENGINE_PIN=\(.*\)$/\1/p' update_libchdb.sh | head -1)
build_pin=$(sed -n 's/^const CHDB_ENGINE_PIN: &str = "\(.*\)";$/\1/p' build.rs | head -1)
# Read only inside [package.metadata.chdb], so an engine= key in another table
# cannot be mistaken for this one.
meta_pin=$(awk '
	/^\[package\.metadata\.chdb\]/ { inside = 1; next }
	/^\[/                          { inside = 0 }
	inside && /^engine[[:space:]]*=/ {
		sub(/^engine[[:space:]]*=[[:space:]]*"/, "")
		sub(/".*$/, "")
		print
		exit
	}
' Cargo.toml)

[ -n "$shell_pin" ] || fail "update_libchdb.sh has no CHDB_ENGINE_PIN= line"
[ -n "$build_pin" ] || fail "build.rs has no CHDB_ENGINE_PIN constant"
[ -n "$meta_pin" ] || fail "Cargo.toml has no engine = \"...\" under [package.metadata.chdb]"

if [ "$shell_pin" != "$build_pin" ] || [ "$shell_pin" != "$meta_pin" ]; then
	cat >&2 <<EOF
::error::the engine pin does not agree across the three places that carry it.

  build.rs                        $build_pin
  update_libchdb.sh               $shell_pin
  Cargo.toml [package.metadata.chdb]  $meta_pin

A build downloads what build.rs names, and a user reads what Cargo.toml says.
Moving the pin means moving all three.
EOF
	exit 1
fi

case "$shell_pin" in
v*) ;;
*) fail "engine pin $shell_pin does not start with v" ;;
esac

rest=${shell_pin#v}
base=${rest%%-*}
suffix=${rest#"$base"}

# Exactly three numeric fields. Counted rather than matched with a glob, because
# * matches a dot too: v26.7.2.59 would pass a v[0-9]*.[0-9]*.[0-9]* pattern.
IFS=. read -r major minor patch extra <<EOF
$base
EOF
[ -n "$extra" ] && fail "engine pin $shell_pin has more than three version fields"
for field in "$major" "$minor" "$patch"; do
	case "$field" in
	"" | *[!0-9]*) fail "engine pin $shell_pin is not vX.Y.Z or vX.Y.Z-rc.N" ;;
	esac
done

case "$suffix" in
"") ;;
-rc.[0-9]*)
	case "${suffix#-rc.}" in
	*[!0-9]*) fail "engine pin $shell_pin has a release-candidate number that is not a number" ;;
	esac
	;;
*) fail "engine pin $shell_pin has the suffix '$suffix'; only -rc.N is understood" ;;
esac

echo "engine pin $shell_pin, agreed by build.rs, update_libchdb.sh and Cargo.toml"
