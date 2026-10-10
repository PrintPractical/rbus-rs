#!/usr/bin/env bash
# Runs commands inside the interop container (tests/interop/Containerfile):
# the real rtrouted, rbuscli, sample apps and rbus test programs, built from
# the pinned third_party/rbus submodule.
#
#   scripts/interop.sh                 # unit + interop tests (includes #[ignore]d interop tests)
#   scripts/interop.sh coverage        # same, under cargo-llvm-cov; writes target/interop/lcov.info
#   scripts/interop.sh record          # re-record wire fixtures into crates/rbus-wire/tests/fixtures
#   scripts/interop.sh shell           # interactive shell with rtrouted running
#   scripts/interop.sh <command...>    # any command, with rtrouted running
#
# CONTAINER_ENGINE selects docker (default) or podman.
set -euo pipefail
cd "$(dirname "$0")/.."

engine="${CONTAINER_ENGINE:-docker}"
image="rbus-rs-interop"

# CI builds the image with a layer cache first and sets INTEROP_SKIP_BUILD=1.
if [[ -z "${INTEROP_SKIP_BUILD:-}" ]]; then
    "$engine" build -q -f tests/interop/Containerfile -t "$image" . >/dev/null
fi

tty_flags=()
[[ -t 0 && -t 1 ]] && tty_flags=(-it)

exec "$engine" run --rm ${tty_flags[@]+"${tty_flags[@]}"} \
    -v "$PWD":/work -w /work \
    -v rbus-rs-cargo-registry:/root/.cargo/registry \
    -e CARGO_TARGET_DIR=/work/target/interop \
    -e COVERAGE_FAIL_UNDER -e RTROUTED_LOG_LEVEL \
    "$image" tests/interop/run.sh "$@"
