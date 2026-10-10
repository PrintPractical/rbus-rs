#!/usr/bin/env bash
# Entry point inside the interop container (see scripts/interop.sh).
set -euo pipefail
cd /work

broker_log=/tmp/rtrouted.log

start_broker() {
    rm -f /tmp/rtrouted /tmp/rtrouted.pid
    rtrouted -f -l "${RTROUTED_LOG_LEVEL:-ERROR}" -s unix:///tmp/rtrouted >"$broker_log" 2>&1 &
    broker_pid=$!
    for _ in $(seq 50); do [[ -S /tmp/rtrouted ]] && return 0; sleep 0.1; done
    echo "rtrouted did not start:" >&2; cat "$broker_log" >&2; exit 1
}

stop_broker() {
    kill "${broker_pid:-}" 2>/dev/null || true
}

mode="${1:-test}"
case "$mode" in
    record)
        exec tests/interop/record-fixtures.sh
        ;;
    test)
        start_broker; trap stop_broker EXIT
        cargo test --workspace --all-features -- --include-ignored
        ;;
    coverage)
        start_broker; trap stop_broker EXIT
        # rbus-testkit is test tooling and rbus-macros runs inside the compiler;
        # the coverage gate covers rbus-wire and rbus.
        ignore='crates/rbus-(testkit|macros)/'
        cargo llvm-cov --workspace --all-features --no-report -- --include-ignored
        cargo llvm-cov report --ignore-filename-regex "$ignore" --lcov --output-path "$CARGO_TARGET_DIR/lcov.info"
        gate=()
        [[ -n "${COVERAGE_FAIL_UNDER:-}" ]] && gate=(--fail-under-lines "$COVERAGE_FAIL_UNDER")
        cargo llvm-cov report --ignore-filename-regex "$ignore" --summary-only "${gate[@]}"
        ;;
    shell)
        start_broker; trap stop_broker EXIT
        bash
        ;;
    *)
        start_broker; trap stop_broker EXIT
        "$@"
        ;;
esac
