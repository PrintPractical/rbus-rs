#!/usr/bin/env bash
# Records real C <-> rtrouted traffic as wire fixtures.
#
# For each scenario: start rtrouted on /tmp/rtrouted.real, put the rbus-record
# proxy on /tmp/rtrouted (where C clients connect), run C programs, and write
# every frame to crates/rbus-wire/tests/fixtures/<scenario>.jsonl.
#
# Scenarios use the device-style build (32-byte headers) unless named *_rt,
# which use the MSG_ROUNDTRIP_TIME build (72-byte headers).
set -euo pipefail
cd /work

out=crates/rbus-wire/tests/fixtures
mkdir -p "$out"
cargo build -q -p rbus-testkit --bin rbus-record
recorder="$CARGO_TARGET_DIR/debug/rbus-record"

pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; pids=(); }
trap cleanup EXIT

wait_socket() {
    for _ in $(seq 50); do [[ -S "$1" ]] && return 0; sleep 0.1; done
    echo "socket $1 did not appear" >&2; exit 1
}

bg() { "$@" >/dev/null 2>&1 & pids+=($!); }

# Runs rbuscli in interactive mode, feeding it commands with pauses:
# interactive "subscribe X" 4 "unsubscribe X" 1
interactive() {
    (
        while [[ $# -gt 0 ]]; do printf '%s\n' "$1"; sleep "$2"; shift 2; done
        printf 'quit\n'
    ) | timeout 30 rbuscli -i >/dev/null 2>&1 || true
}

scenario() {
    local name="$1"; shift
    local prefix="$RBUS_PREFIX"
    [[ "$name" == *_rt ]] && prefix="$RBUS_RT_PREFIX"
    echo "recording $name"
    (
        export PATH="$prefix/bin:$PATH" LD_LIBRARY_PATH="$prefix/lib"
        rm -f /tmp/rtrouted /tmp/rtrouted.real /tmp/rtrouted.pid
        bg rtrouted -f -l ERROR -s unix:///tmp/rtrouted.real
        wait_socket /tmp/rtrouted.real
        bg "$recorder" --listen /tmp/rtrouted --upstream /tmp/rtrouted.real --out "$out/$name.jsonl"
        wait_socket /tmp/rtrouted
        "$@"
        sleep 0.5
        cleanup
    )
}

# --- scenarios ---------------------------------------------------------------

get_single() {
    bg rbusSampleProvider 15; sleep 2
    rbuscli getvalues Device.SampleProvider.SampleData.IntData
}

get_multi() {
    bg rbusSampleProvider 15; sleep 2
    rbuscli getvalues Device.SampleProvider.SampleData.IntData \
        Device.SampleProvider.SampleData.BoolData Device.SampleProvider.NestedObject1.TestParam
}

get_all_types() {   # partial path; every scalar type tag incl. DateTime
    bg rbusSampleProvider 15; sleep 2
    rbuscli getvalues Device.SampleProvider.AllTypes.
}

set_values() {
    bg rbusSampleProvider 15; sleep 2
    rbuscli setvalues Device.SampleProvider.SampleData.IntData int 42
    rbuscli setvalues Device.SampleProvider.SampleData.IntData int 7 \
        Device.SampleProvider.SampleData.BoolData bool false commit
}

undeliverable() {
    rbuscli getvalues Device.Does.Not.Exist
}

tables() {
    bg rbusTableProvider 20; sleep 2
    rbuscli addrow Device.Tables1.T1. myalias
    rbuscli getrows Device.Tables1.T1.
    rbuscli getnames Device.Tables1.T1.
    rbuscli delrow "Device.Tables1.T1.[myalias]."
}

methods() {
    bg rbusMethodProvider 15; sleep 2
    rbuscli method_values "Device.Methods.SimpleMethod()" param1 string hello param2 int 5
    rbuscli method_noargs "Device.Methods.SimpleMethod1()"
}

events_general() {
    bg rbusEventProvider 20; sleep 2
    interactive "subscribe Device.Provider1.Event1!" 4 "unsubscribe Device.Provider1.Event1!" 1
}

events_value_change() {
    bg rbusValueChangeProvider 20; sleep 2
    interactive "subscribe Device.Provider1.Param1" 7 "unsubscribe Device.Provider1.Param1" 1
}

discovery() {
    bg rbusSampleProvider 15; sleep 2
    rbuscli disccomponents Device.SampleProvider.SampleData.IntData
    rbuscli discallcomponents
    rbuscli discelements rbusSampleProvider
}

get_single_rt() {
    get_single
}

for s in get_single get_multi get_all_types set_values undeliverable tables \
         methods events_general events_value_change discovery get_single_rt; do
    scenario "$s" "$s"
done
echo "fixtures written to $out"
