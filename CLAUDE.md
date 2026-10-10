# rbus-rs

An async, idiomatic Rust client and provider for RDK's RBUS. It speaks the `rtrouted` wire protocol directly, with no C library, no FFI and no `unsafe`.

## Rules that must never break

- **No `unsafe`, no FFI.** Every crate root has `#![forbid(unsafe_code)]` and inherits `[workspace.lints]`, and every new crate must do the same. Don't add dependencies that link native libraries or wrap C: no `-sys` crates, no `cc`/`bindgen`/`cmake`/`pkg-config` build dependencies, and no direct `libc` dependency. Before adding any dependency, check that it is pure Rust (look for a `links` key or a `build.rs` that compiles C). If you can't tell, ask the owner first.
- **No C-isms in the public API.** Use `Result` + thiserror, owned types + serde, async fn + `Stream`, typed names, and transactions instead of commit flags. Never mirror the C API shape (design.md 4.2). C wire quirks stay inside `rbus-wire`.
- **Out of scope:** direct mode (reply with an error only), TCP, encryption, raw-data events, and the session manager API. The full list is decisions.md D14.
- **`rbus-wire` does no I/O** and has no async runtime.
- **One `Provider` trait.** The macros and the builder both expand to it. Build the engine on the trait and keep the macros as sugar.

## Testing

- **Coverage gate: 90 % lines** for `rbus-wire` and `rbus`, measured with `cargo-llvm-cov` over unit and interop tests combined. Don't merge below it, and don't lower it without the owner's approval.
- **Every v1 feature needs an integration test** against the real `rtrouted`, written the way an application would use the feature. Name the test after the feature ID (`c3_partial_path_get_...`) and record it in `docs/test-matrix.md`, in the same PR as the code.
- **Cover both directions:**
  - Rust consumer → C provider (`rbusTestProvider`, the sample providers);
  - C consumer → Rust provider (`rbuscli`, and `rbusTestConsumer` groups against the conformance provider);
  - Rust ↔ Rust.
- **Every API example in design.md section 4 must exist as a compiled, passing test.**
- **Wire codecs** are tested against recorded real frames, plus proptest round-trips.
- **Never weaken or delete a test to make CI pass.** If a test is wrong, say why in the PR.

## Commands

```sh
cargo build --workspace
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all
```

Interop tests need Linux and the real broker, so they run in a container built from `third_party/rbus` (`tests/interop/Containerfile`; Docker or Podman via `CONTAINER_ENGINE`):

```sh
scripts/interop.sh               # all tests, including #[ignore]d interop tests, against a live rtrouted
scripts/interop.sh coverage      # same under cargo-llvm-cov; COVERAGE_FAIL_UNDER=90 enforces the gate
scripts/interop.sh record        # re-record wire fixtures (crates/rbus-wire/tests/fixtures)
scripts/interop.sh shell         # shell in the container with rtrouted running
RTROUTED_LOG_LEVEL=DEBUG scripts/interop.sh ...
```

Interop tests are marked `#[ignore = "needs rtrouted: run scripts/interop.sh"]`. Use `rbus_testkit::broker::Peer` to start C peers (killed on drop). Never send zero-length payloads: rtrouted stalls on them (D20).

## Layout

- `crates/rbus-wire`: framing, msgpack payloads, value records, JSON control. Sans-I/O.
- `crates/rbus`: the `Rbus` handle, connection task, consumer API, provider engine and subscription registry (tokio).
- `crates/rbus-macros`: `name!`, `#[derive(Model)]`, `#[derive(Row)]`, `#[rbus::provider]`.
- `crates/rbus-testkit`: test-only tooling, not published. Contains the `rbus-record` fixture proxy, an independent frame splitter, and `RawClient` / `Peer` interop helpers.
- `docs/`: design, wire protocol, decisions, roadmap, test matrix.
- `third_party/rbus`: pinned C reference (rbus v2.14.0, `e8cf808`).

License: Apache-2.0 (`LICENSE`). Never copy files from `third_party/rbus` into this repo; reimplement instead (see decisions.md D18).
