# Changelog

## 0.0.1 (2026-10-10)

First release: `rbus`, `rbus-wire` and `rbus-macros`. Experimental; the API may still change.

- **Consumer:** get, set and transactions across providers, partial-path and wildcard gets, methods, tables, typed event subscriptions as `Stream`s, and discovery.
- **Provider:** `#[derive(Model)]` state structs, `#[rbus::provider]` impl blocks, the `DataModel` builder and the `Provider` trait; subscriptions, filters and publishing.
- **Connection:** configurable timeouts and decoder limits, and reconnecting when the broker restarts.
- Tested against rbus v2.14.0 (`e8cf808`).
