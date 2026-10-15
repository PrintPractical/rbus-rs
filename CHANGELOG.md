# Changelog

## Unreleased

- **Interval subscriptions:** `interval(Duration)` and `interval_raw(u32)` on property and event subscriptions. On a property, items become `Sample<T>`, whose `kind` says whether the provider sent it on the interval, as the initial value or for a change. The interval goes with every resubscribe and unsubscribe, which C providers need to find the subscription.
- `Error::InvalidArgument` for an interval that can't be sent.
- An event for a subscription id the connection doesn't know now goes to a subscription with the same name *and interval*, as in C, so a late sample from a dropped interval subscription can't reach a plain one.
- Rust providers still refuse interval subscriptions with `INVALID_OPERATION`.

## 0.0.1 (2026-10-10)

First release: `rbus`, `rbus-wire` and `rbus-macros`. Experimental; the API may still change.

- **Consumer:** get, set and transactions across providers, partial-path and wildcard gets, methods, tables, typed event subscriptions as `Stream`s, and discovery.
- **Provider:** `#[derive(Model)]` state structs, `#[rbus::provider]` impl blocks, the `DataModel` builder and the `Provider` trait; subscriptions, filters and publishing.
- **Connection:** configurable timeouts and decoder limits, and reconnecting when the broker restarts.
- Tested against rbus v2.14.0 (`e8cf808`).
