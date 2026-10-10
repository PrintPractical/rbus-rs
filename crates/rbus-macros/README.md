# rbus-macros

Procedural macros for [`rbus`](https://crates.io/crates/rbus):

- `name!`: element names checked at compile time;
- `#[derive(Model)]` and `#[derive(Row)]`: serve a struct's state as a data model;
- `#[rbus::provider]`: serve an impl block's functions as properties and methods.

They expand to impls of `rbus` traits you could write by hand. Use them through `rbus`, which re-exports them (feature `macros`, on by default), not by depending on this crate. Its versions are pinned to `rbus`'s.

Part of [rbus-rs](https://github.com/PrintPractical/rbus-rs). Licensed under Apache-2.0.
