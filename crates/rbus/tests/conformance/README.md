# Conformance provider

`mod.rs` is a hand-written `rbus::provider::Provider` that serves the
`Device.TestProvider.*` data model of rbus's own C test provider,
`third_party/rbus/test/rbus/provider/rbusTestProvider.c`. `test_consumer.rs`
runs the C `rbusTestConsumer` against it, one group at a time. Each group must
pass exactly as many checks as it passes against the C provider, with no
failures. That shows C consumers can't tell the Rust provider from the C one.

## Groups

| Group | Since | Checks (C provider = Rust provider) | Expected failures |
|---|---|---|---|
| `--Value` | M4 | 48 | none |
| `--Methods` | M4 | 55 | none |
| `--Tables` | M4 | 1228 | none |
| `--PartialPath` | M4 | 252 | none |
| `--Events`, `--Subscribe`, `--ValueChange`, `--Filter` | M5 | | not run yet |

The check counts were taken from `rbusTestProvider` at the pinned rbus commit
(`e8cf808`). If the pin moves, recount them.

## Differences from `rbusTestProvider.c`

None of these is visible to the groups above:

- **`PartialPath2` rows.** The C provider hides these rows behind a
  table-level get handler, which is PT7 (planned for later). Here they are
  ordinary provider-owned rows, with the same instance numbers and values.
  Partial-path, wildcard and single gets return the same results.
- **`MethodAsync2()` and `MethodAsync3()`.** The C provider answers these
  from a thread, 3 s later (`rbusMethod_SendAsyncResponse`). Here the async
  handler waits 3 s and then returns, which is P6 done the Rust way.
- **`BigString` and `BigBytes`.** The C provider swaps them: its
  `getBigHandler` tests `strstr(..) == 0`. Here each one keeps its own value.
  The `--Value` group sets them before reading them, so the swap never
  shows.
- **Not served yet (M5):** the events (`Event1!`, `Event2!`, ...), the
  value-change properties (`VCParam*`, `NoAutoPubInt`), `EventsTable`,
  `TableReg` and `ResetTables`. The M5 groups use them.
