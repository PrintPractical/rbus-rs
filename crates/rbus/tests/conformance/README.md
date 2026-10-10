# Conformance provider

`mod.rs` serves the `Device.TestProvider.*` data model of rbus's own C test
provider, `third_party/rbus/test/rbus/provider/rbusTestProvider.c`, written
with the macros (M6). It is five providers on one connection:

- `Conformance` (`#[derive(Model)]`) holds the stored values (`Value.*`,
  `BigString`, `BigBytes`, `TestRunning`), every table and every event;
- `Computed` (`#[rbus::provider]`) serves the values that change on every
  read (`VCParam*`), `VCParamBy`, `NoAutoPubInt`, the write-only switches
  and the top-level methods;
- `Table1Methods`, `Table2Methods` and `EventsTableProps` serve the methods
  and the read counter inside table rows.

Until M6 it was a hand-written `Provider` (see git history); the groups
passed with the same counts both ways. `test_consumer.rs`
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
| `--Filter` | M5 | 54 | none |
| `--ValueChange` | M5 | 66 | none |
| `--Events` | M5 | 69 | none |
| `--Subscribe` | M5 | 53 | none |

The provider registers as component `TestProvider`, like the C one, because
`--ValueChange` checks that value-change events name it as `by`.

The check counts were taken from `rbusTestProvider` at the pinned rbus commit
(`e8cf808`). If the pin moves, recount them.

## Differences from `rbusTestProvider.c`

None of these is visible to the groups above:

- **`PartialPath` rows.** Each parameter holds its own name, as in C, but
  stored in the row when the provider adds it, not computed by a getter;
  rows that consumers add start empty. The C provider also hides the
  `PartialPath2` rows behind a table-level get handler, which is PT7
  (planned for later). Here they are ordinary provider-owned rows, with the
  same instance numbers and values. Partial-path, wildcard and single gets
  return the same results.
- **Element order.** The five providers register in turn, so a listing of
  `Device.TestProvider.` shows the stored elements before the computed
  ones, where C interleaves them. Within each table the order is C's.
- **`MethodAsync2()` and `MethodAsync3()`.** The C provider answers these
  from a thread, 3 s later (`rbusMethod_SendAsyncResponse`). Here the async
  handler waits 3 s and then returns, which is P6 done the Rust way.
- **Five parallel `MethodAsync2()` calls.** `rbusTestConsumer` counts passes
  with a non-atomic `gCountPass++` from each reply's callback thread, so
  replies that arrive together can lose a count (seen in CI: 54 of 55). The
  C provider has the same race. These replies are staggered by 100 ms per
  call index; the calls still overlap.
- **`BigString` and `BigBytes`.** The C provider swaps them: its
  `getBigHandler` tests `strstr(..) == 0`. Here each one keeps its own value.
  The `--Value` group sets them before reading them, so the swap never
  shows.
- **Polling.** C polls every subscribed property every 2 s. Here only the
  properties whose values change without a set (`VCParam*`, `VCParamBy`,
  `EventsTable.{i}.Prop`) are declared with `#[rbus(get, poll = "2s")]`. Changes made
  by a consumer's set are published at once, so the `by` checks see them
  sooner than with C.
- **The main loop.** `rbusTestProvider`'s `main` publishes `Event1!`,
  `Event2!`, `EventsTable.{i}.Event` and `NoAutoPubInt` every second while
  they have subscribers, adds the `TableReg` rows once they are subscribed
  to, and registers `ProviderNotFoundEvent1!` 20 s after
  `TestProviderNotFound` is set. `main_loop` does the same, asking each
  `Event` and `Table` for its subscribers where C counts in its subscribe
  handlers; the late event is another provider on the same connection.
- **`ErrorSubHandlerEvent!`** is refused by the model's `on_subscribe`
  hook, where C refuses it in its subscribe handler.
- **`ResetTables`** is accepted and does nothing. The C provider resets its
  own copy of the rows, but the library's rows (which consumers see) stay;
  each group here runs against a fresh provider anyway.
