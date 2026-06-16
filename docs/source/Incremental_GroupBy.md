# Incremental SNAPSHOT GroupBy

## Status

- **GroupBy backfill (incremental):** implemented and merged (PR #1128).
- **GroupBy upload (incremental):** implemented; daily `backfill → upload`
  orchestration is the recommended mode (branch `pengyu--gb-upload-incremental`).
  A dedicated producer Driver mode (`group-by-incremental-build`) and an
  `--incremental-read-only` consumer flag also exist for the multi-consumer case.
- **Join incremental right-parts:** planned (future PR) — the main driver for the
  dedicated-producer topology.

## 1. Problem

A SNAPSHOT-accuracy event GroupBy backfill recomputes windowed aggregates by
re-scanning the **entire window of raw events on every step**. For a long window
(e.g. 365–730 days) this means each daily output re-reads hundreds of days of raw
events, and a multi-step backfill re-scans the same days many times over. The
dominant cost is the raw-event "hop aggregation" (`hopsAggregate`), and it is paid
repeatedly even though the per-day partial aggregates never change once a day is
closed.

The same raw scan is repeated independently by:
- the **backfill** job (produces the offline training table), and
- the daily **upload** job (produces the online-serving KV table).

So the expensive work is done at least twice per GroupBy, every day for thousands of jobs.

## 2. Goal

Compute each day's **partial aggregate (IR)** exactly once, cache it, and reuse it:

1. **Backfill** windows the cached daily IRs into the final output table.
2. **Upload** windows the same cached daily IRs into the serving (KV) table.
3. (Later) **Join** snapshot right-parts consume the same cached IRs.

Target: turn the per-day raw hop-aggregation from "recomputed on every run" into
"write once, read many", giving a large speedup on long-window, high-fan-in
GroupBys while producing **identical** results to the non-incremental path.

Observed on a ~500M-row/day, 730-day-window source: a warm daily run ~10× faster
in wall-clock than the equivalent full backfill, with deterministic columns
matching the non-incremental path exactly.

## 3. Non-goals

- **TEMPORAL accuracy** and **entity sources** are out of scope. Temporal serving
  is point-in-time at sub-daily resolution; a daily IR cache cannot serve it. These
  combinations are rejected at compile time and at runtime.
- Changing the aggregation semantics. Output must equal the raw path (modulo the
  pre-existing, accepted nondeterminism of `LAST`/`FIRST` on tied timestamps and of
  approximate sketches).

## 4. Design

### 4.1 The incremental (daily IR) table

When a GroupBy sets `is_incremental = True`, Chronon maintains a side table:

```
<outputNamespace>.<name>_daily_inc
```

It stores **one row per (key, ds)** holding the day's **un-windowed, normalized
IRs** — i.e. the daily "hops" produced by `hopsAggregate` at `DailyResolution`.
Because the IR is window-independent, the column names drop the window suffix
(`price_sum`, not `price_sum_7d`); a single daily IR column serves all windows of
that `(operation, input_column, bucket)`.

The table is tagged `chronon_generated=true`,
`chronon_table_type=group_by_incremental`.

### 4.2 Two halves: build (producer) and read (consumer)

The feature is split into a **build** half and a **read** half so the table can be
produced once and consumed many times.

**Build — `GroupBy.computeIncrementalDf(conf, range, tableUtils, table, stepDays)`**
- Computes the queryable range `[range.start − maxWindow, range.end]`.
- Uses `unfilledRanges` to find missing partitions; fills only those.
- **Idempotent:** a no-op when the table is already complete.
- **Chunked & restartable:** each hole is filled in `stepDays` sub-ranges, each
  committed independently, bounding per-write memory and surviving partial failure.
- **Clamped write:** each per-day worker aggregates over a window-widened source
  scan but writes only the requested partitions, so a fill can never clobber
  neighboring days with truncated data.
- **Deduped columns:** aggregations that collapse to the same daily IR column are
  written once (identical values), read back by name.

**Read — `GroupBy.fromIncrementalDf(conf, range, tableUtils, stepDays, buildIfMissing)`**
- Returns a `GroupBy` whose `hopsAggregate` is overridden to return the cached daily
  hops instead of scanning raw events. Everything downstream (`snapshotEvents` →
  sawtooth windowing) is unchanged, so it is a **mode-agnostic adapter**: any
  snapshot-events consumer (backfill, upload, join) routes through `hopsAggregate`.
- `buildIfMissing`:
  - `true` (default) → **ensure-then-read**: build any missing partitions, then read.
  - `false` → **read-only**: assume an upstream producer already populated the table.
- Rejects non-`DailyResolution` reads (e.g. a temporal consumer) rather than
  silently mis-serving.

### 4.3 Consumers

- **Backfill** (`computeBackfill`, `incrementalMode=true`): builds the `GroupBy`
  via `fromIncrementalDf` instead of the raw `from`, then `snapshotEvents` as usual.
- **Upload** (`GroupByUpload.run`): for `SNAPSHOT + Events + is_incremental`, builds
  the upload `GroupBy` via `fromIncrementalDf`; all other accuracy/data-model paths
  are unchanged.

Both consumers accept `incrementalReadOnly` (→ `buildIfMissing=false`).

### 4.4 Guards

`is_incremental` is only valid for **SNAPSHOT accuracy + event sources**, enforced:
- at compile time in `validate_group_by` (accepting inferred-SNAPSHOT when accuracy
  is unset), and
- at runtime in `computeBackfill` and `GroupByUpload.run`.

A GroupBy with no windowed aggregation is rejected (incremental requires a window).

## 5. Orchestration

### 5.1 Cadence

In production, both the **backfill** (extends the offline output table by a day) and
the **upload** (publishes the day to the KV serving table) run **daily for the same
`ds`**. Both need the IR cache complete over `[ds − maxWindow, ds]`. Because they run
for the same `ds`, the only genuinely new partition each day is `ds` itself (plus any
recent late-arrival days); the deep history was built once and persists. So the daily
question is narrow: **who builds today's `ds` IR, and how do we avoid the two jobs
racing to build it?**

The two things we want from orchestration:
1. **No recomputation** — build each day's IR once, not once per consumer.
2. **No write race** — never two jobs writing the same `_daily_inc` partition
   concurrently.

### 5.2 Recommended: ordered consumers (`backfill → upload`)

Both consumers run `is_incremental` in **ensure-then-read** mode, with a DAG edge so
that, for a given `ds`, **upload depends on backfill**:

```
   group-by-backfill[ds]   (ensure-then-read: builds the new day's IR, then reads)
            │
            ▼
   group-by-upload[ds]     (ensure-then-read: finds ds present -> pure read; self-heals if not)
```

- **No recompute:** backfill builds the one new day; upload reads it (its ensure step
  is a no-op).
- **No race:** the edge serializes the two jobs, so they never write `ds`
  concurrently.
- **Self-healing:** upload can still fill a hole if backfill somehow missed one — it
  degrades to a recompute, never to wrong output. No "blind read-only" consumer, so
  **no completeness gate is required**.
- **Minimal topology:** one dependency edge, no new task, no new flags.

The only operational rule: **upload[ds] must not run concurrently with backfill[ds]**
(the edge guarantees this). An occasional out-of-band backfill (e.g. a manual
historical re-run) must likewise be ordered ahead of / not overlap the daily upload
for the same range; idempotent overwrite keeps a stray overlap eventually-correct
but may cost a commit retry.

### 5.3 Scale-out: dedicated producer node (FUTURE — ships with the join PR)

> **Not implemented in the current work.** The ordered-consumer approach (§5.2)
> fully covers backfill + upload, so a dedicated producer and a read-only consumer
> mode are intentionally **not** part of this change. They are deferred to the join
> PR, which is the first scenario that actually needs them.

When a single GroupBy's IR cache is consumed by **more than two** jobs — e.g. its own
backfill + upload **plus several joins** that include it — chaining ordering edges
among all consumers gets awkward. There, promote the build to a dedicated producer
task that all consumers depend on and read-only from:

```
        <incremental-build producer>       (builds _daily_inc once, SOLE writer)
                      │
       ┌──────────────┼───────────────┐
  backfill         upload          join parts ...     (parallel, read-only)
```

This is the only mode where consumers run **read-only** (they never write), so it
**requires a completeness gate**: "producer succeeded" must imply "cache is complete
and correct" — otherwise a partial cache silently yields wrong features. That gate
(plus the producer Driver mode and a read-only consumer flag) is the scope of the
join PR, not this one. The producer would call the **same** `computeIncrementalDf`
as today's ensure-step; the only difference is that it becomes the sole writer.

### 5.4 Trade-off analysis

| Approach | New topology | Recompute | Race | Needs completeness gate | Failure behavior | Best for |
|---|---|---|---|---|---|---|
| **Independent jobs, ensure-then-read** (no edge) | none | possible (both build same day) | yes (concurrent same-`ds` write) | no | self-heals; may hit commit conflicts | not recommended in prod |
| **Ordered `backfill → upload`** (§5.2, recommended) | one edge | none | none (serialized) | no | upload self-heals a missed day | backfill + upload (the common case) |
| **Dedicated producer + read-only** (§5.3) | new node + flags | none | none (sole writer) | **yes** | consumer fails on incomplete cache (unless fallback) | many consumers (joins) |
| **Rely on Iceberg atomic commit** (no edge) | none | possible | tolerated (last-writer-wins) | no | occasional commit-conflict retries | not recommended |

Notes:
- **Ordered consumers** gives the full value (build-once, race-free) with the least
  machinery and the safest failure mode, *because both jobs run daily for the same
  `ds`* — the de-facto producer is just "whichever runs first," pinned by the edge.
- **Dedicated producer** is strictly more orchestration and introduces blind
  read-only consumers (hence the completeness-gate prerequisite); its advantage only
  materializes with **>2 consumers**, where it avoids a web of ordering edges.
- The **independent** and **Iceberg-commit** options do not reliably eliminate
  recomputation and are listed for completeness, not recommendation.

### 5.5 Ad hoc / opt-in (default)

A single job in ensure-then-read mode. The first run builds the full window (≈ one
normal backfill cost); later runs fill only the new day and read the cache. No edge,
no producer, no flags — safe for per-config opt-in and for the first enable.

## 6. Rollout

Opt-in, per config. New qualifying configs (SNAPSHOT + events + windowed agg) are
nudged toward `is_incremental=True` via docs/templates (and optionally a compile-time
hint). We do **not** mass-enable existing configs:

- Most configs (short window or low fan-in) gain little; the speedup scales with
  `window / stepDays` and with fan-in (raw-rows-per-key-per-day).
- The first enable pays a one-time full-window build, so blanket enablement would be
  a cluster-capacity event.

Recommended first-enable sequence per config: enable + compile → run once with
ensure-then-read and a sensible `stepDays` (builds `_daily_inc`) → verify the window
is complete (and optionally diff one output day vs the raw path) → then run daily
with the ordered `backfill → upload` edge (§5.2). The dedicated producer/read-only
DAG (§5.3) is only needed once additional consumers (joins) share the same cache.

## 7. Correctness & testing

The incremental path must equal the raw path. Validated by:
- Per-IR-type equivalence (sum/count/avg/min/max/last/percentile/unique-count/
  histogram) vs the non-incremental path.
- The write-clamp regression (filling one hole writes only its partitions).
- Shared-column dedup (collapsing windows still reconstruct correct per-window output).
- **Data quality:** every event-day's IR is built completely including range
  boundaries; incremental snapshot windowing matches the raw path day-by-day at the
  window tail boundary; incremental upload output matches a non-incremental upload.

### Accepted, documented limitations

- `LAST`/`FIRST` and approximate sketches (KLL/CPC) can differ from the raw path on
  tied timestamps / due to sketch nondeterminism — pre-existing, not introduced here.
- **Late arrivals:** an event whose event-time is on day D but lands in source
  partition D+1 is only captured once D+1 exists. For isolated single-day hole-fills
  of historical days this can under-count; a full build reads every partition and is
  unaffected. A bounded forward-read (e.g. +1 day) is a possible future enhancement.

## 8. Future work

- **Join incremental right-parts:** snapshot right-parts consume `fromIncrementalDf`;
  a GroupBy used by its own backfill plus N joins builds `_daily_inc` once and all
  consumers read it.
- **Weekly rollup tier:** for very long windows, a coarse weekly hop band could cut
  read-back/windowing cost. Only worth it if windowing is shown to dominate the warm
  run; not currently planned.
- **Late-arrival forward read** and an optional **write-completeness assertion** for
  stricter production safety.
