---
description: "Structured Streaming with Delta: triggers, checkpoints and exactly-once, a duplicate-on-lost-checkpoint demo, watermarks and foreachBatch upserts."
---

# 07 · Structured Streaming & Checkpoints

Structured Streaming treats a stream as an unbounded table: you write the same DataFrame logic as for batch, and Spark
runs it incrementally as new data arrives. On Databricks it's the engine under Auto Loader, Lakeflow pipelines
and most incremental ETL. Delta is both a great source and a great sink for it.

!!! note "What was run"
    Everything below ran on **local Spark 4.2.0 + Delta 4.4.1**, using a folder of JSON files as the source and Delta
    tables as sinks, with `trigger(availableNow=True)` so each run processes what's there and stops. That is
    deterministic and easy to demonstrate; continuous triggers, cluster restarts and cloud storage failure modes
    weren't exercised. Auto Loader and Lakeflow are covered in later lessons and were not run.

## Source → transform → sink

```python
sch = "id INT, user STRING, amt DOUBLE, ts TIMESTAMP"

stream = spark.readStream.schema(sch).json("in/")            # a directory of JSON files, new files = new data
query = (stream.writeStream.format("delta")
         .option("checkpointLocation", "ckpt/")             # REQUIRED for restartable, exactly-once behaviour
         .trigger(availableNow=True)                          # process all available data, then stop
         .start("out/"))                                      # or .toTable("catalog.schema.table")
query.awaitTermination()
```

Streaming file sources need an explicit schema (or schema inference turned on explicitly), because a stream can't
look ahead at data that hasn't arrived.

**Triggers** control cadence:

| Trigger | Behaviour |
|---|---|
| default (unspecified) | Start the next micro-batch as soon as the previous finishes |
| `processingTime="1 minute"` | Fixed-interval micro-batches |
| `availableNow=True` | Process everything available (possibly in several batches), then stop. Ideal for scheduled "incremental batch" jobs |
| `once=True` | Legacy single-batch version of the above (deprecated in favour of `availableNow`) |

`availableNow` is the workhorse of cost-efficient pipelines: a job that wakes hourly, processes whatever arrived, and
shuts the cluster down, with the checkpoint remembering where it stopped.

## Checkpoints give you exactly-once

First run, with two JSON rows in `in/`; second run with no new files; third run after dropping another file with one row.
Local results:

```text
run 1  numInputRows 2   batchId 0     -> output table has 2 rows
run 2  numInputRows 0                  -> (no new files: nothing to do)
run 3  numInputRows 1   batchId 1     -> output table has 3 rows
```

Each run picked up only files it hadn't seen. The checkpoint folder after these runs contained:

```text
ckpt/
├── metadata          # query id
├── offsets/0, 1      # "about to process this range" - written BEFORE each batch
├── commits/0, 1      # "finished this batch"         - written AFTER each batch
└── sources/          # file-source bookkeeping
```

`offsets/1` held the batch's timestamps and a long list of Spark configs (state store settings, for instance) plus the source
offsets. Restart logic: if `offsets/N` exists but `commits/N` doesn't, the batch is replayed, and Delta's
transaction (keyed by the checkpoint's query id and batch id) makes the re-run idempotent, so a crashed batch doesn't
double-write. That's what "exactly-once" means here: at-least-once reading + idempotent Delta writes.

### What happens if you lose the checkpoint

I deleted `ckpt/` and ran the same query again:

```text
after deleting checkpoint, rows written again: 3     total rows in output: 6

 id | c
  1 | 2
  2 | 2
  3 | 2
```

Without its checkpoint the query has no memory, treats every file as new, and **appends everything again**: every id
now appears twice. The checkpoint is *state*, as important as data. Treat it like a database file: store it durably,
never share one between queries, never edit it, and don't delete it to "fix" a stuck stream without understanding the
duplicate consequence. (Starting a fresh checkpoint on purpose, with a plan for the overlap, is how you replay.)

Changing a query's logic can also make the old checkpoint incompatible (for stateful operators especially), which
forces the same choice between reprocessing and a migration plan.

## Time, windows and watermarks

An aggregation like "revenue per 10-minute window" must decide when a window is *finished*, otherwise it keeps state forever and
can never emit a final result in `append` mode. A **watermark** says how late data may arrive:

```python
agg = (stream.withWatermark("ts", "5 minutes")
             .groupBy(F.window("ts", "10 minutes"), "user")
             .agg(F.sum("amt").alias("t")))
agg.writeStream.format("delta").outputMode("append")...
```

The test data had events up to 10:09 in the first window, and one at 10:20. Output (window start, user, total):

```text
2026-01-01 10:00:00   a   8.0     (5 + 1 + 2)
2026-01-01 10:00:00   b   7.0
```

The `10:00–10:10` window was emitted, but the window containing the 10:20 event was **not**: the watermark is the maximum
event time seen minus 5 minutes (10:15), which has passed the end of `10:00–10:10` but not of the window containing
10:20, so that window is still open. That's correct behaviour, and surprises everyone once: *the last window of a
quiet stream may never appear* until later events advance the watermark. Events arriving later than the watermark for
a closed window are dropped from the aggregate. Pick the watermark by measuring real lateness, not by guessing.

Output modes: `append` (only finalized rows; needs a watermark for aggregations), `update` (changed rows),
`complete` (whole result table every batch; small aggregates only).

## Upserts with foreachBatch

When the sink must be a `MERGE` (Lesson 01), use `foreachBatch`, which hands you each micro-batch as an ordinary
DataFrame:

```python
def upsert(batch_df, batch_id):
    d = batch_df.dropDuplicates(["id"])                       # MERGE needs one source row per key
    (DeltaTable.forPath(spark, "tgt").alias("t")
        .merge(d.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

(stream.writeStream.foreachBatch(upsert)
       .option("checkpointLocation", "ck3/").trigger(availableNow=True).start())
```

Two runs over the same input gave `tgt rows 5` both times: the merge is idempotent, so the second run (when the
checkpoint said everything was processed anyway) added nothing. `foreachBatch` gives *at-least-once* delivery to your
function, so the function itself must be idempotent (merge on a key, or use `batch_id` to deduplicate writes to
non-Delta sinks).

## Reading a Delta table as a stream

```python
spark.readStream.table("bronze_orders")                     # new commits become new micro-batches
spark.readStream.option("skipChangeCommits", "true").table("bronze_orders")   # tolerate updates/deletes upstream
```

A Delta streaming source reads *appended* data. If the upstream table is updated or deleted in place (a
`MERGE`, `DELETE` or `RESTORE`), a plain stream fails when it meets a commit that changes existing data, since it
would otherwise re-emit rewritten files as new rows. Options: `skipChangeCommits` (ignore those commits), or read
the Change Data Feed (Lesson 01) to see deletes and updates properly. Compaction commits from `OPTIMIZE` are marked
`dataChange = false` and are ignored.

## How It Actually Works

Each micro-batch is a normal Spark job. The driver asks every source for new offsets (new files, new Delta versions, Kafka
offsets), records the planned range in `offsets/N` in the checkpoint, runs the incremental query plan over just that
range (including state for aggregations and joins, stored in a state store such as RocksDB and snapshotted to the
checkpoint), writes to the sink, then records `commits/N`. Failure between "planned" and "committed" means replay of the
same range, which the idempotent sink absorbs.

The Delta sink achieves idempotence with transaction identifiers: each commit includes a `txn` action (query id,
batch id), and before writing batch N the sink checks the log for an existing `txn` with that id.

## Common mistakes

- **Deleting or moving the checkpoint** (duplicates, as shown).
- **Sharing one checkpoint** between two queries.
- **No watermark on a stateful query**, so state grows without bound.
- **Expecting the final window to emit** while the stream is quiet.
- **Non-idempotent `foreachBatch` code** (appending to a database without keys).
- **A continuous trigger on an all-purpose cluster** that never terminates, billing around the clock for tiny volumes
  (consider `availableNow` on a schedule).
- **Reading an updated Delta table as a stream** without `skipChangeCommits` or CDF.
- **Many tiny micro-batches** producing many small files (compact and tune triggers).

## Exercise

1. Reproduce the three runs and inspect `offsets/` and `commits/` after each. Delete only `commits/1` and rerun. What
   happens, and why is the output still correct?
2. Remove the watermark from the windowed query and try `outputMode("append")`. What error does Spark raise?
3. Add a duplicate row to a new input file and show that the `foreachBatch` upsert collapses it, while the plain Delta
   sink would append it.
4. Write a 10-line plan: a pipeline running hourly on `availableNow`, with checkpoint location, failure retry, and what
   you'd alert on.
