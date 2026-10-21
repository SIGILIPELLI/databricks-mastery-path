---
description: "How Spark runs a query: jobs, stages, shuffles, reading physical plans, join strategies and what Adaptive Query Execution changes, from local runs."
---

# 05 · How Spark Executes: Stages, Shuffles & AQE

You can write Spark for years without looking inside it, until a job runs for six hours and nobody knows why. The
vocabulary of the Spark UI and of `explain()` (job, stage, task, shuffle, `Exchange`) is the toolkit for finding out. This
lesson builds it from real plans.

!!! note "What was run"
    Plans and partition counts are from **local Spark 4.2.0 + Delta 4.4.1**, reading Delta tables of 100,000 and 1,000
    rows. Plan operator names are standard Spark; on Databricks with Photon you'll see Photon operators in their place
    (documented, not run here), and the Spark UI looks different from a local one.

## The execution hierarchy

- A **job** is triggered by an action (`count`, `show`, a write).
- A job is divided into **stages** at every shuffle boundary.
- A stage is a set of identical **tasks**, one per data partition, run in parallel on executor cores.
- A **shuffle** redistributes rows across the cluster so that rows with the same key end up together (needed for joins,
  groupings, distinct, repartition, global sorts and window functions).

Narrow operations (`filter`, `select`, `withColumn`, `map`-style) process each partition independently and are pipelined
into one stage. Wide operations require a shuffle: write every partition's output to local disk, then fetch the right
slice from every other node. Shuffles are where Spark spends most of its time and fails most often.

## Reading a physical plan

```python
A.groupBy("k").agg(F.sum("v")).explain()    # A: 100,000 rows read from a Delta table
```

Local output (AQE disabled for clarity, shortened):

```text
*(2) HashAggregate(keys=[k], functions=[sum(v)])
+- Exchange hashpartitioning(k, 8), ENSURE_REQUIREMENTS
   +- *(1) HashAggregate(keys=[k], functions=[partial_sum(v)])
      +- *(1) ColumnarToRow
         +- FileScan parquet [k,v] ... PartitionFilters: [], PushedFilters: [], ReadSchema: ...
```

Read bottom to top. The scan reads only `k` and `v` (column pruning). **Stage 1** (`*(1)`) does a *partial*
aggregation inside each scan task, so each task pre-sums its own rows. Then `Exchange hashpartitioning(k, 8)` is the
shuffle: rows are routed into 8 partitions by hash of `k`. **Stage 2** (`*(2)`) merges the partial sums. The partial
aggregate before the shuffle is why grouping 100,000 rows into 1,000 groups shuffles roughly 1,000 rows per task, not
100,000. Anywhere you see `Exchange`, there's a stage boundary and a network/disk cost.

## Join strategies

Joining a 100,000-row table `A` to a 1,000-row `B`, with broadcasting disabled
(`spark.sql.autoBroadcastJoinThreshold = -1`):

```text
SortMergeJoin [cast(k as bigint)], [k2], Inner
:- Sort [cast(k as bigint) ASC]
:  +- Exchange hashpartitioning(cast(k as bigint), 8)
:     +- Filter isnotnull(k) ... FileScan parquet [id,k,v]
+- Sort [k2 ASC]
   +- Exchange hashpartitioning(k2, 8)
      +- Filter isnotnull(k2) ... FileScan parquet [k2,name]
```

Both sides are shuffled by the join key (two `Exchange`s) and sorted, then merged. With the default threshold (10 MB)
Spark chose a different plan, because `B` is tiny:

```text
BroadcastHashJoin [cast(k as bigint)], [k2], Inner, BuildRight
:- Filter isnotnull(k) ... FileScan parquet [id,k,v]
+- BroadcastExchange HashedRelationBroadcastMode(...)
   +- Filter isnotnull(k2) ... FileScan parquet [k2,name]
```

Here the small side is collected, shipped to every executor, and held as a hash table; the large side is never
shuffled. That is the biggest single win in most Spark tuning (Lesson 06 shows when and how to force it).

One more detail in these plans: `cast(k as bigint)`. `k` is an `INT` and `k2` is a `BIGINT`, so Spark cast the
`INT` side to match. Mismatched key types force casts on every row, and can prevent the use of statistics or
partitioning on the cast column. Join keys should have identical types.

## Adaptive Query Execution (AQE)

Static plans use guessed sizes. **AQE** (enabled by default in current Spark and, per its documentation, in Databricks) re-optimizes the plan
at shuffle boundaries using the real sizes of the shuffle output. Three main behaviours:

1. **Coalescing shuffle partitions**: many small post-shuffle partitions are merged.
2. **Switching join strategy**: a sort-merge join becomes a broadcast join if one side turns out to be small.
3. **Skew-join handling**: oversized partitions are split (Lesson 06).

Local demonstration: with `spark.sql.shuffle.partitions = 200` and the same group-by on 1,000 distinct keys:

```text
AQE off:  result has 200 partitions     (199 mostly empty tasks)
AQE on:   result has 1 partition        (plan shows: AQEShuffleRead coalesced)
```

The final plan printed `AdaptiveSparkPlan isFinalPlan=true`, with the Initial Plan (200 shuffle partitions) shown below
the Final Plan (`AQEShuffleRead coalesced`). 200 tasks for a few kilobytes of data would have mostly been scheduling
overhead. On Databricks, the documented default for `spark.sql.shuffle.partitions` is still 200, and setting it to `auto` enables
auto-optimized shuffle, which chooses the number from the query plan and input size (per the AQE documentation).

## The Spark UI mental model

Whether it's the local UI or the Databricks Spark UI, the workflow is the same:

1. **Jobs tab / SQL tab**: find the slow query. The SQL tab shows the plan with row counts and time per operator.
2. **Stage view**: look at the *task duration distribution*. Roughly equal tasks mean healthy parallelism. A maximum far
   above the median means **skew** (one task has too much data).
3. **Shuffle read/write sizes**: huge shuffle volume relative to input suggests a join or aggregation that could be
   reduced (filter earlier, broadcast, pre-aggregate).
4. **Spill (memory/disk)**: tasks that exceed memory write to disk and slow down; fix by more partitions, more memory
   per core, or less data per task.
5. **GC time** high relative to run time: too many objects or too little memory.

I didn't capture UI screenshots because the local runs had the UI disabled; the numbers above come from `explain()`
and partition counts.

## How It Actually Works

When a job is submitted, the DAG scheduler walks the physical plan backwards from the result, cutting at each shuffle to
form stages. Stages are run in dependency order; each stage's tasks go to a task scheduler that places them on executor
cores, preferring nodes that already hold the input data. A shuffle write puts each task's output into one file per
reducer partition on local disk (or an external shuffle service); the downstream stage's tasks then fetch their
partition from every upstream task. If an executor dies, lost shuffle files mean re-running the producing tasks.

AQE adds *query stages* as materialization points: at each shuffle it finishes the upstream stage, reads its
statistics, then re-plans the rest of the query. That's why plans print as `AdaptiveSparkPlan` with an initial and a
final version.

## Common mistakes

- **Tuning `shuffle.partitions` once for all jobs.** The right number varies with data volume; prefer AQE and
  platform defaults over a hard-coded 200 or 2000.
- **Not knowing where the `Exchange`s are.** Each is an opportunity to reduce data first.
- **Joining on mismatched types** (INT vs BIGINT, string vs int).
- **Calling `repartition(...)` reflexively.** It forces an extra shuffle.
- **Judging performance on tiny local data.** The shape of the plan transfers; the timings don't.
- **Using `collect()`/`toPandas()` on a stage's output** and blaming Spark for driver memory errors.

## Exercise

1. Run `explain("formatted")` on a query with a join, a filter and a group-by. Mark every `Exchange` and name the key.
2. Turn AQE off and on for the 200-partition group-by and record the partition counts. Explain the difference in one
   sentence.
3. Change the join key type on one side (cast `k` to string) and compare plans. What changed?
4. Open the Spark UI of a local session (`spark.ui.enabled=true`, port 4040) while a join runs and find the stage whose
   `Exchange` feeds the join.
