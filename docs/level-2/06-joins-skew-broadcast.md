---
description: "Pick join strategies with hints, detect data skew by measuring rows per partition, fix it with broadcast joins and salting, and know what AQE does."
---

# 06 · Joins, Broadcast & Data Skew

Most slow Spark jobs are slow joins, and most slow joins are slow because of one of three things: a join that shuffles
a table it didn't need to, a key that doesn't match well, or **skew** — one key owning so many rows that a single task
does most of the work. This lesson measures each.

!!! note "What was run"
    Plans and partition sizes are from **local Spark 4.2.0**, with a synthetic 400,000-row fact table where 80% of rows
    share one key, and a 1,001-row dimension. Row counts per partition are exact and reproducible; I didn't measure run
    times, since a two-core laptop can't show cluster stragglers faithfully. The AQE skew-join result below is a
    negative result on this tiny dataset and is reported as such.

## The strategies

| Strategy | When Spark picks it | Cost |
|---|---|---|
| **Broadcast hash join** | One side is below `spark.sql.autoBroadcastJoinThreshold` (10 MB default), or hinted | Small side shipped to every executor; large side not shuffled. Fastest |
| **Shuffle hash join** | Hinted, or the planner estimates one side is much smaller | Both sides shuffled; a hash table is built on the smaller side per partition |
| **Sort-merge join** | Default for large-large equi-joins | Both sides shuffled and sorted; scales to huge inputs; sensitive to skew |
| **Broadcast nested loop / cartesian** | Non-equi joins without keys | Quadratic; avoid |

You can ask for a strategy with a hint, in SQL or DataFrame code:

```python
from pyspark.sql.functions import broadcast
fact.join(broadcast(dim), "k")
```

```sql
SELECT /*+ BROADCAST(d) */ * FROM f JOIN d ON f.k = d.k;     -- also SHUFFLE_HASH(d), MERGE(d)
```

The local plans confirmed each hint: `BROADCAST(d)` produced a `BroadcastHashJoin ... BuildRight` with a
`BroadcastExchange` over the dimension; `SHUFFLE_HASH(d)` produced `ShuffledHashJoin ... BuildRight`; the default with
broadcasting disabled produced `SortMergeJoin`. Broadcasting is only safe for genuinely small tables: a hinted join
whose "small" side is a few GB can crash executors with out-of-memory errors, since the whole table is held in
memory on every one of them.

Databricks adds optimizer features (for example, dynamic file pruning and runtime filters that skip fact-table data
using the dimension's matches) that aren't in plain Spark; they're documented but I couldn't run them.

## Detecting skew

Skew means the key distribution isn't uniform. First look at the key counts:

```python
fact.groupBy("k").count().orderBy(F.desc("count")).limit(3).collect()
# [Row(k=0, count=319925), Row(k=100, count=109), Row(k=803, count=107)]
```

One key holds 319,925 of 400,000 rows (80%); the next hundred or so. Now see what that does to a shuffle on `k`, by
counting rows per partition after `repartition(8, "k")`:

```text
rows per shuffle partition (hash by k):
[8786, 9767, 9941, 10145, 10705, 10761, 10812, 329083]
```

Seven partitions hold roughly 10,000 rows each and one holds 329,083, about 32 times the others (the hot key plus its
hash neighbours). Every row for key `0` must be processed by the *same* task. On a cluster that task is the straggler:
the stage finishes only when it does, other cores sit idle, and it may spill to disk or run out of memory.
In the Spark UI you'd see this as one task with a far longer duration and larger shuffle-read size than the median.

Typical sources of skew: NULL or default keys (`-1`, `'unknown'`) that pile up, popular entities (one huge customer,
a viral product), and time-based keys when everything arrives "today".

## Fix 1: broadcast the small side

If the other side is small enough to broadcast, the fact table isn't shuffled at all, so skew in its keys is
irrelevant. This is the first thing to try.

## Fix 2: handle the hot key separately

Filter the hot key out, join it differently (broadcast, or aggregate it directly), and `union` the results. This is
explicit and often the clearest; it needs you to know which keys are hot.

## Fix 3: salting

Add a random suffix to the fact table's key, replicate the dimension rows across all suffixes, and join on
`(key, salt)`. The hot key is split across `N` partitions:

```python
N = 4
salted = fact.withColumn("salt", (F.rand() * N).cast("int"))
dim_s  = dim.crossJoin(spark.range(0, N).withColumnRenamed("id", "salt"))
joined = salted.join(dim_s, ["k", "salt"])
```

Local measurement, rows per partition after `repartition(8, "k", "salt")`:

```text
[9896, 10117, 10392, 10637, 89246, 89482, 89854, 90376]
```

The 329,083-row partition became four partitions of about 90,000 rows. It isn't perfectly flat (the hot key's four
salted slices are still ~9× the others), but the worst task dropped to about 27% of its former size. The result was
unchanged: the salted join returned 400,000 rows, the same as the plain join. Costs: the dimension is replicated `N`
times (fine for 1,001 rows, expensive for big dimensions), and `N` is a tuning parameter. Use a bigger `N` for hotter
keys, and apply salting only to the hot keys when you can.

## AQE skew handling

The Databricks documentation lists skew-join handling among AQE's features: it detects skewed shuffle partitions in
sort-merge and shuffle-hash joins at runtime and splits them into smaller tasks, replicating the other side's matching
partition. It's governed by settings including `spark.sql.adaptive.skewJoin.enabled`, the skewed-partition factor and
threshold in bytes.

I tried it on the local data with thresholds lowered (`skewedPartitionThresholdInBytes = 64KB`, `skewedPartitionFactor =
2`). **It did not trigger**: the final plan was a `SortMergeJoin` without a skew-join marker (`isSkew=true` absent). A
likely reason is that with AQE on and a tiny dataset, the shuffle outputs were too small for the byte thresholds
and advisory sizing to classify anything as skewed, but I didn't confirm the cause. The lesson is practical: don't
assume AQE fixed your skew; look for the skew marker in the final plan, or compare task durations in the Spark UI.

## Join correctness checklist

- **Type-match keys.** `INT` vs `BIGINT` created a `cast(k as bigint)` on every row in the plans above.
- **NULL keys never match** in an equi-join (`NULL = NULL` is not true); use `eqNullSafe` or `<=>` if you need them to.
- **Duplicate keys multiply rows.** Joining on a non-unique dimension key inflates fact counts silently. Check
  `count` before and after: here 400,000 before and 400,000 after.
- **Exploding intermediate joins**: filter and aggregate early.
- **`approx_count_distinct`** is a cheap guide for key cardinality: on the fact table it returned 1,026 vs the exact
  `countDistinct` of 1,001 (an approximation with about a few percent error by default), useful for deciding whether a
  key is safe to partition or salt on, and far cheaper than the exact count on big data.

## How It Actually Works

A shuffle partitions rows with `hash(key) mod numPartitions`. Equal keys always hash to the same partition, which is
what makes joins and groupings possible and also what makes skew inevitable: no hash function can split one key.
Salting works by making the *effective* key `(k, salt)`, which hashes to different partitions.

A broadcast join avoids this by turning the join into a local lookup: the driver collects the small relation, serializes
it as a hash table and sends it to every executor (a `BroadcastExchange`), and each task joins its slice of the large
side against it. The cost is memory per executor and driver collection time, which is why Spark gates it with a size
threshold.

AQE's skew handling is salting done automatically at runtime, using actual partition sizes: it splits a big shuffle
partition into several by map-output ranges, and duplicates the matching partition from the other side for each piece.

## Common mistakes

- **Broadcasting a table that isn't small**, causing executor OOM.
- **Disabling broadcast globally** (`-1`) after one bad case.
- **Salting everything** when only a handful of keys are hot.
- **Ignoring NULL/default keys** as the true hot key.
- **Benchmarking skew fixes on tiny data** and concluding they don't matter, or that AQE "didn't work".
- **Joining on a non-unique key** and not noticing the row count increase.

## Exercise

1. Reproduce the skew measurement with your own data: generate a table where 50% of rows share one key and print rows
   per shuffle partition.
2. Apply salting with `N = 2, 4, 16`. Record the largest partition for each. Where do returns diminish?
3. Make the dimension 5 million rows (use `spark.range`) and decide whether salting or a hot-key-only split is better.
   Say why.
4. Search the final plan of a join in your own session for `isSkew`. If absent, find the stage with the longest task in
   the Spark UI and compare it with the median.
