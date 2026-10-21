---
description: "Databricks performance: what Photon accelerates, disk cache vs Spark cache, serverless limits, UDF costs, a tuning order and honest local results."
---

# 07 · Performance: Photon, Serverless & Caching

Performance work is mostly about *order of operations*: fix what the query reads, then how it joins, then what it runs on. This
lesson places the platform features (Photon, caches, serverless) in that order, and is candid about a local experiment that
produced a weak result and another that hung.

!!! note "What was run, and what wasn't"
    Photon, the Databricks disk cache and serverless compute are platform features and **were not run**; claims about them come
    from the Databricks documentation (October 2026). Locally (Spark 4.2.0 on a two-core laptop) I ran a caching experiment, which
    showed the cached plan but **no measurable speedup** at this tiny scale, and I tried a Python-UDF-versus-built-in-function
    timing, which **hung** and so produced no numbers. I report both. I make no performance-ratio claims that I did not measure.
    Where the documentation states a vendor benchmark ("up to 5x better price/performance" for Photon) I quote it as the vendor's
    claim; I did not verify it.

## A tuning order that works

1. **Read less.** Filter early, select only needed columns, rely on data skipping with good clustering (Level 2 · 03 and 04).
   Nothing else beats not reading the data.
2. **Shuffle less, and shuffle evenly.** Broadcast small tables, fix skew, avoid needless `distinct`/`repartition`
   (Level 2 · 05 and 06).
3. **Avoid row-at-a-time code.** Prefer built-in functions to UDFs (below).
4. **Cache only what you reuse.**
5. **Then choose the engine and compute** (Photon, serverless, size).
6. **Measure with the query profile / Spark UI**, not intuition: find the longest stage, the biggest shuffle, the spill.

Steps 1 and 2 are code and layout changes; steps 5 and 6 are configuration. People tend to start at step 5 because it's a
setting. It is usually the least effective place to start.

## Photon

From the docs: Photon is Databricks' native vectorized query engine. For supported operations it replaces the JVM-based Spark SQL
execution engine with a native C++ runtime, while the Catalyst optimizer still plans the query; on an unsupported operation it
transparently falls back to Spark for the rest of that operation. It processes columnar batches (thousands of rows at a time), can use
SIMD instructions, and avoids JVM garbage-collection and warm-up costs. The docs list benefits for scans (filter pushdown,
row-group skipping), joins and shuffles (hash joins, a columnar shuffle), writes (a native Parquet writer that speeds Delta,
Iceberg and Parquet writes including `MERGE`, `UPDATE`, `DELETE` and CTAS) and the disk cache.

Practical points from the same page:

- It needs **no code changes**: SQL and DataFrame APIs in all languages benefit.
- It helps most for **longer-running queries on large data**; queries finishing in under about two seconds see no meaningful
  gain, because planning and scheduling dominate.
- **Enablement:** on by default for SQL warehouses and serverless compute; for classic all-purpose, jobs and classic pipelines it
  is on by default, controlled by a "Use Photon Acceleration" setting, but when you create compute through the Clusters or Jobs
  **APIs** you must set it explicitly (`runtime_engine` set to `PHOTON`; `photon: true` for the Pipelines API).
- It accelerates **stateless** streaming but **not stateful** streaming (Level 2 · 07's windowed aggregation would not benefit).
- Photon can change a workload's DBU consumption and price; I did not verify the current pricing rules, so test with your jobs
  and check the pricing pages (Lesson 06).
- Operations Photon doesn't support fall back to the Spark engine (per the docs), so a plan can mix Photon and non-Photon operators;
  the query profile shows which. I did not verify which specific expressions fall back.

## Caching: three different things

| Cache | What it caches | How you get it |
|---|---|---|
| **Disk cache** (formerly Delta cache / DBIO cache) | Parquet/Delta *files* on workers' local disks | Automatic on supported instance types; proprietary Databricks feature |
| **Spark cache** (`df.cache()`) | A DataFrame's *computed result* in memory/disk | You call it explicitly; per application |
| **Query/result cache** | The *result of a SQL query* | Automatic for SQL warehouses and dashboards; see the caching docs for its rules |

The docs recommend using the **disk cache instead of the Spark cache for repeated reads of Parquet and Delta data**, and note that
on SQL warehouses and Databricks Runtime 14.2+ the older `CACHE SELECT` command is ignored in favour of an enhanced disk-caching
algorithm. Use Spark `cache()` when you reuse a *computed intermediate* (an expensive join result) several times in one job, and
`unpersist()` it when done: cached data occupies executor memory that your other operations need.

### A local cache experiment, and what it did and didn't show

I built a DataFrame of 400,000 rows with a SHA-256 hash column, grouped it, and counted the distinct hashes per group, three times
uncached and three times after `cache()` plus one materializing action (local Spark, two cores):

```text
uncached: recomputed every action        runs(s)=[1.18, 0.12, 0.09]
cached (after one materializing count)   runs(s)=[0.15, 0.08, 0.07]
```

The physical plan after caching did show the cache in use:

```text
HashAggregate(keys=[g], functions=[count(1)])
+- Exchange hashpartitioning(g, 8)
   +- HashAggregate(keys=[g], functions=[partial_count(1)])
      +- InMemoryTableScan [g]
            +- InMemoryRelation [id, h, g], StorageLevel(disk, memory, deserialized, 1 replicas)
```

But the timings do **not** demonstrate a benefit. The first uncached run (1.18 s) most likely includes one-time JVM and code-generation
warm-up (I did not isolate it); once warm, the uncached runs (0.12 s, 0.09 s) are about as fast as the cached ones (0.08 s, 0.07 s), because this dataset
is tiny and the "expensive" lineage (a range and a hash) isn't expensive enough. The lesson is a methodological one: **a micro-
benchmark on a small laptop dataset can't show a caching effect, and warm-up effects can fool you into seeing one.** To see
real gains you need data large enough that recomputation costs seconds-to-minutes, repeated runs, and the platform's own
metrics. I'd rather show this honestly than print a flattering ratio.

## UDFs: why they are slow, and a failed measurement

A Python UDF runs your function in a separate Python process: for each batch, Spark serializes rows out of the JVM, calls Python, and
serializes results back. Pandas (Arrow-based) UDFs process whole columns at a time, cutting the per-row overhead, and built-in
functions run entirely inside the engine (and in Photon, if enabled). The ordering "built-in, then pandas UDF, then row-at-a-time
Python UDF" is standard Spark guidance, and Python code can't run inside a native engine, so UDF logic itself isn't accelerated by Photon (my reasoning, not a quote from the docs).

I tried to measure it: the same arithmetic over 300,000 rows with a built-in expression, a Python UDF and a pandas UDF. **The
script hung with idle Python workers and I killed it after several minutes, so I have no timings to report.** I did not
find the cause (the same session ran built-in expressions fine, so it was specific to the Python-worker path on that machine's
Python 3.12 / pandas 2 / NumPy 2.5 environment). If you try this yourself and see similar behaviour, check that
`PYSPARK_PYTHON` matches the driver's Python (Level 1 · 10) and test with a tiny input first. I'd rather state that failure than
substitute figures from memory.

Rule that stands regardless: if a built-in function or a SQL expression can do it, use that.

## Serverless compute: convenience with constraints

Serverless removes cluster configuration, starts quickly and scales automatically, but the documentation lists limitations
worth knowing before you migrate a job (October 2026; the list changes):

- Only **Spark Connect** APIs are supported; **RDD APIs are not**; R is not supported.
- Spark Connect defers analysis and name resolution to execution time, which can change behaviour of existing code.
- **ANSI mode is on by default** (invalid casts and overflows raise errors); the default session time zone is **UTC**.
- You must use **Unity Catalog** to reach external data; DBFS access is limited; `dbutils`/working-directory assumptions can break
  (use absolute paths and workspace files).
- Maven library coordinates aren't supported; UDFs can't access the internet and have a 1 GB memory limit; global temp views aren't
  supported; and **the Spark UI is not available**: use the **query profile**.

None of this makes serverless a bad choice; for most new workloads the docs recommend it. It means *test* before moving a legacy job.

## Reading the evidence

Whether in the Spark UI (classic) or the query profile (serverless/SQL):

1. Sort operators by time; find the one that dominates.
2. Compare **rows/bytes read** with **rows/bytes returned**: a huge scan with a tiny result means filtering or clustering is the
   fix.
3. Check **shuffle read/write**, **spill**, and **task time distribution** (skew).
4. Look at the number of **files** read and pruned.
5. Only then change compute (size/type/Photon), and re-measure on the same input.

## How It Actually Works

Photon sits below the optimizer: Catalyst produces a physical plan, and Photon's planner converts the supported operators into
Photon operators running on columnar batches in native code, inserting transitions between Photon and JVM operators where it
must fall back. The disk cache sits below the file reader: when a Parquet file is read from cloud storage it is copied to local
SSD in a fast format, and later reads for the same file, by any query on that node, hit local disk. The Spark cache sits at the
DataFrame level: it stores computed partitions in executors' memory/disk, keyed by the logical plan, so only identical
plans reuse it. That's why a disk cache benefits many different queries over the same files while a Spark cache helps only the
same reused intermediate.

## Common mistakes

- **Changing cluster size first.**
- **`df.cache()` on everything**, starving memory.
- **Benchmarking on tiny data** (as above) or including warm-up in timings.
- **Row-at-a-time Python UDFs** where a built-in exists.
- **Assuming Photon speeds up everything** (short queries, Python UDFs and stateful streaming don't benefit).
- **Migrating to serverless without testing** RDD/DBFS/relative-path usage.
- **Not looking at the query profile** before tuning.

## Exercise

1. Re-run the cache experiment with 40 million rows (if your machine allows) and a genuinely expensive lineage (a join), and report
   whether a difference appears. Include a warm-up run that you discard.
2. Take one of your Level 1 or 2 queries, produce its plan with `EXPLAIN FORMATTED`, and identify the scan's pushed filters and read
   columns. Make one change that reduces bytes read.
3. Write a checklist for migrating an existing job to serverless, using the limitations above.
4. If you have a workspace, run the same query on a classic warehouse and a serverless warehouse and compare the query profile (not just
   the wall-clock time).
