---
description: "Keep Delta tables healthy: OPTIMIZE, VACUUM, ANALYZE, deletion vectors and REORG PURGE measured locally, and Predictive Optimization per the docs."
---

# 08 · Table Maintenance & Predictive Optimization

Delta tables don't maintain themselves. Every write adds files; every `UPDATE` or `DELETE` leaves old files behind; statistics go
stale; clustering decays as new data lands. On Databricks, much of this can now be automated, but you still need to know
what the maintenance operations do, because the defaults interact with time travel, cost and correctness.

!!! note "What was run, and what wasn't"
    The deletion-vector experiment below ran on **open-source Delta Lake 4.4.1 / local Spark 4.2.0**, and its numbers are copied from
    that run. `OPTIMIZE` and `VACUUM` behaviour was measured in Level 1 and 2. **Predictive Optimization, auto compaction and optimized
    writes, and automatic liquid clustering are Databricks features that were not run**; they are described from the Databricks documentation
    (October 2026). Their defaults and rollout schedules change, so verify what your account actually does.

## The four chores

| Chore | What it fixes | Command |
|---|---|---|
| **Compaction / clustering** | Small files; poor data skipping | `OPTIMIZE` (Level 2 · 03) |
| **Cleanup** | Unreferenced files (storage cost) | `VACUUM` (Level 1 · 08) |
| **Statistics** | Stale table/column stats used by the optimizer | `ANALYZE TABLE ... COMPUTE STATISTICS` |
| **Purging deleted data** | Rows that are "deleted" but still physically present | `REORG TABLE ... APPLY (PURGE)` |

The first two you've met. The last two deserve a closer look.

## Deletion vectors: cheap deletes, deferred cost

Without deletion vectors, changing one row means rewriting the whole Parquet file that contains it. With them enabled, Delta marks
rows as deleted in a small side structure and applies it at read time. I measured both on a 200,000-row table in four files
(about 14 MB):

```sql
CREATE TABLE plain  (...) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');
CREATE TABLE withdv (...) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
DELETE FROM plain  WHERE id = 12345;
DELETE FROM withdv WHERE id = 12345;
```

Operation metrics from the history of each `DELETE`:

```text
plain   DELETE one row -> numRemovedFiles 1, numAddedFiles 1, numCopiedRows 49999, numDeletedRows 1, numDeletionVectorsAdded 0
withdv  DELETE one row -> numRemovedFiles 0, numAddedFiles 0, numCopiedRows 0,     numDeletedRows 1, numDeletionVectorsAdded 1
```

Deleting **one row** rewrote a file and copied 49,999 other rows in the plain table; the table with deletion vectors rewrote
nothing and added one deletion vector. The file count stayed 4 in both, and both returned 199,999 rows afterwards. The size of
the table with deletion vectors did not change (14,099,290 bytes before and after): the "deleted" row is still physically
in the file, just hidden.

That's the catch. Reads must apply the deletion vector, and the deleted data **still exists on disk**, which matters for privacy
(a GDPR erasure request isn't satisfied by hiding a row) and for read performance as vectors accumulate. `REORG TABLE ... APPLY
(PURGE)` rewrites only the files that contain soft-deleted rows:

```sql
REORG TABLE withdv APPLY (PURGE);
```

Locally this reported `numFilesAdded=1, numFilesRemoved=1` (a rewritten file of 3,535,050 bytes replacing one of 3,531,929 bytes,
consistent with the 4 files and the one that held the deleted row). Afterward the table size matched the plain table's
(14,102,411 bytes). Purging doesn't delete the old file from storage either: you still need `VACUUM` after the retention window to
physically remove the pre-purge data (and old versions stay readable by time travel until then).

Enabling deletion vectors also raised the table's protocol (`minReaderVersion` 3, `minWriterVersion` 7 locally), so **old readers
can't read the table** (Level 2 · 02's column-mapping warning applies). Per the Databricks docs, deletion vectors accelerate
`DELETE`, `UPDATE` and `MERGE` (Photon uses them for predictive I/O on updates), enable row-level concurrency from Databricks Runtime 14.2,
and for Delta tables must be enabled explicitly (a workspace setting can enable them on new tables; defaults vary by region); client
compatibility varies, with the docs listing runtime versions per operation.

## Statistics

The optimizer uses table and column statistics (row counts, distinct counts, min/max) to choose join orders and strategies, separate
from the per-file min/max used for data skipping (Level 2 · 04).

```sql
ANALYZE TABLE prod.silver.orders COMPUTE STATISTICS;
ANALYZE TABLE prod.silver.orders COMPUTE STATISTICS FOR COLUMNS order_date, customer_id;
```

Stale or missing statistics are a common cause of a bad join strategy (a table believed to be huge never gets broadcast; AQE,
Level 2 · 05, repairs some of this at runtime). Predictive Optimization collects statistics for managed tables.

## Automatic file-size management

From the documentation: Databricks **automatically tunes file sizes for Unity Catalog managed tables**, and manual tuning
recommendations don't apply to them. For external tables and legacy workloads you can configure:

- **Optimized writes** (`delta.autoOptimize.optimizeWrite`): reshuffles data before writing to produce fewer, larger files.
- **Auto compaction** (`delta.autoOptimize.autoCompact`): after a write succeeds, synchronously compacts small files within the
  partition on the cluster doing the write, only files not compacted before.
- **Target file size** settings.

The docs state these reduce the small-file problem but are **not a full replacement for `OPTIMIZE`**: for tables larger than about
1 TB they recommend scheduling `OPTIMIZE` as well, and they recommend liquid clustering for data layout.

## Predictive Optimization

Per the documentation, Predictive Optimization **automatically runs `OPTIMIZE`, `VACUUM` and `ANALYZE` on Unity Catalog managed tables**
(Delta and Iceberg), identifies the tables that would benefit, queues the operations, and collects statistics at write time. Details to
know:

- It is **enabled by default for accounts created on or after 11 November 2024**, with a gradual rollout to existing accounts that the
  docs expect to complete by August 2026, so verify your account.
- It can be enabled or overridden at account, catalog, schema or table level.
- It runs on **serverless compute for jobs** and is billed under a serverless jobs SKU; operations are visible in a predictive
  optimization system table.
- `OPTIMIZE` run by it **does not apply `ZORDER`**; for Z-ordered tables it ignores Z-ordered files. With liquid clustering enabled, it
  triggers incremental clustering, and with *automatic* liquid clustering it may select new clustering keys itself.
- `VACUUM` uses `delta.deletedFileRetentionDuration` (default 7 days). **If you need longer time travel, set the property before you
  enable it**, or it will clean up history you wanted. The docs call this out explicitly.
- Requirements: Premium plan or above in a supported region, SQL warehouses or Databricks Runtime 12.2 LTS+, and **managed** tables
  only. External tables still need your own jobs.

This is the largest operational simplification on the platform, and it changes the question from "who runs OPTIMIZE?" to "what
retention and clustering do we want, and are we verifying it happens?"

## A maintenance policy

| Table type | Policy |
|---|---|
| UC managed, predictive optimization on | Set retention properties deliberately; use liquid clustering; monitor the operations table; run nothing by hand unless investigating |
| UC managed, PO off | Scheduled `OPTIMIZE` (liquid clustering) and `VACUUM` jobs, `ANALYZE` for join-heavy tables |
| External / legacy | Scheduled `OPTIMIZE` + `VACUUM`, optimized writes + auto compaction, own monitoring |
| Tables with erasure obligations | Deletion vectors **plus** `REORG ... PURGE` **plus** `VACUUM` after retention, tested end to end |
| Streaming sinks | Auto compaction/optimized writes or periodic `OPTIMIZE` (many small commits) |

Add monitoring: file count and average file size per table (`DESCRIBE DETAIL`), table size growth, the age of the oldest unvacuumed
file, and failed maintenance runs. A table whose average file size drifts toward kilobytes is telling you maintenance is not happening.

```sql
DESCRIBE DETAIL prod.silver.orders;     -- numFiles, sizeInBytes -> average file size = sizeInBytes / numFiles
DESCRIBE HISTORY prod.silver.orders;    -- when did OPTIMIZE / VACUUM last run?
```

## How It Actually Works

`OPTIMIZE` reads small files, writes larger ones and commits `add`/`remove` actions marked `dataChange = false`. `VACUUM` compares the
set of files referenced by versions within the retention window with the files present, and deletes the rest. A deletion vector is a
compact bitmap (stored inline or in a side file) of row positions inside a specific data file; the `add` action for that file carries a
reference to it, so a query reading the file filters out those positions. `REORG ... PURGE` rewrites a file *without* the marked rows and
commits a new `add` with no vector. Predictive Optimization is a scheduler: it observes table activity (writes, file counts,
query patterns), estimates the benefit of each operation, and enqueues `OPTIMIZE`, `VACUUM` and `ANALYZE` onto serverless compute.

## Common mistakes

- **Running `VACUUM RETAIN 0 HOURS`** to "save storage now" (kills time travel and can break concurrent writers).
- **Enabling Predictive Optimization before setting retention** you need for time travel.
- **Assuming a deletion-vector delete erased the data.**
- **Z-ordering a table and expecting Predictive Optimization to maintain the Z-order.**
- **Forgetting external tables** aren't covered by Predictive Optimization.
- **Optimizing tiny tables** or optimizing on every write.
- **No monitoring** of file counts, so decay is noticed only when queries slow down.

## Exercise

1. Reproduce the deletion-vector experiment. Then delete 5,000 scattered rows with and without deletion vectors and compare
   `numCopiedRows`. At what point does the advantage shrink?
2. After `REORG ... PURGE`, prove the deleted row's bytes are still on disk until `VACUUM` (search the Parquet file for the id or
   use time travel), and then remove them.
3. Write a function that, given `DESCRIBE DETAIL` output, flags tables whose average file size is below a threshold you choose, and
   test it on a table with 200 tiny appends before and after `OPTIMIZE`.
4. Write the retention and clustering decisions for three tables you own (one audit log, one hot dimension, one erasure-sensitive
   table) before enabling Predictive Optimization.
