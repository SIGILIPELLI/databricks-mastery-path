---
description: "Upsert with Delta MERGE: matched and not-matched clauses, the duplicate-source error, an SCD Type 2 pattern and the Change Data Feed, run locally."
---

# 01 · MERGE, Upserts & the Change Data Feed

Real pipelines don't just append: yesterday's customer changes city, an order gets corrected, a record must be deleted.
`MERGE INTO` applies a batch of changes to a Delta table in one atomic transaction, and the **Change Data Feed**
(CDF) records exactly what changed so downstream tables can follow along.

!!! note "What was run"
    All statements and outputs are from **open-source Delta Lake 4.4.1 on local Spark 4.2.0**. `MERGE`, CDF and
    `table_changes` behave the same on Databricks; performance features that only exist there (for example Photon,
    deletion-vector-based merges, low-shuffle merge) change speed, not results, and weren't measured.

## The basic upsert

Target table `cust` (CDF enabled so we can look at it later), and a batch of incoming changes:

```sql
CREATE TABLE cust (id INT, name STRING, city STRING) USING delta
TBLPROPERTIES (delta.enableChangeDataFeed = true);
INSERT INTO cust VALUES (1,'Ann','Pune'), (2,'Bo','Oslo'), (3,'Cy','Lima');

-- incoming batch as a view `upd`: (2,'Bo','Bergen'), (3,'Cy','Lima'), (4,'Di','Rome')

MERGE INTO cust t USING upd u ON t.id = u.id
WHEN MATCHED AND t.city <> u.city THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
```

The merge returns a one-row summary (local run):

```text
|num_affected_rows|num_updated_rows|num_deleted_rows|num_inserted_rows|
|3                |1               |1               |1                |
```

and the table now holds:

```text
|id|name|city  |
|2 |Bo  |Bergen|
|3 |Cy  |Lima  |
|4 |Di  |Rome  |
```

Walk through each clause:

- Row 2 matched and the city differs → **updated**.
- Row 3 matched but nothing changed → the extra condition `t.city <> u.city` made it skip, so it wasn't rewritten
  (`num_updated_rows` is 1, not 2). Always add a change condition to avoid rewriting identical rows.
- Row 4 is only in the source → **inserted**.
- Row 1 is only in the target → **deleted** by `WHEN NOT MATCHED BY SOURCE`. That clause treats the source as the
  complete truth, so use it only for full snapshots, never for incremental batches (where "not in the batch" just
  means "unchanged").

`UPDATE SET *` and `INSERT *` map columns by name. Clauses are evaluated in order, and the first one whose condition
is true for a row wins.

## The rule MERGE enforces: one source row per target row

```sql
-- source `dup` has two rows for id 2
MERGE INTO cust t USING dup u ON t.id = u.id WHEN MATCHED THEN UPDATE SET *;
```

```text
[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and
attempted to modify the same target row in the Delta table in possibly conflicting ways. ...
```

Delta refuses rather than guessing which row wins. Deduplicate the source first, usually keeping the latest by a
sequence or timestamp:

```sql
SELECT * FROM events QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY seq DESC) = 1
```

(On a small test set with ids 7 and 8 this kept `(7,'b',2)` and `(8,'c',1)`.) Note that the error only fires when the
duplicate would modify the *same target row* with a matched clause; duplicates that only reach `INSERT` aren't caught,
and silently insert twice. Deduplicate the source regardless.

## Slowly changing dimension (Type 2)

To keep history instead of overwriting, close the old row and insert a new one. The standard trick unions the source
twice: once with the real key (to find the current row) and once with a NULL key (to force an insert):

```sql
MERGE INTO dim d
USING (SELECT c.id AS mk, c.* FROM chg c
       UNION ALL
       SELECT NULL AS mk, c.* FROM chg c JOIN dim x ON x.id = c.id AND x.is_current AND x.city <> c.city) u
ON d.id = u.mk AND d.is_current
WHEN MATCHED AND d.city <> u.city THEN UPDATE SET is_current = false, valid_to = DATE'2026-02-01'
WHEN NOT MATCHED THEN INSERT (id, city, valid_from, valid_to, is_current)
     VALUES (u.id, u.city, DATE'2026-02-01', NULL, true);
```

With `dim` starting as `(1,Pune)` and `(2,Oslo)` both current, and a change feed of `(1,Mumbai)` and `(3,Rome)`, the
local result was:

```text
|id|city  |valid_from|valid_to  |is_current|
|1 |Pune  |2026-01-01|2026-02-01|false     |
|1 |Mumbai|2026-02-01|NULL      |true      |
|2 |Oslo  |2026-01-01|NULL      |true      |
|3 |Rome  |2026-02-01|NULL      |true      |
```

Running the **same merge again** reported `num_affected_rows = 0` and left the table unchanged: the change
conditions make it idempotent. That matters because jobs retry. (The hard-coded date is for the demo; use the batch's
effective date.)

## The Python API

```python
from delta.tables import DeltaTable
(DeltaTable.forName(spark, "cust").alias("t")
   .merge(updates.alias("u"), "t.id = u.id")
   .whenMatchedUpdateAll(condition="t.city <> u.city")
   .whenNotMatchedInsertAll()
   .execute())
```

It is the same operation as the SQL; use whichever fits your codebase. Remember to **put an equality on the key in the
`ON` condition**, ideally also on a partition or clustering column (below).

## Change Data Feed

With `delta.enableChangeDataFeed = true`, every change to the table is also recorded as row-level change events in a
hidden `_change_data` folder (for updates, deletes and merges; pure appends are derivable from the added files).

```sql
SELECT id, city, _change_type, _commit_version FROM table_changes('cust', 1)
ORDER BY _commit_version, id, _change_type;
```

```text
|id|city  |_change_type    |_commit_version|
|1 |Pune  |insert          |1              |
|2 |Oslo  |insert          |1              |
|3 |Lima  |insert          |1              |
|1 |Pune  |delete          |2              |
|2 |Bergen|update_postimage|2              |
|2 |Oslo  |update_preimage |2              |
|4 |Rome  |insert          |2              |
```

Version 1 is the initial insert; version 2 is the merge: row 1 deleted, row 2 changed (both the old *pre-image* and the
new *post-image*), row 4 inserted, and row 3 absent because it wasn't touched. Downstream jobs read the feed
(`spark.read.option("readChangeFeed", "true").option("startingVersion", n)`, or as a stream) and apply only the delta.
Caveats: the feed only exists from the version you enabled it; it uses extra storage; and it's subject to the same
`VACUUM`/log retention as the table.

## How It Actually Works

A merge runs in (at least) two phases inside one commit:

1. **Find touched files.** Join source to target on the `ON` condition, using file-level statistics to prune files that
   can't match, and collect the names of target files that contain matching rows. In the local metrics: `scanTimeMs`
   and `numTargetFilesRemoved -> 2`.
2. **Rewrite.** Join again, this time producing the new contents of those files: rows updated, rows kept (the
   `numTargetRowsCopied -> 1` in the metrics are untouched rows that lived in a rewritten file), plus inserted rows.
   Write new files, then commit `remove` for the old files and `add` for the new.

Because the whole file is rewritten for one changed row, merges get expensive when the matching rows are scattered
across the whole table. The fixes are all about making the first phase prune well: put the clustering or partition
column in the `ON` clause, keep source batches focused, and use deletion vectors where your platform enables them.
A merge that sees changes land mid-run fails with the concurrent-write errors from Level 1 · 07, so design for retries.

## Common mistakes

- **Duplicate source keys** (error, or silent double inserts).
- **No change condition** on `WHEN MATCHED`, rewriting identical rows, producing noise in CDF, and slowing everything.
- **`WHEN NOT MATCHED BY SOURCE THEN DELETE` on incremental batches**, deleting everything not in this batch.
- **`ON` clause without the partition/cluster key**, forcing a scan of the whole table.
- **Treating CDF as free history.** It must be enabled before the changes happen.
- **Non-deterministic source** (random values, `current_timestamp()` recomputed during rewrite phases) producing
  inconsistent results; materialize the source first.

## Exercise

1. Reproduce the first merge. Then change `WHEN MATCHED AND t.city <> u.city` to plain `WHEN MATCHED` and compare
   `num_updated_rows`, and the CDF output, for row 3.
2. Insert a second source row for id 4 and run an insert-only merge. What happens? Fix it with a `QUALIFY` dedupe.
3. Make the SCD2 merge handle a *deleted* customer by closing the current row. What extra clause do you need?
4. Read CDF from version 2 as a DataFrame in Python and use it to keep a second table `cust_copy` in sync with a
   second merge.
