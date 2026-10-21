---
description: "Query old table versions, undo mistakes with RESTORE, understand retention properties, and see what VACUUM destroys, run locally on Delta."
---

# 08 · Time Travel, RESTORE & VACUUM

Because Delta never edits data files in place, the files for previous versions stay around until something deletes
them. Time travel exposes that history for queries, `RESTORE` rolls a table back, and `VACUUM` is the thing that
finally erases it. Knowing how those three interact tells you what you can and can't recover.

!!! note "What was run"
    The queries and errors below are from **open-source Delta Lake 4.4.1 on local Spark 4.2.0**. `VACUUM` options such as
    `LITE`/`FULL`, Unity Catalog `UNDROP`, and automatic maintenance by Predictive Optimization are Databricks features
    described from the documentation (October 2026) and weren't executed.

## A table with some history

```sql
CREATE TABLE tt (id INT, v STRING) USING delta;
INSERT INTO tt VALUES (1,'a'), (2,'b');      -- version 1
UPDATE tt SET v = 'z';                       -- version 2
DELETE FROM tt WHERE id = 1;                 -- version 3
DESCRIBE HISTORY tt;
```

```text
|version|timestamp              |operation   |
|3      |2026-10-21 23:33:30.772|DELETE      |
|2      |2026-10-21 23:33:27.522|UPDATE      |
|1      |2026-10-21 23:33:20.723|WRITE       |
|0      |2026-10-21 23:33:18.46 |CREATE TABLE|
```

## Querying the past

```sql
SELECT * FROM tt VERSION AS OF 1;                         -- by version number
SELECT * FROM tt TIMESTAMP AS OF '2026-10-21 23:33:20.723';  -- by time
```

Both returned the original two rows `(1,a)` and `(2,b)` in the local run. In Python:

```python
spark.read.format("delta").option("versionAsOf", 1).load(path)
spark.read.format("delta").option("timestampAsOf", "2026-10-21 23:33:20.723").load(path)
```

A timestamp query returns the latest version **committed at or before** that time. A timestamp earlier than the
first commit (or beyond the retained log) is an error, not an empty table. There is also a path shorthand,
``delta.`/path@v1` ``; on that local table `@v0` returned 0 rows because version 0 was the empty `CREATE TABLE`.

Time travel is a handy diff tool:

```sql
SELECT * FROM tt VERSION AS OF 1
EXCEPT
SELECT * FROM tt;          -- rows present at version 1 that are not in the current table
```

This returned both original rows, since `UPDATE` changed `v` for every row and `DELETE` removed row 1. (Spark's
`EXCEPT` compares whole rows; it is a set operation and removes duplicates, so use `EXCEPT ALL` when duplicates matter.)

## RESTORE: roll back

```sql
RESTORE TABLE tt TO VERSION AS OF 1;
-- or: RESTORE TABLE tt TO TIMESTAMP AS OF '...';
```

`RESTORE` is itself a **new commit**, not a deletion of history: it adds back the files that were live at version 1 and
removes the ones added since. In the earlier 12-commit local table (Lesson 07), restoring to version 1 reported:

```text
|table_size_after_restore|num_of_files_after_restore|num_removed_files|num_restored_files|removed_files_size|restored_files_size|
|1335                    |2                         |10               |1                 |6662              |669                |
```

and `DESCRIBE HISTORY` then listed `version 12 RESTORE` on top of the earlier versions. That's an important safety
property: a rollback can be rolled back.

!!! warning "RESTORE needs the old files to still exist"
    After `VACUUM` removed them, the local run's `RESTORE TABLE tt TO VERSION AS OF 1` failed with:
    `Not all files from version 1 are available in file system.`

## VACUUM: the point of no return

```sql
VACUUM tt DRY RUN;                 -- lists files that would be deleted (up to 1000)
VACUUM tt RETAIN 168 HOURS;        -- default: 7 days
```

`VACUUM` deletes files that are (a) not referenced by the current snapshot and (b) were logically removed longer ago
than the retention threshold, plus files in the table folder that Delta doesn't track at all. It never touches
`_delta_log`. Retention is measured from when the `remove` was committed, not the file's modification time.

Delta guards against dangerously short retention:

```sql
VACUUM tt RETAIN 0 HOURS;
-- [DELTA_VACUUM_RETENTION_PERIOD_TOO_SHORT] The specified VACUUM retention period is too low and may corrupt this Delta table if any writes are in progress.
```

For the demo I disabled the check with `SET spark.databricks.delta.retentionDurationCheck.enabled = false`. Then:

```text
VACUUM tt RETAIN 0 HOURS DRY RUN   -> 4 files to delete
files in folder before / after VACUUM:  5 -> 1
SELECT * FROM tt VERSION AS OF 1   -> error (the data files are gone)
RESTORE TABLE tt TO VERSION AS OF 1 -> Not all files from version 1 are available in file system.
```

(The `VERSION AS OF 1` failure surfaced locally as a generic Python "error occurred while calling showString"; the
underlying cause is the missing Parquet files. Don't copy `RETAIN 0` anywhere that matters: it can also delete files
that an in-flight write has produced but not yet committed.)

## Two retention knobs

| Property | Default | Controls |
|---|---|---|
| `delta.logRetentionDuration` | 30 days | How long commit history (JSON/checkpoints) is kept; bounds time travel by *metadata* |
| `delta.deletedFileRetentionDuration` | 7 days | How long removed data files are kept; `VACUUM` uses it as its default threshold |

Time travel needs **both**: the log entry and the data files. So the effective window is the shorter of the two, which
by default is the 7-day file retention. To keep 30 days of travel you must raise both:

```sql
ALTER TABLE tt SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 30 days');
```

Longer retention means more storage: every old file you keep is paid for. And it is **not a backup**: it lives in the
same storage, deleted by the same `VACUUM`, and the retention is a cleanup default, not a guarantee. Use deep clones or
cross-region copies for real backups (Level 4 · 09).

## Dropped tables and Unity Catalog

Time travel does nothing for a dropped table. In Unity Catalog, the Databricks documentation says dropped managed or
external tables can be recovered with `UNDROP TABLE` within a recovery period (7 days by default, configurable at
catalog or schema level), restoring metadata such as privileges and properties but not primary/foreign key
constraints. `SHOW TABLES DROPPED` lists candidates. (Documented, not run here.)

## How It Actually Works

Time travel is log replay stopped early (Lesson 07). `VERSION AS OF 1` loads the newest checkpoint at or before
version 1, applies commits up to version 1, and gets a file list. The reader then simply opens those Parquet files. If
a file in that list has been vacuumed, the open fails, which is exactly the error you saw.

`TIMESTAMP AS OF` first maps the timestamp to a version using the commit timestamps (taken from the log files'
modification times or recorded commit timestamps), then proceeds the same way.

`VACUUM` works by listing the table directory, subtracting the set of files referenced by versions inside the
retention window, and deleting the remainder. That listing is why `VACUUM` on a huge table is slow and why
Databricks added `LITE` mode (uses the log instead of listing the directory) and runs maintenance automatically with
Predictive Optimization (Level 4 · 08).

## Common mistakes

- **Treating time travel as a backup.** `VACUUM` ends it; a dropped, non-UC table has no history at all.
- **Setting `RETAIN 0 HOURS`** to "reclaim space now" and breaking concurrent writers or all rollback.
- **Raising only one retention property.** You need both for longer travel.
- **Using time travel in pipelines** to read "yesterday's data" with a hard-coded timestamp: a vacuum breaks the job.
  Persist what you need as its own table or use the change data feed (Level 2 · 01).
- **Forgetting that `RESTORE` is a write** that creates new data-change events for downstream streams.
- **Expecting `VACUUM` to shrink the log.** The log is cleaned by `logRetentionDuration` at checkpoint time.

## Exercise

1. Build the `tt` table, then use time travel to produce a diff of rows deleted between versions 2 and 3.
2. Wrong delete: run `DELETE FROM tt` (no predicate), then recover with `RESTORE`. Check `DESCRIBE HISTORY`.
3. Run `VACUUM tt DRY RUN` right after your changes. Why does it list nothing with the default retention?
4. Disable the retention check locally, vacuum with `RETAIN 0 HOURS`, and confirm that both `VERSION AS OF` and `RESTORE`
   fail. Write down which files were deleted (use the DRY RUN output first).
