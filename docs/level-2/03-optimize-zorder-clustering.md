---
description: "Fix small files with OPTIMIZE, cluster data with ZORDER and liquid clustering, and measure data skipping by counting candidate files."
---

# 03 · OPTIMIZE, Z-Order & Liquid Clustering

Delta can skip files whose min/max statistics rule out a query's filter, but only if the data is arranged so that each
file covers a narrow value range. Random loading order, streaming trickles and many small commits ruin that. `OPTIMIZE`
compacts files, and `ZORDER` or liquid clustering rearrange rows so skipping works.

!!! note "What was run, and a caveat about the numbers"
    The experiments use **open-source Delta Lake 4.4.1 on local Spark 4.2.0** with 400,000 synthetic rows. To get
    multiple files from a small dataset I lowered `spark.databricks.delta.optimize.maxFileSize` to 200 KB, so absolute
    file counts (like 143) reflect that artificial setting and **would be tiny on real data**. The *pattern* (how many
    files a filter must read before and after) is what to take away. I measured "candidate files" via
    `DataFrame.inputFiles()` after a filtered read, which reports files remaining after file-level skipping on stats,
    not wall-clock time. Databricks-only behaviour (Photon, Predictive Optimization, automatic liquid-clustering key
    selection) is from the documentation and wasn't run.

## Small files and OPTIMIZE

Twenty tiny appends each wrote one small Parquet file. Local result:

```text
files before: 20
OPTIMIZE -> numFilesAdded 1, numFilesRemoved 20
active files after: 1       (but 21 Parquet files still on disk until VACUUM)
after VACUUM RETAIN 0 HOURS: 1 file on disk
```

`OPTIMIZE` bin-packs small files into larger ones (target around 1 GB by default for the setting
`delta.targetFileSize`; Databricks can autotune it based on table size and workload). It writes the combined files,
then commits `add new` + `remove old` as ordinary Delta actions: the old files stay on disk (and travel-able) until
`VACUUM`. It's safe to run while readers and writers are active, because it only rearranges existing data
(`dataChange = false` on its actions, so streaming readers ignore it).

```sql
OPTIMIZE sales.orders;                          -- whole table
OPTIMIZE sales.orders WHERE order_date >= '2026-10-01';    -- partition/predicate-limited
```

## Data skipping in practice

Test table: 400,000 rows in random order, columns `a` and `b` (both uniform 0–999) and a padding column. Query
filters: `a BETWEEN 10 AND 20` (about 1% of rows) and `b BETWEEN 10 AND 20`. Count of files that survive skipping:

| Layout | Total files | Files read for filter on `a` | …on `b` | …on `a` and `b` |
|---|---|---|---|---|
| Written randomly (16 files) | 16 | 16 | 16 | n/a |
| `OPTIMIZE` only (no zorder) | 16 | 16 | 16 | n/a |
| `OPTIMIZE ... ZORDER BY (a)` | 143 | **2** | 143 | n/a |
| `OPTIMIZE ... ZORDER BY (a, b)` | 143 | 14 | 18 | **1** |

Reading it:

- **Random layout:** every file contains values from the whole range 0–999, so its min/max for `a` is about 0–999 and
  can never be excluded. Skipping did nothing; statistics only help when files are *narrow*.
- **Compaction alone** didn't help either: the 16 files were already at the (lowered) target size, so there was nothing
  to merge, and compaction never reorders rows.
- **Z-order by `a`:** each file now covers a thin band of `a`. The file stats of the last commit showed ranges such as
  `0–7`, `7–14`, `14–21`, so the filter `10..20` touches two files out of 143.
- **Z-order by `(a, b)`** interleaves both columns. Neither is as good as a dedicated single-column sort (14 and 18
  files vs 2), but both filters now skip most of the table, and the combined filter reads just one file. The more
  columns you Z-order by, the less each one helps, so use a handful at most.

Z-order is a *physical clustering* technique: it orders rows along a space-filling curve (a Z-curve) so that rows close
in all the chosen dimensions land in the same file.

```sql
OPTIMIZE sales.events ZORDER BY (customer_id, event_date);
```

Z-order is **not incremental in the simple sense**: each `OPTIMIZE ZORDER BY` run rewrites files that aren't already
Z-ordered, and rows you append later are unsorted until the next run. It also needs statistics on the columns chosen
(by default only the first 32 columns have statistics; Level 2 · 04), so don't Z-order by a column beyond that, or a
long string.

## Liquid clustering

Liquid clustering is the newer replacement: you declare clustering keys on the table, and `OPTIMIZE` incrementally
clusters data to match. It can't be combined with partitioning or `ZORDER` on the same table.

```sql
CREATE TABLE lc_t (id BIGINT, a INT, b INT, pad STRING) USING delta CLUSTER BY (a);
-- ...loads...
OPTIMIZE lc_t;
```

Local results, same data and filters:

```text
before OPTIMIZE:   16 files, filter on a reads 16
after OPTIMIZE:    143 files, filter on a reads 3 (of 143), filter on b reads all 143
ALTER TABLE lc_t CLUSTER BY (b);   -- history shows version 'CLUSTER BY'
OPTIMIZE lc_t:     0 files added, 0 removed -> a still 2, b still 143   (nothing re-clustered!)
OPTIMIZE lc_t FULL: 145 added, 143 removed -> a reads 145 (of 145), b reads 3
```

Two lessons:

1. `CLUSTER BY` only declares intent. Until `OPTIMIZE` runs, newly written files aren't clustered.
2. **Changing the clustering keys does not rewrite existing data.** A plain `OPTIMIZE` after
   `ALTER TABLE ... CLUSTER BY (b)` did nothing in my run (0 files added, filters on `b` still read all files). Only
   `OPTIMIZE ... FULL` re-clustered everything. The Databricks documentation says the default behaviour does not cluster previously written data and to use
   `OPTIMIZE ... FULL` to force reclustering; this run shows the open-source build behaves the same way. On Databricks, with
   Predictive Optimization or automatic clustering, parts of this are automated, but check the table's history to see
   whether it actually happened.

You can change keys at any time without rewriting the table, which is the point of the feature: no re-partitioning
migration when query patterns change. `CLUSTER BY AUTO` (Databricks, Unity Catalog managed tables, per the docs) lets
the platform choose keys from query history; that isn't available locally and wasn't tested.

| | Hive-style partitioning | `ZORDER` | Liquid clustering |
|---|---|---|---|
| Declared | At creation | Per `OPTIMIZE` run | On the table (`CLUSTER BY`) |
| Change later | Rewrite table | Choose different columns next run | `ALTER TABLE ... CLUSTER BY` (+ `OPTIMIZE FULL` to apply to old data) |
| High-cardinality columns | Bad (too many directories) | OK | OK |
| Incremental | n/a | Rewrites unsorted files | Yes (clusters new data) |
| Combine with partitions | n/a | Yes | No |

## How It Actually Works

Skipping is a log operation. Each `add` action carries `minValues`/`maxValues`/`nullCount` per column (Level 1 · 07).
For a filter `a BETWEEN 10 AND 20`, the reader evaluates "could any row in this file satisfy it?" from the stats and
discards files where `max < 10` or `min > 20` *before opening any Parquet*. Everything above is therefore about making
those ranges narrow.

Z-ordering computes, for each row, a single integer by bit-interleaving the (ranked) values of the chosen columns,
sorts rows by that integer, and cuts the sorted stream into files. Rows close in the sort are close in every
dimension, so each file covers a small box rather than a whole range. Liquid clustering applies the same idea with
a different clustering algorithm (the exact curve is an implementation detail I haven't verified) and tracks per-file clustering state in the log so that each
`OPTIMIZE` only touches files that aren't yet clustered by the current keys, which explains why a changed key
doesn't trigger a rewrite until you ask for a full one.

## Common mistakes

- **Expecting `OPTIMIZE` alone to speed up selective filters.** Compaction fixes file count; clustering fixes ranges.
- **Z-ordering by many columns** or by a low-value column, diluting the benefit.
- **Never re-running `OPTIMIZE ZORDER`** on a table that keeps receiving appends.
- **Changing liquid clustering keys and assuming old data followed.**
- **Optimizing a tiny table**: the overhead exceeds any benefit.
- **Forgetting `VACUUM`**: OPTIMIZE adds storage until old files are removed.
- **Choosing keys with no statistics** (beyond the first 32 columns, or very long strings).

## Exercise

1. Re-run the data-skipping experiment with `maxFileSize` left at the default. What happens to the file counts, and
   can you still show a difference? (Hint: you may need more rows.)
2. Z-order by `b` instead of `a` and fill in the table row.
3. Create a liquid-clustered table, change its keys twice, and record `numFilesAdded` for `OPTIMIZE` vs `OPTIMIZE FULL`
   each time.
4. For your own largest table, list the top three filter columns in the last month's queries. Which would you cluster
   on, and why not all three?
