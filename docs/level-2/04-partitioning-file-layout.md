---
description: "When to partition a Delta table and when not to: a measured small-files blow-up, partition pruning, generated columns and statistics columns."
---

# 04 · Partitioning, File Sizes & Data Skipping

Partitioning is the first performance idea most people learn, and for Delta it's the one most often misused. This
lesson measures what partitioning does to file counts and write time, shows when it prunes, and explains which
columns get data-skipping statistics.

!!! note "What was run"
    All numbers come from **open-source Delta Lake 4.4.1 on local Spark 4.2.0** (two cores, `shuffle.partitions = 4`,
    200,000 synthetic rows), writing to a laptop SSD. Sizes and file counts are deterministic enough to trust; **write
    times are laptop-specific** and only the ratios mean anything. Databricks' recommendation, per its documentation,
    is liquid clustering instead of partitioning for most new tables; the platform behaviour wasn't run here.

## The experiment

The same 200,000 rows (columns `id`, `user_id` with about 50,000 distinct values, `day` with 30 distinct values, and
`amount`) were written three ways:

```python
base.write.format("delta").save(p)                          # no partitioning
base.write.format("delta").partitionBy("day").save(p)       # 30 partitions
base.write.format("delta").partitionBy("user_id").save(p)   # ~50,000 partitions
```

| Layout | Data files | Total size | Average file | Write time (this laptop) |
|---|---|---|---|---|
| No partitioning | 2 | 2,125 KB | 1,063 KB | 5.9 s |
| `partitionBy("day")` | 60 | 2,580 KB | 43 KB | 1.0 s |
| `partitionBy("user_id")` | **86,665** | **105,606 KB** | **1.2 KB** | **467 s** |

Partitioning by the high-cardinality `user_id` turned 2 MB of data into **50 times more bytes**, spread over
86,665 files (about 1.7 files per partition directory, each ~1 KB), and the write took almost eight minutes. Each tiny
file carries Parquet footer and metadata overhead that dwarfs its few rows. Every later query now has to list, open
and plan around 86,000 files. (The `none` row's 5.9 s probably includes one-time JVM and Delta warm-up (I didn't isolate it), which would explain why `by_day` looks faster.
Don't read anything into that comparison.)

`partitionBy("day")` also yielded 60 files for 30 partitions: two shuffle tasks wrote a file into each. That's already
tiny (43 KB average). On a real table you'd choose a partition column so each partition holds *gigabytes*, not
kilobytes.

## What partitioning buys: pruning

Filter on the partition column, and the engine reads only matching directories. For `day = '2026-01-05'`, the plan
shows `PartitionFilters: [isnotnull(day), (day = 2026-01-05)]`, and the file count surviving the filter was:

```text
candidate files with day filter:   2 of 60
candidate files with user filter:  59 of 60   (the table is partitioned by day, not user)
```

So partitioning helps queries on the partition key and does nothing for others, and the second case is worth noticing:
data skipping on `user_id` statistics couldn't exclude files because users are scattered randomly in each day.

### Generated partition columns

Users filter on timestamps, not on your derived `day` column. A **generated column** lets Delta translate one into
the other:

```sql
CREATE TABLE gp (ts TIMESTAMP, v INT, d DATE GENERATED ALWAYS AS (CAST(ts AS DATE)))
USING delta PARTITIONED BY (d);
-- SELECT ... WHERE ts >= '2026-01-02' AND ts < '2026-01-03'   -> reads partition d = 2026-01-02
```

In the local run, the timestamp range filter read **1 file of 3**: Delta inferred the partition predicate from the
generation expression. (This works for expressions Delta knows how to invert, like casts to date and date-part
functions.)

## When to partition at all

Guidance from the numbers above and the Databricks documentation's general direction:

- **Most tables under about a terabyte shouldn't be partitioned.** Without partitions you still get data skipping
  from statistics; with the wrong partitioning you get the small-files blow-up.
- If you do partition, use a **low-cardinality column that is filtered on constantly** (typically a date or day),
  where each partition would hold roughly a gigabyte or more.
- Never partition by an ID, a free-text field or a timestamp with sub-day precision.
- Prefer **liquid clustering** (Lesson 03) for new tables: it handles high-cardinality columns and changing patterns,
  and can't be mixed with partitioning.
- Partitioning is also an operational tool (drop a whole partition by deleting a directory's worth of data; limit
  maintenance to recent partitions), so "date partition for retention" remains a legitimate reason.

## Statistics: which columns can skip?

Delta stores min/max/null-count for the **first 32 columns** by default (`delta.dataSkippingNumIndexedCols`). Local
test with the limit set to 2 on a 4-column table:

```text
indexed stats columns:            ['c1', 'c2']
filter c2 > 100  (indexed)      -> 0 files read  (stats prove nothing matches)
filter c4 > 100  (not indexed)  -> 2 files read  (no stats, so it can't exclude anything)
```

Both queries match zero rows. The difference is whether Delta could *prove* it from statistics. Practical rules:

1. Put the columns you filter on within the first 32, or set `delta.dataSkippingNumIndexedCols` /
   `delta.dataSkippingStatsColumns` (Databricks documents the latter for naming specific columns) deliberately.
2. Move long strings and blobs out of the leading columns: computing stats on them is expensive and rarely useful.
3. Nested struct fields count as columns for this limit.
4. Changing the setting only affects files written afterward; recompute stats for old files if needed (Databricks
   provides a command for this; check the current docs).

## File sizes

Aim for files of hundreds of MB to about a gigabyte for large tables: big enough to amortize metadata and open costs,
small enough to parallelize and to rewrite cheaply in `MERGE`. Three levers:

| Lever | Effect |
|---|---|
| `OPTIMIZE` (Lesson 03) | Compacts existing small files |
| Optimized writes / auto compaction (Databricks table properties) | Reduce small files at write time. Their availability and defaults differ between Databricks and open-source Delta versions, so check the docs for yours; they weren't run here |
| `repartition(n)` / `coalesce(n)` before a write | Controls the number of output files from a job you own |

Databricks' documentation also describes autotuned target file sizes based on table size, so the "ideal" number moves
with the platform; treat the figures here as orders of magnitude.

## How It Actually Works

A partition column is encoded in the *path* (`day=2026-01-05/part-...parquet`), recorded in each `add` action as
`partitionValues`, and removed from the Parquet file itself. Query planning evaluates partition predicates against the
`partitionValues` in the log (no storage listing needed) and discards non-matching files immediately.

Each partition value requires at least one file per writing task. With many partition values and many tasks, a write of
N rows fans out to up to `tasks × distinct values` files. That's why the `user_id` write exploded: each of the 4 shuffle
tasks wrote a separate file into most of the ~50,000 directories. Skew in the other direction (one giant partition)
creates the opposite problem: a single huge task.

Statistics are column-level aggregates stored in the same `add` action as JSON, computed while the file is written.
That is why they cost nothing to read at query time, and why a column without them can't help.

## Common mistakes

- **Partitioning by a high-cardinality or ID column.** (86,665 files in the experiment.)
- **Over-partitioning on two or three columns** "to be safe": the partition count multiplies.
- **Filtering on a timestamp while the table is partitioned by a derived date**, with no generated column linking them.
- **Wide tables with filter columns past position 32** and no stats configuration.
- **Compacting without clustering** and expecting faster selective queries.
- **Expecting a partition column to behave like an index** for other columns.

## Exercise

1. Reproduce the three layouts with 20,000 rows and 5,000 distinct users (so it finishes quickly). Record file counts and
   total sizes.
2. Write the `day`-partitioned version with `repartition("day")` first. How many files do you get now, and why?
3. Create a 40-column table with the filter column in position 35. Show that a selective filter reads all files, then
   fix it with the table property.
4. For a table you own, decide: partition, cluster, or neither? Justify with row count, size, and top filter columns.
