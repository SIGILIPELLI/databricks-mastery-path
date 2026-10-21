---
description: "Read and write CSV, JSON and Parquet in Spark: why inferSchema misleads, parse modes, save modes, partitionBy pruning and file counts."
---

# 05 · Reading & Writing Files: Formats, Schemas & Modes

Before Delta, every lakehouse pipeline starts with files: CSV exports, JSON events, Parquet from another system.
How you read them decides whether bad data fails loudly, gets quietly nulled, or silently changes your column types.

!!! note "What was run"
    Everything below ran on **local Spark 4.2.0** against files on a laptop disk, and output is copied from those runs.
    On Databricks the same code reads from Unity Catalog volumes or cloud storage paths instead (`/Volumes/...`,
    `s3://...`, `abfss://...`); that part wasn't run here.

## Reading CSV: schema inference is a trap

Here is a small CSV with three planted problems (an empty amount, a bad date, a non-numeric amount):

```text
id,order_date,country,amount
1,2026-01-03,DE,120.50
2,2026-01-03,US,80
3,2026-01-04,US,
4,not-a-date,IN,300
5,2026-01-05,DE,abc
```

```python
d = spark.read.option("header", True).option("inferSchema", True).csv("orders.csv")
d.printSchema()
```

```text
root
 |-- id: integer (nullable = true)
 |-- order_date: string (nullable = true)
 |-- country: string (nullable = true)
 |-- amount: string (nullable = true)
```

Inference scanned the data, found `not-a-date` and `abc`, and **silently fell back to `string`** for those two
columns. Nothing failed, but every downstream `amount > 100` is now a string comparison. Inference also costs an extra
pass over the file, and on a different batch with no bad rows the same code would infer `double`: the schema depends on
the data. Production reads should declare the schema.

### Declare a schema and capture the bad rows

```python
schema = "id INT, order_date DATE, country STRING, amount DOUBLE, _bad STRING"
d = (spark.read.option("header", True)
       .schema(schema)
       .option("columnNameOfCorruptRecord", "_bad")     # PERMISSIVE mode (the default) keeps bad lines here
       .csv("orders.csv"))
d.cache()      # Spark requires materializing before querying only the corrupt column
d.show(truncate=False)
```

```text
|id |order_date|country|amount|_bad               |
|1  |2026-01-03|DE     |120.5 |NULL               |
|2  |2026-01-03|US     |80.0  |NULL               |
|3  |2026-01-04|US     |NULL  |NULL               |
|4  |NULL      |IN     |300.0 |4,not-a-date,IN,300|
|5  |2026-01-05|DE     |NULL  |5,2026-01-05,DE,abc|
```

Three parse modes exist:

| Mode | Bad record behaviour |
|---|---|
| `PERMISSIVE` (default) | Unparseable fields become NULL; the raw line goes to the corrupt-record column if you defined one |
| `DROPMALFORMED` | The row is discarded without a trace (dangerous: rows vanish) |
| `FAILFAST` | The read raises an error on the first bad record |

Notice what `PERMISSIVE` did with row 3: the **empty** amount became NULL but wasn't flagged as corrupt, because an
empty string is a valid "missing" CSV value. Only rows 4 and 5 are quarantined. A common pattern is to write rows with a
non-NULL `_bad` to a quarantine table and continue with the rest. With `FAILFAST` the same read raised an exception when
collected (the local run's top-level message was just "An error occurred while calling collectToPython", with the
parse error deeper in the stack trace).

Other CSV options you'll use: `sep`, `quote`, `escape`, `multiLine` (quoted fields containing newlines), `encoding`,
`nullValue`, `dateFormat`, `timestampFormat`.

## Reading JSON: schemas with nesting

```text
{"id":1,"user":{"name":"a","tags":["x","y"]}}
{"id":2,"user":{"name":"b"},"extra":true}
```

```python
j = spark.read.json("e.json")        # JSON Lines: one object per line
j.printSchema()
```

```text
root
 |-- extra: boolean (nullable = true)
 |-- id: long (nullable = true)
 |-- user: struct (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- tags: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
```

Spark inferred a schema that is the **union** of both records (hence `extra` appears, nullable), and sorted the
top-level fields alphabetically. Nested fields use dot paths; arrays are exploded one row per element:

```python
j.select("id", "user.name", F.explode_outer("user.tags").alias("tag")).show()
# id=1 a x | id=1 a y | id=2 b NULL    (explode_outer keeps the row with no tags; plain explode would drop it)
```

For files that are a single JSON array or pretty-printed, use `option("multiLine", True)`. As with CSV, inference reads
the data; for large feeds supply a schema (or a sample-based schema you then freeze).

## Parquet: the default for a reason

Parquet is a columnar, compressed, typed format; the schema travels inside the file. For 200,000 generated rows with a
string and a double column, the local run wrote:

```text
CSV (with header):   3,446,684 bytes
Parquet (snappy):    1,598,600 bytes
```

Besides being smaller, Parquet lets Spark **read only the columns a query needs** and skip row groups using min/max
statistics. Delta tables (Lesson 06) are Parquet files plus a log.

### Partitioned layout and partition pruning

```python
data.write.mode("overwrite").partitionBy("day").parquet("pq")
```

```text
['_SUCCESS', 'day=2026-01-01', 'day=2026-01-02', 'day=2026-01-03']
```

Each distinct `day` becomes a directory (`day=2026-01-02/part-....parquet`). A filter on the partition column reads
only the matching directories. The physical plan from the local run shows it:

```python
spark.read.parquet("pq").filter("day = '2026-01-02'").select("id").explain()
```

```text
*(1) Project [id#230]
+- *(1) ColumnarToRow
   +- FileScan parquet [id#230,day#233] ... PartitionFilters: [isnotnull(day#233), (day#233 = 2026-01-02)], ...
```

`PartitionFilters` means whole directories were skipped, and the scan lists only `[id, day]`, which is column pruning: the `amount` and `country` columns are never decoded.
Partition columns are stored in the *path*, not inside the files, which is why they reappear on read.
Level 2 · 04 covers choosing partition columns (spoiler: most tables shouldn't be partitioned at all).

## Save modes

```python
df.write.mode("append").parquet(p)      # add files
df.write.mode("overwrite").parquet(p)   # replace everything at the path
df.write.mode("errorifexists").parquet(p)   # default: fail if the path exists
df.write.mode("ignore").parquet(p)      # do nothing if it exists
```

Appending the same 30-row DataFrame twice gave a count of **60**; overwriting once brought it back to **30**; and
`errorifexists` on an existing path raised `[PATH_ALREADY_EXISTS] Path ... already exists. Set mode as "overwrite" to
overwrite the existing path.` Plain Parquet `append` is not idempotent: re-running a job after a partial failure
duplicates data. Delta's transactions and `MERGE` (Level 2) are the fix.

## File counts follow partition counts

One output file is written per task. With `repartition(5)` the local write produced **5** Parquet files; with
`coalesce(1)` it produced **1**. Many tiny files slow down every later read (each file costs a metadata lookup and
task), and one giant file can't be read in parallel. Lesson 04 of Level 2 returns to this as the "small files problem".

## How It Actually Works

A read is lazy: `spark.read...` only lists the files and (if inferring) samples them. Spark divides the files into
**input splits** of roughly `spark.sql.files.maxPartitionBytes` (128 MB by default), and each split becomes a task.
For Parquet each task reads the footer to find column chunks, decodes only requested columns, and tests row-group
min/max statistics against pushed-down filters to skip data. For CSV/JSON there is no such metadata, so every byte is
parsed as text every time, which is much slower.

A write is a distributed commit: each task writes its output into a temporary directory, and a *commit protocol*
renames the files into place when the job succeeds, writing `_SUCCESS` last. This is safe against a job crashing on
its own, but plain file commits aren't truly atomic on cloud object stores: a reader listing the directory mid-commit
may see a partial result. That gap is one of the problems the Delta log solves.

## Common mistakes

- **Using `inferSchema` in production** and finding types change when the data does.
- **`DROPMALFORMED`** hiding data loss. Prefer PERMISSIVE plus a quarantine, or FAILFAST for strict feeds.
- **Partitioning by a high-cardinality column** (user id) and creating millions of tiny directories.
- **Overwriting a path you are also reading from** in the same job.
- **Treating empty CSV fields and the literal text `NULL` as the same.** Set `nullValue` / `emptyValue` deliberately.
- **Writing CSV for internal pipelines.** Use Parquet or Delta; keep CSV for exchange with other people.

## Exercise

1. Run the CSV example yourself. Add a sixth row with an amount of `1e3` and see what the declared schema does with it.
2. Modify the pipeline to write rows with a non-NULL `_bad` to `quarantine/` as JSON and the rest to `clean/` as Parquet.
   Check the row counts add up to 5 (careful: drop `_bad` from the clean output).
3. Write the `data` DataFrame partitioned by `country` instead of `day`. Run `explain()` for `country = 'DE'` and point
   at the `PartitionFilters` line.
4. Generate 200,000 rows and compare file sizes for CSV, JSON and Parquet on your machine. Your numbers will differ
   from the ones above; what ordering do you expect, and do you get it?
