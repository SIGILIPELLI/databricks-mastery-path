---
description: "Create and change Delta tables with SQL and Python: INSERT, UPDATE, DELETE, CTAS, DESCRIBE DETAIL and HISTORY, and why DOUBLE is wrong for money."
---

# 06 · Delta Lake Tables: Create, Read, Update, Delete

Delta Lake is the table format at the heart of Databricks. A Delta table looks like any SQL table (you can `INSERT`,
`UPDATE`, `DELETE` and `MERGE`), but underneath it's a directory of Parquet files plus a transaction log. This lesson is
the day-to-day SQL and Python; Lesson 07 opens the log.

!!! note "What was run"
    All statements and outputs below come from **open-source Delta Lake 4.4.1 on local Spark 4.2.0**, writing to a
    laptop folder. Databricks runs a superset of this engine, so the statements work there too, but table properties,
    defaults (for example which table features are enabled for new tables) and the metadata shown by `DESCRIBE` differ
    between the open-source build and a Databricks runtime. On Databricks you'd normally address tables by a
    three-level Unity Catalog name (`main.sales.emp`), which wasn't run here.

## Create a table

```sql
CREATE TABLE emp (
  id     INT NOT NULL,
  name   STRING,
  dept   STRING,
  salary DOUBLE
) USING delta
COMMENT 'employees';

INSERT INTO emp VALUES (1,'Ann','eng',100), (2,'Bo','eng',90), (3,'Cy','ops',70);
UPDATE emp SET salary = salary * 1.1 WHERE dept = 'eng';
DELETE FROM emp WHERE id = 3;
SELECT * FROM emp ORDER BY id;
```

Locally (I added `LOCATION '<folder>'` so the files land somewhere I could inspect), the final query gave:

```text
+---+----+----+------------------+
| id|name|dept|            salary|
+---+----+----+------------------+
|  1| Ann| eng|110.00000000000001|
|  2|  Bo| eng| 99.00000000000001|
+---+----+----+------------------+
```

That `110.00000000000001` is not a Delta bug: `DOUBLE` is binary floating point, and `100 * 1.1` isn't exactly
110. **Use `DECIMAL(p,s)` for money.** (The same applies to `FLOAT`.)

On Databricks, `USING delta` is the default format for managed tables, so `CREATE TABLE t (...)` alone is enough.
Locally you must have the Delta extension configured, as in the Lesson 01 setup:

```python
from delta import configure_spark_with_delta_pip
builder = (SparkSession.builder.master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

## Create from a query, and from a DataFrame

```sql
CREATE TABLE dept_pay USING delta AS
SELECT dept, avg(salary) AS avg_sal FROM emp GROUP BY dept;      -- CTAS: schema comes from the query
```

```python
(df.write.format("delta")
   .mode("overwrite")                      # also "append", "errorifexists", "ignore"
   .saveAsTable("emp_copy"))               # or .save("/path") for a path-based table
```

Reading is `spark.table("emp")` or `spark.read.format("delta").load("/path")`; the SQL equivalent for a path is
``SELECT * FROM delta.`/path` ``.

## Row-level changes with the Python API

```python
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "emp")            # or DeltaTable.forPath(spark, "/path")
dt.update("dept = 'ops'", {"salary": "1"})       # condition and assignments are SQL strings
dt.delete("id = 3")
dt.toDF().show()                                  # current snapshot as a DataFrame
```

The condition `dept = 'ops'` matched no rows in the local run (Cy had already been deleted), and the table was
unchanged. An UPDATE or DELETE that matches nothing succeeds without error, so check row counts when it matters.

## Inspecting a table

```sql
DESCRIBE DETAIL emp;      -- one row: format, location, numFiles, sizeInBytes, partitionColumns, properties, protocol versions
DESCRIBE HISTORY emp;     -- one row per commit
DESCRIBE EXTENDED emp;    -- columns plus catalog metadata
```

Local `DESCRIBE DETAIL` (selected columns):

```text
|format|numFiles|properties|minReaderVersion|minWriterVersion|partitionColumns|
|delta |2       |{}        |1               |2               |[]              |
```

and `DESCRIBE HISTORY`:

```text
|version|operation   |operationParameters                                  |
|3      |DELETE      |{predicate -> ["(id#1783 = 3)"]}                     |
|2      |UPDATE      |{predicate -> ["(dept#468 = eng)"]}                  |
|1      |WRITE       |{mode -> Append, partitionBy -> []}                  |
|0      |CREATE TABLE|{partitionBy -> [], clusterBy -> [], description -> employees, ...}|
```

Every statement was a separate commit and a separate **version**. The `INSERT` appears as `WRITE`. Notice that the
`predicate` shows Spark's internal attribute ids (`id#1783`), a reminder that `operationParameters` are diagnostic text,
not a stable API to parse.

Constraints are enforced at write time. Inserting a NULL into the `NOT NULL` column failed (the local run raised an
exception from `spark.sql`; the full `[DELTA_NOT_NULL_CONSTRAINT_VIOLATED]`-style message was buried under a generic
Python error, so inspect the exception's cause when you see one). Level 2 · 02 covers `CHECK` constraints and schema
enforcement properly.

## Managed vs external, and what `DROP TABLE` does

Because I created these tables with a `LOCATION`, they are **external** tables, and `DESCRIBE EXTENDED` showed
`Type  EXTERNAL`. After dropping a CTAS table that had been created with a `LOCATION` (it was named `emp2` in my run) in the local run the table name disappeared, but the folder, its
`_delta_log` and its Parquet file were all still on disk. For a **managed** table (no `LOCATION`) the catalog owns the
files, and dropping the table deletes data too (on Databricks with Unity Catalog, managed-table data is retained for a
recovery window and can be undropped; Lesson 08 and Level 3 · 03). Know which kind you have before you run `DROP`.

## How It Actually Works

Each SQL statement above becomes one Delta **commit**: Spark runs the query to produce new Parquet files, then
atomically adds one JSON file to `_delta_log/` listing which data files were *added* and which were *removed*. Parquet
files are immutable, so `UPDATE` and `DELETE` don't edit files: they find the files containing matching rows, rewrite
those files without (or with changed) rows, and commit "remove old file, add new file".

You can see this in the `operationMetrics` of the history (Lesson 07 prints them): the `UPDATE` removed two files
(`numRemovedFiles -> 2`) and rewrote their rows, even though it changed only some of them. Newer Delta versions can
avoid some rewrites with **deletion vectors**, small side files that mark rows as deleted without rewriting the data
file; they're a table feature you opt into or that a platform enables by default. Which behaviour you get depends on
the table's features, not just the SQL you write.

Reads never lock anything. A query takes the table's latest committed version at the start and reads exactly the files
that version lists, so a writer committing mid-query can't change what you see.

## Common mistakes

- **Using `DOUBLE` for currency.** Use `DECIMAL`.
- **Treating `INSERT OVERWRITE`/`mode("overwrite")` as an append.** It replaces the table contents (as a new version).
- **Writing to the table's folder with plain `spark.write.parquet`.** Files not recorded in the log are invisible to
  Delta readers, and `VACUUM` may delete them.
- **Dropping an external table and expecting the data to go away** (or dropping a managed one and expecting it to stay).
- **No-op UPDATEs/DELETEs** passing silently when the predicate is wrong. Check `operationMetrics` or row counts.
- **Parsing `operationParameters`** as stable structured data.

## Exercise

1. Create `emp` as above with a `LOCATION`. After each statement run `DESCRIBE DETAIL emp` and write down `numFiles`.
   Explain the changes.
2. Change `salary` to `DECIMAL(10,2)` in a new table, repeat the 10% raise, and compare the displayed results.
3. Create `emp` as a *managed* table (no `LOCATION`) and find where Spark put its files (`DESCRIBE EXTENDED`). Drop it
   and check whether the folder is gone.
4. Run an `UPDATE` whose predicate matches no row. Compare `DESCRIBE HISTORY` before and after; does it add a version?
   (Run it and write down what you observe rather than guessing.)
