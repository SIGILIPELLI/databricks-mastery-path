---
description: "How Delta enforces schemas, when mergeSchema evolves them, the MERGE trap that ignores new columns, column mapping and CHECK constraints."
---

# 02 · Schema Enforcement, Evolution & Constraints

A plain Parquet folder accepts any file you drop in. A Delta table doesn't: on every write it checks that the incoming
data fits the table's schema and constraints, and rejects the whole commit if it doesn't. That's *schema enforcement*.
Sometimes the schema should change, and Delta supports explicit *schema evolution*. The edge between the two is where
pipelines break or, worse, silently lose data.

!!! note "What was run"
    Every error and result here came from **open-source Delta Lake 4.4.1 on local Spark 4.2.0**. Error text is quoted
    from the first line of the exception. Databricks adds platform behaviour on top (for example, schema-evolution
    defaults in pipelines and Auto Loader, and different table features enabled by default), which isn't covered by
    these runs.

## Enforcement on write

Target table: `tgt (id INT, v STRING)`. Here is what the local run did to each attempt:

| Write | Result |
|---|---|
| `INSERT ... SELECT 9 AS id, 'z' AS v, 'extra' AS w` (extra column) | **Rejected**: `[DELTA_METADATA_MISMATCH] A metadata mismatch was detected when writing to the Delta table.` |
| `INSERT INTO tgt (id) VALUES (10)` (missing column) | Allowed; `v` is NULL |
| `INSERT INTO tgt VALUES ('abc', 'q')` (wrong type, by position) | Rejected: `[CAST_INVALID_INPUT] The value 'abc' of the type "STRING" cannot be cast to "INT"` |
| DataFrame `.write.mode("append")` with columns `(v, id)` in a different order | Allowed: the DataFrame writer matched **by name**; row `(11, 'm')` landed correctly |
| `df.write.insertInto("tgt")` with columns `(v, id)` | Rejected: `insertInto` matches **by position**, so 'n' was cast to INT and failed |
| Append a `BIGINT` id to an `INT` column | Rejected: `[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'id' and 'id'` |

Three rules fall out. Extra columns are rejected; missing columns become NULL (unless `NOT NULL`); and
**by-name vs by-position** depends on the API. With ANSI mode on (Spark 4's default) the by-position mix-up above fails
loudly, but with ANSI off and two columns of the same type it would silently put values in the wrong columns. Prefer
by-name APIs (`write.saveAsTable`/`append`, `INSERT INTO t BY NAME` where available) and always name columns.

## Opting in to evolution

### Per write: `mergeSchema`

```python
(df_with_new_column.write.format("delta").mode("append")
   .option("mergeSchema", "true").save(path))
```

In the local run, appending `(9,'Zed','Rio','gold')` with a new `tier` column added the column to the table; existing
rows read back with `tier = NULL`:

```text
|id|name|city  |tier|
|2 |Bo  |Bergen|NULL|
|3 |Cy  |Lima  |NULL|
|4 |Di  |Rome  |NULL|
|9 |Zed |Rio   |gold|
```

Schema evolution is itself a commit (the history shows the schema change as part of the `WRITE`). Older readers
that pin an old schema can still read old files; new columns read as NULL for old data.

### Session-wide: `autoMerge`

```sql
SET spark.databricks.delta.schema.autoMerge.enabled = true;
```

This makes `MERGE` (and `UPDATE SET *`/`INSERT *`) evolve the schema. Use it carefully: it applies to the whole session.

### The MERGE trap

Without `autoMerge`, this merge **succeeded and silently dropped the new column**:

```sql
-- source has (id, v, tier); target has (id, v)
MERGE INTO tgt t USING nsrc u ON t.id = u.id WHEN NOT MATCHED THEN INSERT *;
-- num_inserted_rows = 1, and the target still has only (id, v)
```

`INSERT *` means "insert the target's columns, taken from the source by name", so the extra source column `tier`
was ignored without an error. The data in `tier` was never written anywhere. If you rely on `*` clauses and a
source gains a column, nothing alerts you unless you compare schemas yourself. A pipeline guard that checks
`set(source.columns) - set(target.columns)` before merging is cheap insurance.

### Replacing the schema

For a deliberate breaking change (a column's type must change), overwrite with `overwriteSchema`:

```python
(df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path))
```

The local run turned `id` from INT to STRING this way, as a new version of the same table. `DESCRIBE HISTORY` showed
it as an ordinary `WRITE`. Older versions are expected to keep their old schema when read with time travel (I did not test this), but anything
reading the current table now sees strings, so treat it as a breaking change for consumers. Databricks also offers a
*type widening* table feature for safe widenings such as INT to BIGINT without rewriting data; the plain append above
failed because that feature wasn't on, and I did not test the feature itself.

## Renaming and dropping columns: column mapping

```sql
ALTER TABLE tgt RENAME COLUMN v TO val;
-- [DELTA_UNSUPPORTED_RENAME_COLUMN] Column rename is not supported for your Delta table.
```

By default, Delta stores column names inside the Parquet files, so renaming would orphan them. Turn on **column
mapping**, which gives each column a stable physical id:

```sql
ALTER TABLE tgt SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5');
ALTER TABLE tgt RENAME COLUMN v TO val;     -- now succeeds, no data rewritten
ALTER TABLE tgt DROP COLUMN val;            -- also a metadata-only change
```

Locally both statements worked, and `DESCRIBE DETAIL` showed the table at reader version 2 / writer version 5 with
`delta.columnMapping.mode -> name`. That protocol bump means **older clients can no longer read the table**. Check
what reads your tables (other engines, old runtimes) before enabling it.

## Constraints

`NOT NULL` and `CHECK` constraints are enforced at write time:

```sql
ALTER TABLE chk ADD CONSTRAINT qty_pos CHECK (qty > 0);
```

If existing rows violate it, Delta refuses to add it (local run: `[DELTA_NEW_CHECK_CONSTRAINT_VIOLATION] 1 rows in
chk violate the new CHECK constraint (qty > 0)`); after deleting the bad row it was accepted, and a later
`INSERT VALUES (3, 0)` was rejected by Delta's invariant check. The constraint shows up as a table property
(`delta.constraints.qty_pos -> qty > 0`). Adding a CHECK also raised the table's writer protocol (to version 3 in the
open-source build), another example of constraints being protocol features.

Note: `PRIMARY KEY`, `FOREIGN KEY` and `UNIQUE` constraints on Databricks are informational and not enforced, per the
Databricks constraints documentation; only `NOT NULL` and `CHECK` stop writes.

**Generated columns** (`GENERATED ALWAYS AS (...)`) are a related feature: the value derives from other columns, and a
write that provides a conflicting value is rejected. They're handy for deriving a partition or clustering column such
as `day` from a timestamp, so queries that filter on the timestamp still benefit.

## How It Actually Works

Before committing, a Delta writer compares the incoming DataFrame's schema with the `metaData` action of the table's
current snapshot. If they match, it writes files. If not, it raises unless an evolution option is set, in which case it
computes the merged schema and includes a new `metaData` action in the same commit as the new `add` files. The check
occurs in the writer, not the storage layer, so tools that write Parquet directly into the table folder bypass it, and
that is exactly how people corrupt tables.

Constraint checks work similarly: the writer adds a filter expression to the write plan that fails the job if any
row evaluates false. NOT NULL is stored in the schema; CHECK constraints are stored as properties in the same
`metaData` action. For the `ALTER TABLE ADD CONSTRAINT` case, Delta first scans the table to validate existing data,
which is why a violation blocked the statement.

## Common mistakes

- **Enabling `mergeSchema` everywhere** so that a typo'd column name creates a new column.
- **`INSERT *` merges that quietly ignore new source columns.**
- **Positional inserts** (`insertInto`, `INSERT INTO t SELECT ...` without column names) reordered by a refactor.
- **Writing Parquet files straight into the Delta folder.**
- **Upgrading the protocol** (column mapping, new features) without checking all readers.
- **Assuming primary, foreign or unique keys are enforced** on Databricks tables.

## Exercise

1. Reproduce the table above. Try each write, predict the outcome first, and compare with the local run.
2. Make the MERGE trap fail loudly: write a function that compares source and target columns and raises when the
   source has extras, then enable `autoMerge` and watch the column appear.
3. Enable column mapping on a table with data, rename a column, then use time travel to read an old version. Which
   column name do you see?
4. Add a `CHECK (amount >= 0)` constraint to a table, then attempt a write containing a negative value in a batch of
   a thousand good rows. Is the good data written? Confirm with the history.
