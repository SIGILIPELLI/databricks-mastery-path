---
description: "Migrate to Databricks: CONVERT TO DELTA and clones run locally, Hive metastore to Unity Catalog, SQL dialect translation pitfalls, validation."
---

# 09 · Migrating to Databricks

Few teams start on a blank slate. Data sits in Parquet folders, in a Hive metastore, in another warehouse, with SQL
and jobs written for another engine. Migration succeeds when it's treated as three separate problems: **moving data**, **moving
metadata and permissions**, and **translating code**, each with its own verification.

!!! note "What was run, and what wasn't"
    I ran locally (open-source Delta Lake 4.4.1, Spark 4.2.0): an in-place `CONVERT TO DELTA`, a `SHALLOW CLONE`, and SQL
    dialect translation with **sqlglot 30.24**, whose actual output (including its flaws) is shown below. `DEEP CLONE` is *not*
    supported by this open-source build (error shown), so I could not test it and describe it from the Databricks docs. Hive
    metastore upgrade tools (`SYNC`, UCX, upgrade wizards), federation and Unity Catalog behaviour need a workspace and were **not
    run**; they are described from the documentation (October 2026). I have not migrated a real system, so the checklist is
    guidance, not field experience.

## 1. Moving data

### Parquet to Delta, in place

If you have a folder of Parquet files, you often don't need to copy anything: `CONVERT TO DELTA` writes a `_delta_log` that
references the existing files.

```sql
CONVERT TO DELTA parquet.`/data/legacy` PARTITIONED BY (day STRING);
```

Local run on a 1,000-row folder partitioned by `day`:

```text
before:  ['_SUCCESS', 'day=0', 'day=1', 'day=2']
after:   ['_SUCCESS', '_delta_log', 'day=0', 'day=1', 'day=2']
rows read through Delta: 1000        DESCRIBE HISTORY operations: ['CONVERT']
```

The data folders were untouched; only `_delta_log` appeared, and the history recorded a `CONVERT` operation. Points to remember:

- You must supply `PARTITIONED BY` with types for partitioned data (Delta can't infer partition column types reliably from
  directory names).
- Delta scans the files to collect statistics and the schema, so conversion takes time proportional to the data size, but it doesn't
  rewrite it.
- The folder is now a Delta table: writers that don't know Delta (old jobs doing plain Parquet writes into it) will corrupt the table
  state. Cut writers over **before** converting, or convert and cut over in the same change.
- Files written with incompatible Parquet features may need rewriting; test on a copy.

### Clones: copies, snapshots and sandboxes

```sql
CREATE TABLE sandbox SHALLOW CLONE prod.silver.orders;
CREATE TABLE backup  DEEP    CLONE prod.silver.orders;
```

A **shallow clone** copies only metadata (the log), so it's instant and tiny, and reads the source's data files by reference.
A **deep clone** copies the data files too. In the local run, after cloning, I deleted 10 rows from the source: the shallow clone
still returned all **1,000** rows while the source returned **990**, showing the clone is isolated from later changes to the source
(it keeps referencing the files that were current when it was made). Cautions: a shallow clone depends on the source's files, so a
`VACUUM` on the source after the retention window can break the clone, and shallow clones aren't backups.

**`DEEP CLONE` failed in my local build** with `[PARSE_SYNTAX_ERROR] Syntax error at or near 'DEEP'`, so it appears to be a Databricks
feature rather than part of open-source Delta 4.4.1; I used a CTAS copy for the comparison (1,000 rows) instead. In the Databricks docs,
`CLONE` or `CTAS` is also how you copy Hive tables into Unity Catalog **managed** tables (they must live in reserved managed storage).

### Loading from elsewhere

- **Files in object storage**: register them as external tables/volumes (Level 3 · 03) and ingest with Auto Loader or `COPY INTO`
  (Level 2 · 08).
- **Databases and warehouses**: read via JDBC/connectors or **Lakehouse Federation** (query the remote system in place through a
  foreign catalog) to avoid copying while you migrate gradually; ingestion tools and CDC feeds for continuous sync (Lakeflow Connect
  and partner tools are described in the docs; I did not review their current feature sets).
- **Other lake formats**: Iceberg and Delta interoperate on Databricks to varying degrees (check "UniForm" and managed Iceberg
  documentation for your versions).

## 2. Moving metadata and permissions: Hive metastore to Unity Catalog

From the Databricks documentation: a workspace's legacy **Hive metastore** appears as a catalog named `hive_metastore`, usable
alongside Unity Catalog, but it doesn't get Unity Catalog's auditing, lineage or access control, and Databricks recommends
migrating. Options named in the docs:

| Approach | Use when |
|---|---|
| **`SYNC`** | Upgrade Hive **external** tables (and managed tables stored outside workspace/DBFS-root storage) to Unity Catalog external tables; can re-sync as the source changes. It cannot upgrade managed tables in workspace storage |
| **`CLONE` / CTAS** | Copy Hive managed tables into UC **managed** tables (needed for tables in DBFS root) |
| **UCX** (Databricks Labs, `databrickslabs/ucx`) | A toolkit that assesses readiness and migrates identities, permissions, storage locations and tables |
| **Upgrade wizards in the UI** | Small numbers of tables |
| **Hive metastore federation** | Gradual: mirror the Hive metastore as a *foreign catalog* governed by UC while you migrate |

```sql
SYNC TABLE prod.sales.orders FROM hive_metastore.sales.orders DRY RUN;   -- preview; syntax: SYNC {SCHEMA|TABLE} target [AS EXTERNAL] FROM source [SET OWNER p] [DRY RUN]
CREATE TABLE prod.sales.orders_managed DEEP CLONE hive_metastore.sales.orders_managed;   -- Databricks-only syntax; not run
```

What does not migrate itself: **permissions** (Hive table ACLs vs UC grants; design the new grants with Level 3 · 02), **mounts and
DBFS paths** in code (`dbfs:/mnt/...` becomes external locations and volumes), **cluster-scoped credentials** (instance profiles
become storage credentials), **jobs** (compute access mode, libraries), **init scripts** (not supported on some UC compute), and
**three-part names** in SQL. Plan each as workstreams.

## 3. Translating code: dialects

Machine translation gets you most of the way for SQL and none of the way for stored-procedure logic. I ran four sample queries
through `sqlglot.transpile(q, read=<dialect>, write="databricks")`. The results below are real output, and most of the value is in
what the tool got *wrong or left alone*.

**Snowflake to Databricks**

```text
in:  SELECT IFF(a > 1,'x','y') f, TO_VARCHAR(ts,'YYYY-MM-DD') d, DATEADD(day,7,ts) w, t.v:name::string n
     FROM t, LATERAL FLATTEN(input => t.arr) x
     QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC) = 1
out: SELECT IF(a > 1,'x','y') AS f, TO_CHAR(ts,'YYYY-MM-DD') AS d, DATEADD(DAY,7,ts) AS w, CAST(t.v:name AS STRING) AS n
     FROM t, LATERAL EXPLODE(input => t.arr) AS x(SEQ, KEY, PATH, INDEX, VALUE, THIS)
     QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC NULLS FIRST) = 1
```

Good: `IFF` to `IF`, `QUALIFY` retained. Two things to catch: the tool added **`NULLS FIRST`** to the descending sort. That preserves Snowflake's
semantics (NULLs sort as largest, so first when descending) where Databricks' default for `DESC` puts NULLs last (the default
you saw in Level 1 · 03's window example). And `LATERAL EXPLODE(input => ...) AS x(SEQ, KEY, ...)` copies
Snowflake's `FLATTEN` output columns onto a function that doesn't have them with a `input =>` argument; I would **not trust that
line to run** on Databricks and would rewrite it by hand as `LATERAL VIEW explode(t.arr)` or `explode(t.arr)`. I did not run it.

**PostgreSQL**

```text
in:  SELECT id, NOW() - INTERVAL '7 days' AS wk, data->>'name' AS n, string_agg(tag, ',') FROM events GROUP BY id, data->>'name' LIMIT 10
out: SELECT id, CURRENT_TIMESTAMP() - INTERVAL '7' DAYS AS wk, GET_JSON_OBJECT(data, '$.name') AS n, LISTAGG(tag, ',') FROM events GROUP BY id, GET_JSON_OBJECT(data, '$.name') LIMIT 10
```

Plausible, but `->>` on a JSON string column becomes `GET_JSON_OBJECT`, which is a different function with its own semantics (and
you might prefer a typed `VARIANT` or `from_json`), and `LISTAGG`'s availability and ordering behaviour should be checked on your
runtime (I did not verify it).

**SQL Server (T-SQL)**

```text
in:  SELECT TOP 5 ISNULL(a,0) a, GETDATE() now, DATEDIFF(day,d1,d2) dd FROM dbo.t WITH (NOLOCK)
out: SELECT COALESCE(a,0) AS a, CURRENT_TIMESTAMP() AS now, DATEDIFF(DAY, CAST(d1 AS TIMESTAMP), CAST(d2 AS TIMESTAMP)) AS dd FROM dbo.t LIMIT 5
```

`TOP` became `LIMIT`, `ISNULL` became `COALESCE`. Notice that **`WITH (NOLOCK)` silently disappeared**: a dirty-read hint
that has no equivalent, because Delta reads a consistent snapshot. That's usually fine, but it's a semantic change the tool made without
telling you. `LIMIT` without an `ORDER BY` returns arbitrary rows in both systems.

**Oracle**

```text
in:  SELECT NVL(a,0) a, SYSDATE, TO_CHAR(d,'YYYY-MM-DD') FROM t WHERE ROWNUM <= 5
out: SELECT COALESCE(a,0) AS a, CURRENT_TIMESTAMP(), TO_CHAR(d,'YYYY-MM-DD') FROM t WHERE ROWNUM <= 5
```

`ROWNUM <= 5` was **left unchanged**, and Databricks has no `ROWNUM`: this would fail. Machine translation can emit SQL that looks fine and
doesn't run. **Never accept translated SQL without executing it**, and compare results.

Other well-known sources of mismatch (general knowledge, not tested here): integer division and decimal precision rules,
string comparison collation and trailing spaces, date/timestamp time zone handling (Databricks serverless defaults to UTC; Level 4 · 07),
NULL ordering, implicit casts (ANSI mode errors), identifier case sensitivity, `MERGE` semantics, temp tables, and procedural
code (PL/SQL, T-SQL procedures), which must be rewritten as notebooks, SQL scripting or Python.

## 4. Validation: how you know it worked

1. **Row counts** per table, per partition or day, source versus target.
2. **Aggregates**: sums, min/max, null counts, distinct counts on key columns. Compare with tolerance for floating point, exactly
   for decimals.
3. **Row-level checksums** on a sample (hash of the concatenated columns) or full-table hash by key range for critical tables.
4. **Query-result diffs** for the top reports, run in parallel for a period ("shadow run").
5. **Performance and cost** on realistic data (Level 4 · 06 and 07).
6. **Permissions audit**: for each role, test with a user who should and should not have access (Level 3).
7. **Cutover plan with rollback**: freeze writes, final sync, switch, keep the old system read-only for a fixed period.

## How It Actually Works

`CONVERT TO DELTA` is cheap because a Delta table *is* "Parquet files plus a log": the command lists the files, reads footers for
the schema and statistics, and writes `00000000000000000000.json` with an `add` action per file (Level 1 · 07). Shallow clone writes a
log whose `add` actions point to the **source's** absolute file paths; deep clone additionally copies the files. `SYNC` creates or updates
a metastore entry pointing at the same storage location, which is why it applies to external data and why it doesn't copy anything.
Dialect translation is parse-tree rewriting: the input SQL becomes a tree, rewrite rules replace nodes the target spells differently
(`IFF` to `IF`), and the tree is printed in the target dialect, which cannot add semantics the tree never represented (a `NOLOCK` hint
or a `ROWNUM` pseudo-column), hence the failures above.

## Common mistakes

- **Converting a folder that other jobs still write to as plain Parquet.**
- **Relying on a shallow clone as a backup.**
- **Translating SQL and not executing it.**
- **Migrating tables but not permissions, mounts and credentials.**
- **Cutting over without a validation suite** or a rollback path.
- **Treating `hive_metastore` as permanent** because it still works.
- **Forgetting time zones and ANSI-mode differences** when comparing numbers.

## Exercise

1. Reproduce the `CONVERT TO DELTA` and shallow-clone experiments. Then `VACUUM` the source with `RETAIN 0 HOURS` after an update and
   see whether the shallow clone still reads. Record what you observe.
2. Run `sqlglot.transpile` on five queries from your own warehouse. For each output, classify it as correct, needs-edit or wrong, and
   explain how you decided (executing it counts).
3. Write a validation script that compares row counts and per-column null counts and sums between two tables and prints a diff.
4. Draft the cutover runbook for one pipeline: freeze, final sync, switch, verify, rollback, and the exact checks at each step.
