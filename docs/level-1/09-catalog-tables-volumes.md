---
description: "Where Databricks data lives: managed vs external tables, views and temp views, Unity Catalog volumes for files, and why DBFS root and mounts are legacy."
---

# 09 · Tables, Views & Volumes: Where Data Lives

"Where is my data?" has four answers on Databricks: in a table, behind a view, in a volume (files), or in a legacy
location you should be migrating away from. Choosing correctly decides who can see the data, what `DROP` deletes, and
whether your code still works after a platform change.

!!! note "What was run"
    Table and view behaviour below was run on **local Spark 4.2.0 with Delta 4.4.1**, using Spark's built-in catalog
    (a local warehouse folder and a `sales` database). That catalog has two-level names (`schema.table`). Databricks
    with Unity Catalog adds a top level (`catalog.schema.table`), volumes, and governance; those parts are described
    from the Databricks documentation (October 2026) and weren't executed.

## The object model

```text
catalog                      (Unity Catalog: a top-level container, e.g. main, prod, dev)
 └── schema (database)       (a grouping: sales, finance)
      ├── tables             (Delta by default; managed or external)
      ├── views              (stored queries) / materialized views
      ├── volumes            (governed file storage: managed or external)
      ├── functions
      └── registered models
```

Names resolve as `catalog.schema.object`. Locally we only had `sales.orders`; on Databricks it would be
`main.sales.orders`. You can set defaults with `USE CATALOG main; USE SCHEMA sales;` so short names work.

## Managed vs external tables

```sql
CREATE TABLE sales.orders (id INT, amt DOUBLE) USING delta;       -- managed: the catalog picks the location
CREATE TABLE sales.orders_ext (id INT, amt DOUBLE) USING delta
  LOCATION 's3://bucket/path/orders_ext';                         -- external: you pick the location
```

`DESCRIBE TABLE EXTENDED sales.orders` locally reported:

```text
|Type    |MANAGED                                            |
|Location|file:/private/tmp/claude-501/exp/wh/sales.db/orders|
|Provider|delta                                              |
```

| | Managed | External |
|---|---|---|
| Location | Chosen and organized by the catalog | Any path you register |
| `DROP TABLE` | Removes metadata **and** data (Unity Catalog keeps managed-table data recoverable for a while) | Removes metadata only; files stay |
| Good for | Default for tables owned by the lakehouse | Data shared with other systems, or already existing in cloud storage |
| Governance | Unity Catalog controls access end to end | Needs an external location and storage credential (Level 3 · 03) |

Databricks recommends managed tables for most uses; Unity Catalog managed tables also get platform optimizations
such as automatic maintenance via Predictive Optimization (Level 4 · 08). Use external tables when something outside
Databricks must read or write the same files directly.

In Lesson 06 a locally created external table (with `LOCATION`) kept its folder after `DROP TABLE`, as the table above
predicts.

## Views

A view is a stored query, not stored data:

```sql
CREATE VIEW sales.big AS SELECT * FROM sales.orders WHERE amt > 5;
SELECT * FROM sales.big;          -- (2, 7.0)
```

`DESCRIBE EXTENDED sales.big` shows `Type VIEW` and the `View Text` (`SELECT * FROM sales.orders WHERE amt>5`). Three
behaviours worth seeing for yourself (all from the local run):

1. After `ALTER TABLE sales.orders ADD COLUMN note STRING`, `SELECT * FROM sales.big` still worked and returned only
   the original two columns. A view captures its column list when it is created, so `SELECT *` inside it does not pick up
   later columns. Recreate (or `ALTER VIEW ... AS`) the view to include them.
2. After `DROP TABLE sales.orders`, the view still existed in the catalog, but querying it failed with
   `[TABLE_OR_VIEW_NOT_FOUND] The table or view sales.orders cannot be found.` Views don't protect against dropped
   dependencies, and don't track them for you.
3. `SHOW TABLES IN sales` lists tables **and** views together; `isTemporary` distinguishes session-scoped ones.

Other view flavours:

| Kind | Lifetime | Use |
|---|---|---|
| `CREATE VIEW` | Persisted in the catalog | Reusable, shareable logic; basis for access control (expose a subset of columns/rows) |
| `CREATE TEMP VIEW` | This Spark session only | Name an intermediate DataFrame for SQL (`df.createOrReplaceTempView("t")`) |
| `CREATE GLOBAL TEMP VIEW` | Application lifetime, in `global_temp` | Share between sessions of one application; rarely needed |
| `CREATE MATERIALIZED VIEW` (Databricks SQL/pipelines) | Stored result, refreshed | Precomputed aggregates; covered with Lakeflow pipelines in Level 3 · 06 |

## Volumes: governed files

Not everything is a table. Raw CSV drops, images, PDFs, model artifacts and checkpoint files need a home with
permissions too. In Unity Catalog that home is a **volume**, a governed logical volume of cloud storage. Per the
documentation:

- **Managed volumes** store files in storage that Unity Catalog manages for the schema.
- **External volumes** register an existing cloud storage path; dropping the volume leaves the files.
- You access them by path: `/Volumes/<catalog>/<schema>/<volume>/<path>`.
- Files in a volume **cannot be registered as tables**: volumes are for path-based access. Load the data into a Delta
  table to govern it as tabular data.

```python
# Needs a workspace; not run locally
df = spark.read.format("csv").option("header", True).load("/Volumes/main/raw/landing/orders/2026-10-21/")
dbutils.fs.ls("/Volumes/main/raw/landing/")
```

```sql
CREATE VOLUME main.raw.landing;
GRANT READ VOLUME ON VOLUME main.raw.landing TO `data-engineers`;
```

The `GRANT` statement above follows the documented syntax; it couldn't be executed here. Volumes are the recommended
landing zone for Auto Loader and `COPY INTO` (Level 2 · 08).

## Legacy locations you will still meet

The documentation marks the **DBFS root** (`dbfs:/`, with folders such as `/FileStore` and `/user/hive/warehouse`) and
**DBFS mounts** as deprecated and not recommended; new accounts are provisioned without access to them. Their
replacements are Unity Catalog volumes and external locations (governed), and workspace files (for code, configs and
small files). The `dbfs:/` scheme itself still works as an optional prefix when addressing volumes. When you inherit
older notebooks, expect `dbfs:/mnt/...` paths and `hive_metastore.default.table` names; migration is covered in
Level 4 · 09.

Also remember **driver-local storage**: `/tmp` or `./file` from `%sh` or plain Python is a disk on the driver node,
visible only there and gone when compute stops.

| Need | Put it in |
|---|---|
| Tabular data used by many | Managed Delta table |
| Existing files other tools also write | External table, or external volume if non-tabular |
| Raw drops, images, documents | Volume |
| Notebooks, small config, `.py` modules | Workspace files / Git folders |
| Scratch for a single run | Driver-local disk (not durable) |

## How It Actually Works

A catalog is a metadata service: a row says "table `main.sales.orders` is a Delta table at location X, owned by Y,
with these grants". The data files exist independently in object storage. Managed means the catalog chose X and is
responsible for its lifecycle; external means you chose X and the catalog merely points at it. That's why
`DROP` differs: it removes the catalog row, and for managed tables also schedules deletion of X.

A view is another catalog row holding SQL text and the resolved column list. The query is re-planned against the
underlying tables on every read, so you always see current data, but a missing or changed dependency only surfaces at
query time (the failure above).

Volumes add a path-translation layer: a `/Volumes/...` path is resolved by Unity Catalog to a cloud storage
location after a permission check, and compute reads the files with credentials that the catalog vends, so users don't
need cloud keys.

## Common mistakes

- **Dropping a managed table you thought was external** (or the reverse).
- **Hard-coding `hive_metastore.` or `dbfs:/mnt/` paths** in new work.
- **Relying on `SELECT *` views** to follow schema changes.
- **Using a temp view across notebooks or jobs.** It exists only in its Spark session.
- **Writing files into a volume and expecting a table.** Volumes aren't tables.
- **Storing durable results in `/tmp` on the driver.**

## Exercise

1. Locally, create a managed table and an external table (`LOCATION`) in a `sales` database. Drop both and list the
   folders. Which data remained?
2. Recreate the "view that stops working": create the view, add a column to the base table, and check which columns the
   view returns. Then fix the view.
3. Look up `DESCRIBE EXTENDED` output for a view on your platform. Which field tells you its defining SQL?
4. On a workspace (or from the documentation), list the three differences between a managed and an external volume.
