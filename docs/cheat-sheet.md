---
title: "Databricks Cheat Sheet: Quick Reference, Beginner to Advanced"
description: "A free Databricks cheat sheet: 27 quick-reference tables from every level of the Databricks Mastery Path, each linked to its full lesson."
---

# Databricks Cheat Sheet

Quick-reference tables from **27 lessons** of the free Databricks Mastery Path, in course order from beginner to advanced. Each section links to the full lesson with explanations and runnable examples.

## Level 1 · Lakehouse & Spark Foundations

### [01 · The Lakehouse: Control Plane, Compute Plane &amp; Storage](level-1/01-lakehouse-architecture.md)

*What "lakehouse" means in practice*

| Idea | Strength | Weakness the lakehouse tries to fix |
|---|---|---|
| Data warehouse | ACID tables, fast SQL, governance | Proprietary storage, expensive for raw/unstructured data, awkward for ML |
| Data lake (files in object storage) | Cheap, open, any file type | No transactions, no schema enforcement, easy to corrupt, hard to govern |
| Spark/ML clusters | Flexible code, scales out | Needs reliable tables underneath |

### [02 · Workspaces, Compute Types &amp; Notebooks](level-1/02-workspace-compute.md)

*The compute menu*

| Compute | Who manages it | Best for | Notes |
|---|---|---|---|
| **Serverless compute** (notebooks, jobs, pipelines) | Databricks | Most interactive work and scheduled jobs with spiky demand | No cluster to configure; starts quickly; has documented limitations on libraries, languages and some configs |
| **All-purpose (interactive) classic compute** | You | Exploration, development, custom libraries/init scripts | Stays up until you stop it or auto-termination fires; billed at the higher all-purpose rate |
| **Job compute (classic)** | You define, the platform creates per run | Production batch jobs | Created for a run, terminated after; priced lower than all-purpose |
| **SQL warehouse** (serverless, pro, classic) | Databricks | BI, SQL analytics, dashboards, JDBC/ODBC | SQL (and SQL-based) workloads only; sized in T-shirt sizes |

### [07 · Inside the Delta Log: Transactions &amp; ACID](level-1/07-delta-log-acid.md)

*What a commit contains*

| Action | Meaning |
|---|---|
| `metaData` | The table's schema, partition columns, and configuration (written at creation or when they change) |
| `protocol` | Minimum reader/writer versions (and table features) a client must support to use the table safely |
| `add` | A data file is part of the table from this version; carries size and **statistics** |
| `remove` | A data file is no longer part of the table (it stays on disk until `VACUUM`) |
| `commitInfo` | Operation name, parameters, metrics, isolation level, `isBlindAppend`, `readVersion` |

### [10 · Project — A Sales Lakehouse You Can Run Locally](level-1/10-project-local-sales-lakehouse.md)

*The task*

| Problem | Planted as |
|---|---|
| Missing price | order 10 has an empty `unit_price` |
| Impossible date | order 20 has date `2026-13-45` |
| Exact duplicate | order 5 appears twice |
| Unknown country | order 999 has country `XX` |

## Level 2 · Delta Lake & Data Engineering

### [02 · Schema Enforcement, Evolution &amp; Constraints](level-2/02-schema-enforcement-evolution.md)

*Enforcement on write*

| Write | Result |
|---|---|
| `INSERT ... SELECT 9 AS id, 'z' AS v, 'extra' AS w` (extra column) | **Rejected**: `[DELTA_METADATA_MISMATCH] A metadata mismatch was detected when writing to the Delta table.` |
| `INSERT INTO tgt (id) VALUES (10)` (missing column) | Allowed; `v` is NULL |
| `INSERT INTO tgt VALUES ('abc', 'q')` (wrong type, by position) | Rejected: `[CAST_INVALID_INPUT] The value 'abc' of the type "STRING" cannot be cast to "INT"` |
| DataFrame `.write.mode("append")` with columns `(v, id)` in a different order | Allowed: the DataFrame writer matched **by name**; row `(11, 'm')` landed correctly |
| `df.write.insertInto("tgt")` with columns `(v, id)` | Rejected: `insertInto` matches **by position**, so 'n' was cast to INT and failed |
| Append a `BIGINT` id to an `INT` column | Rejected: `[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'id' and 'id'` |

### [03 · OPTIMIZE, Z-Order &amp; Liquid Clustering](level-2/03-optimize-zorder-clustering.md)

*Data skipping in practice*

| Layout | Total files | Files read for filter on `a` | …on `b` | …on `a` and `b` |
|---|---|---|---|---|
| Written randomly (16 files) | 16 | 16 | 16 | n/a |
| `OPTIMIZE` only (no zorder) | 16 | 16 | 16 | n/a |
| `OPTIMIZE ... ZORDER BY (a)` | 143 | **2** | 143 | n/a |
| `OPTIMIZE ... ZORDER BY (a, b)` | 143 | 14 | 18 | **1** |

### [04 · Partitioning, File Sizes &amp; Data Skipping](level-2/04-partitioning-file-layout.md)

*The experiment*

| Layout | Data files | Total size | Average file | Write time (this laptop) |
|---|---|---|---|---|
| No partitioning | 2 | 2,125 KB | 1,063 KB | 5.9 s |
| `partitionBy("day")` | 60 | 2,580 KB | 43 KB | 1.0 s |
| `partitionBy("user_id")` | **86,665** | **105,606 KB** | **1.2 KB** | **467 s** |

### [06 · Joins, Broadcast &amp; Data Skew](level-2/06-joins-skew-broadcast.md)

*The strategies*

| Strategy | When Spark picks it | Cost |
|---|---|---|
| **Broadcast hash join** | One side is below `spark.sql.autoBroadcastJoinThreshold` (10 MB default), or hinted | Small side shipped to every executor; large side not shuffled. Fastest |
| **Shuffle hash join** | Hinted, or the planner estimates one side is much smaller | Both sides shuffled; a hash table is built on the smaller side per partition |
| **Sort-merge join** | Default for large-large equi-joins | Both sides shuffled and sorted; scales to huge inputs; sensitive to skew |
| **Broadcast nested loop / cartesian** | Non-equi joins without keys | Quadratic; avoid |

### [07 · Structured Streaming &amp; Checkpoints](level-2/07-structured-streaming.md)

*Source → transform → sink*

| Trigger | Behaviour |
|---|---|
| default (unspecified) | Start the next micro-batch as soon as the previous finishes |
| `processingTime="1 minute"` | Fixed-interval micro-batches |
| `availableNow=True` | Process everything available (possibly in several batches), then stop. Ideal for scheduled "incremental batch" jobs |
| `once=True` | Legacy single-batch version of the above (deprecated in favour of `availableNow`) |

### [08 · Auto Loader: Incremental File Ingestion](level-2/08-auto-loader.md)

*Finding new files: two modes*

| | Directory listing (default) | File notification (recommended) |
|---|---|---|
| How | Lists the input directory each trigger | Subscribes to storage events through a queue; reads the queue |
| Setup | None beyond read access | Notification/queue services, or "file events" on a Unity Catalog external location |
| Scales with | Number of files in the directory | Number of *new* files |

### [09 · The Medallion Architecture](level-2/09-medallion-architecture.md)

*The three layers*

| | Bronze | Silver | Gold |
|---|---|---|---|
| Purpose | Faithful, replayable copy of the source | Clean, typed, deduplicated, conformed entities | Aggregates and models shaped for consumers |
| Schema | Source schema, usually **all strings** + lineage columns | Declared, strongly typed | Business terms, star schemas, KPIs |
| Write pattern | Append only (never update) | Upsert (`MERGE`) / incremental | Rebuild or incremental aggregate |
| Quality rule | Don't lose anything, don't fail on bad data | Enforce types and keys; quarantine failures | Reconcile to silver; stable definitions |
| Audience | Data engineers | Engineers, analysts, data scientists | BI, dashboards, ML features, executives |

## Level 3 · Unity Catalog, Pipelines & Sharing

### [01 · Unity Catalog: Metastore, Catalogs &amp; Three-Level Names](level-3/01-unity-catalog-model.md)

*How people organize catalogs*

| Layout | Example | Strength | Cost |
|---|---|---|---|
| Per environment | `dev`, `test`, `prod` | Simple promotion; permissions per environment; workspace-catalog binding can pin each catalog to its workspace | A business domain's data is split across catalogs |
| Per domain/team | `finance`, `marketing` | Ownership is clear | Environments become schemas or naming conventions |
| Hybrid | `prod_finance`, `dev_finance` | Both | Many catalogs to manage |

### [02 · Privileges, Ownership &amp; Grants](level-3/02-privileges-grants.md)

*Rule 1: usage privileges are prerequisites*

| Operation | Needs |
|---|---|
| Read a table or view | `USE CATALOG` + `USE SCHEMA` + `SELECT` |
| Write a table | `USE CATALOG` + `USE SCHEMA` + `MODIFY` |
| Create a table | `USE CATALOG` + `USE SCHEMA` + `CREATE TABLE` (on the schema, or inherited from the catalog) |
| Create a schema | `USE CATALOG` + `CREATE SCHEMA` on the catalog |
| Run a function | `USE CATALOG` + `USE SCHEMA` + `EXECUTE` |
| Read volume files | `USE CATALOG` + `USE SCHEMA` + `READ VOLUME` |

### [03 · Storage Credentials, External Locations &amp; Volumes](level-3/03-external-locations-volumes.md)

*Volumes in depth*

| From | Path |
|---|---|
| Spark, SQL, pandas, `dbutils.fs`, shell, `%sh` | `/Volumes/<catalog>/<schema>/<volume>/<path>` |
| Apps and REST | Files API: `/api/2.0/fs/files/Volumes/...` |
| Cloud URI | Possible for external volumes with sufficient privileges; Unity Catalog privileges override those on the external location |

### [04 · Row Filters, Column Masks &amp; ABAC](level-3/04-row-filters-column-masks.md)

*Why not just use views?*

| Approach | Attaches to | Managed by | Best for |
|---|---|---|---|
| Row filter / column mask | One table or column | Table owner or `MANAGE` | Table-specific logic |
| ABAC policy | Tables/columns matched by **tags**, at catalog/schema/table (or metastore) scope | Whoever owns/`MANAGE`s the scope | One rule across many tables |
| Dynamic view | A view over base tables | The view's SQL | Curated or joined slices |

### [05 · Lineage, Audit &amp; System Tables](level-3/05-lineage-system-tables.md)

*The two lineage system tables*

| Table | One row per | Selected columns |
|---|---|---|
| `system.access.table_lineage` | Read or write event on a table or path | `source_table_full_name`, `target_table_full_name`, `source_type`, `target_type`, `entity_type` (e.g. `JOB`, `NOTEBOOK`, `PIPELINE`, `DASHBOARD_V3`), `entity_id`, `entity_run_id`, `created_by`, `event_time`, `event_date` |
| `system.access.column_lineage` | Column read/write | `source_column_name`, `target_column_name` plus table identifiers |

### [06 · Lakeflow Spark Declarative Pipelines](level-3/06-lakeflow-declarative-pipelines.md)

*Two dataset types*

| | Streaming table | Materialized view |
|---|---|---|
| Defined by | A query over a **stream** (append-only source) | A batch query over tables |
| Processing | Incremental: each row processed once | Refreshed; the engine may update it incrementally when it can, else recomputes |
| Typical layer | Bronze ingestion, append-style silver | Aggregated silver and gold, joins |
| Python | `@dp.table` returning a streaming DataFrame | `@dp.materialized_view` |
| SQL (Databricks) | `CREATE STREAMING TABLE ... AS SELECT ... FROM STREAM(...)` | `CREATE MATERIALIZED VIEW ... AS SELECT ...` |

### [07 · Lakeflow Jobs: Orchestrating Workloads](level-3/07-lakeflow-jobs.md)

*Run-if conditions*

| Condition | Runs when |
|---|---|
| All succeeded (default) | Every upstream task succeeded; otherwise the task is marked *Upstream failed* |
| At least one succeeded | Any upstream succeeded |
| None failed | No upstream failed, and at least one actually ran |
| All done | Upstream tasks have finished, whatever their result |

### [08 · OpenSharing (Formerly Delta Sharing): Sharing Data Across Organizations](level-3/08-opensharing.md)

*Concepts*

| Mode | Recipient | What you get |
|---|---|---|
| **Databricks-to-Databricks** | A Unity Catalog workspace | No tokens to manage; shares appear as a *catalog*; supports notebooks, volumes, models, views, plus governance, auditing and usage tracking on both sides |
| **Databricks-to-Open** | Any platform (Spark, pandas, Power BI, Iceberg clients...) | A credential file with a bearer token or OIDC federation; tabular data |
| **Self-hosted open-source server** | Anyone | You run the sharing server; the Databricks docs don't cover setting it up |

### [09 · Databricks SQL, Warehouses &amp; Dashboards](level-3/09-databricks-sql-warehouses.md)

*SQL warehouses*

| Type | Engine features (per docs) | Notes |
|---|---|---|
| **Serverless** | Photon, Predictive IO, Intelligent Workload Management | Starts in seconds (the docs say typically 2 to 6), rapid up- and down-scaling; compute runs in Databricks' account; the recommended default |
| **Pro** | Photon, Predictive IO | Classic-plane compute with more features than classic |
| **Classic** | Photon | Basic, runs in your cloud account |

### [10 · Project — Design a Governed Data Platform](level-3/10-project-governed-platform.md)

*The design being encoded*

| Schema | Content | Who may read | Who may write |
|---|---|---|---|
| `prod.bronze` | Raw data | Data engineers, the ETL service principal | The ETL service principal |
| `prod.silver` | Cleaned, contains PII | Data engineers only | Data engineers |
| `prod.gold` | Aggregates for the business | Analysts, data engineers | Data engineers |

## Level 4 · Development, ML & Operations

### [01 · Declarative Automation Bundles &amp; CI/CD](level-4/01-asset-bundles-cicd.md)

*The two deployment modes*

| | `mode: development` | `mode: production` |
|---|---|---|
| Resource names | Prefixed with `[dev <your short name>]` and tagged `dev`, so many developers can deploy the same bundle side by side | No prefix |
| Schedules and triggers | **Paused**, so a dev deploy never starts a nightly job | Active |
| Concurrent runs | Enabled for faster iteration | As configured |
| Pipelines | `development: true` | Validated as `development: false` |
| Git | Not checked | Current branch must match the target's `git.branch` (override with `--force`) |
| Identity | You | Databricks recommends a **service principal** via `run_as`; without it, `run_as` and `permissions` must be stated explicitly |
| Cluster override | `--cluster-id` can override job clusters | Not allowed |
| Deployment lock | Disabled for speed | Enabled |

### [03 · MLflow: Tracking Experiments](level-4/03-mlflow-tracking.md)

*What to log (and why)*

| Log | Why |
|---|---|
| Hyperparameters and the random seed | Reproduce the run |
| Training data identity: table name **and Delta version or snapshot timestamp** | "Same code, different data" explains most unreproducible results (use time travel, Level 1 · 08) |
| Code version (git SHA; Databricks records it for notebooks in Git folders) | Know which code produced the model |
| Metrics on a held-out set, and on slices that matter (per region) | A single AUC hides failures on sub-populations |
| Model signature and input example | Serving validates inputs against the signature |
| Plots and confusion matrices as artifacts | Faster review |

### [07 · Performance: Photon, Serverless &amp; Caching](level-4/07-performance-photon-serverless.md)

*Caching: three different things*

| Cache | What it caches | How you get it |
|---|---|---|
| **Disk cache** (formerly Delta cache / DBIO cache) | Parquet/Delta *files* on workers' local disks | Automatic on supported instance types; proprietary Databricks feature |
| **Spark cache** (`df.cache()`) | A DataFrame's *computed result* in memory/disk | You call it explicitly; per application |
| **Query/result cache** | The *result of a SQL query* | Automatic for SQL warehouses and dashboards; see the caching docs for its rules |

### [08 · Table Maintenance &amp; Predictive Optimization](level-4/08-table-maintenance.md)

*The four chores*

| Chore | What it fixes | Command |
|---|---|---|
| **Compaction / clustering** | Small files; poor data skipping | `OPTIMIZE` (Level 2 · 03) |
| **Cleanup** | Unreferenced files (storage cost) | `VACUUM` (Level 1 · 08) |
| **Statistics** | Stale table/column stats used by the optimizer | `ANALYZE TABLE ... COMPUTE STATISTICS` |
| **Purging deleted data** | Rows that are "deleted" but still physically present | `REORG TABLE ... APPLY (PURGE)` |

### [09 · Migrating to Databricks](level-4/09-migration.md)

*Moving metadata and permissions: Hive metastore to Unity Catalog*

| Approach | Use when |
|---|---|
| **`SYNC`** | Upgrade Hive **external** tables (and managed tables stored outside workspace/DBFS-root storage) to Unity Catalog external tables; can re-sync as the source changes. It cannot upgrade managed tables in workspace storage |
| **`CLONE` / CTAS** | Copy Hive managed tables into UC **managed** tables (needed for tables in DBFS root) |
| **UCX** (Databricks Labs, `databrickslabs/ucx`) | A toolkit that assesses readiness and migrates identities, permissions, storage locations and tables |
| **Upgrade wizards in the UI** | Small numbers of tables |
| **Hive metastore federation** | Gradual: mirror the Hive metastore as a *foreign catalog* governed by UC while you migrate |

### [10 · Capstone — An End-to-End Platform Plan](level-4/10-capstone-platform.md)

*What a good plan answers*

| Area | Questions the plan must answer | Lessons |
|---|---|---|
| Data | Which layers, tables and owners? Retention? Layout (clustering vs partitioning)? | L1 · 06 to 09, L2 · 03, 04, 09 |
| Pipelines | Trigger, compute type, idempotence, retries, SLA, alerting? | L2 · 07 to 10, L3 · 06, 07 |
| Governance | Catalog layout, groups, privileges, PII controls, erasure procedure? | L3 · 01 to 05, L4 · 08 |
| Serving | Warehouse type/size, dashboards, sharing? | L3 · 08, 09 |
| Cost | Tags, budgets, review cadence? | L4 · 06 |
| Delivery | IaC, tests, CI/CD, deploy identity, rollback? | L4 · 01, 02 |
| Operations | Maintenance, monitoring, disaster recovery? | L4 · 07, 08 |

---

Go deeper with the full [Databricks Mastery Path](index.md) curriculum or test yourself with the [interview questions](interview-questions.md).
