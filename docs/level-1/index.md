---
description: "Databricks Level 1: the lakehouse, compute and notebooks, Spark DataFrames and SQL, files, Delta tables, the Delta log, time travel and a local project."
---

# Level 1 · Lakehouse & Spark Foundations

Level 1 takes you from "Databricks is a Spark platform" to building and recovering a small Delta lakehouse. Most of
it runs on your own laptop with open-source Spark and Delta Lake, which means the code is real and the outputs in
these lessons are observed, not paraphrased. Along the way it shows the quiet failures that cost people days:
`inferSchema` turning a numeric column into strings, a `CASE` expression labelling missing amounts "small", a window
function that returns different running totals for tied rows, and a `VACUUM` that makes a rollback impossible.

!!! note "What was run, and what wasn't"
    This course was written without access to a Databricks workspace. Spark 4.2.0 and Delta Lake 4.4.1 (open source,
    from PyPI) were run locally for everything that doesn't need the platform; those outputs are labelled "local".
    Platform-only features (Unity Catalog, serverless compute, SQL warehouses, Auto Loader, Lakeflow, MLflow serving)
    are described from the Databricks documentation as of October 2026 and are marked as not run. A Databricks Free
    Edition workspace is enough to try the platform-only exercises.

**Prerequisites:** basic Python and SQL. Java 17+ and Python 3.12 to run the local labs. If SQL is new, start with the
[SQL Mastery Path](https://sigilipelli.github.io/sql-mastery-path/); for Spark in more depth see the
[PySpark Mastery Path](https://sigilipelli.github.io/pyspark-mastery-path/).

## Modules

1. **[The Lakehouse: Control Plane, Compute Plane & Storage](01-lakehouse-architecture.md)**: what a lakehouse is, the
   two planes, accounts, workspaces and metastores.
2. **[Workspaces, Compute Types & Notebooks](02-workspace-compute.md)**: serverless vs classic vs SQL warehouses,
   notebooks, `dbutils`, the CLI.
3. **[Spark DataFrames: Transformations & Actions](03-spark-dataframes.md)**: lazy plans, `explain`, null semantics, joins.
4. **[Spark SQL, Built-in Functions & Window Functions](04-spark-sql-functions.md)**: `GROUP BY ALL`, windows and ties,
   `QUALIFY`, ANSI mode.
5. **[Reading & Writing Files](05-reading-writing-data.md)**: CSV/JSON/Parquet, parse modes, partition pruning, save modes.
6. **[Delta Lake Tables](06-delta-lake-basics.md)**: create, update, delete, inspect, managed vs external.
7. **[Inside the Delta Log](07-delta-log-acid.md)**: actions, statistics, checkpoints, a real write conflict.
8. **[Time Travel, RESTORE & VACUUM](08-time-travel-restore-vacuum.md)**: what you can recover and when it's gone.
9. **[Tables, Views & Volumes](09-catalog-tables-volumes.md)**: the object model and where files belong.
10. **[Project: A Sales Lakehouse You Can Run Locally](10-project-local-sales-lakehouse.md)**: bronze/silver/gold with
    a quarantine table and a rollback.

## After Level 1

Level 2 makes tables fast and pipelines continuous: `MERGE`, schema enforcement, `OPTIMIZE` and clustering, how Spark
executes queries, joins and skew, Structured Streaming, Auto Loader and the medallion architecture.
