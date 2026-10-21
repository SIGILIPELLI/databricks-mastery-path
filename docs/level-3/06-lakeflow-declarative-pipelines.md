---
description: "Declarative pipelines: streaming tables vs materialized views, SQL and Python, expectations, AUTO CDC, run locally with open-source Spark 4.2."
---

# 06 · Lakeflow Spark Declarative Pipelines

In Level 2 you wrote a bronze/silver/gold pipeline by hand: streams, checkpoints, `foreachBatch`, `MERGE`, ordering and
retries. **Declarative pipelines** invert that. You declare *what each dataset is* (a query), and the framework works out the
dependency graph, execution order, incremental processing, checkpoints and retries. On Databricks this is **Lakeflow Spark
Declarative Pipelines** (the successor to Delta Live Tables), built on the Apache Spark Declarative Pipelines framework.

!!! note "What was run, and what wasn't"
    PySpark 4.2.0 includes the **open-source Spark Declarative Pipelines** framework (`pyspark.pipelines` and the
    `spark-pipelines` command), so the first half of this lesson was **actually executed locally**: the three-file pipeline
    below, two refreshes, and the table contents shown. The open-source runtime has real limits compared with
    Databricks, discovered while running it: its tables are plain **Parquet**, not Delta; it exports **no expectation
    decorators** (`dir(pyspark.pipelines)` lists `table`, `materialized_view`, `temporary_view`, `append_flow`,
    `create_streaming_table`, `create_auto_cdc_flow`, `create_sink` and a few others, but nothing named `expect`); and it needs a
    persistent catalog to re-run (see below). Everything about expectations, the pipeline UI, the event log, serverless
    compute, `AUTO CDC` behaviour and pipeline modes is from the Databricks documentation (October 2026) and **was not run**.

## Two dataset types

| | Streaming table | Materialized view |
|---|---|---|
| Defined by | A query over a **stream** (append-only source) | A batch query over tables |
| Processing | Incremental: each row processed once | Refreshed; the engine may update it incrementally when it can, else recomputes |
| Typical layer | Bronze ingestion, append-style silver | Aggregated silver and gold, joins |
| Python | `@dp.table` returning a streaming DataFrame | `@dp.materialized_view` |
| SQL (Databricks) | `CREATE STREAMING TABLE ... AS SELECT ... FROM STREAM(...)` | `CREATE MATERIALIZED VIEW ... AS SELECT ...` |

Plus **views** (`@dp.temporary_view`, `CREATE TEMPORARY VIEW`) that exist only for the pipeline run, and **flows** (append,
update, AUTO CDC) that feed a table from one or many sources. Pipeline mode is independent of table type: **triggered**
mode refreshes available data and stops (cheap; fine for 10-minute-to-daily freshness), while **continuous** keeps tables
fresh (an always-on cluster; for seconds-to-minutes freshness), per the documentation.

## A runnable pipeline (open-source Spark 4.2)

Project layout, created with `spark-pipelines init --name orders_pipe`, then edited:

```text
orders_pipe/
├── spark-pipeline.yml
├── transformations/
│   ├── bronze.py
│   ├── silver.sql
│   └── gold.py
└── landing/            # JSON files arrive here
```

`spark-pipeline.yml`:

```yaml
name: orders_pipe
storage: file:///path/to/orders_pipe/pipeline-storage      # checkpoints live here
libraries:
  - glob:
      include: transformations/**
configuration:
  spark.sql.shuffle.partitions: "4"
```

`transformations/bronze.py` (a streaming table: it returns a streaming DataFrame):

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = "file:///path/to/orders_pipe/landing"

@dp.table(comment="Raw order events, appended incrementally from JSON files")
def bronze_orders():
    return (spark.readStream.schema("order_id STRING, customer STRING, amount STRING, status STRING").json(LANDING)
            .withColumn("_ingested_at", F.current_timestamp()))
```

`transformations/silver.sql` (SQL and Python files mix freely in one pipeline):

```sql
CREATE MATERIALIZED VIEW silver_orders AS
SELECT CAST(order_id AS INT) AS order_id, customer, CAST(amount AS DECIMAL(10,2)) AS amount, status
FROM bronze_orders
WHERE try_cast(order_id AS INT) IS NOT NULL AND try_cast(amount AS DECIMAL(10,2)) >= 0;
```

`transformations/gold.py`:

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(comment="Revenue per customer")
def gold_revenue():
    return (spark.read.table("silver_orders").where("status = 'ok'")
            .groupBy("customer").agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders")))
```

Notice what's absent: no checkpoint paths, no `trigger`, no `writeStream`, no ordering. `silver_orders` reads
`bronze_orders` by name, and `gold_revenue` reads `silver_orders`; that's the whole dependency graph.

The first file `a.json` held five events, including a non-numeric id (`x`) and a negative amount. Running the pipeline from
the project folder:

```bash
export SPARK_HOME=$(python -c "import pyspark,os;print(os.path.dirname(pyspark.__file__))")
spark-pipelines --conf spark.sql.catalogImplementation=hive run
```

the log showed each flow queued, then `bronze_orders`, `silver_orders`, `gold_revenue` run in dependency order, each
`COMPLETED`, and `Run is COMPLETED`. Reading the output tables:

```text
bronze_orders (5)   1 ana 10.50 ok | 2 bo 20.00 ok | 3 ana -4 ok | 4 ana 7.25 refund | x cy 5 ok
silver_orders (3)   1 ana 10.50 ok | 2 bo 20.00 ok | 4 ana 7.25 refund        <- 'x' and '-4' filtered out
gold_revenue  (2)   ana 10.50 (1 order) | bo 20.00 (1 order)
```

Then I added a second file `b.json` (orders 5 and 6) and ran again:

```text
bronze_orders (7)   the five previous rows + 5 bo 4.00 ok + 6 dee 30.00 ok     <- only new rows were added
silver_orders (5)   1 ana | 2 bo | 4 ana | 5 bo | 6 dee
gold_revenue  (3)   ana 10.50 (1) | bo 24.00 (2) | dee 30.00 (1)
```

The streaming table ingested only the new file (its checkpoint in `pipeline-storage/_checkpoints` remembered the first), while the
materialized views were recomputed from their inputs. The numbers reconcile: bo's `24.00` is `20.00 + 4.00`.

**Things I hit running it** (worth knowing before you try):

- Without the extra `--conf spark.sql.catalogImplementation=hive`, the first run worked but the **second run failed** with
  `[LOCATION_ALREADY_EXISTS] Cannot name the managed table ... as its associated location ... already exists`: the default
  in-memory catalog forgets the tables between runs while their folders remain. Setting the option in `spark-pipeline.yml`
  didn't work either (`CANNOT_MODIFY_STATIC_CONFIG`), because it must be set when the server starts; passing `--conf`
  to `spark-pipelines` did. This is a local-development quirk, not how Databricks behaves.
- A `catalog:`/`database:` key in the YAML fails with `SCHEMA_NOT_FOUND` unless that schema already exists.
- The local tables are Parquet. On Databricks they are Delta tables governed by Unity Catalog.

## What Databricks adds (documented, not run)

### Expectations: data quality as declarations

```python
from pyspark import pipelines as dp

@dp.table
@dp.expect("valid_id", "order_id IS NOT NULL")                   # keep row, record the violation
@dp.expect_or_drop("non_negative_amount", "amount >= 0")         # drop violating rows
@dp.expect_or_fail("known_status", "status IN ('ok','refund')")  # fail the update
def silver_orders():
    return spark.readStream.table("bronze_orders")
```

The decorator names above are from the Databricks Python reference. In SQL the equivalent is a `CONSTRAINT name EXPECT (condition)
[ON VIOLATION DROP ROW | FAIL UPDATE]` clause. Per the documentation, each expectation's counts of valid and invalid
records are logged with the dataset's metrics, `expect_or_fail` stops a single flow without failing other flows, and the
`expect_all*` variants take a dictionary. Compare with Level 2 · 10, where quarantine tables required hand-written code:
expectations count and drop but do **not** quarantine; to keep failing rows you still write them to a separate table.

### AUTO CDC

The AUTO CDC APIs (successors with the same syntax to `APPLY CHANGES`) compute SCD Type 1 or Type 2 targets from a change feed or
from snapshots, handling ordering by a sequence column, deletes and out-of-order events. In Python this is
`dp.create_auto_cdc_flow(...)`, and the open-source package exports `create_auto_cdc_flow` too (I didn't run it). It replaces the
hand-built SCD2 `MERGE` from Level 2 · 01.

### Operations

- **Compute**: serverless or classic; the pipeline owns the cluster, with autoscaling.
- **Updates**: a normal update refreshes; `--full-refresh` (open-source CLI) or a full refresh in the UI resets selected datasets
  and recomputes from scratch. A full refresh of a streaming table reprocesses all source data and, for sources that
  have deleted history, can lose data; check the docs before running one.
- **Monitoring**: a pipeline event log (also queryable) records flow progress, expectation metrics and lineage; the UI shows the
  graph and each flow's status. Pipelines run as tasks inside Lakeflow Jobs (next lesson).
- **Materialized views** refresh incrementally when the engine can compute the change, with a documented set of supported
  operations; otherwise they recompute. Look at the refresh type in the event log to see what happened.

## When to use declarative pipelines

Good fit: standard ingest-clean-aggregate flows, CDC, anything where retries, checkpoint plumbing and ordering are your main
source of bugs. Poorer fit: highly custom per-batch logic (`foreachBatch` with external side effects), operations the framework
doesn't model, or teams that need to run the same code on a cluster without the framework. The code is portable in
spirit only: open-source Spark pipelines and Lakeflow share the programming model, but features differ, as shown above.

## How It Actually Works

When you run a pipeline, the framework first **registers** every dataset definition (executing your Python files and
parsing SQL, but without running the queries) to build a graph: each dataset is a node, and each `read.table("x")` or `FROM x` is
an edge. It validates the graph (no cycles, all references resolve), then executes flows in topological order, running
independent branches in parallel. For a streaming table, it runs a Structured Streaming query with a framework-managed
checkpoint (Level 2 · 07); for a materialized view, it runs the batch query and replaces or incrementally updates the
stored result.

Because the framework owns the checkpoints and the graph, it can retry a failed flow, rerun only downstream datasets, and
reset a single dataset (`--full-refresh`) consistently. That's what you paid for with all the hand-written code in Level 2.

## Common mistakes

- **Putting side effects in dataset functions** (writing files, calling APIs): they may run during planning or more than once.
- **Using a streaming source in a materialized view**, or a batch read in a streaming table by accident.
- **Full-refreshing** a streaming table whose source no longer holds all the history.
- **Expecting expectations to quarantine** rows (they drop, keep or fail).
- **Running continuous mode for hourly data**, paying for an idle cluster.
- **Testing only on Databricks**: you can unit-test transformation logic locally (Level 4 · 02) and keep the pipeline files thin.

## Exercise

1. Reproduce the local pipeline. Add `c.json` with a duplicate of order 5 and see what each table contains afterward.
   (Predict first: which layer deduplicates, if any?)
2. Add a fourth dataset `gold_status_counts` as a SQL materialized view and rerun. Does the log show it running in parallel with
   `gold_revenue`?
3. Convert `silver_orders` to use the Databricks expectation decorators (don't run), and decide which rule should drop,
   which should fail.
4. Run `spark-pipelines run --full-refresh bronze_orders` locally. What happens to `bronze_orders` when the landing folder still has
   both files? What would happen on a real source that had expired old files?
