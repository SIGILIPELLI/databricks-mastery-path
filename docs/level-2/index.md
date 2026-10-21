---
description: "Databricks Level 2: MERGE and change feed, schema rules, OPTIMIZE and clustering, partitioning, Spark execution, streaming, Auto Loader, a pipeline."
---

# Level 2 · Delta Lake & Data Engineering

Level 2 is about making tables correct, fast and continuously updated. It leans on measurements you can repeat on a laptop:
a partition key that turned 2 MB of data into 86,665 files, a Z-order that cut the files read for a filter from 16 to 2,
a hot key that put 329,083 rows into one shuffle partition, a deleted checkpoint that duplicated every row, and an
`ALTER TABLE ... CLUSTER BY` that silently rewrote nothing.

!!! note "What was run, and what wasn't"
    As in Level 1, Spark 4.2.0 and Delta Lake 4.4.1 (open source) were run locally and those outputs are labelled.
    Auto Loader, Photon, Predictive Optimization and other platform-only features are described from the Databricks
    documentation (October 2026) and are marked as not run. Where a local experiment gave a negative or surprising
    result (for example AQE skew handling not triggering on a tiny dataset), the lesson says so.

**Prerequisites:** Level 1, especially Delta tables, the Delta log and time travel.

## Modules

1. **[MERGE, Upserts & the Change Data Feed](01-merge-upserts-cdf.md)**: upserts, the duplicate-source rule, SCD Type 2,
   row-level change events.
2. **[Schema Enforcement, Evolution & Constraints](02-schema-enforcement-evolution.md)**: what Delta rejects, how to evolve,
   the `INSERT *` trap, column mapping, `CHECK`.
3. **[OPTIMIZE, Z-Order & Liquid Clustering](03-optimize-zorder-clustering.md)**: compaction vs clustering, with measured
   data skipping.
4. **[Partitioning, File Sizes & Data Skipping](04-partitioning-file-layout.md)**: when not to partition, and which columns get
   statistics.
5. **[How Spark Executes: Stages, Shuffles & AQE](05-spark-execution-aqe.md)**: reading plans, join strategies, adaptive execution.
6. **[Joins, Broadcast & Data Skew](06-joins-skew-broadcast.md)**: hints, detecting skew, salting.
7. **[Structured Streaming & Checkpoints](07-structured-streaming.md)**: triggers, exactly-once, watermarks, `foreachBatch`.
8. **[Auto Loader: Incremental File Ingestion](08-auto-loader.md)**: `cloudFiles`, detection modes, schema evolution.
9. **[The Medallion Architecture](09-medallion-architecture.md)**: what each layer guarantees.
10. **[Project: A Bronze/Silver/Gold Pipeline](10-project-medallion-pipeline.md)**: incremental, idempotent, reconciled.

## After Level 2

Level 3 moves from "can I build the pipeline" to "can the organization trust and operate it": Unity Catalog governance,
privileges, storage credentials and volumes, row filters and masks, lineage and system tables, Lakeflow declarative
pipelines and jobs, Delta Sharing and SQL warehouses.
