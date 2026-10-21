---
description: "What a lakehouse is, how Databricks splits control and compute planes, where data lives, and how accounts, workspaces and metastores relate."
---

# 01 · The Lakehouse: Control Plane, Compute Plane & Storage

Databricks sells a simple idea with a lot of machinery behind it: keep your data in open files in cheap cloud object
storage, and put a transactional layer, a catalog and several compute engines on top, so that the same copy of the data
serves SQL dashboards, data-engineering pipelines and machine learning. That combination is called a **lakehouse**,
because it tries to give a data lake's cost and openness the reliability and governance that used to be the warehouse's
selling point.

This lesson builds the mental map you'll use for the rest of the course: what sits in your cloud account, what sits in
Databricks', and which piece does what.

!!! note "About this course and what was run"
    Databricks has no free local edition, and this course was written **without access to a Databricks workspace**. So
    no output here is presented as coming from Databricks. What was run, and where:

    - **Apache Spark 4.2.0 and Delta Lake 4.4.1** (open-source builds, `pyspark` and `delta-spark` from PyPI) on a laptop
      with Python 3.12. Spark and Delta are the engine and table format underneath Databricks, so Spark code, Delta
      behaviour and the on-disk layout shown in Levels 1 and 2 are real observed output. Output blocks are labelled
      "local Spark" when they come from these runs.
    - Features that exist only on the platform (Unity Catalog governance, serverless compute, Photon, Auto Loader
      `cloudFiles`, Lakeflow pipelines, Jobs, SQL warehouses, MLflow serving, Genie) can't be run locally. Those lessons
      explain the documented behaviour, show configuration and code written against the documentation, and say plainly
      when nothing was executed. Docs were read on the Databricks documentation site in October 2026.
    - Product names, limits and defaults change quickly. Treat anything marked "as of October 2026" as something to
      re-check.

    A free Databricks account (the Free Edition) or a trial lets you run the platform-only lessons yourself.

## What "lakehouse" means in practice

Three older ideas are being merged:

| Idea | Strength | Weakness the lakehouse tries to fix |
|---|---|---|
| Data warehouse | ACID tables, fast SQL, governance | Proprietary storage, expensive for raw/unstructured data, awkward for ML |
| Data lake (files in object storage) | Cheap, open, any file type | No transactions, no schema enforcement, easy to corrupt, hard to govern |
| Spark/ML clusters | Flexible code, scales out | Needs reliable tables underneath |

The lakehouse answer is **open file formats plus a transaction log**. Data sits as Parquet files in object storage
(S3, ADLS Gen2 or Google Cloud Storage). A table format called **Delta Lake** adds a log that records which files make
up each version of a table, giving you transactions, schema checks and time travel (Lessons 06–08). A catalog, **Unity
Catalog**, adds names, permissions and lineage on top (Level 3).

## Accounts, workspaces and metastores

```text
Databricks account  (billing, identity, which regions, which metastores)
 ├── Unity Catalog metastore  (one per region)  ── catalogs ── schemas ── tables/volumes/models
 ├── Workspace "dev"   ─┐
 ├── Workspace "prod"  ─┼─ attached to the regional metastore, see the same governed data
 └── Workspace "ml"    ─┘
```

- The **account** is the top-level entity: users and groups, billing, and the list of workspaces.
- A **workspace** is the collaboration environment: notebooks, jobs, compute, dashboards. Companies commonly run
  several (dev/test/prod, or per business unit).
- A **metastore** is the Unity Catalog governance container. One metastore can be attached to many workspaces in the
  same region, so every workspace sees the same catalogs and the same permissions.

## The two planes

Databricks describes its architecture as a **control plane** and a **compute plane**.

```text
        ┌────────────── Databricks control plane ──────────────┐
        │ web UI · REST API · notebooks & job definitions ·    │
        │ cluster manager · Unity Catalog service · query UI   │
        └───────────────▲──────────────────────┬───────────────┘
                        │ commands/metadata    │ launch/stop
        ┌───────────────┴──────────────────────▼───────────────┐
        │                     compute plane                    │
        │  classic: VMs in YOUR cloud account (your VPC)       │
        │  serverless: capacity managed in Databricks' account │
        │       Spark driver + executors read/write data ──────┼──► object storage
        └──────────────────────────────────────────────────────┘     (S3 / ADLS / GCS)
```

- The **control plane** is the managed backend that Databricks runs in its own account: the web app, the REST API, job
  scheduling and the cluster manager.
- The **compute plane** is where data is actually processed. With **classic compute**, the virtual machines are launched
  in *your* cloud account, inside your own network. With **serverless compute**, Databricks runs and manages the
  capacity in its account within a network boundary for your workspace.
- Your **data** stays in object storage. Databricks does not need to ingest it into a proprietary store: tables are
  folders of Parquet files plus a `_delta_log` directory (you'll open one in Lesson 07).

This split has practical effects. Cloud networking, firewalls and IAM roles matter for classic compute because the VMs
are yours. Costs have two parts for classic compute: Databricks Units (DBUs) to Databricks and VM/storage charges to
your cloud provider (Level 4 · 06). And a notebook you open in the browser is not "running on your laptop" or "in the
browser": it's a client of a driver process in the compute plane.

## The runtime stack

Databricks compute runs the **Databricks Runtime (DBR)**: a versioned image that bundles Apache Spark, Delta Lake,
Python/Scala/R, common libraries, and Databricks-specific optimizations such as the Photon vectorized engine. A
cluster picks a runtime version when it's created, and runtimes are released in LTS (long-term support) and
non-LTS variants. Because the runtime is pinned per compute resource, upgrading a pipeline's Spark/Delta version is a
configuration change you can test, not a platform-wide event.

Open-source Spark and Delta are what you can run locally. DBR layers extras on top, so code that works locally
generally runs on Databricks, but not every Databricks-only behaviour (Photon, Predictive Optimization, some
Delta features gated by runtime) exists in the open-source builds.

## A first look: the same code, two places

A notebook cell in a workspace and a script on your laptop look identical:

```python
from pyspark.sql import functions as F

df = spark.createDataFrame(
    [(1, "alice", "DE", 120.5), (2, "bob", "US", 80.0), (3, "cara", "US", 45.25), (4, "dev", "IN", 300.0)],
    ["id", "name", "country", "amount"],
)
(df.filter(F.col("amount") > 60)
   .groupBy("country")
   .agg(F.sum("amount").alias("total"), F.count("*").alias("n"))
   .orderBy("country")
   .show())
```

In a Databricks notebook, `spark` already exists. Locally you build it:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").appName("lab").getOrCreate()
```

Output from the local run (Spark 4.2.0):

```text
+-------+-----+---+
|country|total|  n|
+-------+-----+---+
|     DE|120.5|  1|
|     IN|300.0|  1|
|     US| 80.0|  1|
+-------+-----+---+
```

The only difference is where the driver and executors live: one JVM on your laptop versus VMs managed by the platform.

## How It Actually Works

When you press Run on a cell, the browser talks to the **control plane**, which routes your command to the **driver** of
the attached compute resource. The driver is a JVM (plus a Python process for PySpark) that holds your `SparkSession`.
It turns your DataFrame operations into a plan, splits the plan into tasks, and hands tasks to **executors**: worker
processes on the other VMs. Each executor reads its slice of Parquet files directly from object storage using cloud
credentials attached to the compute resource, and processes it in memory. Results come back to the driver, which
returns the displayed rows to the control plane and then your browser.

Two consequences follow. First, **data does not flow through the control plane**; only commands, metadata and small
result sets do. Second, **compute and storage are decoupled**: you can stop every cluster and the data and tables are
untouched in object storage, and you can point many different clusters, jobs and SQL warehouses at the same tables at
once. Everything else in the course (Delta's log, Unity Catalog, serverless) elaborates one of those two facts.

## Common mistakes

- **Treating the lakehouse as "a warehouse with a different name".** Tables are files you own in your storage; deleting
  the files, or writing to the same folder from a tool that ignores the Delta log, can corrupt a table.
- **Mixing up workspace and metastore.** Permissions on tables live in the metastore and apply in every attached
  workspace; notebooks and job definitions live in one workspace.
- **Assuming classic and serverless behave the same.** Serverless removes cluster configuration but has its own limits
  on libraries, languages and settings. Check the documentation for your feature.
- **Expecting the cloud bill to be one number.** On classic compute you pay Databricks (DBUs) and your cloud provider
  separately.
- **Believing local Spark proves performance.** A two-core laptop run proves logic, not scale or speed.

## Exercise

1. Draw your own version of the two-plane diagram for a company with a dev and a prod workspace in one region. Mark
   where the metastore, the object storage buckets and the classic compute VMs sit.
2. Install the local stack (`python3.12 -m venv .venv && .venv/bin/pip install pyspark delta-spark`), run the example
   above, and confirm you get the same table.
3. Explain in two sentences why stopping all clusters does not delete any table. (Hint: where do the files live?)
4. In the [Databricks architecture documentation](https://docs.databricks.com/aws/en/getting-started/high-level-architecture),
   find what is stored in the *workspace storage bucket* and say which of those items is not your table data.
