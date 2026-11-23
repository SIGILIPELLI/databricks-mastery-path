---
title: "Databricks Interview Questions and Answers (179 Questions)"
description: "179 Databricks interview questions with answers and explanations, from beginner to advanced, organised by level. Free, from the Databricks Mastery Path."
---

# Databricks Interview Questions and Answers

**179 questions** with the correct answer and a short explanation, organised from beginner (Level 1) to advanced (Level 4). Every question comes from a lesson in the free Databricks Mastery Path, so you can open the lesson to go deeper.

!!! tip "Practise, don't just read"
    Each lesson also ends with an interactive quiz, and the [final quiz](quiz.md) tests all four levels. Try to answer each question below before reading the answer.

## Level 1 · Lakehouse & Spark Foundations

### [01 · The Lakehouse: Control Plane, Compute Plane &amp; Storage](level-1/01-lakehouse-architecture.md)

**Q1.** What is a lakehouse?

**Answer:** Open files in cheap cloud storage with a transactional layer, catalog and several engines on top, serving SQL, ETL and ML from one copy

Lake cost and openness with warehouse reliability.

**Q2.** What gives Delta tables ACID transactions on object storage?

**Answer:** The transaction log (_delta_log) recording each commit

The log is the table.

**Q3.** Why keep raw data in open formats like Parquet/Delta?

**Answer:** Avoids lock-in and lets many engines read the same data

Openness is the pitch.

**Q4.** What separates storage from compute in a lakehouse?

**Answer:** Data lives in object storage while clusters and warehouses scale independently

Elastic compute.

### [02 · Workspaces, Compute Types &amp; Notebooks](level-1/02-workspace-compute.md)

**Q5.** Which compute fits scheduled production jobs?

**Answer:** Job clusters or serverless jobs compute, created per run and terminated after

Cheaper and isolated.

**Q6.** Why is an all-purpose cluster costly for scheduled jobs?

**Answer:** It's meant for interactive work and keeps running (and billing) unless auto-terminated

Choose the right compute type.

**Q7.** What does auto-termination do?

**Answer:** Shuts down idle compute to stop charges

Set a short idle time.

**Q8.** What can notebooks mix?

**Answer:** Python, SQL, Scala and R using magic commands like %sql

One notebook, multiple languages.

### [03 · Spark DataFrames: Transformations &amp; Actions](level-1/03-spark-dataframes.md)

**Q9.** What is lazy evaluation in Spark?

**Answer:** Transformations build a plan and nothing runs until an action like count() or write

The optimizer sees the whole plan.

**Q10.** Which is an action?

**Answer:** count(), collect() or write

Transformations are lazy.

**Q11.** What does `df.explain()` show?

**Answer:** The physical plan Spark will execute

Find shuffles (Exchange).

**Q12.** Why avoid `collect()` on large data?

**Answer:** It pulls everything to the driver and can run it out of memory

Aggregate first.

### [04 · Spark SQL, Built-in Functions &amp; Window Functions](level-1/04-spark-sql-functions.md)

**Q13.** Why can SQL produce 'wrong-but-plausible' numbers?

**Answer:** Traps like NULL handling, join row duplication and implicit casts

Check row counts after joins.

**Q14.** What does COUNT(col) ignore?

**Answer:** NULL values in col (COUNT(*) counts rows)

A classic trap.

**Q15.** What's a safe way to avoid division by zero?

**Answer:** try_divide or NULLIF on the denominator

Avoid errors or infinities.

**Q16.** What does a window function with PARTITION BY do?

**Answer:** Computes over related rows without collapsing them

Ranking and running totals.

### [05 · Reading &amp; Writing Files: Formats, Schemas &amp; Modes](level-1/05-reading-writing-data.md)

**Q17.** Why define an explicit schema when reading CSV?

**Answer:** Inference can guess wrong types and costs a pass over the data

Fail loudly.

**Q18.** What does PERMISSIVE parse mode do with bad rows?

**Answer:** Keeps them with nulls (and optionally a corrupt-record column)

Silent nulls are risky.

**Q19.** What does FAILFAST do?

**Answer:** Errors out on the first malformed record

Good for strict pipelines.

**Q20.** Which format preserves types and compresses well?

**Answer:** Parquet (or Delta)

Typed columnar.

### [06 · Delta Lake Tables: Create, Read, Update, Delete](level-1/06-delta-lake-basics.md)

**Q21.** What is a Delta table physically?

**Answer:** Parquet data files plus a _delta_log directory of JSON commits

Open and inspectable.

**Q22.** What does UPDATE do to Delta files?

**Answer:** Writes new files and marks old ones removed in the log; files aren't edited in place

Copy-on-write.

**Q23.** Which SQL commands does Delta support beyond INSERT?

**Answer:** UPDATE, DELETE and MERGE

Transactional DML.

**Q24.** What does DESCRIBE HISTORY show?

**Answer:** The table's commit history (operations, versions, timestamps)

Audit.

### [07 · Inside the Delta Log: Transactions &amp; ACID](level-1/07-delta-log-acid.md)

**Q25.** What does the _delta_log contain?

**Answer:** Ordered JSON commit files (and periodic checkpoints) listing added and removed data files

The log is the table.

**Q26.** How does Delta handle concurrent writers?

**Answer:** Optimistic concurrency: a conflicting commit fails and must retry

ACID on object storage.

**Q27.** What's a checkpoint in Delta?

**Answer:** A Parquet snapshot of the log state that speeds up reading it

Every N commits.

**Q28.** Why do data files only matter 'as far as the log mentions them'?

**Answer:** Files not referenced by the current snapshot are invisible garbage

VACUUM removes them.

### [08 · Time Travel, RESTORE &amp; VACUUM](level-1/08-time-travel-restore-vacuum.md)

**Q29.** What does time travel let you do?

**Answer:** Query a table as of a previous version or timestamp

VERSION AS OF.

**Q30.** What does RESTORE do?

**Answer:** Rolls a table back to an earlier version (as a new commit)

Needs the files to still exist.

**Q31.** What does VACUUM do and what's the default retention?

**Answer:** Deletes unreferenced files older than the retention threshold (7 days by default)

After it, old versions can't be read.

**Q32.** Why is a very short VACUUM retention dangerous?

**Answer:** Running readers or time travel may need the removed files

Keep safe retention.

### [09 · Tables, Views &amp; Volumes: Where Data Lives](level-1/09-catalog-tables-volumes.md)

**Q33.** What is Unity Catalog's three-level namespace?

**Answer:** catalog.schema.table

Plus volumes and functions.

**Q34.** What does DROP TABLE do for a managed table?

**Answer:** Removes the metadata and the underlying data files

External tables keep data.

**Q35.** What is a volume for?

**Answer:** Governed storage for non-tabular files

Files under UC governance.

**Q36.** Why avoid legacy DBFS mounts for new work?

**Answer:** Governance is weaker; use Unity Catalog volumes and external locations

Migrate.

### [10 · Project — A Sales Lakehouse You Can Run Locally](level-1/10-project-local-sales-lakehouse.md)

**Q37.** What does the local lakehouse project do with bad rows?

**Answer:** Quarantines them and cleans the rest into a typed Delta table

Reject and report.

**Q38.** How does the project recover from a bad write?

**Answer:** Delta time travel / RESTORE

Versioned tables.

**Q39.** Why use explicit schemas in the project?

**Answer:** Predictable types and loud failures

Contracts.

**Q40.** What runs in Level 1 projects?

**Answer:** A local Spark session against laptop files

Verified locally.

## Level 2 · Delta Lake & Data Engineering

### [01 · MERGE, Upserts &amp; the Change Data Feed](level-2/01-merge-upserts-cdf.md)

**Q41.** What does MERGE INTO do?

**Answer:** Applies inserts, updates and deletes from a source to a target in one atomic transaction

WHEN MATCHED / NOT MATCHED.

**Q42.** What's a pitfall of MERGE sources?

**Answer:** Multiple source rows matching one target row causes an error or ambiguity; deduplicate first

Make keys unique.

**Q43.** What does Change Data Feed (CDF) record?

**Answer:** Row-level inserts, updates (pre/post images) and deletes per commit

Downstream tables follow changes.

**Q44.** How do you enable CDF?

**Answer:** Set delta.enableChangeDataFeed = true on the table

Applies to later commits.

### [02 · Schema Enforcement, Evolution &amp; Constraints](level-2/02-schema-enforcement-evolution.md)

**Q45.** What does schema enforcement do on write?

**Answer:** Rejects data that doesn't match the table's schema or constraints

Whole commit fails.

**Q46.** How do you add new columns deliberately?

**Answer:** Schema evolution (mergeSchema or ALTER TABLE ADD COLUMNS)

Explicit.

**Q47.** What can silently lose data around schema?

**Answer:** Casting or overwriting with the wrong schema

Be explicit.

**Q48.** What do CHECK constraints do?

**Answer:** Reject writes violating a boolean condition

Data quality at the table.

### [03 · OPTIMIZE, Z-Order &amp; Liquid Clustering](level-2/03-optimize-zorder-clustering.md)

**Q49.** What does OPTIMIZE do?

**Answer:** Compacts many small files into larger ones

Better scan performance.

**Q50.** What does ZORDER BY (or clustering) improve?

**Answer:** Data skipping by co-locating related values in files

Filter on those columns.

**Q51.** What's liquid clustering?

**Answer:** A flexible, incremental clustering method replacing partitioning/ZORDER

CLUSTER BY.

**Q52.** Why do many tiny commits hurt?

**Answer:** Small files ruin data skipping and add metadata overhead

Compact.

### [04 · Partitioning, File Sizes &amp; Data Skipping](level-2/04-partitioning-file-layout.md)

**Q53.** When does partitioning help?

**Answer:** Low-cardinality columns that queries filter on, with large partitions

Avoid over-partitioning.

**Q54.** What's the 'small files' problem?

**Answer:** Too many tiny files slow reads and writes

Partition less, optimize.

**Q55.** Which columns get data-skipping stats by default?

**Answer:** The first 32 columns

Order columns wisely.

**Q56.** What does partition pruning do?

**Answer:** Skips partitions that can't match the filter

Needs filter on the partition column.

### [05 · How Spark Executes: Stages, Shuffles &amp; AQE](level-2/05-spark-execution-aqe.md)

**Q57.** In Spark UI vocabulary, what is a stage boundary caused by?

**Answer:** A shuffle (wide transformation)

Exchange in plans.

**Q58.** What does AQE (adaptive query execution) do?

**Answer:** Re-optimizes the plan at runtime using real statistics (e.g. coalescing partitions, switching joins)

Enabled by default.

**Q59.** What's a task?

**Answer:** The unit of work on one partition

Parallelism = partitions.

**Q60.** Why do shuffles matter?

**Answer:** They move data across the network and dominate cost

Minimize them.

### [06 · Joins, Broadcast &amp; Data Skew](level-2/06-joins-skew-broadcast.md)

**Q61.** What causes data skew in a join?

**Answer:** One key having far more rows than others, overloading one task

Salting, AQE skew join.

**Q62.** When use a broadcast join?

**Answer:** When one side is small enough to send to every executor

Avoids shuffle.

**Q63.** How can you spot a skewed task in Spark UI?

**Answer:** One task much slower/larger than the median

Look at task metrics.

**Q64.** What does a join on a mismatched key type risk?

**Answer:** Implicit casts causing no matches or slow plans

Cast deliberately.

### [07 · Structured Streaming &amp; Checkpoints](level-2/07-structured-streaming.md)

**Q65.** How does Structured Streaming treat a stream?

**Answer:** As an unbounded table processed incrementally

Same DataFrame API.

**Q66.** What is a checkpoint location for?

**Answer:** Storing progress and state so the query restarts without loss or duplication

Required for fault tolerance.

**Q67.** What is a trigger like availableNow?

**Answer:** Process all available data then stop (incremental batch)

Cost-efficient.

**Q68.** What does a watermark do?

**Answer:** Bounds how late data is accepted for stateful windows

Limits state.

### [08 · Auto Loader: Incremental File Ingestion](level-2/08-auto-loader.md)

**Q69.** What does Auto Loader provide?

**Answer:** Incremental, exactly-once file ingestion with schema inference and evolution

cloudFiles source.

**Q70.** Why is directory listing of a million files a problem?

**Answer:** It's slow and costly; Auto Loader can use file notification mode

Scalable discovery.

**Q71.** What does schema evolution mode 'rescue' do?

**Answer:** Captures unexpected columns in a _rescued_data column

No silent loss.

**Q72.** Where does Auto Loader track processed files?

**Answer:** In its checkpoint (RocksDB state)

Exactly once.

### [09 · The Medallion Architecture](level-2/09-medallion-architecture.md)

**Q73.** What are the medallion layers?

**Answer:** Bronze (raw), silver (cleaned/conformed), gold (business-ready)

Progressive quality.

**Q74.** Why never overwrite bronze?

**Answer:** It's the replayable source of truth

Reprocess from it.

**Q75.** Where should validation live?

**Answer:** In one place, typically the bronze-to-silver step

Single responsibility.

**Q76.** What should gold tables be?

**Answer:** Stable, trustworthy, business-aligned aggregates

Consumers rely on them.

### [10 · Project — A Bronze/Silver/Gold Pipeline](level-2/10-project-medallion-pipeline.md)

**Q77.** What does the medallion project show?

**Answer:** End-to-end bronze, silver and gold with quality checks

A full pipeline.

**Q78.** How do you make reruns safe?

**Answer:** Idempotent writes via MERGE or partition overwrite

No duplicates.

**Q79.** What do you do with invalid records?

**Answer:** Quarantine them with reasons

Audit trail.

**Q80.** How do you verify the pipeline?

**Answer:** Row counts and expectations between layers

Reconcile.

## Level 3 · Unity Catalog, Pipelines & Sharing

### [01 · Unity Catalog: Metastore, Catalogs &amp; Three-Level Names](level-3/01-unity-catalog-model.md)

**Q81.** What does Unity Catalog centralize?

**Answer:** Governance: permissions, lineage, audit and discovery across workspaces

One metastore per region.

**Q82.** What are the UC securable objects hierarchy?

**Answer:** Metastore, catalog, schema, then tables/views/volumes/functions

Privileges inherit down.

**Q83.** Why use groups for grants?

**Answer:** Easier management than granting to individuals

Least privilege.

**Q84.** What does the metastore bind to?

**Answer:** A region and multiple workspaces

Shared governance.

### [02 · Privileges, Ownership &amp; Grants](level-3/02-privileges-grants.md)

**Q85.** What privilege is needed to read a table?

**Answer:** SELECT on the table, USE SCHEMA and USE CATALOG on parents

Parents need USE.

**Q86.** What does GRANT do?

**Answer:** Gives a principal a privilege on a securable

REVOKE removes.

**Q87.** What's the best practice for ownership?

**Answer:** Owned by a group/service principal, not a person

Avoid orphaned ownership.

**Q88.** What does `ALL PRIVILEGES` risk?

**Answer:** Over-permissioning beyond least privilege

Grant narrowly.

### [03 · Storage Credentials, External Locations &amp; Volumes](level-3/03-external-locations-volumes.md)

**Q89.** What is an external location?

**Answer:** A UC object tying a cloud storage path to a storage credential

Governed access.

**Q90.** Managed vs external tables: what happens on DROP?

**Answer:** Managed drops data; external keeps the files

Choose deliberately.

**Q91.** What do volumes govern?

**Answer:** Access to arbitrary files like images and CSVs

Path /Volumes/catalog/schema/vol.

**Q92.** Why not grant raw cloud storage access to users?

**Answer:** It bypasses Unity Catalog governance

Use UC objects.

### [04 · Row Filters, Column Masks &amp; ABAC](level-3/04-row-filters-column-masks.md)

**Q93.** What does a row filter do?

**Answer:** Restricts which rows a user can see based on a function

Policy attached to the table.

**Q94.** What does a column mask do?

**Answer:** Transforms a column's values for users lacking permission

E.g. show last 4 digits.

**Q95.** Why use these over copies of data?

**Answer:** One governed copy with dynamic policies

Less drift.

**Q96.** What function checks group membership in a policy?

**Answer:** is_account_group_member()

Used in UDFs.

### [05 · Lineage, Audit &amp; System Tables](level-3/05-lineage-system-tables.md)

**Q97.** What does lineage show?

**Answer:** Upstream sources and downstream consumers of tables and columns

Impact analysis.

**Q98.** What are system tables?

**Answer:** Databricks-managed tables with audit, billing and usage data

system.billing.usage.

**Q99.** How can you find who accessed a table?

**Answer:** Query audit logs in system tables

Governance evidence.

**Q100.** Why is lineage useful before changing a table?

**Answer:** See what downstream assets would break

Impact.

### [06 · Lakeflow Spark Declarative Pipelines](level-3/06-lakeflow-declarative-pipelines.md)

**Q101.** What do Lakeflow Declarative Pipelines (formerly DLT) let you do?

**Answer:** Declare tables and expectations; the engine manages orchestration, retries and dependencies

Declarative ETL.

**Q102.** What are expectations?

**Answer:** Data quality rules with actions (warn, drop, fail)

EXPECT ... ON VIOLATION.

**Q103.** What's the difference between streaming tables and materialized views?

**Answer:** Streaming tables process new data incrementally; materialized views compute results of a query

Choose by pattern.

**Q104.** What does the pipeline infer automatically?

**Answer:** Dependency order from references between datasets

No manual DAG.

### [07 · Lakeflow Jobs: Orchestrating Workloads](level-3/07-lakeflow-jobs.md)

**Q105.** What do Lakeflow Jobs orchestrate?

**Answer:** Tasks like notebooks, SQL, pipelines and Python wheels with dependencies and schedules

Multi-task workflows.

**Q106.** How do you handle failures in a Job?

**Answer:** Retries, alerts and conditional tasks

Operational.

**Q107.** Why use job parameters?

**Answer:** Reuse the same job for different inputs/dates

Parameterize.

**Q108.** What compute should production Jobs use?

**Answer:** Job clusters or serverless, not shared interactive clusters

Isolation and cost.

### [08 · OpenSharing (Formerly Delta Sharing): Sharing Data Across Organizations](level-3/08-opensharing.md)

**Q109.** What does Delta Sharing do?

**Answer:** Shares live data with other organizations over an open protocol without copying

Recipients can use any client.

**Q110.** Who controls what is shared?

**Answer:** The provider, via shares and recipients

Governed.

**Q111.** Does the recipient need Databricks?

**Answer:** No: open sharing works with other clients

Databricks-to-Databricks is richer.

**Q112.** What is a share?

**Answer:** A named collection of tables/views shared with recipients

Grant access.

### [09 · Databricks SQL, Warehouses &amp; Dashboards](level-3/09-databricks-sql-warehouses.md)

**Q113.** What are SQL warehouses for?

**Answer:** BI and SQL workloads with elastic, optimized compute

Serverless or pro.

**Q114.** What drives warehouse cost?

**Answer:** Size and running time (DBUs)

Auto-stop.

**Q115.** What improves dashboard speed?

**Answer:** Caching, right-sizing and well-modeled gold tables

Design for BI.

**Q116.** What's a warehouse's auto-stop for?

**Answer:** Stop charges when idle

Set low.

### [10 · Project — Design a Governed Data Platform](level-3/10-project-governed-platform.md)

**Q117.** What does the governed platform project combine?

**Answer:** Unity Catalog, grants, masks/filters, lineage and jobs

End to end.

**Q118.** How do you prove least privilege works?

**Answer:** Test access as different principals

Verify.

**Q119.** What should be automated?

**Answer:** Grants and objects as code

Reproducible.

**Q120.** What do you document?

**Answer:** Owners, classifications and data contracts

Governance.

## Level 4 · Development, ML & Operations

### [01 · Declarative Automation Bundles &amp; CI/CD](level-4/01-asset-bundles-cicd.md)

**Q121.** What are Databricks Asset Bundles?

**Answer:** Infrastructure-as-code (YAML) for jobs, pipelines and resources deployed via CLI/CI

databricks bundle deploy.

**Q122.** Why use bundles in CI/CD?

**Answer:** Reproducible, reviewed deployments across dev/staging/prod targets

Targets.

**Q123.** What should differ per target?

**Answer:** Workspace, catalog and parameters

Same code, different config.

**Q124.** What should CI run?

**Answer:** Validation, unit tests and deployment to a test target

Gate merges.

### [02 · Testing PySpark &amp; Delta Code Locally](level-4/02-testing-pyspark.md)

**Q125.** How do you unit test PySpark transforms?

**Answer:** Pure functions on small local DataFrames with expected outputs

pytest + local session.

**Q126.** Why keep transform logic in importable modules?

**Answer:** Testable outside notebooks

Separation.

**Q127.** What should tests avoid?

**Answer:** Dependence on large data or cloud state

Fast and deterministic.

**Q128.** What does chispa/assertDataFrameEqual help with?

**Answer:** Comparing DataFrames in tests

Clear diffs.

### [03 · MLflow: Tracking Experiments](level-4/03-mlflow-tracking.md)

**Q129.** What does MLflow Tracking record?

**Answer:** Parameters, metrics, artifacts and code version per run

Reproducibility.

**Q130.** What's the model registry for?

**Answer:** Versioning and stage/alias management of models (in UC)

Promotion workflow.

**Q131.** Why log parameters and metrics?

**Answer:** Compare runs and reproduce results

Experiment management.

**Q132.** What's an MLflow run?

**Answer:** One execution of training code with its logged info

Grouped in experiments.

### [04 · Model Registry &amp; Serving](level-4/04-models-serving.md)

**Q133.** What does Model Serving provide?

**Answer:** Managed REST endpoints for models with autoscaling

Real-time inference.

**Q134.** What should you monitor on endpoints?

**Answer:** Latency, errors, throughput and input/output quality

Operate it.

**Q135.** How do you roll out a new model safely?

**Answer:** Traffic splitting/canary with rollback

Progressive.

**Q136.** What does scale-to-zero trade off?

**Answer:** Cost savings versus cold-start latency

Choose per SLA.

### [05 · AI Functions, Genie Agents &amp; AI Search](level-4/05-ai-functions-genie.md)

**Q137.** What are AI Functions in Databricks SQL?

**Answer:** SQL functions that call models for tasks like classification and summarization on table data

ai_query etc.

**Q138.** What is Genie?

**Answer:** A natural-language interface to ask questions of governed data

Needs good metadata.

**Q139.** What makes natural-language analytics reliable?

**Answer:** Well-modeled, documented tables and verified answers

Curate.

**Q140.** Why verify AI-generated SQL/results?

**Answer:** Models can be wrong or hallucinate

Human review.

### [06 · Cost Management: DBUs, Budgets &amp; System Billing](level-4/06-cost-management.md)

**Q141.** How do you attribute cost to teams?

**Answer:** Tagging compute and using billing system tables

Chargeback.

**Q142.** What are top cost drivers?

**Answer:** Idle compute, oversized clusters and inefficient queries

Optimize.

**Q143.** What do budgets and policies do?

**Answer:** Cap spend and constrain compute configuration

Cluster policies.

**Q144.** Why prefer serverless/job compute for batch?

**Answer:** Pay only while running

Lower waste.

### [07 · Performance: Photon, Serverless &amp; Caching](level-4/07-performance-photon-serverless.md)

**Q145.** What is Photon?

**Answer:** A vectorized native query engine that speeds SQL and DataFrame workloads

Often cheaper per job.

**Q146.** What does serverless compute remove?

**Answer:** Cluster management and startup waits

Elastic.

**Q147.** What should you do before tuning?

**Answer:** Measure with query profile and Spark UI

Evidence.

**Q148.** What's predictive optimization?

**Answer:** Automatic maintenance like OPTIMIZE and VACUUM for UC managed tables

Hands-off upkeep.

### [08 · Table Maintenance &amp; Predictive Optimization](level-4/08-table-maintenance.md)

**Q149.** Which maintenance tasks matter for Delta tables?

**Answer:** OPTIMIZE (compaction), VACUUM and statistics

Keep reads fast.

**Q150.** What does VACUUM free?

**Answer:** Storage from unreferenced old files

Mind time-travel needs.

**Q151.** How often compact streaming tables?

**Answer:** Regularly, since many small commits create small files

Or use auto compaction.

**Q152.** How do you check table health?

**Answer:** DESCRIBE DETAIL and file-count/size metrics

Monitor.

### [09 · Migrating to Databricks](level-4/09-migration.md)

**Q153.** What's a safe migration approach?

**Answer:** Inventory, migrate in phases, run old and new in parallel and validate

Reduce risk.

**Q154.** What changes when moving from Hive metastore to UC?

**Answer:** Objects become three-level and permissions move to UC grants

Plan upgrades.

**Q155.** How do you validate a migrated pipeline?

**Answer:** Compare outputs and row counts against the legacy system

Reconcile.

**Q156.** What should be decommissioned last?

**Answer:** Legacy sources after consumers are verified on the new system

Cutover carefully.

### [10 · Capstone — An End-to-End Platform Plan](level-4/10-capstone-platform.md)

**Q157.** What does the capstone integrate?

**Answer:** Ingestion, medallion, governance, CI/CD, ML/AI and cost controls on one platform

Production lakehouse.

**Q158.** How do you prove production readiness?

**Answer:** Tests, monitoring, runbooks and failure drills

Operate.

**Q159.** What should documentation cover?

**Answer:** Architecture, ownership and operations

Onboarding.

**Q160.** What guards against cost surprises?

**Answer:** Tags, budgets, policies and alerts

FinOps.

## Mixed questions across all levels

**Q161.** What makes a Delta table different from a plain folder of Parquet files?

**Answer:** A transaction log that defines which files make up each version, giving ACID commits and time travel

The log is the table.

**Q162.** A job overwrote a Delta table with bad data an hour ago. What's the fastest recovery?

**Answer:** RESTORE the table to the previous version (if the files haven't been vacuumed)

Time travel plus RESTORE.

**Q163.** After VACUUM with a short retention, a query `VERSION AS OF 5` fails. Why?

**Answer:** The data files for that version were deleted as unreferenced

Retention governs how far back you can go.

**Q164.** A MERGE fails saying multiple source rows matched one target row. What's the fix?

**Answer:** Deduplicate the source to one row per key (e.g. keep the latest) before merging

Make the merge deterministic.

**Q165.** An append fails with a schema mismatch error. Is that a bug?

**Answer:** No: schema enforcement is protecting the table; evolve the schema explicitly if the change is intended

Enforcement fails loudly.

**Q166.** Queries filter on customer_id but scan the whole table. What helps?

**Answer:** OPTIMIZE with ZORDER or liquid clustering on customer_id so data skipping can work

High-cardinality columns are poor partition keys.

**Q167.** A table partitioned by user_id has millions of tiny files. What went wrong?

**Answer:** Over-partitioning on a high-cardinality column

Partition on low-cardinality columns or use clustering.

**Q168.** In a Spark job one task runs for an hour while the rest finish in seconds. What's the likely cause and a fix?

**Answer:** Skew on a join key; use AQE skew handling, salting or broadcast the small side

Look at the Spark UI task metrics.

**Q169.** Why prefer Auto Loader over listing a directory every minute?

**Answer:** It tracks files incrementally with exactly-once ingestion and handles schema evolution (rescued data)

Scales to millions of files.

**Q170.** A streaming query is restarted and reprocesses old data. What was missing?

**Answer:** A stable checkpoint location

Checkpoints store progress.

**Q171.** Why keep bronze tables append-only and untouched?

**Answer:** They're the replayable source of truth if silver logic changes

Medallion principle.

**Q172.** A column should show only the last 4 digits to non-privileged users. What's the Unity Catalog feature?

**Answer:** A column mask on the table

Dynamic, policy-based.

**Q173.** Analysts see all regions but should see only their own. What do you use?

**Answer:** A row filter function based on group membership

One governed table.

**Q174.** A user can't read a table even though they have SELECT on it. What else is required?

**Answer:** USE CATALOG and USE SCHEMA on the parents

Privileges are hierarchical.

**Q175.** What happens on DROP TABLE for an external table?

**Answer:** Metadata is removed but the underlying files remain

Managed tables delete data.

**Q176.** A scheduled nightly job runs on an always-on all-purpose cluster. What's the issue?

**Answer:** Wasted spend and shared interference; use job/serverless compute

Right-size compute.

**Q177.** A deployment must be identical across dev, staging and prod with only config changing. What do you use?

**Answer:** Declarative Asset Bundles (databricks bundle deploy) with per-target settings

IaC and CI/CD.

**Q178.** A dashboard is slow and costly. Which is the best first check?

**Answer:** Profile the query and verify gold tables and warehouse sizing/auto-stop

Measure first.

**Q179.** Which is the right way to share data with an external partner without copying it?

**Answer:** Delta Sharing with a share and recipient

Governed, live.

## Practical coding questions

### Build a toy Delta log

Implement MiniDelta: commit() records added and removed files, snapshot(version) replays the log, read() returns the rows of live files, and vacuum() deletes physical files not needed by the latest version. Show current data, time travel to v1, and what vacuum breaks.

Starter code:

```
class MiniDelta:
    """A toy transaction log: each commit lists files it ADDs and REMOVEs."""
    def __init__(self):
        self.log = []                    # log[v] = (adds, removes)
        self.files = {}                  # file name -> list of rows

    def commit(self, adds=None, removes=()):
        # store the new files, append (added names, removed names) to the log,
        # and return the new version number
        return 0

    def snapshot(self, version=None):
        # replay the log up to `version` (default: latest) and return the live file names
        return []

    def read(self, version=None):
        # all rows from the live files of that version, sorted
        return []

    def vacuum(self, keep_versions=1):
        # delete physical files not needed by the latest `keep_versions` versions
        pass

t = MiniDelta()
t.commit({"f1": [1, 2, 3]})                         # v0
t.commit({"f2": [4, 5]})                            # v1
t.commit({"f3": [1, 2, 3, 40, 5]}, removes=["f1", "f2"])   # v2 (a rewrite)
print("current:", t.read())
print("v1:", t.read(1))
t.vacuum()
print("files after vacuum:", sorted(t.files))
try:
    print("v1 after vacuum:", t.read(1))
except KeyError as e:
    print("time travel to v1 fails: missing file", e)
```

Expected output:

```text
current: [1, 2, 3, 5, 40]
v1: [1, 2, 3, 4, 5]
files after vacuum: ['f3']
time travel to v1 fails: missing file 'f1'
```

??? success "Show a solution"

    ```
    class MiniDelta:
        """A toy transaction log: each commit lists files it ADDs and REMOVEs."""
        def __init__(self):
            self.log = []                    # log[v] = (adds, removes)
            self.files = {}                  # file name -> list of rows

        def commit(self, adds=None, removes=()):
            adds = adds or {}
            self.files.update(adds)
            self.log.append((list(adds), list(removes)))
            return len(self.log) - 1

        def snapshot(self, version=None):
            version = len(self.log) - 1 if version is None else version
            live = []
            for adds, removes in self.log[: version + 1]:
                live = [f for f in live if f not in removes] + adds
            return live

        def read(self, version=None):
            return sorted(r for f in self.snapshot(version) for r in self.files[f])

        def vacuum(self, keep_versions=1):
            keep = set(self.snapshot())
            for v in range(max(0, len(self.log) - keep_versions), len(self.log)):
                keep |= set(self.snapshot(v))
            for f in list(self.files):
                if f not in keep:
                    del self.files[f]

    t = MiniDelta()
    t.commit({"f1": [1, 2, 3]})                         # v0
    t.commit({"f2": [4, 5]})                            # v1
    t.commit({"f3": [1, 2, 3, 40, 5]}, removes=["f1", "f2"])   # v2 (a rewrite)
    print("current:", t.read())
    print("v1:", t.read(1))
    t.vacuum()
    print("files after vacuum:", sorted(t.files))
    try:
        print("v1 after vacuum:", t.read(1))
    except KeyError as e:
        print("time travel to v1 fails: missing file", e)
    ```

    Each commit is only a list of file additions and removals; a table version is the replay of that log. Vacuum deletes unreferenced files, so older versions can no longer be read. This is a teaching model of Delta's idea, not Delta's actual protocol. Verified by running the solution.

### MERGE with deduplicated CDC

Implement merge(target, source) to update matches, insert new rows, and delete rows flagged op='d', raising an error when the source has duplicate keys. Then deduplicate the change feed with latest_per_key and merge again.

Starter code:

```
def merge(target, source, key="id"):
    # update matching rows, insert new ones, delete rows with op == "d";
    # raise ValueError if the source has two rows with the same key
    return target

def latest_per_key(changes, key="id", order="ts"):
    best = {}
    for r in changes:
        if r[key] not in best or r[order] > best[r[key]][order]:
            best[r[key]] = r
    return list(best.values())

target = [{"id": 1, "city": "Oslo"}, {"id": 2, "city": "Rome"}, {"id": 3, "city": "Kyiv"}]
changes = [
    {"id": 2, "city": "Milan", "ts": 1, "op": "u"},
    {"id": 2, "city": "Turin", "ts": 2, "op": "u"},
    {"id": 3, "ts": 3, "op": "d"},
    {"id": 4, "city": "Lima", "ts": 4, "op": "i"},
]
try:
    merge(target, changes)
except ValueError as e:
    print("error:", e)
print(merge(target, latest_per_key(changes)))
```

Expected output:

```text
error: multiple source rows for id=2
[{'id': 1, 'city': 'Oslo'}, {'id': 2, 'city': 'Turin', 'ts': 2}, {'id': 4, 'city': 'Lima', 'ts': 4}]
```

??? success "Show a solution"

    ```
    def merge(target, source, key="id"):
        """MERGE INTO: update matches, insert new rows, delete rows flagged op='d'.
        Duplicate source keys are an error (ambiguous merge) unless deduplicated first."""
        seen = set()
        for r in source:
            if r[key] in seen:
                raise ValueError(f"multiple source rows for {key}={r[key]}")
            seen.add(r[key])
        out = {r[key]: dict(r) for r in target}
        for r in source:
            k = r[key]
            if r.get("op") == "d":
                out.pop(k, None)
            elif k in out:
                out[k].update({c: v for c, v in r.items() if c != "op"})
            else:
                out[k] = {c: v for c, v in r.items() if c != "op"}
        return [out[k] for k in sorted(out)]

    def latest_per_key(changes, key="id", order="ts"):
        best = {}
        for r in changes:
            if r[key] not in best or r[order] > best[r[key]][order]:
                best[r[key]] = r
        return list(best.values())

    target = [{"id": 1, "city": "Oslo"}, {"id": 2, "city": "Rome"}, {"id": 3, "city": "Kyiv"}]
    changes = [
        {"id": 2, "city": "Milan", "ts": 1, "op": "u"},
        {"id": 2, "city": "Turin", "ts": 2, "op": "u"},
        {"id": 3, "ts": 3, "op": "d"},
        {"id": 4, "city": "Lima", "ts": 4, "op": "i"},
    ]
    try:
        merge(target, changes)
    except ValueError as e:
        print("error:", e)
    print(merge(target, latest_per_key(changes)))
    ```

    MERGE needs at most one source row per target row, so CDC feeds are reduced to the latest change per key first. Verified by running the solution.

---

More free practice: the [Databricks Mastery Path](index.md) curriculum, the [Interview Prep Pack](https://learnmasterypath.com/interview-pack/) and the [free resume ATS check](https://learnmasterypath.com/resume/).
