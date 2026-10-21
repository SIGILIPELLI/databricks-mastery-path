---
description: "Databricks Level 3: Unity Catalog, privileges, storage, masks, lineage, declarative pipelines, jobs, OpenSharing, SQL warehouses and a governance project."
---

# Level 3 · Unity Catalog, Pipelines & Sharing

Levels 1 and 2 were about getting data in and making it fast. Level 3 is about operating it as a platform: who may see what, where
the files live, how pipelines are declared and scheduled, how data leaves the organization, and how SQL users consume it.

!!! note "What was run, and what wasn't"
    This level is the most platform-bound of the course. **Unity Catalog, jobs, SQL warehouses, OpenSharing and Databricks
    dashboards cannot run locally, and nothing in Lessons 01 to 05 and 07 to 09 was executed on Databricks.** Those lessons
    are built from the Databricks documentation (October 2026), and each one says what was and wasn't run. What *was* run:
    Lesson 06 uses the open-source Spark Declarative Pipelines runtime that ships with PySpark 4.2.0, running a real
    three-layer pipeline; Lessons 02, 03, 05, 07 and 10 include small local Python or Spark programs that model the documented
    rules (and are labelled as models, not Databricks code); Lesson 04 runs the filter and mask logic as ordinary SQL functions
    on local Spark. A Databricks Free Edition workspace is the way to try the platform parts.

**Prerequisites:** Levels 1 and 2.

## Modules

1. **[Unity Catalog: Metastore, Catalogs & Three-Level Names](01-unity-catalog-model.md)**: the object hierarchy, managed
   storage precedence, and how to lay out catalogs.
2. **[Privileges, Ownership & Grants](02-privileges-grants.md)**: usage privileges, inheritance, ownership vs `MANAGE`, with a
   step-through model.
3. **[Storage Credentials, External Locations & Volumes](03-external-locations-volumes.md)**: how UC governs cloud storage and why
   locations can't overlap.
4. **[Row Filters, Column Masks & ABAC](04-row-filters-column-masks.md)**: fine-grained access control and tag-based policies.
5. **[Lineage, Audit & System Tables](05-lineage-system-tables.md)**: impact analysis and access reviews, on mock tables.
6. **[Lakeflow Spark Declarative Pipelines](06-lakeflow-declarative-pipelines.md)**: streaming tables and materialized views,
   run locally with open-source Spark.
7. **[Lakeflow Jobs: Orchestrating Workloads](07-lakeflow-jobs.md)**: DAGs, run-if conditions, retries and repair.
8. **[OpenSharing (Formerly Delta Sharing)](08-opensharing.md)**: sharing live data outside your workspace.
9. **[Databricks SQL, Warehouses & Dashboards](09-databricks-sql-warehouses.md)**: sizing, scaling, auto stop and the Python connector.
10. **[Project: Design a Governed Data Platform](10-project-governed-platform.md)**: a spec, a generator and a tested linter.

## After Level 3

Level 4 covers how teams build and ship all this: bundles and CI/CD, testing PySpark code, MLflow, model serving and AI features,
cost management, performance, table maintenance, migration, and a capstone plan.
