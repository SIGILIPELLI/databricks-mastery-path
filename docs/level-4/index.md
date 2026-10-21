---
description: "Databricks Level 4: bundles and CI/CD, testing, MLflow and serving, AI features, cost, performance, table maintenance, migration and a capstone plan."
---

# Level 4 · Development, ML & Operations

Level 4 is about running the platform as a team over years rather than as a single person for an afternoon: shipping changes safely,
testing them, tracking models, controlling cost and speed, keeping tables healthy, moving in from other systems, and planning the
whole thing.

!!! note "What was run, and what wasn't"
    As before, open-source Spark 4.2.0 and Delta Lake 4.4.1 ran locally, and so did several other open-source tools used here:
    MLflow 3.17.2, scikit-learn, pytest, sqlglot and the `delta-sharing` and `databricks-sql-connector` clients. Platform-only features
    (bundle deployment, Model Serving, AI Functions, Genie, AI Search, Predictive Optimization, Photon, serverless compute, billing
    system tables) were not run and are described from the Databricks documentation (October 2026); the cost lesson uses invented
    mock billing rows and says so. Two local experiments gave weak or no results (a caching micro-benchmark with no measurable
    gain, and a Python UDF timing that hung), and the performance lesson reports them as they happened. Several product names changed
    recently; the lessons note the old and new names.

**Prerequisites:** Levels 1 to 3.

## Modules

1. **[Declarative Automation Bundles & CI/CD](01-asset-bundles-cicd.md)**: `databricks.yml`, dev vs production mode, a CI workflow and
   a bundle linter.
2. **[Testing PySpark & Delta Code Locally](02-testing-pyspark.md)**: pure transforms, pytest, `assertDataFrameEqual`, a caught bug.
3. **[MLflow: Tracking Experiments](03-mlflow-tracking.md)**: runs, comparison, and the skops trust finding.
4. **[Model Registry & Serving](04-models-serving.md)**: aliases, signatures, a local REST server and Databricks Model Serving.
5. **[AI Functions, Genie Agents & AI Search](05-ai-functions-genie.md)**: using AI features responsibly, plus a retrieval toy.
6. **[Cost Management: DBUs, Budgets & System Billing](06-cost-management.md)**: attribution, retractions and levers.
7. **[Performance: Photon, Serverless & Caching](07-performance-photon-serverless.md)**: a tuning order and honest measurements.
8. **[Table Maintenance & Predictive Optimization](08-table-maintenance.md)**: deletion vectors measured, purge and vacuum.
9. **[Migrating to Databricks](09-migration.md)**: convert, clone, Hive to Unity Catalog, and dialect translation pitfalls.
10. **[Capstone: An End-to-End Platform Plan](10-capstone-platform.md)**: a plan as data and a checker.

## After the course

Use the exercises as a checklist for a real workspace: nearly every lesson names something you can verify on a Free Edition account that
this course could only describe.
