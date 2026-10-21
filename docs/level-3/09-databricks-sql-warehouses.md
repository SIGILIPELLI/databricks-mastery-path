---
description: "Databricks SQL warehouses: serverless vs pro vs classic, sizing and scaling, auto stop, query history, caching, dashboards and the Python connector."
---

# 09 · Databricks SQL, Warehouses & Dashboards

Not everyone who needs your data writes Spark. Analysts, BI tools and applications want to run SQL, fast, with predictable
cost. **Databricks SQL** is the SQL-first surface of the lakehouse: a query editor, **SQL warehouses** (compute for SQL),
dashboards, alerts, and connectors (JDBC/ODBC, Python, REST) for other tools.

!!! note "What was run, and what wasn't"
    SQL warehouses and dashboards exist only on Databricks, so **none of this lesson's platform behaviour was executed**.
    Warehouse types, defaults and sizing advice come from the Databricks documentation pages for SQL warehouses and
    dashboards (October 2026); defaults and prices change, so re-check them. Locally I installed the open-source
    `databricks-sql-connector` package to check its import path and connection signature, and tried it against a reserved
    `.invalid` host; what happened is described in the connector section.

## SQL warehouses

A SQL warehouse is compute dedicated to SQL (and SQL-based) workloads, available in three types:

| Type | Engine features (per docs) | Notes |
|---|---|---|
| **Serverless** | Photon, Predictive IO, Intelligent Workload Management | Starts in seconds (the docs say typically 2 to 6), rapid up- and down-scaling; compute runs in Databricks' account; the recommended default |
| **Pro** | Photon, Predictive IO | Classic-plane compute with more features than classic |
| **Classic** | Photon | Basic, runs in your cloud account |

(A newer *Lakehouse Real-Time* warehouse type for sub-second, high-concurrency reads is described as Beta in the docs.)

### The settings that matter

- **Cluster size** (2X-Small to the largest sizes): the resources of *one cluster*. Bigger means lower latency for heavy
  queries; the default per the docs is X-Large. The docs advise for serverless: start with one larger warehouse and size
  down if needed, since it is usually cheaper to size down than to start small and scale up. If the query profile shows
  **spilling to disk**, increase the size.
- **Scaling (min/max clusters)**: how many same-size clusters Databricks may run in parallel to handle concurrency. The default is
  1 to 1. The docs recommend about one cluster per 10 concurrent queries. Watch the **Peak Queued Queries** metric on the monitoring
  page.
- **Auto stop**: how long idle before shutting down. Defaults per the docs: 45 minutes for pro and classic (minimum 10), 10 minutes
  for serverless (minimum 5 in the UI; the API allows as low as 1). Idle warehouses accrue DBU and cloud charges until stopped,
  so for classic/pro this is a cost lever; for serverless the short default is part of its appeal.
- **Type**, **tags** (for cost attribution; Level 4 · 06) and permissions (`CAN USE`, `CAN MANAGE`).

**Size vs. clusters** is the central design question: size improves *single-query speed*; clusters improve *concurrency*. A
dashboard used by 200 people at 9am needs more clusters, not a bigger one; a monthly report scanning terabytes needs a bigger
one. Separate warehouses for separate workloads (ETL, BI, ad-hoc) also isolate them from one another.

Per the docs, serverless uses **Intelligent Workload Management**: when a query arrives, the system predicts its resource
needs; if capacity exists it starts at once, otherwise it queues, and if queue wait times rise the autoscaler provisions more
clusters; when demand drops it scales down while keeping enough capacity for recent peaks. When a configuration change
recycles clusters, the old cluster may linger up to 24 hours for running queries, after which long-running queries could
be terminated; so keep queries well under a day.

## Writing and tuning queries

Everything from Levels 1 and 2 applies: columnar Delta tables, data skipping, clustering. Warehouse-specific tools:

- **Query history** records each query's duration, rows, the user and warehouse; a table in the system catalog holds the same
  data for analysis (system table list, Lesson 05).
- **Query profile** shows the plan with time and rows per operator, spill, and scan statistics (files read vs pruned).
  This is where you confirm Level 2's advice (skipping works; join strategy; skew).
- **Caching**: results can be reused when the same query runs again over unchanged data, and disk caching speeds repeat scans.
  The exact caching behaviours and settings are in the docs; the practical rule is that a repeated dashboard query should be fast
  the second time, and a first slow run isn't representative.
- **EXPLAIN** gives the plan as text (it worked on local Spark in Level 2).
- **Materialized views** (Lesson 06) precompute expensive aggregates; dashboards over a materialized view are cheap and
  consistent.

```sql
-- Typical analyst query shape: filter on a clustering/partition column, aggregate, limit output
SELECT order_date, customer_id, SUM(amount) AS revenue
FROM prod.gold.orders
WHERE order_date >= current_date() - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY revenue DESC
LIMIT 100;
```

## AI/BI dashboards

Dashboards are authored from **datasets** (tables, views or custom queries), with visualizations (AI-assisted or manual), filters at
global, page and widget level, cross-filtering, and calculated fields. Per the docs, published dashboards can be shared with
anyone registered in the account, even people without workspace access. Practical points:

- A dashboard's **data permissions** are either the viewer's own (each viewer sees what Unity Catalog lets them see, so
  row filters and masks from Lesson 04 apply) or the publisher's embedded credentials (everyone sees the publisher's view).
  Choosing the second one for sensitive data is a governance decision, not a convenience setting; check the dashboard
  sharing documentation for the exact options.
- Dashboards run on a SQL warehouse; each refresh runs the dataset queries. Use materialized views and the dashboard's
  caching options to keep cost predictable.
- Dashboards can be exported/imported, and managed in Git and in bundles (Level 4 · 01).
- **Alerts** run a query on a schedule and notify when a condition is met (they can also be a job task).
- **Genie** spaces let users ask questions in natural language over curated tables (Level 4 · 05).

## Connecting from code and tools

JDBC/ODBC drivers connect BI tools (Power BI, Tableau and others) to a warehouse. For Python there is the **Databricks SQL
Connector**:

```python
from databricks import sql
import os

with sql.connect(server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                 http_path=os.getenv("DATABRICKS_HTTP_PATH"),
                 access_token=os.getenv("DATABRICKS_TOKEN")) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM samples.nyctaxi.trips LIMIT ?", [2])      # parameterized: never f-string user input
        for row in cursor.fetchall():
            print(row)
```

This is the documented usage pattern; it was not run against a real warehouse. Authentication options in the docs include personal
access tokens (fine for experiments), **OAuth for users** (`auth_type="databricks-oauth"`), and **OAuth for service principals**
(machine-to-machine) which is what production applications should use. The connector also supports
query tags, and the REST **SQL Statement Execution API** is available when you can't use a driver.

What I observed locally with `databricks-sql-connector` 4.7.0: `sql.connect` takes `server_hostname`, `http_path`, `access_token`
and further keyword arguments. Pointed at a reserved `.invalid` hostname (no real server), the default call **kept retrying
for more than three minutes** before I stopped it, so a wrong hostname or a network problem can look like a hang in a
scheduled job. With short retry settings (I used the underscore-prefixed `_retry_stop_after_attempts_duration=5`,
`_retry_stop_after_attempts_count=1` and `_socket_timeout=5`, which I did not find in the documentation's public option list, so
treat them as unsupported internals) it failed after about 3 seconds with `MaxRetryError ... NameResolutionError`. The
practical lesson: wrap connector calls in your own timeout and retry policy, and never let an unbounded retry run inside a
job with no task timeout (Lesson 07).

## How It Actually Works

A warehouse is a cluster (or several) of executors running the same Spark/Photon engine you used in Levels 1 and 2, with a
front end that accepts SQL statements over HTTP, manages a queue, and returns results. A query is parsed, planned and
authorized against Unity Catalog (Lesson 02), optimized (partition and file skipping, AQE), then executed in
Photon, the vectorized C++ engine, reading Delta files directly from cloud storage with credentials vended by Unity
Catalog. Results are cached and returned in chunks, which is why very large result sets should be written to a table
rather than downloaded by a BI tool.

Serverless adds a pool of pre-initialized capacity in Databricks' account, which is why starting takes seconds, and a
control loop (IWM) that watches queue length to add and remove clusters. Classic and pro warehouses must provision VMs,
so starting takes minutes and scaling is slower.

## Common mistakes

- **Using one big warehouse for everything** so a runaway ad-hoc query slows the executive dashboard.
- **A bigger size to fix concurrency**, or more clusters to fix a single slow query.
- **Leaving classic/pro auto stop at a long idle time** on a warehouse used twice a day.
- **Dashboards on raw tables** instead of gold tables or materialized views, re-aggregating on every refresh.
- **Publishing with embedded credentials** for sensitive data.
- **Concatenating user input into SQL** instead of using parameters.
- **Downloading millions of rows through a BI tool** instead of aggregating first.

## Exercise

1. A BI dashboard has 120 users, most active 9:00 to 10:00, and each load runs eight 2-second queries. Propose the warehouse
   type, size, min/max clusters and auto stop; justify each setting, citing the docs' guidance, and say what metric you'd watch.
2. Rewrite a query that scans `prod.silver.orders` to read from a materialized view, and describe how the dashboard's cost
   changes (qualitatively).
3. Write a small Python script with `databricks-sql-connector` that runs a parameterized query and writes the result to CSV.
   Add the environment variables it needs and an OAuth service principal variant.
4. In a workspace, run the same query twice and compare the query profile and duration. What changed, and why?
