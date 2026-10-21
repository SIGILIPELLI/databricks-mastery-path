---
description: "Unity Catalog lineage and system tables: table and column lineage, the audit log, retention, impact analysis (on mock rows) and access reviews."
---

# 05 · Lineage, Audit & System Tables

Governance isn't only about blocking access; it's about being able to answer questions afterward. *Which dashboards break if I
change this column? Who read the customer table last week? What did we spend yesterday?* Unity Catalog records
the answers automatically, in lineage views and in **system tables** you query with SQL.

!!! note "What was run, and what wasn't"
    System tables and lineage exist only in a Unity Catalog workspace, so **no real system-table output appears in this
    lesson**. Table names, columns, retention periods and requirements come from the Databricks documentation (lineage and
    system-tables pages, October 2026). To show what the queries *do*, I ran them on **local Spark 4.2.0** against small
    **mock** tables that I wrote to mimic a few columns of `system.access.table_lineage` and `system.access.audit`. The mock
    rows are invented; the real tables have many more columns and rows, and you must check the current schema in
    your workspace.

## Lineage

Per the documentation, Unity Catalog automatically captures lineage for queries run on Databricks, down to the column level,
and aggregates it across all workspaces attached to the metastore. Nodes can be tables and views, model versions, external
assets and file paths, and the entities that read or write them (notebooks, jobs, pipelines, dashboards, SQL queries).

What it is for: **impact analysis** (what depends on this table or column?), **root-cause investigation** (where did this
number come from?), **sensitive-data tracking** for audits, and understanding cross-team dependencies.

Requirements and limits worth knowing (all from the docs):

- Tables must be registered in Unity Catalog; queries must use the Spark DataFrame API or Databricks SQL interfaces.
  Lineage isn't captured for every access pattern, and the lineage system tables explicitly "represent a subset of all
  read/write events" because it's not always possible to infer lineage.
- To **view** lineage you need at least `BROWSE` on the parent catalog (and permissions on notebooks/jobs/dashboards you want
  to see details of).
- External systems (Salesforce, MySQL, Tableau, Power BI) can be registered as **external lineage** assets to extend the graph.
- Lineage in Catalog Explorer is retained indefinitely for lineage captured after 1 September 2024 per the docs, while the lineage
  *system tables* keep a rolling one-year window.

## The two lineage system tables

| Table | One row per | Selected columns |
|---|---|---|
| `system.access.table_lineage` | Read or write event on a table or path | `source_table_full_name`, `target_table_full_name`, `source_type`, `target_type`, `entity_type` (e.g. `JOB`, `NOTEBOOK`, `PIPELINE`, `DASHBOARD_V3`), `entity_id`, `entity_run_id`, `created_by`, `event_time`, `event_date` |
| `system.access.column_lineage` | Column read/write | `source_column_name`, `target_column_name` plus table identifiers |

An event with a source and no target (a dashboard reading a table) has `target_table_full_name = NULL`. Column lineage
excludes events with no source (an `INSERT` of literal values).

### Impact analysis, run on mock data

Mock rows for a small estate: raw CSV → bronze → silver → gold, a feature table built from silver, and a dashboard reading gold:

```sql
CREATE OR REPLACE TEMP VIEW table_lineage AS SELECT * FROM VALUES
 ('prod.raw.orders_csv',   'prod.bronze.orders',  'PIPELINE','p1', TIMESTAMP'2026-10-20 01:00:00'),
 ('prod.bronze.orders',    'prod.silver.orders',  'PIPELINE','p1', TIMESTAMP'2026-10-20 01:05:00'),
 ('prod.silver.orders',    'prod.gold.daily_rev', 'JOB',     'j7', TIMESTAMP'2026-10-20 01:30:00'),
 ('prod.silver.customers', 'prod.gold.daily_rev', 'JOB',     'j7', TIMESTAMP'2026-10-20 01:30:00'),
 ('prod.gold.daily_rev',   NULL,                  'DASHBOARD_V3','d3', TIMESTAMP'2026-10-20 08:00:00'),
 ('prod.silver.orders',    'prod.ml.features',    'NOTEBOOK','n9', TIMESTAMP'2026-10-19 12:00:00')
 AS t(source_table_full_name, target_table_full_name, entity_type, entity_id, event_time);
```

"If I change `prod.bronze.orders`, what is downstream?" is a graph walk. Spark 4.2 supports recursive CTEs, so:

```sql
WITH RECURSIVE down(tbl, depth) AS (
  SELECT 'prod.bronze.orders', 0
  UNION ALL
  SELECT l.target_table_full_name, d.depth + 1
  FROM table_lineage l JOIN down d ON l.source_table_full_name = d.tbl
  WHERE l.target_table_full_name IS NOT NULL AND d.depth < 10)
SELECT DISTINCT tbl, depth FROM down ORDER BY depth, tbl;
```

Local output on the mock data:

```text
|prod.bronze.orders |0|
|prod.silver.orders |1|
|prod.gold.daily_rev|2|
|prod.ml.features   |2|
```

and the non-table consumers (rows with a NULL target) found the dashboard:

```text
|DASHBOARD_V3|d3|prod.gold.daily_rev|
```

So a change to bronze reaches silver, the gold revenue table and the ML feature table, and ultimately dashboard `d3`.
Real lineage has cycles (a table that reads itself), so keep the `depth` limit; and because the system table is a
*sample of events*, treat the result as "at least these", not as proof nothing else depends on the table.

## The audit log

`system.access.audit` records account activity. Documented columns include `event_time`, `event_date`, `workspace_id`,
`service_name` (for example `unityCatalog`), `action_name` (for example `getTable`), `user_identity`, `request_id`,
`request_params` (a map whose contents depend on the request) and `response`. Notes from the docs: filter on `event_date`
for performance; some `request_params` keys containing SQL are omitted unless you are an account admin or in the
`databricks_pii_access` group; and deleting a workspace removes its audit events older than 14 days.

An access review on mock audit rows (columns simplified to `user_email` and `object_name`):

```sql
SELECT user_email, action_name, count(*) AS n, min(event_time) AS first_seen
FROM audit
WHERE object_name = 'prod.silver.customers'
GROUP BY ALL ORDER BY first_seen;
```

```text
|mallory@x.com|getTable         |2|2026-10-20 09:05:00|
|admin@x.com  |updatePermissions|1|2026-10-21 02:00:00|
```

On the real table you'd pull the user from `user_identity.email` and the object from `request_params`, and the available
`action_name` values differ by service. Check the audit documentation for exact keys; I have not validated them against
a live workspace. Good habits: alert on permission changes (`update...Permissions` actions), unusual hours or volumes
of reads on sensitive tables, and failed authorization events.

## Other system tables you'll use

The system catalog also hosts (from the system-tables index) compute and warehouse tables, billing and pricing, query
history, jobs and pipeline events, marketplace, model serving, data classification and more. Level 4 · 06 uses billing
tables for cost. Access requires Unity Catalog and an account admin to enable the relevant schemas; reading them is
governed by normal `GRANT`s on the `system` catalog objects, and the docs strongly discourage copying this data out of the
platform because it can expose sensitive information.

```sql
-- the shapes of the queries you'll write (not run; check columns first)
SELECT * FROM system.access.table_lineage WHERE event_date >= current_date() - INTERVAL 7 DAYS LIMIT 10;
SELECT usage_date, sku_name, SUM(usage_quantity) FROM system.billing.usage GROUP BY ALL ORDER BY 1 DESC LIMIT 20;
```

## Tags, classification and discovery

Governance metadata complements lineage: **tags** (key/value labels on catalogs, schemas, tables and columns),
**governed tags** (with controlled values, used by ABAC in Lesson 04), **comments**, certification and deprecation
markers, and **data classification**, which the documentation describes as automatically classifying and tagging sensitive
data. `BROWSE` plus Catalog Explorer search makes this discoverable without exposing data.

## How It Actually Works

Lineage is captured by the engine as queries execute: when a Spark plan reads and writes governed tables, the runtime
reports (source, target, columns, entity) events to the control plane, which aggregates them into the lineage graph and
exports them to the system tables. That's why lineage appears only for supported interfaces and is "a subset": a query
run through a path that doesn't report (certain libraries, direct file reads outside UC) leaves no edge. The audit log is
produced by the Unity Catalog service itself for each API action (including authorization decisions), so it is complete
for UC operations in a way lineage is not.

## Common mistakes

- **Treating lineage as complete.** Absence of an edge is not proof of no dependency.
- **Reading audit data without filtering on `event_date`** and scanning a year of events.
- **Granting broad access to system tables**, which expose user names, queries and request parameters.
- **Never alerting on the audit log**, so it is only read after an incident.
- **Copying system-table data to external stores** without a security review.
- **Forgetting retention**: lineage tables keep one year; audit retention follows the docs table for your plan.

## Exercise

1. Extend the mock lineage with a cycle and show that the `depth` limit stops the recursion. What does `UNION` vs
   `UNION ALL` do to the recursion termination?
2. Write the query that lists, for a given table, all *entities* (jobs, notebooks, dashboards) that read it, with their
   last read time.
3. Design three audit-log alerts for a PII table and write the SQL shape for each.
4. In a workspace, enable system tables and compare `system.access.table_lineage` with Catalog Explorer's lineage graph
   for the same table. Where do they differ?
