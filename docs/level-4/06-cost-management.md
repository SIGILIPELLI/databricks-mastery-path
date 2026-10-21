---
description: "Understand and control Databricks cost: DBUs and SKUs, system.billing.usage with retractions, tags and budgets, cost levers, mock-data analysis."
---

# 06 · Cost Management: DBUs, Budgets & System Billing

Databricks bills in **Databricks Units (DBUs)**: a unit of processing capability per hour, priced per SKU (compute type, cloud and
plan). Cost problems on the platform are rarely mysterious; they are usually an idle all-purpose cluster, a job on the wrong
compute type, a runaway query, or spend nobody can attribute to a team. This lesson shows how to see the spend, assign it
and limit it.

!!! note "What was run, and what wasn't"
    I have no Databricks account, so **no real billing data was queried**. The system-table names, columns and the retraction/
    restatement rule come from the Databricks documentation (October 2026). The analysis below runs on **local Spark with mock
    rows** that I invented; **every quantity and every price in this lesson is made up** to illustrate the queries (the documented
    schema is real, the numbers are not). I give no actual prices: DBU rates vary by SKU, cloud, region, plan and contract, so look
    them up in the pricing pages and in `system.billing.list_prices`.

## What you pay for

- **Databricks charges (DBUs)** × a rate for the SKU: for example all-purpose compute, jobs compute, SQL, serverless variants, model
  serving, Lakeflow pipelines. All-purpose (interactive) compute is priced higher than jobs compute for the same cluster, which is one
  reason production work belongs on job compute or serverless jobs (Level 1 · 02).
- **Cloud charges (classic compute only):** the VMs, disks and network in *your* cloud account, billed by your cloud provider.
  Serverless compute bundles the infrastructure into the Databricks charge, so there is a single bill (the exact accounting is in the
  pricing documentation).
- **Storage** in your object store, and, for features like AI Functions or model serving, separate usage lines.

## The billing system tables

`system.billing.usage` is the central table (Lesson 05 of Level 3 covers system tables). Per the docs, records are typically available
within 12 hours, and the documented columns include `record_id`, `account_id`, `workspace_id`, `sku_name`, `cloud`, `usage_start_time`,
`usage_end_time`, `usage_date`, `custom_tags` (a map), `usage_unit` (for example DBU), `usage_quantity`, `usage_metadata` (a struct
with fields such as `cluster_id`, `job_id`, `warehouse_id`, populated depending on the compute type), `identity_metadata`,
`record_type`, `billing_origin_product` and `product_features`. `system.billing.list_prices` logs SKU prices over time, with a row
added whenever a price changes.

### Corrections: retractions and restatements

If Databricks corrects a usage record it does not edit the original. Per the docs, `record_type` is `ORIGINAL`, or `RETRACTION`
(identical to the original except `usage_quantity` is **negative** and cancels it) or `RESTATEMENT` (the corrected record). The docs'
own example sums usage so that the retraction negates the original and only the restatement's values remain. Here is what that does on
my mock data (seven rows, including a retraction of `-40` for `job-7` and a restatement of `36`):

```text
naive: ORIGINAL rows only              correct: all record types
ALL_PURPOSE_COMPUTE  102.0             ALL_PURPOSE_COMPUTE  102.0
JOBS_COMPUTE          65.0             JOBS_COMPUTE          61.0
SERVERLESS_SQL        30.0             SERVERLESS_SQL        30.0
```

Filtering to `ORIGINAL` (a natural-looking cleanup!) overstated jobs compute by 4 DBUs here, because it kept the 40-DBU original and
ignored the correction. A plain `SUM(usage_quantity)` over all record types is right. That lines up with the docs, but check
whether you want to restrict by date or ingestion date for reproducible month-end reports (late corrections can change old totals).

## Attribution with tags

You can't manage what you can't attribute. Per the documentation there are **default tags** (applied automatically to cloud
resources) and **custom tags** (user-defined on compute and serverless workloads) for tracking by team, project or cost center. Custom tags
flow into `usage.custom_tags`. Warnings from the docs: tag data is stored as plain text and may be replicated globally, so
never put personal or sensitive data in tag names or values.

Mock query, using invented prices from an invented price table:

```sql
SELECT coalesce(custom_tags['team'], 'UNTAGGED') AS team,
       ROUND(SUM(u.usage_quantity * p.usd_per_dbu), 2) AS usd, SUM(u.usage_quantity) AS dbus
FROM usage u JOIN list_prices p USING (sku_name)
GROUP BY ALL ORDER BY usd DESC;
```

```text
|data-science|36.00|90.0|
|analytics   |21.00|30.0|
|growth      | 6.10|61.0|
|UNTAGGED    | 4.80|12.0|
```

(Mock data, with made-up prices.) The `UNTAGGED` row is the metric to drive to zero. Enforce tagging with **compute policies**
(Level 1 · 02: a policy can require a tag) and, for serverless workloads, **budget policies** that attach tags to usage. The
docs also describe **budgets** with alerts, in the account console, that notify when spend crosses a threshold; check the current
page for the supported actions (alerts versus hard limits).

Other queries from the same mock table: per-job DBUs (`job-7` 36.0, `job-8` 25.0, computed from `usage_metadata.job_id`) and the
all-purpose share of total DBUs (52.8% in the mock data). A rising all-purpose share is a classic red flag that production
work is running on interactive clusters.

The mock script (runs on local Spark; replace the temp views with `system.billing.*` tables on a workspace):

```python
from common import spark
s=spark()
# MOCK rows shaped like system.billing.usage (a few columns). SKU names are real-looking labels; quantities and prices are INVENTED.
s.sql("""CREATE OR REPLACE TEMP VIEW usage AS SELECT * FROM VALUES
 ('r1','JOBS_COMPUTE','ORIGINAL',    DATE'2026-10-20', 40.0,  MAP('team','growth'),'job-7'),
 ('r2','JOBS_COMPUTE','ORIGINAL',    DATE'2026-10-20', 25.0,  MAP('team','growth'),'job-8'),
 ('r3','ALL_PURPOSE_COMPUTE','ORIGINAL', DATE'2026-10-20', 90.0, MAP('team','data-science'),NULL),
 ('r4','ALL_PURPOSE_COMPUTE','ORIGINAL', DATE'2026-10-20', 12.0, MAP(),NULL),
 ('r5','SERVERLESS_SQL','ORIGINAL',  DATE'2026-10-20', 30.0,  MAP('team','analytics'),NULL),
 ('r6','JOBS_COMPUTE','RETRACTION',  DATE'2026-10-20', -40.0, MAP('team','growth'),'job-7'),
 ('r7','JOBS_COMPUTE','RESTATEMENT', DATE'2026-10-20', 36.0,  MAP('team','growth'),'job-7')
 AS t(record_id, sku_name, record_type, usage_date, usage_quantity, custom_tags, job_id)""")
s.sql("CREATE OR REPLACE TEMP VIEW list_prices AS SELECT * FROM VALUES ('JOBS_COMPUTE',0.10),('ALL_PURPOSE_COMPUTE',0.40),('SERVERLESS_SQL',0.70) AS t(sku_name, usd_per_dbu)")  # INVENTED
print("naive: ORIGINAL rows only"); s.sql("SELECT sku_name, SUM(usage_quantity) dbus FROM usage WHERE record_type='ORIGINAL' GROUP BY sku_name ORDER BY sku_name").show()
print("correct: all record types (retraction cancels the original, restatement replaces it)"); s.sql("SELECT sku_name, SUM(usage_quantity) dbus FROM usage GROUP BY sku_name ORDER BY sku_name").show()
print("spend by team tag, hypothetical prices"); s.sql("""SELECT coalesce(custom_tags['team'],'UNTAGGED') team, ROUND(SUM(u.usage_quantity * p.usd_per_dbu),2) usd, SUM(u.usage_quantity) dbus
  FROM usage u JOIN list_prices p USING (sku_name) GROUP BY ALL ORDER BY usd DESC""").show()
print("per job (job compute only)"); s.sql("SELECT job_id, SUM(usage_quantity) dbus FROM usage WHERE job_id IS NOT NULL GROUP BY job_id ORDER BY job_id").show()
print("share of all-purpose DBUs"); s.sql("SELECT ROUND(100*SUM(CASE WHEN sku_name='ALL_PURPOSE_COMPUTE' THEN usage_quantity END)/SUM(usage_quantity),1) pct_all_purpose FROM usage").show()
```

## Cost levers by compute type

| Compute | Main levers |
|---|---|
| All-purpose clusters | **Auto-termination** (idle minutes), right-sized nodes, autoscaling, single-node for small work, compute policies capping size, **do not run schedules here** |
| Job compute | Right-size per job, **use the cheaper job rate**, pools or serverless to cut startup, spot/preemptible workers where interruption is tolerable (classic), fewer, larger jobs vs. many tiny clusters |
| SQL warehouses | Auto stop, cluster size vs. max clusters (Level 3 · 09), separate warehouses per workload, query tuning |
| Serverless jobs / notebooks | Efficient code (you pay for compute time), avoid long idle sessions, set timeouts |
| Model serving / AI Functions | Filter rows first, cache results in tables, scale-to-zero only for non-production, cheaper models for simple tasks |
| Storage | `VACUUM`, delete stale clones/experiments, lifecycle rules on raw landing zones, avoid tiny files (Level 2 · 04) |

And the big one: **efficiency of the work itself**. A query that scans 100× too much data costs 100× too much on any compute type.
Everything in Level 2 (data skipping, clustering, joins, small files) is a cost lever, and Level 4 · 07 and 08 cover more.

## A weekly review routine

1. Total DBUs by SKU and by day: any step change?
2. Top ten jobs and warehouses by DBUs, with their owners (from tags).
3. Untagged share, and idle all-purpose clusters (cluster events or compute system tables).
4. Jobs whose cost per run rose more than, say, 30% (make the threshold your own): look at data volume, retries, skew.
5. Failed and retried runs: a job that fails after 90% of its work and reruns doubles its cost.
6. Open an issue for each finding with an owner and a date.

## How It Actually Works

Each compute resource emits metering events (cluster running time × node DBU rate for classic; resource-seconds for serverless).
A billing pipeline aggregates them into usage records, hourly, stamps them with SKU, workspace, tags and metadata, and writes them to
the system table; corrections are appended as the retraction-plus-restatement pairs above, never as updates, so the table is
append-only and auditable. Prices are a separate versioned log, so cost = usage × the price effective at that time, joined by
SKU and time range (my mock join ignored the time range for brevity; a real one must respect `price_start_time` and
`price_end_time`).

## Common mistakes

- **Filtering `record_type = 'ORIGINAL'`** and missing corrections.
- **Joining list prices without time ranges**, or treating list price as what you actually pay (contracts and discounts change that).
- **Production jobs on all-purpose clusters.**
- **No auto-termination** on interactive clusters.
- **Untagged compute** so costs can't be charged back.
- **Putting sensitive values in tags.**
- **Optimizing the bill instead of the work** (scheduling fewer runs vs. fixing the scan).
- **Forgetting cloud-provider costs** on classic compute when comparing with serverless.

## Exercise

1. Extend the mock to add a second day and a price change on `ALL_PURPOSE_COMPUTE`; write the time-ranged join and show the
   spend difference vs. the untimed join.
2. Write the SQL (shape only) that finds jobs on all-purpose compute, using the fact that `job_id` is not populated for jobs run on
   all-purpose compute (per the docs). What does that tell you about how to detect them?
3. Design the tagging standard for your organization (tag keys, allowed values, enforcement via compute policy, who owns the list).
4. Decide an alert threshold for a team with a monthly budget of X DBUs, and write the rule in plain words, including what
   action each threshold triggers.
