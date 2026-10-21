---
description: "Capstone: write an end-to-end Databricks platform plan as data and check it against the rules from all four levels with a runnable checker."
---

# 10 · Capstone — An End-to-End Platform Plan

You have now met the whole stack: lakehouse and Delta, ingestion and pipelines, governance, orchestration, sharing and serving,
delivery, ML, cost and maintenance. The capstone asks you to put it together *on paper first*, because most expensive platform
mistakes are design mistakes. You'll write a platform plan as structured data, then run a checker that applies rules from every level
to it.

!!! note "What was run, and what wasn't"
    The plan checker is plain Python (PyYAML) and was run locally; its outputs below are copied from that run. **It checks a
    design for consistency against the rules taught in this course; it does not touch Databricks, and a passing plan is not proof that a
    platform will work, be secure, or be cheap.** The company ("acme-retail"), table names, SLAs and numbers are invented. The rules encode
    guidance from earlier lessons (several of which are themselves from the Databricks documentation, October 2026, and not run).

## The brief

*Acme Retail* ingests order files from stores into a lakehouse, builds a cleaned orders and customers model, and publishes a daily
revenue dashboard. Constraints: customer data is personal data (GDPR-style erasure requests); the analytics team must see revenue but
not raw customer details; the platform team is three people; a production incident must be recoverable within a day.

## What a good plan answers

| Area | Questions the plan must answer | Lessons |
|---|---|---|
| Data | Which layers, tables and owners? Retention? Layout (clustering vs partitioning)? | L1 · 06 to 09, L2 · 03, 04, 09 |
| Pipelines | Trigger, compute type, idempotence, retries, SLA, alerting? | L2 · 07 to 10, L3 · 06, 07 |
| Governance | Catalog layout, groups, privileges, PII controls, erasure procedure? | L3 · 01 to 05, L4 · 08 |
| Serving | Warehouse type/size, dashboards, sharing? | L3 · 08, 09 |
| Cost | Tags, budgets, review cadence? | L4 · 06 |
| Delivery | IaC, tests, CI/CD, deploy identity, rollback? | L4 · 01, 02 |
| Operations | Maintenance, monitoring, disaster recovery? | L4 · 07, 08 |

## The plan (as data)

`plan.yml`:

```yaml
platform: acme-retail
environments: [dev, prod]
catalogs: {dev: {owner: platform-admins}, prod: {owner: platform-admins}}
tables:
  - {name: prod.bronze.orders_raw,  layer: bronze, owner: data-engineers, retention_days: 365, layout: "append-only, no clustering", pii: false, tests: [reconciliation]}
  - {name: prod.silver.orders,      layer: silver, owner: data-engineers, retention_days: 90,  layout: "liquid clustering by order_date, customer_id", pii: true, pii_control: abac_mask, tests: [unit, reconciliation, expectations]}
  - {name: prod.silver.customers,   layer: silver, owner: data-engineers, retention_days: 90,  layout: "liquid clustering by customer_id", pii: true, pii_control: abac_mask, erasure: "deletion vectors + REORG PURGE + VACUUM", tests: [unit]}
  - {name: prod.gold.daily_revenue, layer: gold,   owner: analytics-eng,  retention_days: 30,  layout: "liquid clustering by order_date", pii: false, tests: [reconciliation]}
pipelines:
  - {name: orders_ingest,  owner: data-engineers, trigger: "file arrival", compute: serverless, idempotent: true, retries: 2, sla_minutes: 30, alert: "pagerduty", reads: [landing volume], writes: [prod.bronze.orders_raw]}
  - {name: orders_silver,  owner: data-engineers, trigger: "after orders_ingest", compute: serverless, idempotent: true, retries: 1, sla_minutes: 45, alert: "slack", reads: [prod.bronze.orders_raw], writes: [prod.silver.orders, prod.silver.customers]}
  - {name: revenue_gold,   owner: analytics-eng,  trigger: "02:30 UTC daily", compute: serverless, idempotent: true, retries: 1, sla_minutes: 60, alert: "slack", reads: [prod.silver.orders], writes: [prod.gold.daily_revenue]}
serving: {dashboards: [revenue], warehouse: {type: serverless, size: Medium, max_clusters: 3, auto_stop_minutes: 10}}
cost: {tags_required: [team, env, cost_center], budget_alerts: [50, 80, 100], review: weekly}
governance: {groups_only: true, owners_are_groups: true, predictive_optimization: true, retention_set_before_po: true}
delivery: {iac: bundles, ci: [unit tests, bundle validate], deploy: "service principal from main", rollback: "redeploy previous tag; RESTORE affected tables"}
```

Some design reasoning behind it:

- **Serverless everywhere** for jobs and the warehouse: a three-person team shouldn't manage clusters (Level 1 · 02, Level 3 · 09), and
  serverless's constraints (Level 4 · 07) don't hit this workload, as far as the plan author knows. Verify with a test run.
- **Bronze is append-only with 365 days of retention** so silver and gold can be rebuilt after a bug (Level 2 · 09); silver keeps 90 days
  and gold 30, matching how far back anyone needs to time travel (Level 1 · 08). **These retention numbers are illustrative.**
- **Personal data** lives in `silver.orders` and `silver.customers`; analysts read gold and masked silver via ABAC (Level 3 · 04);
  `customers` has an explicit **erasure procedure** (deletion vectors, then `REORG ... PURGE`, then `VACUUM` after retention: Level 4 · 08),
  and note that bronze also contains the personal data: a complete erasure design must say what happens there (the plan above leaves it
  unanswered, which is exactly the kind of gap review should find: see Exercise 2).
- **File arrival trigger** for ingest (irregular store uploads: Level 3 · 07), chained downstream tasks, and a daily gold job.
- **Delivery** through bundles with a service principal deploying from `main` (Level 4 · 01).

## The checker

```python
"""Capstone plan checker: cross-cutting rules from Levels 1-4 applied to a platform plan. A design review aid, not a Databricks API."""
import sys, yaml
plan = yaml.safe_load(open(sys.argv[1])); problems = []
tables = {t["name"]: t for t in plan["tables"]}; pipes = plan["pipelines"]
for t in plan["tables"]:
    for k in ("owner", "layer", "retention_days", "layout", "tests"):
        if k not in t: problems.append(f"table {t['name']}: missing '{k}'")
    if t.get("pii") and not t.get("pii_control"): problems.append(f"table {t['name']}: contains PII but no pii_control (mask/ABAC/row filter)")
    if t.get("layer") == "bronze" and "append" not in t.get("layout", ""): problems.append(f"table {t['name']}: bronze should be append-only")
    if "partition" in t.get("layout", "").lower() and "liquid" not in t.get("layout", "").lower(): problems.append(f"table {t['name']}: prefer liquid clustering to partitioning for new tables (Level 2 · 03/04)")
    if t.get("layer") in ("silver", "gold") and not t.get("tests"): problems.append(f"table {t['name']}: no tests listed")
    if t.get("pii") and t["layer"] == "silver" and "erasure" not in t and t["name"].endswith("customers"): problems.append(f"table {t['name']}: personal data table has no erasure procedure")
written = {w for p in pipes for w in p["writes"]}
for name in tables:
    if name not in written: problems.append(f"table {name}: no pipeline writes it")
for p in pipes:
    if not p.get("idempotent"): problems.append(f"pipeline {p['name']}: not idempotent, so retries can duplicate data")
    if p.get("compute") in ("all-purpose", "all_purpose"): problems.append(f"pipeline {p['name']}: production job on all-purpose compute")
    if p.get("retries", 0) > 0 and not p.get("idempotent"): problems.append(f"pipeline {p['name']}: retries without idempotence")
    for k in ("owner", "sla_minutes", "alert"):
        if k not in p: problems.append(f"pipeline {p['name']}: missing '{k}'")
    for w in p["writes"]:
        if w.startswith("prod.") and plan["environments"] and "prod" not in plan["catalogs"]: problems.append(f"pipeline {p['name']}: writes to undefined catalog")
g = plan["governance"]
if not g.get("owners_are_groups"): problems.append("governance: owners must be groups")
if g.get("predictive_optimization") and not g.get("retention_set_before_po"): problems.append("governance: set VACUUM retention BEFORE enabling predictive optimization (Level 4 · 08)")
if len(plan["cost"].get("tags_required", [])) < 2: problems.append("cost: require at least team + env tags")
if "bundles" not in plan["delivery"].get("iac", "") and "terraform" not in plan["delivery"].get("iac", ""): problems.append("delivery: no infrastructure as code")
if not plan["delivery"].get("rollback"): problems.append("delivery: no rollback plan")
w = plan["serving"]["warehouse"]
if w.get("auto_stop_minutes", 999) > 45: problems.append("serving: auto stop above the documented classic default")
print(f"{len(plan['tables'])} tables, {len(pipes)} pipelines checked")
print("\n".join(problems) if problems else "OK: plan passes all checks")
sys.exit(1 if problems else 0)
```

On the plan above:

```text
4 tables, 3 pipelines checked
OK: plan passes all checks
```

To see the rules bite, I made a copy of the plan with six deliberate defects (removed the PII control on `silver.orders`, switched the
gold layout to partitioning, put ingest on all-purpose compute, made the gold pipeline non-idempotent while still retrying, turned off
"retention set before predictive optimization", and emptied the rollback plan):

```text
4 tables, 3 pipelines checked
table prod.silver.orders: contains PII but no pii_control (mask/ABAC/row filter)
table prod.gold.daily_revenue: prefer liquid clustering to partitioning for new tables (Level 2 · 03/04)
pipeline orders_ingest: production job on all-purpose compute
pipeline revenue_gold: not idempotent, so retries can duplicate data
pipeline revenue_gold: retries without idempotence
governance: set VACUUM retention BEFORE enabling predictive optimization (Level 4 · 08)
delivery: no rollback plan
```

Seven messages for six defects (the non-idempotent pipeline trips two rules). The checker is deliberately small; its value is that every
message points back to a lesson, and that you can add a rule each time an incident teaches you one.

Notice what the checker does **not** catch in the passing plan: the bronze erasure gap above; whether the clustering columns match
real query patterns; whether serverless limitations apply to your libraries; whether the SLAs are achievable; and whether the
costs fit the budget. A checker enforces consistency; judgment about trade-offs stays with people.

## Your capstone

Produce, for a domain you know (or the Acme brief), the following. Each item should be short and specific.

1. **Plan file** (like `plan.yml`) with at least four tables across three layers and three pipelines. Make it pass the checker, then
   extend the checker with two rules of your own.
2. **Data-flow diagram** (boxes and arrows are fine): sources, ingestion, layers, serving, consumers, and where each governance control
   (mask, filter, grant) applies.
3. **Unity Catalog layout and grants**: the spec for Level 3 · 10's generator, including groups and a service principal.
4. **Cost model**: tags, expected workloads by compute type, budget thresholds, and the first three queries you'd run weekly
   (Level 4 · 06). Use real prices from the pricing pages, not guesses.
5. **Failure analysis**: for each pipeline, what happens if it fails halfway, runs twice, or receives late data. How do you recover
   (retry, repair, RESTORE, replay from bronze)?
6. **Test plan**: unit tests for transformations (Lesson 02), a reconciliation check per layer, one integration test on a staging target.
7. **Risks and unknowns**: what you could not verify without a workspace (be as candid as this course tries to be).

## How It Actually Works

A plan checker is a *policy engine over a declarative model*, the same pattern as the governance linter (Level 3 · 10) and the bundle
linter (Lesson 01): represent decisions as data, express rules as functions over that data, run them in CI so a design review
becomes a pull request with failing checks instead of a meeting. The leverage comes from the data model: because tables, pipelines and
policies are all explicit records, a rule like "every PII table has a control" is five lines. The limit is also the data model:
anything not represented (like bronze's personal data) is invisible to every rule.

## Common mistakes

- **Designing from tools instead of requirements** (picking Photon, serverless or a feature before the SLA and the budget).
- **A plan with no failure analysis.**
- **Forgetting that erasure applies to every layer and every copy**, including bronze, backups and clones.
- **Treating the checker as the review.**
- **A plan nobody updates**: keep it in Git next to the bundle, and change it in the same pull request as the code.
- **Over-engineering for a three-person team**: every extra layer, job and policy has a maintenance cost; delete what you can't justify.

## Exercise

1. Run `check_plan.py` on `plan.yml`, then on your own mutated copy. Add a rule that every pipeline writing PII tables must run as a
   service principal (add the field to the data model).
2. Close the bronze erasure gap: write the procedure (what is deleted, where, in what order, how verified) and add a rule that
   flags any PII table feeding a bronze table without a documented erasure path.
3. Estimate which Level 4 lesson's advice would change your plan if the team were 30 people instead of 3, and why.
4. Present your plan to a colleague, ask them to find one thing the checker missed, and add that as a rule.

## Where to go next

You now have a map of the platform and a habit of testing claims. The best next steps: get a Free Edition workspace and re-run the
platform-only lessons that this course could only describe; deploy one small bundle with a pipeline and a job; connect a SQL warehouse
dashboard to a gold table; and read the Databricks release notes monthly, since names, defaults and features keep changing, as the
renames noted throughout this course (Delta Live Tables, Asset Bundles, Delta Sharing, Genie Spaces, Vector Search) show.
