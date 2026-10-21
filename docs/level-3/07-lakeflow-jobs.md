---
description: "Orchestrate with Lakeflow Jobs: task DAGs, run-if conditions, retries, repair runs, parameters, triggers, with a runnable model of the rules."
---

# 07 · Lakeflow Jobs: Orchestrating Workloads

A pipeline defines how data is transformed; a **job** decides *when and in what order* things run, what happens when
something fails, and who gets told. Lakeflow Jobs is Databricks' orchestrator: a DAG of tasks (notebooks, pipelines, SQL, Python
scripts, dbt, other jobs) with triggers, parameters, retries and notifications.

!!! note "What was run, and what wasn't"
    **Jobs run only on Databricks, so nothing below was executed on the platform.** Task types, trigger behaviour, run-if
    conditions, limits and parameter syntax come from the Databricks documentation on jobs (October 2026). I did
    run two local things: a YAML parse of the job definition below (to check it is well-formed YAML only; it was not validated
    against the Jobs schema or deployed), and a small Python model of the dependency and retry rules, **which is my
    reading of the documented behaviour, not Databricks code**, and simplifies the real repair semantics.

## Jobs, tasks, triggers

- A **job** is the unit you schedule and monitor. It has a trigger, parameters, notifications and optional Git settings,
  and up to 1,000 tasks per job (documented limit; a workspace is limited to 2,000 concurrent task runs).
- A **task** is one unit of work: notebook, Python script, wheel/JAR, SQL (query, file, alert, dashboard), pipeline,
  dbt, another job (**Run Job**), plus control-flow tasks **If/else** and **For each**.
- A **trigger** starts the job: a **schedule** (Quartz cron with a time zone; the docs state a minimum 10-second interval between
  scheduled runs), a **file arrival trigger**, a **continuous** trigger, a model-update trigger, or manual/API/`run now`.

File arrival triggers watch a Unity Catalog external location or volume path (recursively, including subdirectories) and
check for new files "best effort every minute" per the docs, costing only the cloud provider's listing charges. They suit
irregular arrivals, where a fixed schedule either wastes runs or adds latency.

## A job definition

Jobs are defined in the UI, with the REST API/SDKs, or as code in **bundles** (next level). This is a four-task job in bundle YAML
(field names follow the Jobs API; it is untested and the notebook paths are invented):

```yaml
resources:
  jobs:
    orders_daily:
      name: orders-daily
      schedule:
        quartz_cron_expression: "0 30 2 * * ?"      # 02:30 every day
        timezone_id: UTC
      parameters:
        - name: run_date
          default: "{{job.start_time.iso_date}}"
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ../src/ingest.py
            base_parameters:
              run_date: "{{job.parameters.run_date}}"
        - task_key: pipeline
          depends_on: [{task_key: ingest}]
          pipeline_task:
            pipeline_id: ${resources.pipelines.orders_pipeline.id}
        - task_key: publish
          depends_on: [{task_key: pipeline}]
          max_retries: 2
          notebook_task:
            notebook_path: ../src/publish.py
        - task_key: notify
          depends_on: [{task_key: publish}, {task_key: pipeline}]
          run_if: ALL_DONE
          notebook_task:
            notebook_path: ../src/notify.py
```

Points to notice:

- `depends_on` builds the DAG. Tasks with no dependency between them run in parallel.
- `run_if: ALL_DONE` makes `notify` run whether `publish` and `pipeline` succeeded or failed.
- `max_retries` retries *this task* on failure. The docs say the default for most configurations is no retries,
  that serverless jobs auto-optimize retries by default, and that continuous jobs use exponential backoff.
- `{{job.parameters.run_date}}` and `{{job.start_time.iso_date}}` are **dynamic value references**: double-curly-brace
  strings replaced at run time. The docs stress that braces are *not* evaluated as expressions (no functions or arithmetic),
  and that a non-existent reference is a syntax error.

## Run-if conditions

A task's **Run if dependencies** field controls when it executes relative to its upstream tasks:

| Condition | Runs when |
|---|---|
| All succeeded (default) | Every upstream task succeeded; otherwise the task is marked *Upstream failed* |
| At least one succeeded | Any upstream succeeded |
| None failed | No upstream failed, and at least one actually ran |
| All done | Upstream tasks have finished, whatever their result |

(The UI also offers failure-oriented options such as "at least one failed" and "all failed" for error-handling branches; check the
current run-if page for the full list.) Use *All done* for cleanup and notification tasks that must run regardless. Beyond
run-if, **If/else** branches on a boolean expression (for example, only transform if ingestion added rows, with the value passed
between tasks as a *task value*), and **For each** loops a task over an array of inputs.

## A runnable model of the semantics

This model implements topological ordering, the run-if conditions above, per-task retries and a "repair" that keeps
already-successful tasks:

```python
"""Teaching model of Lakeflow Jobs task-dependency semantics (run-if conditions, retries, repair).
Written from the documented rules. NOT Databricks code, no API calls."""
from graphlib import TopologicalSorter

class Task:
    def __init__(self, name, fn, deps=(), run_if="ALL_SUCCEEDED", retries=0):
        self.name, self.fn, self.deps, self.run_if, self.retries = name, fn, tuple(deps), run_if, retries

def should_run(task, status):
    d = [status[x] for x in task.deps]
    if not d: return True
    ok = [s == "SUCCESS" for s in d]
    ran = [s in ("SUCCESS", "FAILED") for s in d]
    return {"ALL_SUCCEEDED": all(ok), "AT_LEAST_ONE_SUCCEEDED": any(ok),
            "NONE_FAILED": all(s != "FAILED" and s != "UPSTREAM_FAILED" for s in d) and any(ran),
            "ALL_DONE": True}[task.run_if]

def run_job(tasks, status=None, log=print):
    status = dict(status or {})
    order = TopologicalSorter({t.name: t.deps for t in tasks}).static_order()
    by = {t.name: t for t in tasks}
    for name in order:
        t = by[name]
        if status.get(name) == "SUCCESS": log(f"  {name}: already SUCCESS (repair keeps it)"); continue
        if not should_run(t, status): status[name] = "UPSTREAM_FAILED"; log(f"  {name}: UPSTREAM_FAILED (run-if {t.run_if})"); continue
        for attempt in range(t.retries + 1):
            try: t.fn(); status[name] = "SUCCESS"; log(f"  {name}: SUCCESS (attempt {attempt+1})"); break
            except Exception as e: status[name] = "FAILED"; log(f"  {name}: FAILED attempt {attempt+1}: {e}")
    return status

state = {"warehouse_up": False}
def ingest(): pass
def validate(): pass
def transform():
    if not state["warehouse_up"]: raise RuntimeError("warehouse not reachable")
def publish(): pass
def notify(): pass
tasks = [Task("ingest", ingest), Task("validate", validate, ["ingest"]),
         Task("transform", transform, ["validate"], retries=1),
         Task("publish", publish, ["transform"]),
         Task("notify_always", notify, ["publish", "transform"], run_if="ALL_DONE")]
print("run 1:"); st = run_job(tasks)
print("statuses:", st)
state["warehouse_up"] = True
print("\nrepair run (only failed/skipped tasks re-run):"); st = run_job(tasks, st)
print("statuses:", st)
```

Output of running it:

```text
run 1:
  ingest: SUCCESS (attempt 1)
  validate: SUCCESS (attempt 1)
  transform: FAILED attempt 1: warehouse not reachable
  transform: FAILED attempt 2: warehouse not reachable
  publish: UPSTREAM_FAILED (run-if ALL_SUCCEEDED)
  notify_always: SUCCESS (attempt 1)
statuses: {'ingest': 'SUCCESS', 'validate': 'SUCCESS', 'transform': 'FAILED', 'publish': 'UPSTREAM_FAILED', 'notify_always': 'SUCCESS'}

repair run (only failed/skipped tasks re-run):
  ingest: already SUCCESS (repair keeps it)
  validate: already SUCCESS (repair keeps it)
  transform: SUCCESS (attempt 1)
  publish: SUCCESS (attempt 1)
  notify_always: already SUCCESS (repair keeps it)
statuses: {'ingest': 'SUCCESS', 'validate': 'SUCCESS', 'transform': 'SUCCESS', 'publish': 'SUCCESS', 'notify_always': 'SUCCESS'}
```

`transform` failed twice (one retry), `publish` never started because its run-if condition wasn't met, and
`notify_always` ran anyway. The repair re-ran only the two tasks that hadn't succeeded, so `ingest` and `validate` (which could
be expensive) were not repeated. In the real product, **Repair run** re-runs failed and skipped tasks of an existing
run; how it treats an `ALL_DONE` task that already ran is a detail my model glosses over (it simply keeps it), and I did not
verify it. Read the repair-failures documentation before relying on it.

## Parameters and task values

- **Job parameters** are key/value pairs pushed to every task that declares them (via dynamic references or widgets).
  Define them once at job level; override on manual runs and backfills.
- **Task values** let one task publish a small value for downstream tasks (the `dbutils.jobs.taskValues` utility), for
  example a row count that an If/else task checks.
- **Dynamic value references** include job and task metadata (`{{job.run_id}}`, `{{job.start_time.iso_date}}`, task values).
- The documentation has a dedicated page on backfilling jobs over date ranges; the principle is to select the date with
  parameters, never by editing code.

Parameters for a notebook arrive as widgets (Level 1 · 02), so the same notebook runs interactively and in a job.

## Compute for jobs

A job task can run on **serverless** compute (no cluster to manage; auto-optimized retries), on **job compute** (a classic cluster
created for the run and terminated after, cheaper than all-purpose), or on an existing all-purpose cluster (avoid: higher
price and shared interference). Several tasks in a job can share one **job cluster** to avoid repeated startup.

## Operating jobs

- **Notifications** by email, Slack, webhooks on start/success/failure and on duration thresholds.
- **Timeouts** per task so a hung task doesn't hold the job forever.
- **Concurrency**: limit maximum concurrent runs (commonly 1 for jobs that must not overlap).
- **Permissions**: view, manage-run and manage levels; run jobs as a **service principal** so the identity doesn't depend on
  a person (and Unity Catalog grants go to the principal).
- **Observability**: run history UI, plus job and task run system tables for cost and reliability analysis (look up the exact
  table names in the system tables documentation; I did not verify them).
- **Git**: run notebooks and files from a Git ref rather than workspace copies, so deployments are versioned.

## How It Actually Works

When a trigger fires, the jobs service creates a **run** record and a task-run per task. It performs a topological walk of the
DAG: tasks whose dependencies are resolved (and whose run-if condition is true) are submitted to compute; others are marked skipped
or *upstream failed*. Each task run is an independent execution (a notebook run on a cluster, a pipeline update, a SQL
statement) with its own logs, retries and status; the job run's status is derived from them. Because task runs are separate
records, a repair can create a new attempt for specific tasks while keeping the original run's successful results.
(That last sentence describes the documented user-visible behaviour; the internal record-keeping is my inference.)

## Common mistakes

- **One giant notebook task** instead of several tasks (no partial retry, no parallelism, no per-step visibility).
- **Overlapping runs** because max concurrent runs wasn't limited.
- **Retrying non-idempotent tasks** (a retry duplicates side effects); make tasks idempotent first (Level 2).
- **Running production on all-purpose clusters** or as a person's identity.
- **Hard-coding dates** in notebooks instead of job parameters, making backfills code changes.
- **Using the schedule for irregular arrivals** when a file-arrival trigger fits better.
- **No notification on failure**: jobs fail silently until someone looks.

## Exercise

1. Add a fifth task to the model, `cleanup` with run-if "none failed" depending on `publish`, and predict its status in
   both runs before running.
2. Modify the model so that repair also re-runs `ALL_DONE` tasks whose upstream changed status. Is that more
   or less like what you would want?
3. Write the job YAML for a file-arrival-triggered job (consult the Jobs API reference for the trigger field names) that runs the
   pipeline when files land in `/Volumes/prod/raw/landing/orders/`.
4. Choose retry counts for four tasks (ingest from a flaky API, transform, publish to a warehouse, send a notification) and
   justify each.
