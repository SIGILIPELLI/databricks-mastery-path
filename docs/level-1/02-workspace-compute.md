---
description: "Choose between serverless, classic and SQL warehouse compute; learn notebook basics, magics, widgets, dbutils and the Databricks CLI."
---

# 02 · Workspaces, Compute Types & Notebooks

You can't run anything on Databricks without **compute**, and choosing the wrong kind is the most common cause of both
slow work and surprise bills. This lesson maps the compute options, then covers the notebook environment you'll
spend most of your time in.

!!! note "What was run"
    Compute creation, notebooks and the CLI talk to a live workspace, so nothing in this lesson was executed against
    Databricks. The compute descriptions follow the Databricks documentation (October 2026). The one thing run locally
    is the Python that mimics notebook widgets and parameter handling, shown at the end. See Lesson 01 for the full
    note on what this course ran.

## The compute menu

| Compute | Who manages it | Best for | Notes |
|---|---|---|---|
| **Serverless compute** (notebooks, jobs, pipelines) | Databricks | Most interactive work and scheduled jobs with spiky demand | No cluster to configure; starts quickly; has documented limitations on libraries, languages and some configs |
| **All-purpose (interactive) classic compute** | You | Exploration, development, custom libraries/init scripts | Stays up until you stop it or auto-termination fires; billed at the higher all-purpose rate |
| **Job compute (classic)** | You define, the platform creates per run | Production batch jobs | Created for a run, terminated after; priced lower than all-purpose |
| **SQL warehouse** (serverless, pro, classic) | Databricks | BI, SQL analytics, dashboards, JDBC/ODBC | SQL (and SQL-based) workloads only; sized in T-shirt sizes |

Classic compute also has two **access modes** worth knowing. *Standard* (multi-user) compute lets several people share a
cluster, with Lakeguard isolating each user's code. *Dedicated* compute is assigned to a single user or group and is
used when you need features that need full machine access. Pick the access mode before anything else, since it
determines which libraries and languages are allowed.

!!! tip "Free Edition"
    Databricks Free Edition is a no-cost, serverless-only, quota-limited workspace meant for learning. It replaced the
    older Community Edition, which was retired in 2025. It's the cheapest way to run the platform-only lessons in this
    course. Check its current limitations page before relying on a feature.

### What you configure on classic compute

- **Runtime version** (e.g. an LTS release): the bundled Spark, Delta, Python and library versions.
- **Node types** for the driver and workers: general-purpose, memory-optimized or compute-optimized instances.
- **Autoscaling** (min and max workers) or a fixed worker count, and **single-node** mode for small work.
- **Auto-termination** minutes: the single most useful cost control on all-purpose compute.
- **Photon** acceleration (a vectorized engine; availability and pricing multipliers depend on the cloud and plan).
- **Policies**: admin-defined rules that limit what users may choose (max size, allowed runtimes, tags).
- **Tags** for cost attribution (Level 4 · 06).

Because **job compute** is created per run and torn down afterwards, you never leave a forgotten job cluster running.
That, plus a lower rate, is why production schedules use job compute or serverless, not an all-purpose cluster.

## Notebooks

A Databricks notebook is a sequence of cells attached to one compute resource and one language. Key behaviours:

- **Default language** per notebook (Python, SQL, Scala or R). Override per cell with a *magic* on the first line.
- **Shared state within a notebook session**: variables set in one cell exist in the next. Detaching or restarting the
  compute clears that state; Delta tables you saved survive.
- **`spark`, `sc`, `dbutils`, `display`** are predefined in notebooks.

```python
# Cell 1 (Python)
df = spark.range(5)
display(df)            # rich table/chart renderer; not available outside notebooks
```

```sql
%sql
-- Cell 2: magic switches this cell to SQL
SELECT id, id * id AS square FROM range(5)
```

```text
%md
Cell 3 renders **Markdown** for documentation inside the notebook.
```

Other magics: `%sh` runs a shell command on the *driver only*, `%pip install ...` installs notebook-scoped Python
libraries, and `%run ./other_notebook` executes another notebook in the same session.

### dbutils: the notebook utility belt

```python
dbutils.widgets.text("run_date", "2026-10-21", "Run date")   # parameter input shown at top of notebook
run_date = dbutils.widgets.get("run_date")                    # read it (always a string)

dbutils.fs.ls("/Volumes/main/raw/landing/")                  # list files in a Unity Catalog volume
dbutils.notebook.exit("ok")                                   # return a value to a calling job/notebook
```

Widgets are how a notebook receives parameters from a job: a Jobs task passes `run_date=2026-10-22` and the widget
with the same name picks it up. (Lakeflow Jobs also supports job parameters; Level 3 · 07 covers them.)

### Try the widget pattern locally

`dbutils` does not exist outside Databricks, so production-bound code should keep parameter handling separate and
testable. A pattern that runs anywhere:

```python
import argparse, datetime

def parse_run_date(value: str) -> datetime.date:
    return datetime.date.fromisoformat(value)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", default="2026-10-21")
    args = p.parse_args()
    print(parse_run_date(args.run_date))
```

Run locally with `python job.py --run_date 2026-10-22`; local output: `2026-10-22`. In a notebook you'd replace the
`argparse` lines with `dbutils.widgets.get("run_date")` and keep `parse_run_date` unchanged, which is what makes it
unit-testable (Level 4 · 02).

## The Databricks CLI and other clients

You aren't limited to the browser. The **Databricks CLI** (a Go binary installed separately, not a PyPI package of
the same name) authenticates with a profile in `~/.databrickscfg` and wraps the REST API:

```bash
databricks auth login --host https://<your-workspace-host>     # OAuth browser login
databricks clusters list
databricks workspace export /Users/me@example.com/etl --format SOURCE
```

Other ways in: the **Databricks SDKs** (Python, Java, Go), the **SQL Connector for Python** and JDBC/ODBC drivers for
SQL warehouses, and **Databricks Connect** which lets an IDE run Spark code against remote compute. The commands above
were written from the documentation and weren't run (no workspace was available).

## How It Actually Works

A classic all-purpose cluster is a set of cloud VMs. When it starts, the control plane asks the cloud provider for
instances, installs the chosen runtime image, starts a Spark driver on one node and executors on the others, and
registers them. That is why starting takes minutes and why *instance pools* (idle pre-warmed VMs) exist.

A notebook attaches to the driver. Each cell is sent to a long-lived interpreter on the driver, so variables persist
between cells; your `spark` object is a client of the Spark session in that same process. Because the driver is one
machine, `%sh` and plain Python (pandas, `open()`) operate on the driver's local disk, not on the distributed
filesystem. Distributed work only happens when you call Spark APIs.

Serverless removes the VM lifecycle from your view: Databricks keeps capacity ready in its own compute plane, hands
your session an isolated environment and scales it for you. You trade configuration freedom (no custom init scripts, a
managed runtime version, restrictions on some APIs) for fast start and no idle cluster to forget.

## Common mistakes

- **Developing on the production job's cluster type.** Use a small all-purpose cluster or serverless for development,
  and let production run on job compute or serverless jobs.
- **No auto-termination** on all-purpose compute. Idle clusters bill until stopped.
- **Doing heavy pandas work on the driver.** `df.toPandas()` pulls all data to one machine; it works on a sample and
  fails or crawls on a large table.
- **Storing results in notebook variables.** State disappears when compute restarts. Save to a table or volume.
- **Assuming `%sh` runs on workers.** It runs only on the driver.
- **Hard-coding secrets in cells.** Use secret scopes or a secrets manager (Level 4 · 01).

## Exercise

1. A team runs a nightly 40-minute ETL and 6 analysts run ad-hoc queries during the day. Pick a compute type for each
   and justify it in one sentence.
2. Write `parse_run_date` tests (valid date, invalid date) that run with `pytest` and need no Spark.
3. In the compute documentation, find the difference between standard and dedicated access mode, and list one thing you
   can do in dedicated mode that standard mode restricts.
4. (Needs a workspace or Free Edition.) Create a notebook, run the three cells above, and observe that `display(df)`
   gives a table with chart controls while `df.show()` prints plain text.
