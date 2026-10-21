---
description: "Ship Databricks projects as code with Declarative Automation Bundles: databricks.yml, targets, dev vs production mode, variables and CI/CD."
---

# 01 · Declarative Automation Bundles & CI/CD

Clicking jobs together in a UI works for one person on one afternoon. A team needs the job, its pipeline, its schedule, its
permissions and its code **in Git**, reviewed in pull requests, tested automatically and deployed the same way to dev and
prod. **Declarative Automation Bundles** (the new name for what was Databricks Asset Bundles; older material uses the old
name) are Databricks' infrastructure-as-code answer for workspace resources.

!!! note "What was run, and what wasn't"
    The Databricks CLI wasn't installed on the machine this course was written on, and there was no workspace, so **no
    `databricks bundle` command was run**: nothing was validated against the real bundle schema or deployed. What I did run
    locally: the YAML files below were parsed with PyYAML, and a small linter I wrote (shown below) applied a few of the
    documented best practices to them. Field names and behaviours come from the Databricks documentation pages on
    bundles and deployment modes (October 2026). Treat the YAML as a documented-pattern sketch to adapt and validate with
    `databricks bundle validate`.

## What a bundle is

A bundle is a project folder with a root `databricks.yml` that declares: the bundle's name, **targets** (environments), **variables**,
and the **resources** to deploy: Lakeflow Jobs, pipelines, dashboards, model-serving endpoints, MLflow experiments and registered
models, plus source files (notebooks, Python, wheels) and tests. The CLI then validates, deploys and runs them:

```bash
databricks bundle init                  # start from a template
databricks bundle validate -t dev       # check configuration
databricks bundle deploy -t dev         # create/update resources in the dev workspace
databricks bundle run orders_daily -t dev
databricks bundle destroy -t dev        # delete what the bundle created
```

(Commands from the documentation, not run here.) Deployment is **idempotent and declarative**: you describe the end state; the
CLI works out what to create, update or delete, and tracks what it deployed in a state file.

## `databricks.yml`

This is the root file I used (the lint output is further down):

```yaml
bundle:
  name: orders_platform

include:
  - resources/*.yml

variables:
  catalog:
    description: Unity Catalog catalog for this environment
    default: dev
  warehouse_id:
    description: SQL warehouse used by SQL tasks

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dev-workspace.example.cloud.databricks.com
    variables:
      catalog: dev

  prod:
    mode: production
    workspace:
      host: https://prod-workspace.example.cloud.databricks.com
      root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
    git:
      branch: main
    run_as:
      service_principal_name: "00000000-0000-0000-0000-000000000000"
    permissions:
      - group_name: data-engineers
        level: CAN_MANAGE
      - group_name: analysts
        level: CAN_VIEW
    variables:
      catalog: prod
```

Reading it:

- **`include`** pulls resource definitions from other files, keeping the root small.
- **`variables`** are declared once, referenced as `${var.catalog}`, and given per-target values. The `warehouse_id` has no
  default, so supplying it is required wherever it's used.
- **Targets** are environments. `dev` is the default for local work. `prod` uses `mode: production` and `run_as`.
- `${bundle.name}`, `${bundle.target}` and similar are substitution variables resolved at deploy time.

### The two deployment modes

From the deployment-modes documentation:

| | `mode: development` | `mode: production` |
|---|---|---|
| Resource names | Prefixed with `[dev <your short name>]` and tagged `dev`, so many developers can deploy the same bundle side by side | No prefix |
| Schedules and triggers | **Paused**, so a dev deploy never starts a nightly job | Active |
| Concurrent runs | Enabled for faster iteration | As configured |
| Pipelines | `development: true` | Validated as `development: false` |
| Git | Not checked | Current branch must match the target's `git.branch` (override with `--force`) |
| Identity | You | Databricks recommends a **service principal** via `run_as`; without it, `run_as` and `permissions` must be stated explicitly |
| Cluster override | `--cluster-id` can override job clusters | Not allowed |
| Deployment lock | Disabled for speed | Enabled |

Presets let you customize these defaults. The lesson: `development` is for *you*, `production` is guardrails.

## A resource file

```yaml
resources:
  jobs:
    orders_daily:
      name: orders-daily-${bundle.target}
      schedule:
        quartz_cron_expression: "0 30 2 * * ?"
        timezone_id: UTC
      max_concurrent_runs: 1
      parameters:
        - name: catalog
          default: ${var.catalog}
      tasks:
        - task_key: build_silver
          spark_python_task:
            python_file: ../src/build_silver.py
            parameters: ["--catalog", "${var.catalog}"]
```

The job name embeds `${bundle.target}`, so dev and prod deployments never collide. `${var.catalog}` makes the same code write
to `dev` or `prod` Unity Catalog catalogs depending on the target (Level 3 · 01), a pattern that replaces hard-coded catalog
names inside notebooks. `max_concurrent_runs: 1` prevents overlapping runs (Level 3 · 07). Paths in `python_file` are relative to
the resource file.

## Checking the bundle with a linter

A real `databricks bundle validate` checks against the schema. My stand-in checks a few things the docs recommend, so you can
run them without a workspace:

```python
"""Tiny static checks for a Declarative Automation Bundle project. Not the Databricks CLI: it cannot validate against the real schema."""
import re, sys, glob, yaml
root = sys.argv[1]
cfg = yaml.safe_load(open(f"{root}/databricks.yml"))
problems = []
declared = set(cfg.get("variables", {}))
text = "".join(open(f).read() for f in [f"{root}/databricks.yml", *glob.glob(f"{root}/resources/*.yml")])
for ref in sorted(set(re.findall(r"\$\{var\.([A-Za-z0-9_]+)\}", text))):
    if ref not in declared: problems.append(f"variable ${{var.{ref}}} is used but not declared")
for name, t in cfg["targets"].items():
    if name.startswith("prod"):
        if t.get("mode") != "production": problems.append(f"target {name}: prod-like target should set mode: production")
        if "run_as" not in t: problems.append(f"target {name}: no run_as (use a service principal)")
        if "permissions" not in t: problems.append(f"target {name}: no permissions block")
        if "@" in str(t.get("workspace", {}).get("root_path", "")): problems.append(f"target {name}: root_path points into a user's folder")
for f in glob.glob(f"{root}/resources/*.yml"):
    r = yaml.safe_load(open(f))
    for jn, j in r["resources"].get("jobs", {}).items():
        if j.get("max_concurrent_runs") is None: problems.append(f"job {jn}: max_concurrent_runs not set")
        keys = [t["task_key"] for t in j["tasks"]]
        if len(keys) != len(set(keys)): problems.append(f"job {jn}: duplicate task_key")
print("targets:", list(cfg["targets"]), "| variables declared:", sorted(declared))
print("\n".join(problems) if problems else "OK: no problems found by these checks")
```

On the project above:

```text
targets: ['dev', 'prod'] | variables declared: ['catalog', 'warehouse_id']
OK: no problems found by these checks
```

On a copy where I removed `mode: production` and `run_as` from `prod`, pointed `root_path` into a person's folder, deleted the
`catalog` variable declaration and removed `max_concurrent_runs`:

```text
targets: ['dev', 'prod'] | variables declared: ['warehouse_id', 'wh']
variable ${var.catalog} is used but not declared
target prod: prod-like target should set mode: production
target prod: no run_as (use a service principal)
target prod: root_path points into a user's folder
job orders_daily: max_concurrent_runs not set
```

This catches obvious mistakes in seconds in CI, but it knows nothing about the real schema, so it complements
`databricks bundle validate` and doesn't replace it.

## CI/CD with GitHub Actions

A typical flow: pull request runs unit tests and `bundle validate`; merging to `main` deploys to production with a service
principal; secrets live in the CI system, never in the repo. This workflow follows that shape (it parses as YAML; it wasn't run
in GitHub Actions, and `databricks/setup-cli` is the CLI setup action named in the Databricks documentation, which you should pin to a
version rather than `@main` in real use):

```yaml
name: deploy
on:
  pull_request:
  push:
    branches: [main]
jobs:
  test-and-validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.12"}
      - run: pip install -e ".[test]" && pytest -q
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate -t prod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
  deploy-prod:
    if: github.ref == 'refs/heads/main'
    needs: test-and-validate
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy -t prod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
```

Design points:

- **Test first** (Lesson 02): unit tests of transformation code run on the CI runner with local Spark; no cluster needed.
- **Validate on PRs, deploy on `main`**, with the `environment: production` gate giving you an approval step.
- **Authenticate as a service principal** with OAuth client credentials (`DATABRICKS_CLIENT_ID/SECRET`), not a person's token.
- A typical promotion path is dev (every developer), staging (merge to a release branch, integration tests on real data), prod
  (tag or main). The same bundle with different targets and variables means the code under test is the code that ships.
- Add an **integration test** step that runs the deployed job in a staging target and asserts on output tables.

## Secrets and configuration

Don't put secrets in YAML. Use Databricks **secrets** (secret scopes backed by Databricks or your cloud's secret manager) and read
them at run time with `dbutils.secrets.get(scope, key)`; CI secrets only authenticate the deploy. Environment-specific
non-secret settings belong in variables/targets.

## How It Actually Works

`bundle deploy` does three things. It **builds** artifacts (such as a Python wheel); it **uploads** source files to the
workspace folder named by `root_path` (default under the deployer's home, keyed by bundle name and target); and it **applies
resources** by calling the Jobs/Pipelines/other REST APIs, creating objects it hasn't seen and updating those it has, recording the
mapping from bundle resource name to workspace object ID in its state. That state is how `destroy` knows what to delete and how a
rename is detected. Documentation describes both Terraform-based and a newer "direct" deployment engine; the YAML you write is the
same either way. Because everything flows through the same APIs you could call yourself, a bundle is a convenience and a
discipline, not a new capability.

## Common mistakes

- **Deploying prod from a laptop** with a personal token instead of CI with a service principal.
- **Hard-coded catalog/schema/warehouse IDs** in notebooks rather than variables.
- **Dev deployments that start production schedules** (use development mode, which pauses them).
- **Editing deployed jobs in the UI**; the next deploy overwrites the change (drift).
- **One giant `databricks.yml`** instead of `include`d resource files.
- **Unpinned actions/CLI versions** in CI, so a pipeline changes behaviour on its own.
- **Skipping tests because "it's just config"**: job config is code.

## Exercise

1. Add a pipeline resource (Lesson 06 of Level 3) to the bundle and make the job depend on it. Extend the linter to flag any
   pipeline in a production target with `development: true`.
2. Add a `staging` target with its own variables and workspace host, and decide which mode it should use (justify).
3. Write down the exact steps for rolling back a bad production deploy using Git and the CI workflow above.
4. (Needs the CLI) Install the Databricks CLI, run `databricks bundle init` with a default template, and compare the generated
   `databricks.yml` with this one; list three differences.
