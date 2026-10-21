---
description: "Register MLflow models with aliases, enforce signatures, serve over REST locally, and see how Databricks Model Serving and Unity Catalog fit."
---

# 04 · Model Registry & Serving

A model sitting in an experiment run is a file. A model in a **registry** is a governed, versioned asset with a name, an owner,
an audit trail and a way to say "this version is the one production uses". **Serving** exposes it behind an API. This lesson
runs the registry and a REST server locally with open-source MLflow, then maps each piece to Databricks.

!!! note "What was run"
    The registry and server below were **open-source MLflow 3.17.2** on a laptop (SQLite registry, `mlflow models serve` on
    `127.0.0.1`), using the models from the previous lesson; outputs are copied from those runs. **Databricks Model Serving,
    the Unity Catalog model registry, serverless endpoints and `ai_query` were not run**; they're described from the Databricks
    documentation (October 2026). Local serving has none of the platform's autoscaling, authentication, monitoring or governance.

## Registering a model

```python
import mlflow
from mlflow import MlflowClient

mlflow.set_tracking_uri("sqlite:///mlflow_demo.db"); mlflow.set_registry_uri("sqlite:///mlflow_demo.db")
c = MlflowClient()
best = mlflow.search_runs(experiment_names=["churn-demo"], order_by=["metrics.auc DESC"]).iloc[0].run_id

mv = mlflow.register_model(f"runs:/{best}/model", "churn_model")        # -> version 1
c.set_registered_model_alias("churn_model", "champion", mv.version)

model = mlflow.pyfunc.load_model("models:/churn_model@champion")        # load by alias, not by number
```

Local results: `registered version 1`; `alias -> 1`; the loaded object was a `PyFuncModel` carrying a signature. Registering a
second run's model created **version 2**, and the `champion` alias **stayed on version 1** until explicitly moved, while a
`challenger` alias could point at version 2:

```text
champion  -> 1
challenger -> 2
```

Concepts:

- A **registered model** (`churn_model`) has numbered, immutable **versions**. Each version points at the logged model files and
  records its source run.
- **Aliases** (`champion`, `challenger`) are mutable named pointers to a version. Consumers load `models:/churn_model@champion`,
  so promoting a new version is a single alias update with no consumer code change, and rollback is moving the alias back.
- **Tags** and descriptions annotate versions (`validated_by = qa-bot` was stored and read back).
- The older **stages** (Staging, Production) in the classic workspace registry are the legacy mechanism; Databricks documentation steers
  new work to Unity Catalog and aliases.

MLflow printed a warning during registration, "Run ... has no artifacts at artifact path 'model', registering model based on
models:/m-... instead": in MLflow 3, `log_model` creates a *Logged Model* entity (with its own `m-...` ID) and the run
refers to it, so registering from the run's path is redirected to the logged model. It worked; it's a sign that the "model is a
file under a run" picture from MLflow 2 is outdated.

### Signatures: contracts you can enforce

The loaded model's signature was:

```text
inputs:  [Tensor('float64', (-1, 12))]
outputs: [Tensor('int64', (-1,))]
```

Passing 5 features instead of 12 raised `MlflowException: Failed to enforce schema ... Shape of input (2, 5) does not match expected shape (-1, 12)`.
The signature was inferred automatically from the input example logged in the previous lesson. For tabular models trained on a DataFrame,
log a **named-column** signature (use a pandas DataFrame input example) so a request with a missing or misspelled column fails
clearly. In my example the inputs were a NumPy array, hence an unnamed tensor.

## Serving over REST (locally)

```bash
export PATH=<venv>/bin:$PATH MLFLOW_TRACKING_URI=sqlite:///mlflow_demo.db MLFLOW_REGISTRY_URI=sqlite:///mlflow_demo.db
mlflow models serve -m "models:/churn_model@champion" --env-manager local -p 5055 --host 127.0.0.1
```

Requests and local responses (two 12-feature rows; then a bad shape):

```text
POST /invocations  {"inputs": [[0.0]*12, [1.0]*12]}            -> 200 {"predictions": [0, 1]}
POST /invocations  {"dataframe_split": {"columns": [...12 names...], "data": [[0.0]*12]}}   -> 200 {"predictions": [0]}
POST /invocations  {"inputs": [[0.0]*5]}                       -> 400 {"error_code": "INVALID_PARAMETER_VALUE",
                       "message": "Failed to predict data ... Shape of input (1, 5) does not match expected shape (-1, 12)."}
GET  /ping                                                      -> 200
```

Two things I hit: my first attempt failed with a launcher error "returned non zero return code. Return code = 127" because
`uvicorn` wasn't on `PATH` (the serve command shells out to it; the virtual environment's `bin` folder needs to be on `PATH`); and
the server needed `--env-manager local` to reuse the current environment instead of building a fresh one, which in
production you'd avoid (building an isolated environment from the model's `requirements.txt` is the reproducible way).

## Databricks Model Serving

Per the documentation, Model Serving deploys models behind a REST API with a managed, serverless, autoscaling service:

- **What can be served**: custom models in MLflow format (scikit-learn, XGBoost, PyTorch, Hugging Face transformers...), agents, and
  foundation models; registered in Unity Catalog (or the legacy workspace registry).
- **Interfaces**: the serving UI, the REST endpoint `POST /serving-endpoints/{name}/invocations`, the MLflow Deployments SDK, and SQL
  via the `ai_query` function (batch inference inside a query). The payload formats documented include `dataframe_split` and
  `dataframe_records` for tabular models and tensor `inputs`: the same shapes the local server accepted.
- **Auth**: a token or OAuth credential, with permission to query the endpoint (look up the exact permission name in the docs); the call looks like
  `curl -X POST -u token:$DATABRICKS_API_TOKEN $ENDPOINT_INVOCATION_URL -H 'Content-Type: application/json' -d '{"dataframe_split": ...}'`
  per the docs' example.
- **Scale**: endpoints autoscale with demand, and you can choose to scale to zero when idle; the docs say scale to zero is
  **not recommended for production endpoints**, because capacity isn't guaranteed and the first request pays a cold-start latency.
- **Traffic splitting** between model versions for canary or A/B rollout is a common serving feature; I did not verify the
  current Databricks mechanism, so check the endpoint configuration docs.
- **Monitoring**: inference logging and metrics export; endpoint request and cost tables in system tables (look them up in the
  system tables docs).
- **Pre-deployment validation** and a debugging guide exist for the common failure of a model whose dependencies don't resolve on the
  serving image.

For **batch** scoring you rarely need an endpoint: load the model as a Spark UDF (`mlflow.pyfunc.spark_udf`) in a job and write
predictions to a table, which is cheaper and simpler than millions of REST calls.

## Governing models in Unity Catalog

Models in Unity Catalog use the same three-level names (`catalog.schema.model`), `CREATE MODEL` on a schema (plus `USE CATALOG`
and `USE SCHEMA`) to register, `EXECUTE` to use, lineage to the training data, and audit logging (Level 3). The docs state that in
MLflow 3 on Databricks the default registry URI is `databricks-uc`, and that creating a serving endpoint for a Unity Catalog model
requires `USE CATALOG`, `USE SCHEMA` and `EXECUTE` on the model. They also say the compute for registering models must use
a Unity Catalog-compatible access mode (dedicated, for ML workloads). I didn't verify these requirements on a workspace.

## A promotion workflow

1. Training job logs candidate versions with metrics and the data version.
2. An **evaluation job** loads the `challenger`, scores a fixed holdout set plus slices, and compares with `champion`.
3. If it passes thresholds, the job sets the `champion` alias to the new version (and tags it with the evaluation run).
4. Serving picks up the alias (or a deployment job updates the endpoint).
5. Rollback is one alias update.
Put steps 1 to 3 in a Lakeflow job (Level 3 · 07) and the endpoint in a bundle (Lesson 01).

## How It Actually Works

The registry is two tables of metadata (registered models; model versions with source, status, tags and aliases) plus the model
files in artifact storage. `models:/name@alias` resolves the alias to a version, the version to a source URI, and loads that artifact.
The local server loads the model once at startup into a scoring app; each request body is parsed into a DataFrame or array,
**validated against the signature**, passed to `predict`, and the result serialized as JSON. Databricks serving wraps the same
MLflow scoring contract in a container that is built from the model's recorded environment, run on managed compute with
authentication in front, which is why the payload shapes match.

## Common mistakes

- **Loading `models:/name/3` by number in production code**, making every promotion a code change. Use aliases.
- **No signature**, so bad inputs fail inside the model with an unreadable error (or silently produce nonsense).
- **Promoting on a single aggregate metric** with no slice checks.
- **Serving a model whose training dependencies aren't pinned** (the container build fails or behaves differently).
- **Using endpoints for batch inference** and paying per-request overhead.
- **Treating `--env-manager local` as production-ready.**
- **Forgetting that registered-model permissions** are Unity Catalog permissions (Level 3 · 02).

## Exercise

1. Re-run the registration locally, register a third version, move `challenger` to it, and use `search_model_versions` to list versions.
   Notice what the last call in my run returned for aliases (`[[], []]`) and find out why it differs from `get_model_version_by_alias`.
2. Log the model with a pandas DataFrame input example so the signature has named columns; call the server with `dataframe_split` using
   a misspelled column and record the error.
3. Write a script that scores a holdout set with `champion` and `challenger` and refuses to move the alias unless AUC improves by at
   least 0.01 on every one of two slices you define.
4. Sketch the Lakeflow job and bundle YAML for the promotion workflow above (don't run it).
