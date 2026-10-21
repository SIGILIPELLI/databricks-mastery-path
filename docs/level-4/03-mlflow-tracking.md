---
description: "Track ML experiments with MLflow: runs, params, metrics and models, comparing runs, reloading a model, and Databricks hosting; run locally."
---

# 03 · MLflow: Tracking Experiments

Machine learning code produces many near-identical experiments, and "which settings gave that 0.96 AUC last Tuesday?" is
a question people regularly fail to answer. **MLflow** is the open-source standard for recording the answer, and
Databricks provides a managed MLflow integrated with Unity Catalog. The tracking API is the same in both places, which means
you can learn and test it locally.

!!! note "What was run"
    Everything in the "runnable" sections used **open-source MLflow 3.17.2 with scikit-learn 1.9.1** on a laptop, tracking to a local SQLite
    file; output blocks are copied from that run. The data is synthetic (`make_classification`), so the AUC values only illustrate
    the workflow and say nothing about any real problem. Databricks-specific behaviour (managed tracking server, notebook
    experiments, autologging, Unity Catalog model registry) is from the Databricks MLflow documentation (October 2026) and was
    not run.

## Concepts

- An **experiment** groups related runs (for example "churn-demo").
- A **run** is one execution: its **parameters** (inputs such as `C=0.01`), **metrics** (numbers such as `auc`, optionally logged over
  time), **tags** (free labels), and **artifacts** (files: plots, the serialized model, input examples).
- A **model** logged to a run can later be registered, loaded and served (next lesson).
- The **tracking store** holds run metadata (a database or the Databricks backend); the **artifact store** holds the files.

## Tracking four runs

```python
import mlflow, numpy as np, os, warnings
warnings.filterwarnings("ignore")
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, accuracy_score

mlflow.set_tracking_uri("sqlite:///mlflow_demo.db")
mlflow.set_experiment("churn-demo")
X, y = make_classification(n_samples=4000, n_features=12, n_informative=5, weights=[0.8, 0.2], random_state=7)
Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.25, random_state=7, stratify=y)

def run(name, model, params):
    with mlflow.start_run(run_name=name) as r:
        mlflow.log_params(params); mlflow.set_tag("team", "growth")
        model.fit(Xtr, ytr)
        p = model.predict_proba(Xte)[:, 1]
        mlflow.log_metric("auc", roc_auc_score(yte, p)); mlflow.log_metric("accuracy", accuracy_score(yte, p > 0.5))
        mlflow.sklearn.log_model(model, name="model", input_example=Xte[:2], skops_trusted_types=["sklearn.tree._tree.Tree"] if params["model"] == "rf" else None)
        return r.info.run_id

ids = {}
for C in (0.01, 1.0):
    ids[f"logreg_C{C}"] = run(f"logreg_C{C}", LogisticRegression(C=C, max_iter=500), {"model": "logreg", "C": C})
for d in (3, 12):
    ids[f"rf_depth{d}"] = run(f"rf_depth{d}", RandomForestClassifier(n_estimators=100, max_depth=d, random_state=7), {"model": "rf", "max_depth": d})

df = mlflow.search_runs(experiment_names=["churn-demo"], order_by=["metrics.auc DESC"])
print(df[["tags.mlflow.runName", "params.model", "metrics.auc", "metrics.accuracy"]].round(4).to_string(index=False))
best = df.iloc[0]["run_id"]; print("best run:", df.iloc[0]["tags.mlflow.runName"])
m = mlflow.sklearn.load_model(f"runs:/{best}/model"); print("reloaded model predicts:", m.predict(Xte[:5]), "truth:", yte[:5])
print(sorted(os.listdir("mlruns"))[:3] if os.path.exists("mlruns") else "no mlruns dir")
art = [r for r in os.walk(".") if r[0].endswith("/artifacts") or "/artifacts" in r[0]]
print("artifact files for one run:", sorted({f for _, _, fs in art[:6] for f in fs})[:8])
```

Output of the run:

```text
tags.mlflow.runName params.model  metrics.auc  metrics.accuracy
         rf_depth12           rf       0.9600             0.928
          rf_depth3           rf       0.8671             0.839
       logreg_C0.01       logreg       0.8467             0.838
        logreg_C1.0       logreg       0.8447             0.848
best run: rf_depth12
reloaded model predicts: [1 0 0 0 0] truth: [1 0 0 0 0]
artifact files for one run: ['MLmodel', 'conda.yaml', 'input_example.json', 'model.skops', 'python_env.yaml', 'requirements.txt', 'serving_input_example.json']
```

What happened:

1. `set_tracking_uri("sqlite:///...")` chose a local SQLite database as the tracking store (the model files still went to a local `./mlruns` folder: the run
   listing printed `['1']`, the experiment's artifact directory). On Databricks the URI is `databricks` and you don't set it.
2. Each `start_run` logged parameters, two metrics, a tag and the model. `search_runs` returns a pandas DataFrame, here sorted by AUC,
   which is the comparison table you'd otherwise keep in a spreadsheet. Random forest with depth 12 won on this synthetic data,
   and deeper trees beating shallow ones is just a property of this made-up dataset.
3. The model was **reloaded from the run** with `runs:/<run_id>/model` and predicted the first five test labels correctly. The artifact
   folder contains the serialized model plus `MLmodel` (the flavor and signature metadata), environment files
   (`conda.yaml`, `python_env.yaml`, `requirements.txt`) and an input example. Those environment files are what make a model
   reproducible elsewhere.

### A security finding worth knowing

My first run failed when logging the random forest:

```text
mlflow.exceptions.MlflowException: The saved sklearn model references untrusted types. If you are sure loading these types is safe,
set the 'skops_trusted_types' parameter when calling 'log_model' or 'save_model' to the list of trusted types. Root error: Untrusted
types found in the file: ['sklearn.tree._tree.Tree'].
```

In this MLflow version the default scikit-learn serialization is the **skops** format (`model.skops` above), not pickle. Pickle files can
execute arbitrary code when loaded, whereas skops refuses types it hasn't been told to trust. The tree-based models contain
`sklearn.tree._tree.Tree`, and the error text explains that a malicious file could set out-of-range node indices that crash or read
out-of-bounds memory when `predict` is called. I passed `skops_trusted_types=["sklearn.tree._tree.Tree"]` for the random forest
only, after reading that explanation, and the logistic regression needed no allowance. The general rule: **loading a model is
executing someone else's artifact**, so treat model files like code, and trust only types you have reviewed. This is an
MLflow 3.x behaviour as observed here; behaviour in other versions may differ.

## Autologging

Instead of calling `log_param` yourself, `mlflow.autolog()` instruments supported libraries (scikit-learn, XGBoost, PyTorch,
Spark ML and others) to record parameters, metrics and the model automatically. Per the Databricks documentation, when you attach
an interactive Python notebook to a cluster, Databricks Autologging calls `mlflow.autolog()` for you (generally available with
Databricks Runtime 10.4 LTS ML or above), so training sessions in notebooks are tracked without code. The convenience has a cost:
you log whatever the integration decides to, so explicitly log business-specific things (the data version, the git commit,
the training table's Delta version) yourself.

## MLflow on Databricks

From the documentation:

- **Experiments** are either *workspace experiments* (created in the UI/API, usable by any notebook by name or ID) or *notebook
  experiments* (created automatically when you start a run with no active experiment). Experiment names are workspace paths such
  as `/Users/you@example.com/churn-demo`.
- Tracking is a **managed service**: no server to run, runs appear in the Experiments UI and are governed by workspace permissions.
- **MLflow 3** adds *Logged Models* and *Deployment Jobs*, and extends tracking to GenAI: traces, evaluation, prompt management.
- The **model registry** lives in Unity Catalog (next lesson), so models get the same three-level names, grants, lineage and audit
  as tables.
- Stale experiments are purged from workspaces (a GDPR-related policy noted in the docs), so do not treat the tracking store as
  your only record of a model you must keep. Register models you need.

A typical Databricks notebook snippet (documented pattern; not run):

```python
import mlflow
mlflow.set_registry_uri("databricks-uc")          # Unity Catalog as the model registry
mlflow.set_experiment("/Users/you@example.com/churn-demo")
with mlflow.start_run():
    ...
    mlflow.log_param("training_table_version", 42)
```

## What to log (and why)

| Log | Why |
|---|---|
| Hyperparameters and the random seed | Reproduce the run |
| Training data identity: table name **and Delta version or snapshot timestamp** | "Same code, different data" explains most unreproducible results (use time travel, Level 1 · 08) |
| Code version (git SHA; Databricks records it for notebooks in Git folders) | Know which code produced the model |
| Metrics on a held-out set, and on slices that matter (per region) | A single AUC hides failures on sub-populations |
| Model signature and input example | Serving validates inputs against the signature |
| Plots and confusion matrices as artifacts | Faster review |

## How It Actually Works

The tracking client is a thin wrapper that talks to a tracking store. `start_run` creates a run row with a generated ID;
`log_param` and `log_metric` insert rows (a metric row also stores a timestamp and a step, which is how training curves work);
`log_model` serializes the model into a temporary folder and uploads the folder to the run's artifact location, then records a
reference. `search_runs` is a query against the store with a small filter language (`metrics.auc > 0.9 and params.model = 'rf'`)
that returns a DataFrame. `runs:/<run_id>/model` is a URI the loader resolves to that artifact folder.

On Databricks the store is the managed backend and the artifact location is cloud storage governed by the platform; for models in
Unity Catalog, versions are stored in the schema's managed storage (Level 3 · 01), which is why registered models inherit the
catalog's permissions.

## Common mistakes

- **Not logging the data version**, then being unable to explain a metric change.
- **Comparing runs on different test sets** or after tuning on the test set.
- **Logging only the best run**, losing the record of what didn't work.
- **Treating autologging as complete.**
- **Loading models from untrusted sources** (serialization is code execution).
- **Keeping the only copy of a model in an experiment** that may be purged.
- **Huge artifacts per run** that bloat storage: log what you need.

## Exercise

1. Run the script. Then add a `HistGradientBoostingClassifier` run and a metric logged *per step* (`step=` argument) for a model that
   trains iteratively. View it however you like.
2. Use `mlflow.search_runs(filter_string="metrics.auc > 0.85 and params.model = 'rf'")` and print the run names.
3. Add the training data identity (a hash of `Xtr`) as a tag, and demonstrate that two runs on different seeds have different tags.
4. Replace the skops trust parameter with `serialization_format="cloudpickle"` for the random forest if your MLflow version supports it,
   and write two sentences on the trade-off against the skops approach.
