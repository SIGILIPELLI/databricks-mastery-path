---
description: "Test PySpark and Delta code without a cluster: pure transforms, a pytest fixture, assertDataFrameEqual, a caught bug and what local tests miss."
---

# 02 · Testing PySpark & Delta Code Locally

Notebook code that was only ever "tested" by looking at a `display()` is a liability. The good news from Level 1 is that
the same Spark runs on a laptop, so ordinary unit tests can cover most transformation logic in seconds, with no cluster, no
workspace and no cloud bill. This lesson sets up that project and is honest about what such tests cannot prove.

!!! note "What was run"
    The project and tests below were executed with **pytest on local Spark 4.2.0** (Python 3.12); the pass count and the failure
    message are copied from those runs. Anything that depends on the Databricks runtime, Unity Catalog, serverless compute, Photon,
    Auto Loader or Lakeflow semantics is outside what these tests can see (see the last section).

## Structure code so it can be tested

The single most useful habit: **keep transformations as pure functions** that take DataFrames (and plain parameters) and return a
DataFrame, with no reading, writing, `spark.sql` against catalogs, or `dbutils` inside them. I/O goes in a thin "main"
notebook or script that reads, calls the function and writes. That thin layer is covered by an integration test on the platform
(Lesson 01's staging target); the logic is covered by fast local tests.

```text
tproj/
├── src/orders/transforms.py     # pure functions
└── tests/
    ├── conftest.py              # a shared SparkSession fixture
    └── test_transforms.py
```

`src/orders/transforms.py`:

```python
"""Pure DataFrame-in, DataFrame-out functions: easy to test, no I/O, no global state."""
from pyspark.sql import DataFrame, Window, functions as F


def clean_orders(df: DataFrame) -> DataFrame:
    """Type the raw columns; unparseable or negative amounts are returned in the 'rejects' flag instead of failing."""
    return (df.withColumn("order_id", F.expr("try_cast(order_id AS INT)"))
              .withColumn("amount", F.expr("try_cast(amount AS DECIMAL(10,2))"))
              .withColumn("is_valid", F.col("order_id").isNotNull() & F.col("amount").isNotNull() & (F.col("amount") >= 0)))


def latest_per_key(df: DataFrame, key: str, order_col: str) -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def revenue_by_customer(df: DataFrame) -> DataFrame:
    return (df.where(F.col("status") == "ok")
              .groupBy("customer").agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders")))
```

`tests/conftest.py` creates one Spark session for the whole test run (starting Spark costs seconds; creating one per test would
make the suite crawl):

```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests").config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()
```

`tests/test_transforms.py`:

```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
from decimal import Decimal
import pytest
from pyspark.testing import assertDataFrameEqual
from orders.transforms import clean_orders, latest_per_key, revenue_by_customer


def test_clean_orders_flags_bad_rows(spark):
    raw = spark.createDataFrame([("1", "10.50"), ("x", "5"), ("3", "-4"), ("4", "abc")], "order_id string, amount string")
    got = clean_orders(raw).select("order_id", "is_valid")
    want = spark.createDataFrame([(1, True), (None, False), (3, False), (4, False)], "order_id int, is_valid boolean")
    assertDataFrameEqual(got, want)


def test_latest_per_key_keeps_newest(spark):
    df = spark.createDataFrame([(1, "old", 1), (1, "new", 2), (2, "only", 1)], "id int, v string, ts int")
    got = latest_per_key(df, "id", "ts").select("id", "v")
    assertDataFrameEqual(got, spark.createDataFrame([(1, "new"), (2, "only")], "id int, v string"))


def test_revenue_ignores_refunds(spark):
    df = spark.createDataFrame([("ana", Decimal("10.00"), "ok"), ("ana", Decimal("5.00"), "refund"), ("bo", Decimal("2.50"), "ok")],
                               "customer string, amount decimal(10,2), status string")
    got = revenue_by_customer(df)
    want = spark.createDataFrame([("ana", Decimal("10.00"), 1), ("bo", Decimal("2.50"), 1)], "customer string, revenue decimal(20,2), orders bigint")
    assertDataFrameEqual(got, want, checkRowOrder=False)


def test_empty_input_does_not_crash(spark):
    empty = spark.createDataFrame([], "customer string, amount decimal(10,2), status string")
    assert revenue_by_customer(empty).count() == 0


def test_latest_per_key_tie_is_a_bug_magnet(spark):
    """Documenting a real hazard: with equal timestamps the winner is not defined, so the result may vary."""
    df = spark.createDataFrame([(1, "a", 1), (1, "b", 1)], "id int, v string, ts int")
    assert latest_per_key(df, "id", "ts").count() == 1     # we can assert the count, but not which row survives
```

Running `pytest -q` from the project folder with `PYSPARK_PYTHON` set (Level 1 · 10):

```text
.....                                                                    [100%]
5 passed in 9.27s
```

Five tests in about nine seconds, most of it Spark's start-up. Adding fifty more tests costs little, because the session is shared.

## `assertDataFrameEqual`

PySpark ships `pyspark.testing.assertDataFrameEqual` (and `assertSchemaEqual`). It compares schema and rows, ignores row
order unless you ask (`checkRowOrder=True`), and produces a diff. Here is what it reported when I deliberately introduced the
classic bug of forgetting the `status = 'ok'` filter in `revenue_by_customer`:

```text
[DIFFERENT_ROWS] Results do not match: ( 100.00000 % )
*** actual ***
! Row(customer='ana', revenue=Decimal('15.00'), orders=2)

*** expected ***
! Row(customer='ana', revenue=Decimal('10.00'), orders=1)
```

(The colour codes in the terminal output are removed.) That is precisely why tests are written against known tiny inputs: the
failure tells you what the code did wrong, at a glance.

Details worth knowing:

- **Types are part of equality.** The expected frame declares `revenue decimal(20,2)`; Spark widened `decimal(10,2)` sums to
  `decimal(20,2)`. A test written with the wrong decimal precision fails even when the numbers agree, which is a good thing: schema
  drift is a bug.
- **Floating point**: use `rtol`/`atol` arguments for doubles, or better, use `DECIMAL`.
- **Row order** is not defined in Spark; do not assert on it unless the code sorts.

## What to test

1. **The happy path** with a handful of rows.
2. **Edge cases** that real data produces: NULLs, empty input (the empty-DataFrame test above), duplicates, the boundary of a
   filter (`amount = 0`), a single row, unicode strings, timestamps around daylight-saving shifts.
3. **Data-quality rules** as functions: `clean_orders` returns a flag instead of failing, and the test pins the exact
   behaviour for `'x'`, `'-4'` and `'abc'`.
4. **Properties** where examples are awkward: "output rows never exceed input rows", "sum of revenue before = sum after".
5. **Known hazards written down as tests.** The last test documents that `latest_per_key` with tied timestamps keeps one row but
   *which* row is undefined (Level 1 · 04's tie trap). A test that asserts only what is guaranteed is honest, and a reader sees the
   hazard.

## Testing Delta behaviour locally

Delta-specific code (`MERGE`, schema evolution, CDF) can be tested with open-source Delta on local Spark, using a temp directory
per test (`tmp_path` fixture) and the `configure_spark_with_delta_pip` builder from Level 1. For example, a merge function can be
tested by creating a table in `tmp_path`, running the function twice and asserting the second run changed nothing (idempotence,
Level 2 · 10). Be aware that open-source Delta differs from the Databricks runtime in defaults and available features,
so a local pass says "the logic is right", not "it will behave identically on the platform".

## Mocking the platform

`dbutils`, `spark.table("catalog.schema.t")` against Unity Catalog and secrets don't exist locally. Options: pass the table names
and DataFrames in as parameters; wrap platform calls in a small adapter class you can fake in tests; and avoid importing
`dbutils` at module level. If you need `dbutils` in a library, the Databricks SDK and **Databricks Connect** can run Spark code from
your IDE against a real remote compute resource (documented; not run here), which is useful for integration-style tests but needs a
workspace and bills compute.

## What local tests cannot prove

- **Performance and scale**: ten rows say nothing about skew, shuffles or memory (Level 2 · 05 and 06).
- **Platform behaviour**: Unity Catalog permissions, serverless library restrictions, Photon differences, Auto Loader semantics.
- **Concurrency and failure recovery**: retries, conflicts, partial failures.
- **Real data shapes**: your production data will contain cases you didn't invent. Feed anonymized samples or property-based
  generators into tests.

So the full pyramid is: many fast local unit tests, some integration tests on a staging target with real catalogs and realistic
data volumes, and monitoring in production (Level 4 · 06 and 08). CI runs the first two (Lesson 01).

## How It Actually Works

pytest collects the test functions, builds the session-scoped `spark` fixture once, and injects it into each test by name. Each
test constructs tiny DataFrames with `createDataFrame` (in-memory, schema-declared), applies the function under test (which only
builds a plan), and `assertDataFrameEqual` triggers actions on both sides, collects both results to the driver, and compares
schema and rows. Because `local[2]` runs Spark in one process, nothing leaves your machine and failures give normal Python
stack traces.

## Common mistakes

- **A new SparkSession per test** (slow) or sharing mutable global state between tests.
- **Tests that read production tables**: slow, flaky, expensive, and risky.
- **Asserting on `show()` output** or on row order.
- **Skipping the empty and NULL cases** that cause most production incidents.
- **Putting logic in notebooks only**: unit-testable code belongs in a package or files imported by notebooks (Git folders and
  bundles make this straightforward).
- **Trusting a green local suite** as proof the platform job works.

## Exercise

1. Add tests for `clean_orders` with an amount of `"0"`, `"1e3"` and a 9-digit amount that overflows `decimal(10,2)`. Decide what
   the function should do and encode it.
2. Fix the tie hazard: add a deterministic tie-breaker parameter to `latest_per_key` and a test proving the winner is stable.
3. Write a Delta test (using the Level 1 builder) that merges a batch into a table twice and asserts the table is unchanged after
   the second run.
4. Add a GitHub Actions job (Lesson 01) that runs `pytest` on every pull request. What must the runner have installed for Spark to
   start?
