---
description: "Learn Spark DataFrames: lazy transformations vs actions, reading query plans with explain, null semantics, joins, conditionals and why plans get optimized."
---

# 03 · Spark DataFrames: Transformations & Actions

Almost everything on Databricks compiles down to Apache Spark, and the DataFrame is Spark's main abstraction: a
distributed table with named, typed columns. If you understand two ideas, *lazy evaluation* and *the plan*, most
Spark behaviour (including most performance problems) stops being mysterious.

!!! note "What was run"
    All Spark output in this lesson is from **local Spark 4.2.0** (`pyspark` from PyPI, Python 3.12, `local[2]`),
    not from a Databricks cluster. The API is identical; the execution environment, file locations and speed are not.
    Plan text (`explain`) can differ in small details, such as the extra operators Photon adds on Databricks.

## A DataFrame is a plan, not data

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("lab").getOrCreate()   # a notebook already has `spark`

orders = spark.createDataFrame(
    [(1, "2026-01-03", "DE", 120.5, "paid"),
     (2, "2026-01-03", "US",  80.0, "paid"),
     (3, "2026-01-04", "US",  None, "refunded"),
     (4, "2026-01-05", "IN", 300.0, "paid"),
     (5, "2026-01-05", "DE",  45.25, "paid")],
    "id INT, order_date STRING, country STRING, amount DOUBLE, status STRING",
)

paid = (orders
        .withColumn("order_date", F.to_date("order_date"))
        .filter(F.col("status") == "paid")
        .withColumn("amount_eur", F.round(F.col("amount") * 0.92, 2)))

print(type(paid).__name__)     # DataFrame
```

Nothing has been computed. Each call returned a new DataFrame describing "the previous one plus this step".

**Transformations** build the plan and return a DataFrame: `select`, `filter`/`where`, `withColumn`, `groupBy().agg`,
`join`, `orderBy`, `union`, `distinct`. **Actions** force execution and return a result to the driver or write
output: `show`, `collect`, `count`, `first`, `take`, `write.save`, `toPandas`.

```python
paid.select("id", "order_date", "amount_eur").show()
```

```text
+---+----------+----------+
| id|order_date|amount_eur|
+---+----------+----------+
|  1|2026-01-03|    110.86|
|  2|2026-01-03|      73.6|
|  4|2026-01-05|     276.0|
|  5|2026-01-05|     41.63|
+---+----------+----------+
```

## Reading the plan with `explain`

`paid.explain(True)` prints four stages of planning. The last two are the ones to watch (local Spark output, trimmed to
the relevant parts):

```text
== Optimized Logical Plan ==
Project [id#0, cast(order_date#1 as date) AS order_date#5, country#2, amount#3, status#4, round((amount#3 * 0.92), 2) AS amount_eur#6]
+- Filter (isnotnull(status#4) AND (status#4 = paid))
   +- LogicalRDD [id#0, order_date#1, country#2, amount#3, status#4], false

== Physical Plan ==
*(1) Project [id#0, cast(order_date#1 as date) AS order_date#5, ...]
+- *(1) Filter (isnotnull(status#4) AND (status#4 = paid))
   +- *(1) Scan ExistingRDD[id#0,order_date#1,country#2,amount#3,status#4]
```

Read plans **bottom to top**: data enters at the scan, flows through the filter, then the projection. Notice that the
optimizer added `isnotnull(status)` (an equality can never be true for NULL, so it's a free extra filter) and
replaced `to_date(...)` with a plain `cast` because the string was already in `yyyy-MM-dd` form. Those rewrites are
Catalyst, Spark's optimizer, at work. The `*(1)` marks operators fused into one code-generated stage.

## Null semantics: the quiet source of wrong numbers

SQL's three-valued logic applies to DataFrames. Row 3 has a NULL `amount`:

```python
orders.filter(F.col("amount") > 50).count()        # 3
orders.filter(~(F.col("amount") > 50)).count()     # 1  -> the 45.25 row only
orders.filter(F.col("amount").isNull()).count()    # 1
```

The first two counts add up to 4, not 5: the NULL row is in neither `> 50` nor its negation, because `NULL > 50` is
`NULL`, not false. Equality has the same trap, and the null-safe operator `<=>` (`eqNullSafe`) fixes it:

```python
orders.select(F.col("amount") == None, F.col("amount").eqNullSafe(None)).limit(3).show()
```

```text
+---------------+-----------------+
|(amount = NULL)|(amount <=> NULL)|
+---------------+-----------------+
|           NULL|            false|
|           NULL|            false|
|           NULL|             true|
+---------------+-----------------+
```

Aggregates skip NULLs, and `count("*")` differs from `count("col")`:

```python
orders.groupBy("country").agg(
    F.count("*").alias("rows"), F.count("amount").alias("non_null"),
    F.sum("amount").alias("sum"), F.avg("amount").alias("avg")).orderBy("country").show()
```

```text
+-------+----+--------+------+------+
|country|rows|non_null|   sum|   avg|
+-------+----+--------+------+------+
|     DE|   2|       2|165.75|82.875|
|     IN|   1|       1| 300.0| 300.0|
|     US|   2|       1|  80.0|  80.0|
+-------+----+--------+------+------+
```

For `US`, the average is 80.0, not 40.0: the refunded row's missing amount is excluded from both numerator and
denominator. Whether that's what the business wants is a modelling question you have to ask on purpose.

## Conditionals and the NULL fall-through

```python
orders.select("id",
    F.when(F.col("amount") >= 100, "large")
     .when(F.col("amount") >= 50, "medium")
     .otherwise("small").alias("size")).show()
```

```text
+---+------+
| id|  size|
+---+------+
|  1| large|
|  2|medium|
|  3| small|
|  4| large|
|  5| small|
+---+------+
```

Order 3 has *no* amount, yet it is labelled `small`: both `when` conditions evaluated to NULL, which isn't true, so
`otherwise` fired. Handle NULL explicitly (`F.when(F.col("amount").isNull(), "unknown")` first) whenever a missing
value must not be bucketed with real ones.

## Joins

```python
countries = spark.createDataFrame([("DE", "Germany"), ("US", "United States"), ("FR", "France")],
                                  "code STRING, name STRING")
orders.join(countries, orders.country == countries.code, "left")     # keep every order
orders.join(countries, orders.country == countries.code, "left_anti")  # orders with NO matching country
orders.join(countries, orders.country == countries.code, "full")     # everything from both sides
```

Local output for the `left_anti` join is a single row, order 4 (country `IN`, which the lookup table lacks). The `full`
join returns all five orders, with `NULL` in `code`/`name` for order 4, **plus** a sixth row for `FR` with `NULL` order
columns. `left_anti` is the cleanest way to find orphan keys; `left_semi` is its mirror (keep rows that do match,
without adding the other table's columns).

## Window functions in the DataFrame API

```python
from pyspark.sql import Window
w = Window.partitionBy("country").orderBy(F.col("amount").desc())
orders.withColumn("rk", F.row_number().over(w)).filter("rk = 1").select("country", "id", "amount").orderBy("country").show()
```

```text
+-------+---+------+
|country| id|amount|
+-------+---+------+
|     DE|  1| 120.5|
|     IN|  4| 300.0|
|     US|  2|  80.0|
+-------+---+------+
```

"Top row per group" is a window plus a filter, the Spark equivalent of Snowflake's `QUALIFY`. Note that for `US`,
`row_number` puts the NULL amount last in a descending sort by default in Spark (nulls are the smallest values), so the
80.0 order wins as intended. Lesson 04 covers windows in SQL.

## How It Actually Works

When you call an action, Spark runs a pipeline:

1. **Parse** your DataFrame calls into an *unresolved logical plan*.
2. **Analyze**: resolve column names and types against the schema (this is why a typo'd column fails *at the action
   or the next transformation*, not at import time).
3. **Optimize** (Catalyst): push filters toward the scan, prune unused columns, fold constants, reorder joins, simplify
   expressions.
4. **Physical planning**: pick concrete operators (broadcast vs sort-merge join, hash vs sort aggregate).
5. **Execution**: split the plan into **stages** at shuffle boundaries, split each stage into **tasks** (one per data
   partition), and run tasks on executor cores. Adaptive Query Execution can re-plan between stages (Level 2 · 05).

Laziness is what lets step 3 exist: because Spark sees the whole chain before running it, it can drop columns you never
use and apply filters before expensive joins. It also means that **calling an action twice runs the plan twice**
unless you `cache()` or save an intermediate result.

## Common mistakes

- **`collect()` on a large DataFrame.** It brings every row to the driver. Use `limit`, `take` or write the result.
- **Row-by-row Python loops** over `collect()` output, or Python UDFs where a built-in function exists. Built-ins stay
  inside the optimized engine; Python UDFs serialize every row out to a Python worker.
- **Forgetting NULL handling** in filters, `when` chains and averages, as shown above.
- **Expecting `show()` to be cheap.** It's an action. `df.count()` followed by `df.show()` computes the plan twice.
- **`withColumn` in a loop** for dozens of columns builds an enormous plan. Use one `select` with a list of
  expressions, or `withColumns({...})`.
- **Comparing with `== None`** instead of `isNull()` or `eqNullSafe`.

## Exercise

1. Using the `orders` DataFrame, compute revenue per country for `paid` orders only, rounded to one decimal. Predict
   the output, then run it locally.
2. Add a column `size` that labels missing amounts `unknown`. Verify order 3 no longer says `small`.
3. Run `.explain(True)` on `orders.filter(F.col("country") == "US").select("id")`. Compare the analyzed and optimized
   plans: what extra predicate appears on `country`, and which operator order differs from how you wrote the code?
   (Lesson 05 shows column pruning on a real file scan, which an in-memory DataFrame like this one can't demonstrate.)
4. Join `orders` to `countries` so that every order appears exactly once, with `Unknown` for missing country names.
   (Use `coalesce`.)
