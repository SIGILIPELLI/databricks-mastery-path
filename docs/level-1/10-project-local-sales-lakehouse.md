---
description: "Level 1 project: build a bronze/silver/gold sales lakehouse locally, quarantine bad rows, answer questions in SQL, then roll back a bad delete."
---

# 10 · Project — A Sales Lakehouse You Can Run Locally

You'll build a miniature lakehouse end to end on your laptop: ingest a messy CSV, quarantine bad rows, clean the rest
into a typed Delta table, build an aggregate, answer questions in SQL, then make a mistake and recover from it with time
travel. It uses only what Level 1 taught: explicit schemas, parse modes, Delta tables, SQL windows and `RESTORE`.

!!! note "What was run"
    The script below was run exactly as shown on **local Spark 4.2.0 + Delta Lake 4.4.1** (Python 3.12), and every
    output block in this lesson is copied from that run. The data is generated with a fixed random seed, so you should
    get the same counts; the revenue figures depend on Python's seeded generator and are not meant to represent any real
    business. On Databricks you'd swap the local folder for a Unity Catalog schema and the landing file for a volume.

## Setup

```bash
python3.12 -m venv .venv && .venv/bin/pip install pyspark delta-spark
export PYSPARK_PYTHON=$PWD/.venv/bin/python      # workers must use the same Python as the driver
mkdir lab && cd lab
```

(Java 17 or newer must be installed. Without `PYSPARK_PYTHON`, a mismatch between the driver's and the workers' Python
minor version stops the job with `PYTHON_VERSION_MISMATCH`, which happened during development on a machine with two
Pythons.)

## The task

The landing file has 200 orders plus four deliberate problems:

| Problem | Planted as |
|---|---|
| Missing price | order 10 has an empty `unit_price` |
| Impossible date | order 20 has date `2026-13-45` |
| Exact duplicate | order 5 appears twice |
| Unknown country | order 999 has country `XX` |

Requirements: (1) corrupt rows go to a quarantine table, never silently dropped; (2) the silver table has no
duplicates, no missing prices and only known countries; (3) a gold table gives revenue per country; (4) answer three
questions; (5) delete a country by mistake and restore it.

## The script

```python
"""Level 1 project: a small sales lakehouse, runnable locally with pyspark + delta-spark."""
import csv, random, shutil, pathlib
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip

ROOT = pathlib.Path("lakehouse"); shutil.rmtree(ROOT, ignore_errors=True); (ROOT / "landing").mkdir(parents=True)

# 1. Generate a landing file with planted problems (seeded, so everyone gets the same data)
random.seed(7)
countries = ["DE", "US", "IN", "BR"]
rows = []
for i in range(1, 201):
    rows.append([i, f"2026-03-{1 + i % 28:02d}", f"C{random.randint(1, 40):03d}", random.choice(countries),
                 random.choice(["book", "pen", "lamp", "desk"]), random.randint(1, 5), round(random.uniform(3, 120), 2)])
rows[9][6] = ""                    # missing price
rows[19][1] = "2026-13-45"         # impossible date
rows.append(rows[4][:])            # exact duplicate order
rows.append([999, "2026-03-05", "C001", "XX", "pen", 2, 4.0])   # unknown country code
with open(ROOT / "landing/orders.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["order_id", "order_date", "customer", "country", "item", "qty", "unit_price"]); w.writerows(rows)

builder = (SparkSession.builder.master("local[2]").appName("proj1")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false")
           .config("spark.sql.warehouse.dir", str(ROOT / "warehouse")))
spark = configure_spark_with_delta_pip(builder).getOrCreate(); spark.sparkContext.setLogLevel("ERROR")

# 2. Raw -> bronze: explicit schema, keep bad rows
schema = ("order_id INT, order_date DATE, customer STRING, country STRING, item STRING, qty INT, "
          "unit_price DOUBLE, _bad STRING")
raw = (spark.read.option("header", True).schema(schema).option("columnNameOfCorruptRecord", "_bad")
       .csv(str(ROOT / "landing/orders.csv")).cache())
print("raw rows:", raw.count())
raw.filter("_bad IS NOT NULL").select("_bad").show(truncate=False)
raw.filter("_bad IS NULL").drop("_bad").write.format("delta").mode("overwrite").saveAsTable("bronze_orders")
raw.filter("_bad IS NOT NULL").select("_bad").write.format("delta").mode("overwrite").saveAsTable("quarantine_orders")

# 3. Silver: dedupe, validate, enrich
countries_df = spark.createDataFrame([("DE", "Germany"), ("US", "United States"), ("IN", "India"), ("BR", "Brazil")],
                                     "code STRING, country_name STRING")
silver = (spark.table("bronze_orders").dropDuplicates()
          .filter(F.col("unit_price").isNotNull() & (F.col("qty") > 0))
          .join(countries_df, F.col("country") == F.col("code"), "inner").drop("code")
          .withColumn("revenue", F.round(F.col("qty") * F.col("unit_price"), 2)))
silver.write.format("delta").mode("overwrite").saveAsTable("silver_orders")
print("bronze:", spark.table("bronze_orders").count(), "silver:", spark.table("silver_orders").count(),
      "quarantine:", spark.table("quarantine_orders").count())

# 4. Gold + analytics
spark.sql("""CREATE OR REPLACE TABLE gold_revenue_by_country USING delta AS
  SELECT country_name, COUNT(*) AS orders, ROUND(SUM(revenue),2) AS revenue FROM silver_orders GROUP BY country_name""")
spark.sql("SELECT * FROM gold_revenue_by_country ORDER BY revenue DESC").show()
spark.sql("""SELECT item, SUM(qty) units FROM silver_orders GROUP BY item ORDER BY units DESC""").show()
# top customer per country
spark.sql("""WITH spend AS (SELECT country_name, customer, ROUND(SUM(revenue),2) AS spend FROM silver_orders GROUP BY country_name, customer)
  SELECT * FROM spend QUALIFY ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY spend DESC, customer) = 1
  ORDER BY country_name""").show()

# 5. History + time travel
spark.sql("DESCRIBE HISTORY silver_orders").select("version", "operation").show()
spark.sql("DELETE FROM silver_orders WHERE country_name = 'Brazil'")
print("after delete:", spark.table("silver_orders").count(),
      "| version 0:", spark.sql("SELECT * FROM silver_orders VERSION AS OF 0").count())
spark.sql("RESTORE TABLE silver_orders TO VERSION AS OF 0")
print("after restore:", spark.table("silver_orders").count())

# 6. Self-check
assert spark.table("silver_orders").groupBy("order_id").count().filter("count > 1").count() == 0
assert spark.table("silver_orders").count() == 198
assert spark.table("quarantine_orders").count() == 1
print("checks passed")
```

## Expected output

The counts are the key check. 202 raw lines were read (200 orders + the duplicate + order 999), and exactly one was
unparseable:

```text
raw rows: 202
|_bad                             |
|20,2026-13-45,C029,IN,desk,3,5.64|

bronze: 201 silver: 198 quarantine: 1
```

Reconcile them: 202 lines, minus 1 quarantined = 201 in bronze. Silver removes the duplicate (200), the missing-price
order (199) and the `XX` country (198). **Every row is accounted for.** Note that the missing-price row *wasn't*
quarantined: an empty field is a valid CSV null, so it reached bronze and was filtered in silver. Whether that row
should be quarantined too is a design choice; write it down.

```text
+-------------+------+--------+
| country_name|orders| revenue|
+-------------+------+--------+
|        India|    52|11121.09|
|       Brazil|    51|10171.84|
|United States|    49| 9544.12|
|      Germany|    46| 8515.32|
+-------------+------+--------+

+----+-----+
|item|units|
+----+-----+
|lamp|  166|
| pen|  149|
|desk|  147|
|book|  139|
+----+-----+

+-------------+--------+-------+
| country_name|customer|  spend|
+-------------+--------+-------+
|       Brazil|    C007|1379.64|
|      Germany|    C019| 968.71|
|        India|    C031|1000.03|
|United States|    C015|  834.8|
+-------------+--------+-------+
```

And the recovery:

```text
|version|operation              |
|0      |CREATE OR REPLACE ...  |     (operation name truncated in the console; this was printed before the DELETE)
after delete: 147 | version 0: 198
after restore: 198
checks passed
```

Deleting Brazil dropped silver from 198 to 147 rows (51 Brazilian orders gone); `VERSION AS OF 0` still saw 198; and
`RESTORE` brought them back as a new commit.

## Things the project is quietly teaching

1. **Explicit schema + corrupt-record column** is what made the quarantine possible. With `inferSchema`, order 20's
   date would have turned the whole column into strings.
2. **Window + QUALIFY needs a tie-breaker.** The top-customer query orders by `spend DESC, customer` so ties can't
   change the answer between runs. My first attempt put `SUM(revenue)` inside the window of a `GROUP BY` query and Spark
   raised `UNRESOLVED_COLUMN`; computing the sums in a CTE first is the portable pattern.
3. **A plain `CREATE OR REPLACE TABLE ... AS SELECT` on local Spark failed** with `UNSUPPORTED_FEATURE.TABLE_OPERATION`
   until I added `USING delta`: the local default table provider isn't Delta. Databricks defaults to Delta, so you can
   usually omit it there, but being explicit never hurts.
4. **Reconciliation is a test.** Row counts at every layer, adding up, is the cheapest data-quality check there is.

## How It Actually Works

Each `saveAsTable` or `CREATE OR REPLACE TABLE` above is a Delta commit (a `WRITE` or `CREATE OR REPLACE TABLE AS
SELECT` action in the log), and each table has its own folder under the local `warehouse` directory with Parquet files
and a `_delta_log`. `dropDuplicates()` triggers a shuffle (rows are hashed by all columns so duplicates land on one
task); the join to the tiny countries DataFrame is a candidate for a broadcast join, which Spark chooses for small inputs; I did not inspect this run's plan, and Level 2 · 06 shows how to check.
`RESTORE` didn't copy data back: it re-added the file entries that the `DELETE` had removed from the snapshot, which is
possible only because the files hadn't been vacuumed.

## Extensions

- Add a `silver_customers` dimension and a gold "revenue by month" table. Which columns need `DECIMAL`?
- Re-run the script twice. What happens to row counts? (`mode("overwrite")` makes this one idempotent. Would `append`?)
- Write a `check.py` that asserts the reconciliation identity and the absence of duplicates, and exits non-zero on failure.

## Common mistakes

- **Running without `PYSPARK_PYTHON`** and getting `PYTHON_VERSION_MISMATCH` from the workers.
- **Using `inferSchema`** so a bad date silently turns a whole column into strings.
- **Dropping bad rows** instead of quarantining them, so the counts can no longer be reconciled.
- **`CREATE OR REPLACE TABLE ... AS` without `USING delta`** on a local Spark whose default provider isn't Delta.
- **Putting an aggregate inside a window in the same `GROUP BY` query** instead of aggregating in a CTE first.
- **Restoring after a `VACUUM`**: `RESTORE` needs the old files to still exist.

## Exercise

1. Run the project and confirm your counts equal 202 / 201 / 198 / 1.
2. Quarantine missing-price rows as well, so quarantine becomes 2 and silver stays 198. What changes in the script?
3. Change the seed to 8. Which numbers must stay the same (and why), and which change?
4. Add a second planted problem (a negative quantity) and make the pipeline handle it without losing track of the row.
