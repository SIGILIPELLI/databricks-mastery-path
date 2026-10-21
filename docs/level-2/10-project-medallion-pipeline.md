---
description: "Level 2 project: an incremental bronze/silver/gold pipeline with streaming ingestion, MERGE, quarantine, late data and reconciliation, run locally."
---

# 10 · Project — A Bronze/Silver/Gold Pipeline

This project pulls Level 2 together: files land in a folder, a stream ingests them to bronze, a `foreachBatch` merge
maintains a typed and deduplicated silver table with a quarantine for rejects, and a gold table aggregates daily revenue.
You'll run it across two "days" of data that include a duplicate, malformed rows, a correction and a late event, then
re-run it with no new data to prove it is idempotent.

!!! note "What was run"
    The script below was executed as shown on **local Spark 4.2.0 + Delta Lake 4.4.1** (Python 3.12), and the outputs in
    this lesson are copied from that run. The data is synthetic (seeded random numbers). On Databricks you'd replace
    `readStream.json(LAND)` with Auto Loader on a volume (Lesson 08), local table names with Unity Catalog names, and the
    hand-written steps with a Lakeflow pipeline if you prefer declarative code (Level 3 · 06). Those substitutions were
    not run.

## The task

1. **Bronze** `bronze_events`: ingest JSON files incrementally; keep every field as a string; add `_src_file` and
   `_ingested_at`; append only.
2. **Silver** `silver_events`: typed (`INT`, `DECIMAL(10,2)`, `TIMESTAMP`), one row per `event_id` (latest ingestion wins),
   updated by `MERGE`. Rows that can't be typed or have negative amounts go to `quarantine_events` with a reason.
3. **Gold** `gold_daily_revenue`: per day and customer: order count, revenue (status `ok`), refunds.
4. Run **day 1** (40 events + 1 duplicate + 1 non-numeric amount + 1 negative amount), then **day 2** (20 new events + a
   correction of event 5 to 99.99 + a late event for day 1), then **re-run with no new files**.

## The script

```python
"""Level 2 project: incremental medallion pipeline (local Spark + Delta). Run twice-over with new files to see incrementality."""
import json, pathlib, shutil, random
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip, DeltaTable

ROOT = pathlib.Path("medallion"); shutil.rmtree(ROOT, ignore_errors=True); (ROOT / "landing").mkdir(parents=True)
b = (SparkSession.builder.master("local[2]").appName("proj2")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
     .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false")
     .config("spark.sql.warehouse.dir", str(ROOT / "wh")))
spark = configure_spark_with_delta_pip(b).getOrCreate(); spark.sparkContext.setLogLevel("ERROR")
LAND = str(ROOT / "landing"); CK = ROOT / "checkpoints"

def drop_file(n, events):
    with open(f"{LAND}/events_{n}.json", "w") as f:
        for e in events: f.write(json.dumps(e) + "\n")

random.seed(11)
def make(first_id, n, day):
    out = []
    for i in range(first_id, first_id + n):
        out.append({"event_id": i, "customer": f"C{random.randint(1,6):02d}", "amount": str(round(random.uniform(5, 90), 2)),
                    "event_ts": f"{day}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00", "status": random.choice(["ok", "ok", "ok", "refund"])})
    return out

# --- BRONZE: raw, all strings, append-only, plus lineage columns
spark.sql("CREATE TABLE IF NOT EXISTS bronze_events (event_id STRING, customer STRING, amount STRING, event_ts STRING, status STRING, _src_file STRING, _ingested_at TIMESTAMP) USING delta")
def run_bronze():
    s = (spark.readStream.schema("event_id STRING, customer STRING, amount STRING, event_ts STRING, status STRING").json(LAND)
         .withColumn("_src_file", F.input_file_name()).withColumn("_ingested_at", F.current_timestamp()))
    q = s.writeStream.format("delta").option("checkpointLocation", str(CK / "bronze")).trigger(availableNow=True).toTable("bronze_events")
    q.awaitTermination()

# --- SILVER: typed, deduplicated by event_id (latest ingest wins), bad rows quarantined
spark.sql("""CREATE TABLE IF NOT EXISTS silver_events (event_id INT, customer STRING, amount DECIMAL(10,2), event_ts TIMESTAMP, status STRING) USING delta""")
spark.sql("CREATE TABLE IF NOT EXISTS quarantine_events (event_id STRING, reason STRING, raw_amount STRING, raw_ts STRING) USING delta")
def silver_batch(df, batch_id):
    typed = (df.select("event_id", "customer", "status", "amount", "event_ts", "_ingested_at")
               .withColumn("e_id", F.expr("try_cast(event_id AS INT)")).withColumn("amt", F.expr("try_cast(amount AS DECIMAL(10,2))"))
               .withColumn("ts", F.expr("try_cast(event_ts AS TIMESTAMP)")))
    bad = typed.filter("e_id IS NULL OR amt IS NULL OR ts IS NULL OR amt < 0")
    bad.select("event_id", F.lit("unparseable or negative").alias("reason"), F.col("amount").alias("raw_amount"), F.col("event_ts").alias("raw_ts")) \
       .write.format("delta").mode("append").saveAsTable("quarantine_events")
    good = (typed.filter("e_id IS NOT NULL AND amt IS NOT NULL AND ts IS NOT NULL AND amt >= 0")
            .withColumn("rn", F.row_number().over(Window.partitionBy("e_id").orderBy(F.col("_ingested_at").desc())))
            .filter("rn = 1").select(F.col("e_id").alias("event_id"), "customer", F.col("amt").alias("amount"), F.col("ts").alias("event_ts"), "status"))
    (DeltaTable.forName(spark, "silver_events").alias("t").merge(good.alias("s"), "t.event_id = s.event_id")
        .whenMatchedUpdateAll(condition="t.amount <> s.amount OR t.status <> s.status OR t.customer <> s.customer OR t.event_ts <> s.event_ts")
        .whenNotMatchedInsertAll().execute())
def run_silver():
    q = (spark.readStream.option("skipChangeCommits", "true").table("bronze_events").writeStream.foreachBatch(silver_batch)
         .option("checkpointLocation", str(CK / "silver")).trigger(availableNow=True).start()); q.awaitTermination()

# --- GOLD: daily revenue by customer, recomputed from silver (small) as a table
def run_gold():
    spark.sql("""CREATE OR REPLACE TABLE gold_daily_revenue USING delta AS
      SELECT CAST(event_ts AS DATE) AS day, customer, COUNT(*) AS orders, SUM(CASE WHEN status='ok' THEN amount ELSE 0 END) AS revenue,
             SUM(CASE WHEN status='refund' THEN amount ELSE 0 END) AS refunds
      FROM silver_events GROUP BY ALL""")

def counts(label):
    c = {t: spark.table(t).count() for t in ["bronze_events", "silver_events", "quarantine_events", "gold_daily_revenue"] if spark.catalog.tableExists(t)}
    print(label, c)

# Day 1
ev = make(1, 40, "2026-04-01"); ev.append(dict(ev[3])); ev.append({"event_id": "x", "customer": "C01", "amount": "oops", "event_ts": "2026-04-01T09:00:00", "status": "ok"})
ev.append({"event_id": 41, "customer": "C02", "amount": "-5", "event_ts": "2026-04-01T09:00:00", "status": "ok"})
drop_file(1, ev); run_bronze(); run_silver(); run_gold(); counts("after day 1:")
# Day 2: 20 new events + a correction to event 5 (new amount) + a late event for day 1
ev2 = make(42, 20, "2026-04-02"); fix = dict(ev[4]); fix["amount"] = "99.99"; ev2.append(fix)
late = {"event_id": 62, "customer": "C03", "amount": "10.00", "event_ts": "2026-04-01T23:30:00", "status": "ok"}; ev2.append(late)
drop_file(2, ev2); run_bronze(); run_silver(); run_gold(); counts("after day 2:")
# idempotence: run everything again with no new files
run_bronze(); run_silver(); run_gold(); counts("re-run, no new data:")
spark.sql("SELECT event_id, amount FROM silver_events WHERE event_id = 5").show()
spark.sql("SELECT * FROM quarantine_events").show(truncate=False)
spark.sql("SELECT * FROM gold_daily_revenue ORDER BY day, customer LIMIT 4").show()
spark.sql("SELECT day, SUM(orders) orders, ROUND(SUM(revenue),2) revenue FROM gold_daily_revenue GROUP BY day ORDER BY day").show()
# reconciliation: silver sum == gold sum
a = spark.sql("SELECT SUM(amount) FROM silver_events").collect()[0][0]; g = spark.sql("SELECT SUM(revenue)+SUM(refunds) FROM gold_daily_revenue").collect()[0][0]
print("reconciles:", a == g, a, g)
spark.sql("DESCRIBE HISTORY silver_events").select("version", "operation").show()
```

Run it in an empty folder with the Level 1 environment (`pip install pyspark delta-spark`, `PYSPARK_PYTHON` set).

## Expected output

```text
after day 1: {'bronze_events': 43, 'silver_events': 40, 'quarantine_events': 2, 'gold_daily_revenue': 6}
after day 2: {'bronze_events': 65, 'silver_events': 61, 'quarantine_events': 2, 'gold_daily_revenue': 12}
re-run, no new data: {'bronze_events': 65, 'silver_events': 61, 'quarantine_events': 2, 'gold_daily_revenue': 12}
```

Check the arithmetic: day 1 has 40 + 1 + 2 = 43 bronze rows; silver = 43 − 1 duplicate − 2 quarantined = 40. Day 2 adds 22
bronze rows (20 new, the correction, the late event); silver gains 21, because the correction updates a row instead of
inserting one. The re-run changes nothing anywhere.

```text
+--------+------+
|event_id|amount|
+--------+------+
|       5| 99.99|      <- the correction reached silver

+--------+-----------------------+----------+-------------------+
|event_id|reason                 |raw_amount|raw_ts             |
+--------+-----------------------+----------+-------------------+
|x       |unparseable or negative|oops      |2026-04-01T09:00:00|
|41      |unparseable or negative|-5        |2026-04-01T09:00:00|
+--------+-----------------------+----------+-------------------+

+----------+------+-------+
|       day|orders|revenue|
+----------+------+-------+
|2026-04-01|    41|1682.49|      <- 40 day-1 events + the late event that arrived on day 2
|2026-04-02|    20| 569.11|
+----------+------+-------+

reconciles: True 2688.03 2688.03
```

and the silver history shows two `MERGE` commits (one per day) after `CREATE TABLE`. Because the second silver run had
no new bronze rows, it added no commit.

## What to notice

1. **`skipChangeCommits`.** Silver streams from bronze, which is append-only, so it isn't strictly needed here; it is
   included because the moment anyone runs a `DELETE` on bronze (for GDPR, say), a plain stream would fail.
2. **`try_cast`, not `cast`.** With ANSI mode on (the default in Spark 4), `cast('oops' AS DECIMAL)` raises an error that
   would fail the whole batch. `try_cast` returns NULL so the row can be routed to quarantine.
3. **Dedupe inside the batch, then merge.** `MERGE` rejects multiple source rows per key (Level 2 · 01). The window
   `row_number()` keeps the latest ingest per `event_id`.
4. **The change condition in `whenMatchedUpdateAll`.** Without it every re-delivered duplicate would rewrite a file.
5. **Gold from silver, not from the batch.** The late event landed in the right day because gold is rebuilt from silver
   state.
6. **A gap I left in on purpose.** If the silver step crashes after writing quarantine rows but before the merge commits,
   a replay will append those quarantine rows a second time. Fix it as Exercise 2 asks.
7. **Development friction worth knowing about.** My first run failed with `The input source(parquet) is different from
   the table bronze_events's data source provider(delta)`: `writeStream.toTable(...)` defaults to Parquet unless you set
   `.format("delta")`. On Databricks, Delta is the default, but explicit is safer.

## How It Actually Works

Each stage is a separate `availableNow` stream with its own checkpoint. Stopping and restarting the script is safe
because every stream's checkpoint records the last committed batch, and every sink is idempotent (Delta's `txn`
bookkeeping for bronze, `MERGE` for silver, `CREATE OR REPLACE` for gold). The silver stream reads bronze as an
incremental source by remembering the last bronze **table version** it processed; only commits after that version
produce new rows. That is why the no-data re-run does zero work.

## Extensions

- Replace the gold rebuild with an incremental merge keyed on `(day, customer)` that recomputes only the days touched by
  the batch. What do you need to know about each batch to do that?
- Add `delta.enableChangeDataFeed` on silver and drive gold from the change feed.
- Add a data-quality table that records, per run, the counts of each layer and whether they reconcile.
- Compact and cluster silver (Lesson 03) and check `DESCRIBE DETAIL` before and after.

## Common mistakes

- **`writeStream.toTable` without `.format("delta")`** against an existing Delta table (a provider mismatch error).
- **Using `cast` instead of `try_cast`** under ANSI mode, so one bad value fails the whole batch.
- **Merging a source with duplicate keys** (deduplicate inside the batch first).
- **Aggregating only the newest batch** and missing late or corrected events.
- **Deleting a checkpoint to "fix" a stream**, which re-ingests everything and duplicates rows in append-only layers.
- **Appending to quarantine in a non-idempotent step** (the known gap in this project).

## Exercise

1. Run the project and confirm your numbers match.
2. Make quarantine idempotent: give each quarantine row a key (for example a hash of `event_id` and raw values) and
   write it with `MERGE ... WHEN NOT MATCHED THEN INSERT`. Prove it by replaying a batch.
3. Add day 3 containing an event with `status` changed from `ok` to `refund` for an existing event. Verify silver and gold.
4. Break the pipeline deliberately (delete silver's checkpoint) and predict what happens before running it. What does
   `MERGE` protect you from? Which table, if any, ends up with duplicates?
