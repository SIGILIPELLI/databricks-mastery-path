---
description: "The bronze/silver/gold medallion pattern by what each layer must guarantee: raw fidelity, typed and deduplicated data, aggregates, quarantine."
---

# 09 · The Medallion Architecture

"Medallion" is Databricks' name for organizing a lakehouse into three quality layers: **bronze** (raw), **silver**
(cleaned and conformed) and **gold** (business-ready). The names are marketing; the idea is old and sound — never
overwrite the raw data, put validation in one place, and give consumers a stable, trustworthy layer. This lesson is about
what each layer must *guarantee*, because that's what decides where a given piece of logic belongs.

!!! note "What was run"
    The numbers in this lesson come from the Level 2 project (next lesson): an incremental pipeline on **local Spark
    4.2.0 + Delta 4.4.1** with synthetic events (seeded random data, so results are reproducible but not "real").
    Platform features that automate medallion pipelines (Lakeflow pipelines with expectations, streaming tables,
    materialized views; Level 3 · 06) are described, not run.

## The three layers

| | Bronze | Silver | Gold |
|---|---|---|---|
| Purpose | Faithful, replayable copy of the source | Clean, typed, deduplicated, conformed entities | Aggregates and models shaped for consumers |
| Schema | Source schema, usually **all strings** + lineage columns | Declared, strongly typed | Business terms, star schemas, KPIs |
| Write pattern | Append only (never update) | Upsert (`MERGE`) / incremental | Rebuild or incremental aggregate |
| Quality rule | Don't lose anything, don't fail on bad data | Enforce types and keys; quarantine failures | Reconcile to silver; stable definitions |
| Audience | Data engineers | Engineers, analysts, data scientists | BI, dashboards, ML features, executives |

### Bronze: capture everything

Bronze answers "what exactly did the source send, and when?" Keep fields as strings so no value can fail a type cast; add
**lineage columns** such as `_src_file` and `_ingested_at`; append only. If anything downstream is wrong, you can rebuild
silver and gold from bronze without going back to the source system (which may no longer have the data). In the project,
bronze after day 1 held 43 rows from a file with 40 real events, 1 exact duplicate event and 2 malformed rows: **all 43
were kept**.

### Silver: where data becomes trustworthy

Silver does the cleaning that every consumer would otherwise redo differently:

1. **Type** with `try_cast` so bad values become NULL instead of failing the batch.
2. **Validate** (non-negative amount, parseable timestamp, key present).
3. **Quarantine** rejects with a reason instead of dropping them.
4. **Deduplicate** on the business key (latest ingestion wins).
5. **Upsert** with `MERGE`, with a change condition so unchanged rows aren't rewritten.

In the project, day 1 produced silver 40 (43 − 1 duplicate − 2 quarantined), and quarantine holding the unparseable
amount `oops` and the negative amount `-5` with their raw values and a reason. Rejects are data too; someone has to look
at them.

Day 2 added 20 new events, a **correction** to an earlier event (same id, new amount 99.99) and a **late** event
timestamped on day 1. Bronze grew by 22 rows (65 total) and silver by 21 (61), because the correction *updated* the existing silver row
rather than adding one: `SELECT amount FROM silver_events WHERE event_id = 5` returned `99.99`. Bronze still contains both versions, which is
exactly the audit trail you want.

### Gold: shaped for questions

Gold tables are defined in business terms and kept small and fast: daily revenue per customer, active users per week,
a fact table joined to its dimensions. In the project:

```text
|day       |orders|revenue|
|2026-04-01|41    |1682.49|
|2026-04-02|20    |569.11 |
```

Day 1 shows **41** orders even though its file held 40 events: the late event arrived on day 2 but belongs to day 1, and
gold, which is rebuilt from silver, put it where it belongs. A pipeline that aggregated only the newest file would have
missed it. That is the practical reason gold is derived from silver state rather than from "today's batch".

## Reconciliation: the cheapest test you have

After each run, check that layers agree:

```python
silver_total = spark.sql("SELECT SUM(amount) FROM silver_events").collect()[0][0]
gold_total   = spark.sql("SELECT SUM(revenue) + SUM(refunds) FROM gold_daily_revenue").collect()[0][0]
assert silver_total == gold_total          # local run: both 2688.03
```

And the row-count identity per run: `bronze = silver + quarantine + duplicates removed`. Day 1: 43 = 40 + 2 + 1.
Checks like these catch dropped files, double loads, join explosions and filter mistakes before a dashboard does.

## Idempotence and replays

Re-running every stage with no new files changed nothing (bronze 65, silver 61, quarantine 2, gold 12 before and after).
Each stage is safe to retry because bronze uses a checkpoint (Lesson 07), silver uses `MERGE` on a key (Lesson 01) and
only writes quarantine rows for batches it actually processes, and gold is rebuilt from scratch. If you can't re-run a
stage safely, you'll eventually have a duplicate-data incident. One known gap in the project is worth naming: if the
silver batch crashes after appending to quarantine but before its merge commits, the replay would append those
quarantine rows again. A production design would give quarantine rows a deterministic key, or merge them too.

## Variations you'll see

- **More layers.** "Raw / base / curated / marts" is the same idea with more steps; don't add a layer without a rule it
  enforces.
- **Streaming vs batch per layer.** Bronze often streams (`availableNow` or continuous); gold is often a materialized view
  or scheduled SQL.
- **Gold per team.** Different consumers need different shapes; they all read the same silver.
- **Data products**: in Unity Catalog each layer typically gets its own schema (or catalog) with different permissions:
  engineers write bronze, analysts only read gold (Level 3).
- **Materialized views and streaming tables** (Databricks, Level 3 · 06) can implement silver/gold declaratively with
  expectations, instead of hand-written `foreachBatch` code.

## How It Actually Works

The pattern works because Delta gives each layer transactional tables and a change history. Each layer reads the previous
one as a *stream* of committed changes (reading bronze with `readStream.table(...)`, so only new commits are processed)
or as a snapshot (gold reading silver). Because commits are atomic, a downstream reader never sees half of an upstream
batch. And because bronze is immutable and append-only, the entire chain can be recomputed, which is the real safety
net: fix the silver logic, truncate silver and gold, restart from bronze with a new checkpoint.

Backfills and corrections follow from this. A bug found after a month is fixed by changing the code and replaying;
it's not fixed by hand-editing tables. That requires bronze retention longer than your longest plausible bug.

## Common mistakes

- **Cleaning in bronze**, destroying the evidence of what the source sent.
- **Dropping bad rows silently** instead of quarantining them with a reason.
- **Validating in gold or in dashboards**, so each report invents its own rules.
- **Aggregating only the newest batch** and missing late or corrected data.
- **Overwriting silver with each batch** instead of merging.
- **No reconciliation checks**, so a layer can silently drift from the one before it.
- **Treating the medallion as mandatory.** A small, one-off analysis doesn't need three tables.

## Exercise

1. For a data source you know, list the fields you would keep as strings in bronze and the cast and validation rule for
   each in silver.
2. Design the quarantine table schema so that reprocessing the same bad record does not duplicate it.
3. Explain what would go wrong if gold were computed as "sum of today's silver changes added to yesterday's gold" when a
   correction arrives for an old event. How would you handle it instead?
4. Write the reconciliation checks you'd run after each pipeline execution, with the error you'd raise.
