---
description: "Auto Loader (cloudFiles): incremental file ingestion, detection modes, schema inference and evolution, rescued data, with a local analogue."
---

# 08 · Auto Loader: Incremental File Ingestion

Data from outside the lakehouse mostly arrives as files: exports dropped by partners, logs shipped from applications,
CDC dumps from databases. Auto Loader is Databricks' answer to "ingest each new file exactly once, without listing a
million-file directory every minute, and without breaking when the source adds a column."

!!! note "What was run, and what wasn't"
    **Auto Loader (`cloudFiles`) exists only on Databricks, and was not run for this lesson.** Everything about its
    options and behaviour comes from the Databricks documentation (October 2026). What I did run locally is (a) the
    plain Structured Streaming file source and checkpoint behaviour from Lesson 07, which is the mechanism Auto Loader
    builds on, and (b) a small Spark experiment, below, that is an *analogue* of the rescued-data column, with results
    that differ from Auto Loader in an instructive way. Code samples here follow the documentation but are untested.

## The basic pattern

```python
(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", "/Volumes/main/raw/_schemas/orders")   # enables inference + evolution
   .load("/Volumes/main/raw/landing/orders/")
 .writeStream
   .option("checkpointLocation", "/Volumes/main/raw/_checkpoints/orders")
   .trigger(availableNow=True)
   .toTable("main.bronze.orders"))
```

`cloudFiles` is a Structured Streaming source. The checkpoint and trigger behave exactly as in Lesson 07, so the
pipeline runs incrementally on a schedule (`availableNow`) or continuously. Per the documentation, Auto Loader
supports S3, ADLS, GCS and Unity Catalog volumes as locations, and these formats: JSON, CSV, XML, Parquet, Avro, ORC,
text and binary files. In a Lakeflow pipeline (Level 3 · 06) the schema and checkpoint locations are managed for you.

**How progress is tracked:** discovered files' metadata is persisted in a RocksDB key-value store in the checkpoint
location, which is how a file already ingested is skipped even if the stream restarts. Auto Loader doesn't guarantee
the *order* in which files are processed, so design downstream logic (like `MERGE` with sequence numbers) to cope with
out-of-order arrival.

## Finding new files: two modes

| | Directory listing (default) | File notification (recommended) |
|---|---|---|
| How | Lists the input directory each trigger | Subscribes to storage events through a queue; reads the queue |
| Setup | None beyond read access | Notification/queue services, or "file events" on a Unity Catalog external location |
| Scales with | Number of files in the directory | Number of *new* files |

The documentation recommends file notification mode with **file events** for most workloads, and states you can switch
modes across restarts while keeping exactly-once guarantees. Directory listing is fine for small, slow landing zones,
and has an optimization for lexically ordered file names (like date-prefixed paths).

## Schema inference and evolution

Pointing Auto Loader at a `schemaLocation` makes it infer the schema, store it in a `_schemas` folder, and evolve
it. Facts from the documentation:

- It **samples the first 50 GB or 1,000 files**, whichever limit is crossed first, to infer the schema.
- For text formats that don't encode types (JSON, CSV, XML), it **infers every column as `string`** (including nested JSON
  fields) to avoid type-mismatch failures. Set `cloudFiles.inferColumnTypes` to `true` for Spark-style type inference.
  Parquet and Avro keep their encoded types.
- You can fix types with **schema hints** (`cloudFiles.schemaHints`: `"id BIGINT, amount DECIMAL(10,2)"`).
- Rescue: unexpected data goes to a `_rescued_data` column.

When the source gains a column, what happens depends on `cloudFiles.schemaEvolutionMode`:

| Mode | Behaviour on a new column |
|---|---|
| `addNewColumns` (default when no schema is given) | Stream **fails** with `UnknownFieldException` after Auto Loader adds the column to the stored schema; restarting resumes with the new schema |
| `addNewColumnsWithTypeWidening` | As above, plus safe widenings (`int` to `long`); unsupported type changes go to the rescued column. Public Preview per the docs, on recent runtimes |
| `rescue` | Never evolves; the stream doesn't fail; new columns land in `_rescued_data` |
| `failOnNewColumns` | Fails and stays failed until you update the schema or remove the file |
| `none` (default when you provide a schema) | No evolution |

The "fail then restart" design surprises people: a source schema change *stops your job once on purpose*, so the
production recipe is a Lakeflow Job with automatic retries (the docs recommend this) that restarts the stream.

### A local analogue of the rescued data column (and where it differs)

The rescued-data idea is: never lose a value that didn't fit the schema. I tried the closest thing available in plain
Spark: read three JSON lines with a typed schema and a corrupt-record column, on **local Spark 4.2.0**:

```text
{"id":1,"amt":"12.5","city":"Pune"}
{"id":2,"amt":"abc","city":"Oslo","coupon":"X1"}
{"id":"three","amt":"7","city":"Rome"}
```

```python
sch = "id INT, amt DOUBLE, city STRING, _rescued STRING"
spark.read.schema(sch).option("columnNameOfCorruptRecord", "_rescued").json(path).show(truncate=False)
```

```text
|id  |amt |city|_rescued                                        |
|1   |NULL|Pune|{"id":1,"amt":"12.5","city":"Pune"}             |
|2   |NULL|Oslo|{"id":2,"amt":"abc","city":"Oslo","coupon":"X1"}|
|NULL|NULL|Rome|{"id":"three","amt":"7","city":"Rome"}          |
```

This is **not** Auto Loader behaviour, and the difference is the point. Plain Spark's JSON reader, given a `DOUBLE`
column and a JSON *string* `"12.5"`, treated the **whole record** as corrupt: `amt` became NULL even for the valid-looking
`"12.5"`, and the record was preserved only as raw text. Auto Loader's rescued data column, per the docs, captures the
column(s) that didn't match (and any new/unexpected columns) in a JSON blob while the rest of the row parses normally.
The lesson for a plain Spark pipeline: types in JSON are strict, so an upstream that sends numbers as strings can null
whole rows. Ingest as strings first (what Auto Loader's default inference does), then cast deliberately in the silver layer
with `try_cast`. A regular DataFrame way to find unexpected keys is `from_json(value, "map<string,string>")` and
`map_filter` for keys outside the known list, which on the same data isolated `{coupon -> X1}`.

## Production configuration notes (from the docs)

- **One checkpoint per ingestion workload.** Two source locations loading into one table need two streams.
- **Source data retention**: Auto Loader can archive or delete files after ingestion to keep listing fast and storage small.
  Use it or you'll have millions of files in the landing area.
- **Backfill vs incremental**: the first run processes existing files too (`cloudFiles.includeExistingFiles` defaults to
  true per the documentation), so the first run of a large directory is a bulk load.
- **Monitoring**: Auto Loader exposes file-level state you can query (the docs describe functions such as
  `cloud_files_state`) and stream metrics for backlog.
- **COPY INTO** is the older SQL command for idempotent file loading; the docs generally recommend Auto Loader or
  pipelines for new work, but COPY INTO is simpler for small, one-off loads.

## How It Actually Works

Auto Loader is a Structured Streaming source that replaces "list the directory and diff" with a cheaper discovery
mechanism. In listing mode it lists the path each micro-batch and compares with RocksDB state in the checkpoint; in
notification mode it drains a queue of object-created events (with file events, via an event service tied to the Unity
Catalog external location) and doesn't list at all. For each discovered file it records its path and modification
metadata; the micro-batch's offset is then "the set of files" and the usual checkpoint/commit protocol from Lesson 07
makes the delivery exactly-once into Delta.

Schema inference runs *before* the first batch, over a sample, and then again on each micro-batch's data when new
columns appear, which is when the stored schema (a versioned file in `_schemas`) is updated.

## Common mistakes

- **Assuming file order.** Out-of-order arrival is normal.
- **Letting the landing directory grow forever** (slow listing, large state).
- **Treating the evolution failure as an outage.** It's by design; make the job restart itself.
- **Inferring types from a sample** and being surprised later; prefer all-strings in bronze plus casts in silver, or
  explicit schema hints for critical columns.
- **Sharing a checkpoint** between loads or deleting it (full re-ingestion, as Lesson 07's duplicate demo showed).
- **Forgetting the rescued column** downstream; if you never look at `_rescued_data`, you never see bad data.

## Exercise

1. (Local) Re-run the three-line JSON example. Change `amt` to `STRING` in the schema and see which rows now parse. What
   would you do downstream to convert safely?
2. (Local) Using Lesson 07's pattern, build a "poor man's Auto Loader": a streaming file source with a checkpoint,
   `availableNow`, and an all-strings schema. Add a new column to a later file and see what the plain source does.
3. (Workspace) Create an Auto Loader stream on a volume with `schemaEvolutionMode = addNewColumns`, add a new field to the
   JSON, and record the failure and the restart behaviour.
4. Decide, for a landing zone receiving 50 files per hour versus 5 million per day, which detection mode you'd use and
   why.
