---
description: "Open a Delta table's _delta_log: add, remove, metaData and protocol actions, file statistics, checkpoints and a real write conflict, run locally."
---

# 07 · Inside the Delta Log: Transactions & ACID

A Delta table is a folder with two kinds of content: Parquet data files and a `_delta_log/` directory of small JSON
files. The log *is* the table: the data files are only meaningful as far as the log mentions them. Reading the log
once removes most of the mystery from Delta behaviour, from time travel to why concurrent writers sometimes fail.

!!! note "What was run"
    The log contents, checkpoint and conflict below were produced with **open-source Delta Lake 4.4.1 on local
    Spark 4.2.0**. Databricks tables follow the same log protocol, but Databricks may enable extra table features (so
    `protocol` versions, extra properties and file layout can differ), and cloud storage makes commit atomicity depend
    on the storage service's guarantees. File names are shortened in the output for readability.

## Anatomy of a table folder

```python
spark.createDataFrame([(1, "a"), (2, "b")], "id INT, v STRING").coalesce(1).write.format("delta").save(p)
```

```text
p/
├── part-00000-bd3c7a7e-....snappy.parquet      # data
└── _delta_log/
    └── 00000000000000000000.json               # commit 0
```

(The local run also had `.crc` helper files, which are Hadoop checksum artifacts of the local filesystem, and a
`_staged_commits` folder used by newer Delta versions for commit coordination.) Each commit is one file named with a
20-digit version number. The log is **append-only**: commits are never edited.

## What a commit contains

Each line of a commit file is one JSON **action**. Here is version 0, as printed by a small script that reads the file
(file names shortened):

```text
== v0
commitInfo WRITE isBlindAppend True readVersion None
metaData   {"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,...
protocol   {"minReaderVersion": 1, "minWriterVersion": 2}
add        part-00000-bd3c7a7e-81b3.. size 669 stats {"numRecords":2,"minValues":{"id":1,"v":"a"},"maxValues":{"id":2,"v":"b"},"nullCount":{"id":0,"v":0}}
```

The action types you need to know:

| Action | Meaning |
|---|---|
| `metaData` | The table's schema, partition columns, and configuration (written at creation or when they change) |
| `protocol` | Minimum reader/writer versions (and table features) a client must support to use the table safely |
| `add` | A data file is part of the table from this version; carries size and **statistics** |
| `remove` | A data file is no longer part of the table (it stays on disk until `VACUUM`) |
| `commitInfo` | Operation name, parameters, metrics, isolation level, `isBlindAppend`, `readVersion` |

Now an append (v1) and an update (v2):

```text
== v1
commitInfo WRITE isBlindAppend True readVersion 0
add        part-00001-be08c006-7112.. size 666 stats {"numRecords":1,"minValues":{"id":3,"v":"c"},...}

== v2
commitInfo UPDATE isBlindAppend False readVersion 1
add        part-00000-910a0e7d-40da.. size 668 stats {"numRecords":2,"minValues":{"id":1,"v":"B"},"maxValues":{"id":2,"v":"a"},...}
remove     part-00000-bd3c7a7e-81b3.. dataChange True
```

The update didn't touch the file added in v1. It found the one file that contained `id = 2` (the v0 file),
wrote a replacement file with the changed row, and committed `add new` + `remove old`. Note `readVersion 1`: the writer
records which version it read, which is how conflicts are detected (below). Also note the statistics in v2:
`"minValues":{"v":"B"},"maxValues":{"v":"a"}`. Uppercase `B` sorts before lowercase `a` in a byte-wise string
ordering, a reminder that min/max for strings isn't alphabetical in the human sense.

## The table's state = replaying the log

To know what's in the table at version N, a reader applies actions from version 0 to N: start with an empty set of
files, `add` inserts, `remove` deletes. The result is the **snapshot**. Time travel (Lesson 08) is just "stop replay at
version N".

## Checkpoints

Replaying thousands of JSON files would be slow, so every 10 commits Delta writes a **checkpoint**: a Parquet file
holding the complete state (the live `add` files plus protocol and metadata) as of that version. After 12 commits in
the local run, the log folder contained:

```text
00000000000000000000.json … 00000000000000000009.json
00000000000000000010.checkpoint.parquet
_last_checkpoint        ->  {"version":10,"size":13,"sizeInBytes":20891,"numOfAddFiles":10,...}
```

A reader first reads `_last_checkpoint`, loads the checkpoint, then applies only the JSON commits after it. (Old
JSON files are cleaned up after `delta.logRetentionDuration`, 30 days by default; Lesson 08.)

## ACID, concretely

- **Atomicity.** A commit is one file being created. Either `…0002.json` exists and the whole transaction is visible,
  or it doesn't and none of it is. A job that crashes after writing Parquet files but before the commit leaves
  orphan data files that no snapshot references; `VACUUM` removes them later.
- **Consistency.** Before committing, Delta checks the new schema, `NOT NULL` and `CHECK` constraints against the
  data (Level 2 · 02).
- **Isolation.** Readers use a fixed snapshot, so they never see half a commit. Writers use **optimistic concurrency
  control**: do the work, then try to create the next version number; if someone else got there first, check whether
  the two transactions conflict.
- **Durability.** Once the commit file is in object storage, it's as durable as the storage.

### A real conflict

Two threads ran `UPDATE ... WHERE g = 0` on the same table at the same time (one adding 100 to `id`, the other adding
200). The local result:

```text
{'u2': '[DELTA_CONCURRENT_APPEND.WITHOUT_HINT] Transaction conflict detected. a concurrent UPDATE added data to table ...', 'u1': 'ok'}
count = 10, sum(id) = 545
history: version 1 UPDATE, version 0 WRITE
```

One writer won version 1; the other read version 0, planned to rewrite the same files, found at commit time that
version 1 had changed them, and raised `DELTA_CONCURRENT_APPEND`. The table holds exactly one writer's change
(sum 545 = the +100 update on five rows), with no lost or doubled rows. The loser must retry, which re-reads the new
snapshot. Delta's rule of thumb: writes to *disjoint* data (different partitions, or files that don't overlap) can
both succeed; writes that touch the same files conflict. That's why partitioning and clustering choices affect
concurrency (Level 2).

Databricks documents isolation levels (`WriteSerializable` is the default for tables; `Serializable` is stricter) and
row-level concurrency features that can reduce conflicts on tables with deletion vectors or liquid clustering. Those
platform behaviours weren't exercised here.

## How It Actually Works

Atomic commit is the one primitive everything depends on: "create file `N.json` only if it doesn't exist." On storage
systems that offer conditional put or atomic rename (HDFS, ADLS, GCS, and S3 with conditional writes), that is the
whole story. Where the store lacked it (older S3), Delta needed an external coordination service (such as a DynamoDB
table) to guarantee a single winner. Databricks manages this for you, and the protocol also has a *catalog-managed
commits* feature where the catalog arbitrates commits; its availability depends on platform and version, so check the
docs before relying on it.

The `stats` in each `add` action is what makes **data skipping** possible: when you query `WHERE id > 1000`, the
engine reads the log (not the data), drops every file whose `maxValues.id` is at most 1000, and opens only the rest.
By default Delta collects statistics for the first 32 columns. That number, and which columns have stats, is why
column order in a wide table can matter (Level 2 · 04).

## Common mistakes

- **Editing or deleting log files by hand.** You can make the table unreadable; there is no undo for a corrupted log.
- **Deleting data files with a file-system tool** instead of `DELETE` + `VACUUM`. The log still references them.
- **Treating a conflict error as a bug.** It's the design working; add retries (and idempotent logic) around writes.
- **Sharing one table between engines** that don't support the table's `protocol` features; upgrading the protocol
  can lock out older readers and can't easily be undone.
- **Many tiny commits** (a streaming job committing every second) create lots of small files and a long log.

## Exercise

1. Reproduce the walk-through: write a table, append, update, then write a ~15-line script that prints each action
   type and path for every commit. Explain which file each `remove` refers to.
2. Make 12 more commits and list the `_delta_log` folder. Find the checkpoint and read `_last_checkpoint`.
3. Provoke a conflict yourself with two threads (or two terminals), first updating the *same* rows, then rows in
   *different* partitions of a partitioned table. Report which combination fails on your setup.
4. In the log, find the `numRecords` stat for each file and verify it by `spark.read.parquet(<file>).count()`.
