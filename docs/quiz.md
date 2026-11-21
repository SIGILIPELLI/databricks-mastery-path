---
title: "Final Quiz"
description: "Test what you have learned across all four levels of Databricks Mastery Path with a 19-question final quiz."
---

# Final Quiz

19 questions drawn from all four levels of this course. You need **70% (14 out of 19)** to pass —
you can retake it as many times as you like. Your best result is saved in this browser.

<div id="mp-quiz-root"></div>

<script type="application/json" id="mp-quiz-data">
[
  {
    "question": "What makes a Delta table different from a plain folder of Parquet files?",
    "options": [
      "Encryption",
      "A database server",
      "A different file format",
      "A transaction log that defines which files make up each version, giving ACID commits and time travel"
    ],
    "correctIndex": 3,
    "explanation": "The log is the table."
  },
  {
    "question": "A job overwrote a Delta table with bad data an hour ago. What's the fastest recovery?",
    "options": [
      "Delete the table",
      "Rebuild from scratch",
      "RESTORE the table to the previous version (if the files haven't been vacuumed)",
      "Restore a backup"
    ],
    "correctIndex": 2,
    "explanation": "Time travel plus RESTORE."
  },
  {
    "question": "After VACUUM with a short retention, a query `VERSION AS OF 5` fails. Why?",
    "options": [
      "The log was deleted",
      "Delta is broken",
      "Permissions",
      "The data files for that version were deleted as unreferenced"
    ],
    "correctIndex": 3,
    "explanation": "Retention governs how far back you can go."
  },
  {
    "question": "A MERGE fails saying multiple source rows matched one target row. What's the fix?",
    "options": [
      "Disable checks",
      "Deduplicate the source to one row per key (e.g. keep the latest) before merging",
      "Use INSERT",
      "Retry"
    ],
    "correctIndex": 1,
    "explanation": "Make the merge deterministic."
  },
  {
    "question": "An append fails with a schema mismatch error. Is that a bug?",
    "options": [
      "Cluster issue",
      "Corrupt table",
      "No: schema enforcement is protecting the table; evolve the schema explicitly if the change is intended",
      "Yes, a bug"
    ],
    "correctIndex": 2,
    "explanation": "Enforcement fails loudly."
  },
  {
    "question": "Queries filter on customer_id but scan the whole table. What helps?",
    "options": [
      "More partitions on customer_id",
      "Bigger cluster only",
      "SELECT *",
      "OPTIMIZE with ZORDER or liquid clustering on customer_id so data skipping can work"
    ],
    "correctIndex": 3,
    "explanation": "High-cardinality columns are poor partition keys."
  },
  {
    "question": "A table partitioned by user_id has millions of tiny files. What went wrong?",
    "options": [
      "Too few partitions",
      "Over-partitioning on a high-cardinality column",
      "Compression",
      "Photon"
    ],
    "correctIndex": 1,
    "explanation": "Partition on low-cardinality columns or use clustering."
  },
  {
    "question": "In a Spark job one task runs for an hour while the rest finish in seconds. What's the likely cause and a fix?",
    "options": [
      "Network",
      "Disk",
      "Skew on a join key; use AQE skew handling, salting or broadcast the small side",
      "Increase partitions only"
    ],
    "correctIndex": 2,
    "explanation": "Look at the Spark UI task metrics."
  },
  {
    "question": "Why prefer Auto Loader over listing a directory every minute?",
    "options": [
      "It copies files",
      "It tracks files incrementally with exactly-once ingestion and handles schema evolution (rescued data)",
      "It's SQL",
      "It's older"
    ],
    "correctIndex": 1,
    "explanation": "Scales to millions of files."
  },
  {
    "question": "A streaming query is restarted and reprocesses old data. What was missing?",
    "options": [
      "A trigger",
      "A cluster",
      "A stable checkpoint location",
      "A watermark"
    ],
    "correctIndex": 2,
    "explanation": "Checkpoints store progress."
  },
  {
    "question": "Why keep bronze tables append-only and untouched?",
    "options": [
      "Speed",
      "Cost",
      "They're the replayable source of truth if silver logic changes",
      "Law"
    ],
    "correctIndex": 2,
    "explanation": "Medallion principle."
  },
  {
    "question": "A column should show only the last 4 digits to non-privileged users. What's the Unity Catalog feature?",
    "options": [
      "A filter",
      "A column mask on the table",
      "A grant",
      "A view copy"
    ],
    "correctIndex": 1,
    "explanation": "Dynamic, policy-based."
  },
  {
    "question": "Analysts see all regions but should see only their own. What do you use?",
    "options": [
      "Column mask",
      "Copies per region",
      "Grants only",
      "A row filter function based on group membership"
    ],
    "correctIndex": 3,
    "explanation": "One governed table."
  },
  {
    "question": "A user can't read a table even though they have SELECT on it. What else is required?",
    "options": [
      "USE CATALOG and USE SCHEMA on the parents",
      "OWNER",
      "ALL",
      "MODIFY"
    ],
    "correctIndex": 0,
    "explanation": "Privileges are hierarchical."
  },
  {
    "question": "What happens on DROP TABLE for an external table?",
    "options": [
      "Files deleted",
      "Metadata is removed but the underlying files remain",
      "Nothing",
      "Moves files"
    ],
    "correctIndex": 1,
    "explanation": "Managed tables delete data."
  },
  {
    "question": "A scheduled nightly job runs on an always-on all-purpose cluster. What's the issue?",
    "options": [
      "Wasted spend and shared interference; use job/serverless compute",
      "Nothing",
      "Security",
      "Speed"
    ],
    "correctIndex": 0,
    "explanation": "Right-size compute."
  },
  {
    "question": "A deployment must be identical across dev, staging and prod with only config changing. What do you use?",
    "options": [
      "Manual clicks",
      "Scripts on laptops",
      "Copy notebooks",
      "Declarative Asset Bundles (databricks bundle deploy) with per-target settings"
    ],
    "correctIndex": 3,
    "explanation": "IaC and CI/CD."
  },
  {
    "question": "A dashboard is slow and costly. Which is the best first check?",
    "options": [
      "Drop columns",
      "Add colors",
      "Profile the query and verify gold tables and warehouse sizing/auto-stop",
      "More users"
    ],
    "correctIndex": 2,
    "explanation": "Measure first."
  },
  {
    "question": "Which is the right way to share data with an external partner without copying it?",
    "options": [
      "Delta Sharing with a share and recipient",
      "Email CSVs",
      "Open the bucket",
      "Grant workspace access"
    ],
    "correctIndex": 0,
    "explanation": "Governed, live."
  }
]
</script>

## Coding challenges

Try these on your own computer, then compare your output. Nothing you write is sent anywhere.

### Build a toy Delta log

Implement MiniDelta: commit() records added and removed files, snapshot(version) replays the log, read() returns the rows of live files, and vacuum() deletes physical files not needed by the latest version. Show current data, time travel to v1, and what vacuum breaks.

**Starter code**

```python
class MiniDelta:
    """A toy transaction log: each commit lists files it ADDs and REMOVEs."""
    def __init__(self):
        self.log = []                    # log[v] = (adds, removes)
        self.files = {}                  # file name -> list of rows

    def commit(self, adds=None, removes=()):
        # store the new files, append (added names, removed names) to the log,
        # and return the new version number
        return 0

    def snapshot(self, version=None):
        # replay the log up to `version` (default: latest) and return the live file names
        return []

    def read(self, version=None):
        # all rows from the live files of that version, sorted
        return []

    def vacuum(self, keep_versions=1):
        # delete physical files not needed by the latest `keep_versions` versions
        pass

t = MiniDelta()
t.commit({"f1": [1, 2, 3]})                         # v0
t.commit({"f2": [4, 5]})                            # v1
t.commit({"f3": [1, 2, 3, 40, 5]}, removes=["f1", "f2"])   # v2 (a rewrite)
print("current:", t.read())
print("v1:", t.read(1))
t.vacuum()
print("files after vacuum:", sorted(t.files))
try:
    print("v1 after vacuum:", t.read(1))
except KeyError as e:
    print("time travel to v1 fails: missing file", e)
```

**Expected output**

```text
current: [1, 2, 3, 5, 40]
v1: [1, 2, 3, 4, 5]
files after vacuum: ['f3']
time travel to v1 fails: missing file 'f1'
```

<details><summary>Show solution and explanation</summary>

<pre><code>class MiniDelta:
    &quot;&quot;&quot;A toy transaction log: each commit lists files it ADDs and REMOVEs.&quot;&quot;&quot;
    def __init__(self):
        self.log = []                    # log[v] = (adds, removes)
        self.files = {}                  # file name -&gt; list of rows

    def commit(self, adds=None, removes=()):
        adds = adds or {}
        self.files.update(adds)
        self.log.append((list(adds), list(removes)))
        return len(self.log) - 1

    def snapshot(self, version=None):
        version = len(self.log) - 1 if version is None else version
        live = []
        for adds, removes in self.log[: version + 1]:
            live = [f for f in live if f not in removes] + adds
        return live

    def read(self, version=None):
        return sorted(r for f in self.snapshot(version) for r in self.files[f])

    def vacuum(self, keep_versions=1):
        keep = set(self.snapshot())
        for v in range(max(0, len(self.log) - keep_versions), len(self.log)):
            keep |= set(self.snapshot(v))
        for f in list(self.files):
            if f not in keep:
                del self.files[f]

t = MiniDelta()
t.commit({&quot;f1&quot;: [1, 2, 3]})                         # v0
t.commit({&quot;f2&quot;: [4, 5]})                            # v1
t.commit({&quot;f3&quot;: [1, 2, 3, 40, 5]}, removes=[&quot;f1&quot;, &quot;f2&quot;])   # v2 (a rewrite)
print(&quot;current:&quot;, t.read())
print(&quot;v1:&quot;, t.read(1))
t.vacuum()
print(&quot;files after vacuum:&quot;, sorted(t.files))
try:
    print(&quot;v1 after vacuum:&quot;, t.read(1))
except KeyError as e:
    print(&quot;time travel to v1 fails: missing file&quot;, e)
</code></pre>

<p>Each commit is only a list of file additions and removals; a table version is the replay of that log. Vacuum deletes unreferenced files, so older versions can no longer be read. This is a teaching model of Delta&#x27;s idea, not Delta&#x27;s actual protocol. Verified by running the solution.</p>
</details>

### MERGE with deduplicated CDC

Implement merge(target, source) to update matches, insert new rows, and delete rows flagged op='d', raising an error when the source has duplicate keys. Then deduplicate the change feed with latest_per_key and merge again.

**Starter code**

```python
def merge(target, source, key="id"):
    # update matching rows, insert new ones, delete rows with op == "d";
    # raise ValueError if the source has two rows with the same key
    return target

def latest_per_key(changes, key="id", order="ts"):
    best = {}
    for r in changes:
        if r[key] not in best or r[order] > best[r[key]][order]:
            best[r[key]] = r
    return list(best.values())

target = [{"id": 1, "city": "Oslo"}, {"id": 2, "city": "Rome"}, {"id": 3, "city": "Kyiv"}]
changes = [
    {"id": 2, "city": "Milan", "ts": 1, "op": "u"},
    {"id": 2, "city": "Turin", "ts": 2, "op": "u"},
    {"id": 3, "ts": 3, "op": "d"},
    {"id": 4, "city": "Lima", "ts": 4, "op": "i"},
]
try:
    merge(target, changes)
except ValueError as e:
    print("error:", e)
print(merge(target, latest_per_key(changes)))
```

**Expected output**

```text
error: multiple source rows for id=2
[{'id': 1, 'city': 'Oslo'}, {'id': 2, 'city': 'Turin', 'ts': 2}, {'id': 4, 'city': 'Lima', 'ts': 4}]
```

<details><summary>Show solution and explanation</summary>

<pre><code>def merge(target, source, key=&quot;id&quot;):
    &quot;&quot;&quot;MERGE INTO: update matches, insert new rows, delete rows flagged op=&#x27;d&#x27;.
    Duplicate source keys are an error (ambiguous merge) unless deduplicated first.&quot;&quot;&quot;
    seen = set()
    for r in source:
        if r[key] in seen:
            raise ValueError(f&quot;multiple source rows for {key}={r[key]}&quot;)
        seen.add(r[key])
    out = {r[key]: dict(r) for r in target}
    for r in source:
        k = r[key]
        if r.get(&quot;op&quot;) == &quot;d&quot;:
            out.pop(k, None)
        elif k in out:
            out[k].update({c: v for c, v in r.items() if c != &quot;op&quot;})
        else:
            out[k] = {c: v for c, v in r.items() if c != &quot;op&quot;}
    return [out[k] for k in sorted(out)]

def latest_per_key(changes, key=&quot;id&quot;, order=&quot;ts&quot;):
    best = {}
    for r in changes:
        if r[key] not in best or r[order] &gt; best[r[key]][order]:
            best[r[key]] = r
    return list(best.values())

target = [{&quot;id&quot;: 1, &quot;city&quot;: &quot;Oslo&quot;}, {&quot;id&quot;: 2, &quot;city&quot;: &quot;Rome&quot;}, {&quot;id&quot;: 3, &quot;city&quot;: &quot;Kyiv&quot;}]
changes = [
    {&quot;id&quot;: 2, &quot;city&quot;: &quot;Milan&quot;, &quot;ts&quot;: 1, &quot;op&quot;: &quot;u&quot;},
    {&quot;id&quot;: 2, &quot;city&quot;: &quot;Turin&quot;, &quot;ts&quot;: 2, &quot;op&quot;: &quot;u&quot;},
    {&quot;id&quot;: 3, &quot;ts&quot;: 3, &quot;op&quot;: &quot;d&quot;},
    {&quot;id&quot;: 4, &quot;city&quot;: &quot;Lima&quot;, &quot;ts&quot;: 4, &quot;op&quot;: &quot;i&quot;},
]
try:
    merge(target, changes)
except ValueError as e:
    print(&quot;error:&quot;, e)
print(merge(target, latest_per_key(changes)))
</code></pre>

<p>MERGE needs at most one source row per target row, so CDC feeds are reduced to the latest change per key first. Verified by running the solution.</p>
</details>
