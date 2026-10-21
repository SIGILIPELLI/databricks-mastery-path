---
description: "How Unity Catalog governs cloud storage: storage credentials, external locations, READ FILES, managed and external volumes, and no-overlap rules."
---

# 03 · Storage Credentials, External Locations & Volumes

Unity Catalog can govern tables and files only if it controls the *door to the storage*. That door is built from three
objects: a **storage credential** (a cloud identity), an **external location** (a path plus a credential) and the
privileges on them. Volumes then give governed, path-based access to files.

!!! note "What was run"
    Nothing here touches a real cloud account or workspace. The facts come from the Databricks documentation pages on
    external locations, storage credentials, managed storage, volumes and "path overlap restrictions" (October 2026),
    for **AWS**; Azure and GCP use different credential types but the same ideas. The SQL statements couldn't be validated
    by `sqlglot` (it treats Unity Catalog DDL as opaque commands), so I checked their syntax against the SQL reference.
    The one program in this lesson is a small model I wrote of the documented *no-overlap* rule for external locations;
    it is a teaching aid, not Databricks code.

## The three objects

```text
cloud IAM role / managed identity ──►  STORAGE CREDENTIAL        (who UC acts as when touching storage)
                  + a path           ►  EXTERNAL LOCATION         (where, and which credential)
                  + grants           ►  READ FILES / WRITE FILES / CREATE EXTERNAL TABLE / CREATE EXTERNAL VOLUME / CREATE MANAGED STORAGE
```

- A **storage credential** wraps a cloud identity: on AWS an IAM role that Databricks can assume; on Azure a managed
  identity or service principal; on GCP a service account. Databricks recommends granting only `CREATE EXTERNAL LOCATION`
  on credentials, and no direct use.
- An **external location** binds a storage path (like `s3://acme-raw/landing`) to a credential. It is the unit you grant
  access on.
- On AWS you create the credential first, then the location. The documentation prerequisites include: the bucket must
  exist; its name can't contain dots; the path must use only standard ASCII characters; and the bucket cannot have an S3
  ACL attached. You need `CREATE STORAGE CREDENTIAL` on the metastore for the first, and `CREATE EXTERNAL LOCATION` on both
  the metastore and the credential for the second (metastore admins have those on the metastore by default).

```sql
-- Credentials are normally created in Catalog Explorer or through the API (they need cloud-side IAM setup);
-- then, in SQL:
CREATE EXTERNAL LOCATION IF NOT EXISTS landing
  URL 's3://acme-raw/landing'
  WITH (STORAGE CREDENTIAL acme_raw_cred)
  COMMENT 'Raw drops from partners';

DESCRIBE EXTERNAL LOCATION landing;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION landing TO `ingest-service`;
GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION landing TO `data-engineers`;
SHOW STORAGE CREDENTIALS;
SHOW GRANTS ON EXTERNAL LOCATION landing;
```

(`CREATE EXTERNAL LOCATION ... URL ... WITH (STORAGE CREDENTIAL ...)`, `DESCRIBE EXTERNAL LOCATION`, `SHOW STORAGE
CREDENTIALS` and `SHOW GRANTS ... ON EXTERNAL LOCATION` match the reference pages; the bucket and credential names are
invented.) One documented gotcha: **`WRITE FILES` requires `READ FILES` on the same location**, because writes involve
metadata checks and path validation; a user with only `WRITE FILES` gets `PERMISSION_DENIED`. Grant both together.
The documentation also recommends using volumes to govern file access, rather than granting `READ FILES` directly to many users.

## Path rules: no overlaps

Unity Catalog guarantees that two securables never govern the same bytes, via documented rules. External locations
can't overlap other external locations; volumes can't overlap volumes; tables can't overlap tables; tables and volumes
can't overlap each other; managed storage locations can't overlap each other, external tables or external volumes. Consequently
you can't define an external location inside another one, can't register a table on files inside a volume, and can't
define a volume on a directory inside a table. Files inside a volume can always be read and written by path (including Delta
files), but they cannot be registered as tables.

Here is a tiny model of the first two rules. It registers locations (refusing overlaps in either direction) and finds
which one governs a path:

```python
"""Teaching model of one documented Unity Catalog rule: external locations can't overlap, and a path is
governed by the single external location that contains it. Not Databricks code."""
def contains(base, path):
    base = base.rstrip("/") + "/"; return (path.rstrip("/") + "/").startswith(base)

class Metastore:
    def __init__(self): self.locations = {}
    def create_external_location(self, name, url):
        clash = [n for n, u in self.locations.items() if contains(u, url) or contains(url, u)]
        if clash: raise ValueError(f"cannot register {url!r}: overlaps external location(s) {clash}")
        self.locations[name] = url
    def governing(self, path):
        hits = [n for n, u in self.locations.items() if contains(u, path)]
        return hits[0] if hits else None

ms = Metastore()
ms.create_external_location("landing", "s3://acme-raw/landing")
ms.create_external_location("curated", "s3://acme-curated")
for p in ["s3://acme-raw/landing/orders/2026-10-21/f.json", "s3://acme-raw/other/x.csv",
          "s3://acme-curated/sales/2026/part.parquet", "s3://acme-raw/landing-archive/x"]:
    print(f"{p:<50} -> {ms.governing(p) or 'no external location: access denied'}")
for name, url in [("sales", "s3://acme-curated/sales"), ("raw_all", "s3://acme-raw"), ("archive", "s3://acme-raw/landing-archive")]:
    try: ms.create_external_location(name, url); print(f"registered {name}: {url}")
    except ValueError as e: print("REJECTED:", e)
```

```text
s3://acme-raw/landing/orders/2026-10-21/f.json     -> landing
s3://acme-raw/other/x.csv                          -> no external location: access denied
s3://acme-curated/sales/2026/part.parquet          -> curated
s3://acme-raw/landing-archive/x                    -> no external location: access denied
REJECTED: cannot register 's3://acme-curated/sales': overlaps external location(s) ['curated']
REJECTED: cannot register 's3://acme-raw': overlaps external location(s) ['landing']
registered archive: s3://acme-raw/landing-archive
```

Notice the string-prefix trap the model has to avoid: `landing-archive` starts with the characters `landing` but is a
sibling folder, not a child. Comparing paths by `startswith` without the trailing `/` would wrongly treat them as
overlapping; the first version of my model handled that by adding a separator before comparing. Overlap planning matters in
practice: pick **location boundaries before you create them** (a bucket-wide location blocks every future per-folder
location), and create external tables and volumes in sub-directories, not at the root of a location, as the docs advise.

## Volumes in depth

A volume is a governed directory in a schema. Managed volumes live in the schema's managed storage; external volumes
point at a path in an external location.

```sql
CREATE VOLUME prod.raw.landing_managed COMMENT 'Staging area';
CREATE EXTERNAL VOLUME prod.raw.partner_drops
  LOCATION 's3://acme-raw/landing/partners' COMMENT 'Files delivered by partners';
GRANT READ VOLUME ON VOLUME prod.raw.partner_drops TO `analysts`;
GRANT WRITE VOLUME ON VOLUME prod.raw.partner_drops TO `ingest-service`;
```

Per the reference, `CREATE VOLUME` needs `CREATE VOLUME` on the schema, and creating an *external* volume also needs `CREATE
EXTERNAL VOLUME` on the external location. Dropping an external volume leaves the files; dropping a managed volume deletes
its data (with a 7-day retention per the volumes documentation).

Access paths:

| From | Path |
|---|---|
| Spark, SQL, pandas, `dbutils.fs`, shell, `%sh` | `/Volumes/<catalog>/<schema>/<volume>/<path>` |
| Apps and REST | Files API: `/api/2.0/fs/files/Volumes/...` |
| Cloud URI | Possible for external volumes with sufficient privileges; Unity Catalog privileges override those on the external location |

## The cardinal rule: don't bypass UC

The documentation is blunt: **do not give non-UC identities storage-level access to managed tables or volumes**. Anyone
with direct bucket access bypasses UC's access control, escapes auditing and lineage, and can corrupt tables by writing
files Delta doesn't know about. The only identity that should touch managed storage is the one UC uses. For external data
that other systems also use, grant those systems access to *their* path through your cloud IAM, but make Databricks users
go through UC.

## How It Actually Works

When compute needs `s3://acme-raw/landing/orders/f.json`, it presents the user's identity to UC. UC finds the governing
external location (or the table/volume that maps to the path), checks the user's privileges on that object, then asks the
cloud provider to **assume the storage credential's role with a session policy narrowed to that path**, and returns the
resulting short-lived token to the compute. The engine reads with that token. Because credentials are minted per request,
scoped and expiring, nothing long-lived is ever on a cluster, and revoking a grant takes effect at the next request.

A managed table path is a generated random directory (the docs say the name is randomly generated), which is why
**path-based access to managed tables isn't supported**: use the table name.

## Common mistakes

- **Registering an external location at a bucket root**, blocking every smaller location later.
- **Granting `WRITE FILES` without `READ FILES`** and getting `PERMISSION_DENIED`.
- **Giving users direct S3/ADLS access** to the buckets behind managed storage.
- **Trying to create a table on files inside a volume** (not allowed; load them into a table).
- **Granting `READ FILES` on locations broadly** instead of governing through volumes and tables.
- **Dots in S3 bucket names**, or non-ASCII paths, which the AWS prerequisites exclude.
- **One credential for everything**, so a compromise (or mistake) has maximum reach.

## Exercise

1. Add a method to the model that finds all objects a given path would overlap, and test it with a child, a parent and a
   sibling path (`landing`, `landing/x`, `landing-archive`).
2. Plan the external locations for three buckets (`raw`, `curated`, `sandbox`) such that no overlap will block you later.
   Which privileges do the ingest job and the analysts need?
3. Explain why granting analysts `READ FILES` on a location is weaker than giving them `READ VOLUME` on a volume.
4. List three ways a person with direct bucket access could damage a managed table without Unity Catalog noticing.
