---
description: "Unity Catalog's object model: metastore, catalogs, schemas, tables and volumes, three-level names, managed storage precedence and layout choices."
---

# 01 · Unity Catalog: Metastore, Catalogs & Three-Level Names

Level 1 and 2 treated "the catalog" as a place to put table names. Unity Catalog (UC) is much more: it is the governance
layer that decides who can read what, records where every table's files live, tracks lineage and writes an audit log.
This lesson is the object model; the following lessons are privileges, storage, fine-grained security and observability.

!!! note "What was run"
    **Unity Catalog cannot be run locally, and this lesson contains no executed output.** Everything is taken from the
    Databricks documentation pages for Unity Catalog, managed storage, securable objects and the SQL reference, read in
    October 2026. SQL statements were parsed with `sqlglot` 30.24's Databricks dialect where possible; the dialect doesn't
    understand most Unity Catalog DDL (`GRANT`, `CREATE CATALOG ... MANAGED LOCATION`, `CREATE VOLUME` fall back to
    generic "command" parsing), so their syntax was checked by hand against the SQL reference pages instead. An open-source
    Unity Catalog project exists, but its behaviour isn't identical to the Databricks product and wasn't used here.
    A Free Edition workspace lets you run everything in this lesson.

## What Unity Catalog is

Per the documentation, Unity Catalog is the unified governance layer for data and AI built into Databricks. When enabled
for a workspace it sits beneath data and AI interactions: it enforces access control when you query a table or call a
model, tracks lineage, and logs activity for auditing. It is automatically enabled for workspaces created after
8 November 2023; older workspaces must be upgraded.

Governance belongs in a service rather than in each cluster's configuration because the alternative breaks down: with
per-cluster settings and cloud-storage permissions, every new tool and every shared cluster is a new way around the rules.

## The object hierarchy

```text
account
 └── metastore                       (one per region; attached to many workspaces)
      ├── catalog                    (top of the 3-level namespace: org unit or environment)
      │    └── schema                (a use case, project, team area)
      │         ├── table / view / materialized view
      │         ├── volume           (files)
      │         ├── function
      │         └── registered model
      ├── storage credential         (a cloud identity to access storage)
      ├── external location          (a storage path + credential)
      ├── connection                 (to external systems: Lakehouse Federation)
      ├── share / recipient / provider   (Delta Sharing)
      └── ...
```

Every object here is a **securable**: you can grant privileges on it to a principal (a user, a group or a service
principal). Names resolve as `catalog.schema.object`:

```sql
SELECT * FROM prod.sales.orders;
USE CATALOG prod;
USE SCHEMA sales;
SELECT * FROM orders;             -- now resolves to prod.sales.orders
SELECT current_catalog(), current_schema();
```

### How people organize catalogs

The documentation describes catalogs as organizing data by organizational unit or software-development-lifecycle scope.
Two common layouts, and the trade-offs:

| Layout | Example | Strength | Cost |
|---|---|---|---|
| Per environment | `dev`, `test`, `prod` | Simple promotion; permissions per environment; workspace-catalog binding can pin each catalog to its workspace | A business domain's data is split across catalogs |
| Per domain/team | `finance`, `marketing` | Ownership is clear | Environments become schemas or naming conventions |
| Hybrid | `prod_finance`, `dev_finance` | Both | Many catalogs to manage |

Schemas then follow use cases, and medallion layers (Level 2 · 09) are commonly schemas (`bronze`, `silver`, `gold`) or
name prefixes. Whatever you pick, decide it **before** the first tables exist: moving data between catalogs later
means rewriting every reference.

## Creating the structure

```sql
CREATE CATALOG IF NOT EXISTS prod MANAGED LOCATION 's3://acme-prod/uc/prod' COMMENT 'Production data';
CREATE SCHEMA IF NOT EXISTS prod.sales COMMENT 'Sales domain';
CREATE TABLE prod.sales.orders (order_id BIGINT, customer_id BIGINT, amount DECIMAL(12,2), order_ts TIMESTAMP);
COMMENT ON TABLE prod.sales.orders IS 'One row per confirmed order';
```

The `CREATE CATALOG` syntax, from the SQL reference, includes `MANAGED LOCATION`, `COMMENT`, `RETAIN DROPPED FOR n DAYS`
and `DEFAULT COLLATION` options; `CREATE SCHEMA` takes `MANAGED LOCATION`, `COMMENT` and `RETAIN DROPPED FOR`. To create a
schema you need `CREATE SCHEMA` and `USE CATALOG` on the catalog (and, if you specify a managed location,
`CREATE MANAGED STORAGE` on the external location backing it). The bucket path above is a made-up example.

## Where do managed tables go?

A **managed storage location** is cloud storage where UC keeps the data of managed tables and volumes. It can be set on
the metastore, a catalog or a schema, and the most specific one wins. The documented rules:

1. If the schema has a managed location, data goes there.
2. Otherwise, if the catalog has one, data goes there.
3. Otherwise it goes to the metastore's.

Databricks recommends setting managed storage at the **catalog** level for logical isolation (the data of `prod` and `dev`
in different buckets, with different cloud permissions), with metastore-level and schema-level as options. Newly created
UC workspaces get a workspace catalog with its own managed storage, without a metastore-level location. Three more
documented properties: managed storage can't overlap external tables or volumes; a catalog or schema location must sit
inside an **external location** (next lessons); and UC appends hashed subdirectories (like
`<root>/__unitystorage/schemas/<uuid>`) so that every managed entity has a unique path.

You can see where things landed:

```sql
DESCRIBE CATALOG EXTENDED prod;
DESCRIBE SCHEMA EXTENDED prod.sales;
DESCRIBE DETAIL prod.sales.orders;         -- Delta: includes the table's storage location
DESCRIBE EXTENDED prod.sales.orders;
```

A practical consequence: the folder name of a managed table is a UUID path, not `.../orders`. You shouldn't read or write
managed-table files by path; always use the table name, and let UC resolve the storage and vend the credentials.

## Metadata you get for free

- **`information_schema`** in every catalog: SQL views listing tables, columns, privileges, constraints, volumes, tags.
  A catalog's `information_schema.tables` lists its tables; `information_schema.table_privileges` lists grants.
- **The `system` catalog**: Databricks-provided tables with audit logs, lineage, billing usage and more (Lesson 05
  and Level 4 · 06).
- **Tags and comments** on catalogs, schemas, tables and columns for discovery and classification (Lesson 05).
- **Discovery**: Catalog Explorer lets people browse and search objects they have `BROWSE` on.

## Hive metastore, the legacy world

Workspaces created before UC also have the **legacy `hive_metastore`**: a workspace-local, two-level (`schema.table`)
catalog, with table access control that is weaker and per-workspace. In a UC workspace it shows up as a catalog named
`hive_metastore` so old code keeps working. New work should go in UC catalogs; migration is covered in Level 4 · 09.

## How It Actually Works

UC is a metadata service in the Databricks control plane, plus enforcement points in compute. When a query names
`prod.sales.orders`, the compute resource asks UC: *who is this principal, does it hold `USE CATALOG`, `USE SCHEMA` and
`SELECT`, and where are the files?* If the answer is yes, UC returns the table's location and **short-lived, scoped
storage credentials** for that path; the engine then reads the files directly from cloud storage using those
credentials. If any check fails, the query fails before touching data. The user never holds cloud keys, which is why the
same rules apply to notebooks, jobs, SQL warehouses and BI tools: the check happens in the platform, not in each tool.

On shared (standard) compute, user code is isolated so one user's code can't read another's credentials (the
documentation describes this as Lakeguard). That is why some things behave differently on standard vs dedicated compute
(Level 1 · 02).

## Common mistakes

- **Putting everything in `main.default`.** It works, and gives you no isolation or ownership story.
- **Mixing environments in one catalog** and relying on naming conventions for access control.
- **Reading managed-table files by path** and bypassing governance (it often just fails; the path is not stable).
- **Forgetting the managed location rules**: a table "mysteriously" lands in the metastore root because the schema and
  catalog had none.
- **Using `hive_metastore`** for new tables because old tutorials did.
- **Granting before designing** the catalog/schema layout.

## Exercise

1. Sketch the catalog/schema layout for a company with two business domains and dev/prod environments. State which
   layout you chose and what you give up.
2. For each of your catalogs, decide where managed storage lives, and write the three-line precedence rule that explains
   where a new table in `prod.sales` is stored.
3. (Workspace) Run `DESCRIBE CATALOG EXTENDED` and `SELECT * FROM system.information_schema.catalogs` (or your own catalog's
   `information_schema.tables`), and list five columns you found useful.
4. Find in the documentation what the `RETAIN DROPPED FOR` clause on `CREATE SCHEMA` does and link it to
   `UNDROP` from Level 1 · 08.
