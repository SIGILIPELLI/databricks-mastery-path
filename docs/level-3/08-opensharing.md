---
description: "Share live data outside your workspace with OpenSharing (formerly Delta Sharing): shares, recipients, SQL setup and reading with the Python client."
---

# 08 · OpenSharing (Formerly Delta Sharing): Sharing Data Across Organizations

Eventually someone outside your workspace needs your data: a partner, a customer, another business unit on another cloud, or a
team that uses a different tool. The traditional answers (export CSVs, copy to a bucket, give out credentials) create stale
copies and lose governance. Databricks' answer is a sharing protocol that exposes governed tables **live, read-only, without
copying**.

!!! note "A naming note, and what was run"
    The current Databricks documentation calls this feature **OpenSharing** (its documentation hub is at the old `delta-sharing`
    URL path, and API paths still contain `delta-sharing`); older material and the open-source Python client call it
    **Delta Sharing**. I use OpenSharing, and mention the old name so searches work. I did not verify whether the
    open-source project has been renamed. **Nothing was run against a Databricks workspace.** The concepts and SQL come
    from the documentation (October 2026). Locally I installed the open-source Python client (`delta-sharing` 1.4.2, from
    PyPI) and checked its API surface and how it fails when no server is reachable, against a reserved `.invalid` host, so no
    real server was contacted.

## Concepts

- A **share** is a read-only collection of data assets that a **provider** exposes: tables and table partitions, and
  (for Databricks recipients) also views (including dynamic views with row and column rules), streaming tables, materialized
  views, volumes, models and notebooks. In Unity Catalog a share is a securable object; dropping it cuts off every recipient.
- A **recipient** is the named identity that consumes shares. It is an object in the provider's metastore.
- A **provider**, from the recipient's side, is the organization that shares with you.

Three ways to share, from the documentation:

| Mode | Recipient | What you get |
|---|---|---|
| **Databricks-to-Databricks** | A Unity Catalog workspace | No tokens to manage; shares appear as a *catalog*; supports notebooks, volumes, models, views, plus governance, auditing and usage tracking on both sides |
| **Databricks-to-Open** | Any platform (Spark, pandas, Power BI, Iceberg clients...) | A credential file with a bearer token or OIDC federation; tabular data |
| **Self-hosted open-source server** | Anyone | You run the sharing server; the Databricks docs don't cover setting it up |

## Providing data: the SQL

```sql
CREATE SHARE IF NOT EXISTS partner_sales COMMENT 'Orders and revenue for ACME Corp';

ALTER SHARE partner_sales ADD TABLE prod.gold.daily_revenue COMMENT 'Daily revenue per customer' WITH HISTORY;
ALTER SHARE partner_sales ADD SCHEMA prod.gold_public;

CREATE RECIPIENT IF NOT EXISTS acme_corp COMMENT 'ACME data team';
GRANT SELECT ON SHARE partner_sales TO RECIPIENT acme_corp;
```

Syntax follows the reference: `CREATE SHARE [IF NOT EXISTS] name [COMMENT ...]`; `ALTER SHARE name ADD [TABLE] t [COMMENT] [PARTITION ...]
[AS alias] [WITH HISTORY | WITHOUT HISTORY]` (also `SCHEMA`, `VIEW`, `MATERIALIZED VIEW`, `MODEL`, `REMOVE`, `RENAME`, `OWNER`);
`CREATE RECIPIENT [IF NOT EXISTS] name [USING ID sharing_identifier] [COMMENT ...] [PROPERTIES (...)]`; and
`GRANT SELECT ON SHARE s TO RECIPIENT r`. `SELECT` is the only privilege you can grant on a share. The names are invented
and none of it was run; `sqlglot` can't parse sharing DDL, so I checked each form against the SQL reference pages.
Creating a share needs `CREATE SHARE` on the metastore; creating a recipient needs `CREATE RECIPIENT` (or metastore admin).

Two design choices in the syntax matter:

- **`WITH HISTORY`** shares the table's history, which lets recipients time-travel and read the change data feed; without it
  they see only the current version. It also exposes your past data, including rows you may have deleted, so choose deliberately.
- **Partitions**: you can share only some partitions (for example one customer's data), but a plain table share exposes everything.
  For per-recipient data, prefer a view or a dynamic view (Databricks recipients only) over many partition shares.

### Recipients without Databricks (open sharing)

For a recipient outside Databricks you create a recipient with token-based authentication; Databricks produces an activation link
or a **credential file** (a small JSON profile with the endpoint and a bearer token, possibly with an expiry). The provider
controls expiry and rotation. Treat it like a password: send it over a secure channel, never commit it to Git.
The documentation also describes OIDC federation for recipients, IP access lists, and network egress controls for the provider.

## Consuming data

### Databricks recipient

A privileged user (metastore admin, or someone with `CREATE CATALOG` plus `USE PROVIDER`) creates a catalog from the share:

```sql
SHOW PROVIDERS;
SHOW SHARES IN PROVIDER `acme_provider`;
CREATE CATALOG acme_shared USING SHARE `acme_provider`.partner_sales;
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG acme_shared TO `analysts`;
SELECT * FROM acme_shared.gold.daily_revenue;
```

(`CREATE CATALOG ... USING SHARE provider.share` is in the `CREATE CATALOG` syntax; the other statements are standard.) The
shared catalog is read-only, and Unity Catalog privileges inside it follow the normal hierarchy. One documented quirk: the
`information_schema` of a shared catalog is updated from the provider only when you query a shared table or run a command like
`DESCRIBE`, so it can look stale.

### Any other platform: the open-source client

The open-source Python client reads a profile file and speaks the sharing REST protocol:

```python
import delta_sharing

client = delta_sharing.SharingClient("config.share")
client.list_all_tables()
df = delta_sharing.load_as_pandas("config.share#partner_sales.gold.daily_revenue")
# Spark: spark.read.format("deltaSharing").load("config.share#partner_sales.gold.daily_revenue")
```

The usage pattern (`profile#share.schema.table`, `load_as_pandas`, `load_as_spark`, the `deltaSharing` Spark format and
`versionAsOf` for shared history) follows the Databricks documentation. Locally, with a dummy profile pointing at a
reserved `.invalid` host, the client's API listed `SharingClient`, `load_as_pandas`, `load_as_spark`, `load_table_changes_as_pandas`,
`get_table_version` and friends, and a `list_shares()` call failed with a `ConnectionError` for
`https://example.invalid/delta-sharing/shares`: confirming the client calls `<endpoint>/shares`. I couldn't test real
reads without a share.

## How It Actually Works

OpenSharing is a REST protocol with one clever trick: the server never streams your data. A recipient asks for a table;
the server checks the token and the share, then returns a list of **pre-signed URLs** to the table's Parquet files in cloud
storage (plus the table's metadata and version). The client then downloads those files directly from the cloud storage using the
URLs, which expire quickly. The data doesn't pass through the sharing server, the recipient never receives cloud credentials,
and the provider's storage serves the bytes. For Databricks-to-Databricks sharing the same idea applies with Unity Catalog
doing the authentication.

Consequences: (1) reading is a file listing plus downloads, so **egress costs** fall on the provider's cloud account (the docs
cover egress management and cost considerations; check them before sharing large tables across regions or clouds); (2) data is
live, so recipients see new versions as the provider commits them; (3) recipients can read and copy what they can see, so
sharing is not a way to retain control over data once seen; and (4) shared Delta features depend on the recipient's client
supporting the table's reader protocol.

## Operating shares

- **Audit**: sharing activity is recorded in the audit log system table and in provider/recipient usage views (the docs have
  an audit-logs page for sharing).
- **Revoke**: `REVOKE SELECT ON SHARE s FROM RECIPIENT r` stops access at the next request; rotate bearer tokens periodically.
- **Schema changes**: adding columns is generally safe; dropping or renaming columns can break recipients. Treat a shared table as
  an API with a contract and version it (`orders_v1`).
- **PII**: share a curated gold or a dynamic view, never the raw table. Row/column rules on shared dynamic views apply for
  Databricks recipients.
- **Marketplace and clean rooms** are built on the same protocol: public listings, and joint analysis without exposing raw rows.

## Common mistakes

- **Sharing a bronze table** that includes data you didn't mean to expose, with `WITH HISTORY` exposing deleted rows too.
- **Committing the credential file** to a repository or emailing it unencrypted.
- **Forgetting egress cost** for a large shared table that many recipients scan fully.
- **No contract on schema**: a column rename breaks someone you've never met.
- **Using shares as an access-control substitute** inside one organization; use Unity Catalog grants there.
- **Expecting to revoke what was already copied.**

## Exercise

1. Write the full provider-side SQL to share only the `gold` aggregates for one customer using a view, and explain why a view
   beats a partition share when you have 200 customers.
2. Decide for each case whether to use Databricks-to-Databricks or open sharing: (a) another business unit on Databricks,
   (b) a partner who uses Power BI, (c) a university running R.
3. Install `delta-sharing` locally and write a function that validates a profile file (required keys present, token not
   committed in the working tree). Test it with a fake profile.
4. Estimate the monthly egress cost of 50 recipients each reading a 200 GB table once a day, using your cloud's published
   pricing. State your assumptions and what would reduce the cost.
