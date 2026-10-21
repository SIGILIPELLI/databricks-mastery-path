---
description: "Fine-grained access in Unity Catalog: row filters, column masks, ABAC policies on tags, dynamic views, and a local analogue of the logic."
---

# 04 · Row Filters, Column Masks & ABAC

Table privileges are all-or-nothing: a user either can `SELECT` the table or can't. Real requirements are finer: the
EU team sees only EU customers; analysts see an email as `a***@x.com`; compliance sees it in full. Unity Catalog provides
**row filters** and **column masks**, applied by the platform at query time, and **ABAC policies** that apply them across
many tables by tag.

!!! note "What was run, and what wasn't"
    The Unity Catalog features here (`SET ROW FILTER`, `SET MASK`, `CREATE POLICY`, governed tags) exist only on
    Databricks and **were not run**. Their syntax and rules come from the Databricks documentation (October 2026) and the
    SQL reference. To show the *logic* running, I built an analogue on **local Spark 4.2.0**: SQL functions and a session
    variable standing in for the current user. That analogue uses an ordinary function call inside a query, which
    is not what Unity Catalog does (UC attaches the function to the table so users can't skip it); it only demonstrates
    what such functions compute.

## Row filters

A row filter is a SQL user-defined function that returns a boolean for each row. Rows where it returns `FALSE` (or NULL)
are excluded. It's bound to a table with `ALTER TABLE ... SET ROW FILTER`:

```sql
CREATE FUNCTION prod.sec.us_filter(region STRING)
RETURN IF(IS_ACCOUNT_GROUP_MEMBER('admin'), true, region = 'US');

ALTER TABLE prod.sales.customers SET ROW FILTER prod.sec.us_filter ON (region);
ALTER TABLE prod.sales.customers DROP ROW FILTER;
```

(The function body follows the documentation's example; names are invented.) Anyone who queries `customers` now gets only
`region = 'US'` rows unless they're in the `admin` account group. The filter is part of the table, so it applies in
notebooks, jobs, SQL warehouses and BI tools alike.

## Column masks

A column mask is a SQL function that receives the column's value and returns what the user should see; its return type
must match or be castable to the column's type. Each column can have one mask, and a mask can take additional columns as
inputs:

```sql
CREATE FUNCTION prod.sec.mask_email(email STRING)
RETURN CASE WHEN IS_ACCOUNT_GROUP_MEMBER('compliance') THEN email
            ELSE CONCAT(SUBSTR(email, 1, 1), '***@', SPLIT(email, '@')[1]) END;

ALTER TABLE prod.sales.customers ALTER COLUMN email SET MASK prod.sec.mask_email;
ALTER TABLE prod.sales.customers ALTER COLUMN email DROP MASK;
```

Syntax per the reference: `ALTER TABLE t ALTER COLUMN c SET MASK fn [USING COLUMNS (...)]` and `... DROP MASK`.

### The logic, running locally

To see what these functions compute, I ran the same two functions on local Spark, with a `grants` table and a session
variable `who` as the "current user":

```sql
DECLARE OR REPLACE VARIABLE who STRING DEFAULT 'nobody';
CREATE OR REPLACE TEMPORARY FUNCTION region_ok(r STRING) RETURNS BOOLEAN
  RETURN EXISTS(SELECT 1 FROM grants g WHERE g.usr = who AND (g.region = '*' OR g.region = r));
CREATE OR REPLACE TEMPORARY FUNCTION mask_email(e STRING) RETURNS STRING
  RETURN CASE WHEN EXISTS(SELECT 1 FROM grants g WHERE g.usr = who AND g.role = 'admin') THEN e
              ELSE concat(substr(e,1,1), '***@', split(e,'@')[1]) END;

SET VAR who = 'alice';
SELECT id, mask_email(email) AS email, region, salary FROM customers WHERE region_ok(region) ORDER BY id;
```

With `grants` = (alice, analyst, US), (erin, analyst, EU), (root, admin, `*`):

```text
alice (US analyst)            erin (EU analyst)           root (admin)                       mallory (not in grants)
 1 a***@x.com US 50000         2 b***@y.org EU 72000       1 ana@x.com US   50000            (no rows)
 3 c***@z.net US 64000                                     2 bo@y.org  EU   72000
                                                           3 cy@z.net  US   64000
                                                           4 di@w.io   APAC 58000
```

Alice sees US rows with masked emails, Erin sees one EU row, root sees all rows unmasked, and a user absent from the
table gets **no rows**: the filter *fails closed*. That's the property to design for: the default for an unknown user
must be "nothing". (A mask that returned the real value when a lookup found nothing would fail open.)

## Why not just use views?

A **dynamic view** wraps tables in a view that filters/masks using functions like `is_account_group_member()`, and users get
access to the view but not the base table. It still works, and the documentation recommends it when you want to expose a
curated, transformed or joined version of data to people with no access to the underlying tables. But it has weaknesses
for pure security: you must remember to *not* grant the base table, every view is another object to maintain, and the same
rule is copied into many views.

| Approach | Attaches to | Managed by | Best for |
|---|---|---|---|
| Row filter / column mask | One table or column | Table owner or `MANAGE` | Table-specific logic |
| ABAC policy | Tables/columns matched by **tags**, at catalog/schema/table (or metastore) scope | Whoever owns/`MANAGE`s the scope | One rule across many tables |
| Dynamic view | A view over base tables | The view's SQL | Curated or joined slices |

## ABAC: policies matched by tags

Attribute-based access control moves the rule from the table to the *classification of the data*. You tag columns
(`pii`, `class = ssn`) using **governed tags** (tags whose allowed values and who may apply them are controlled), then
write one policy that says "mask every column tagged `pii` for these principals". New tables that carry the tag are covered
automatically, and because policies are attached at catalog or schema level by higher-level admins, table owners can't
remove them.

```sql
CREATE OR REPLACE POLICY mask_pii_email
ON SCHEMA prod.sales
COLUMN MASK prod.sec.mask_email
TO `analysts` EXCEPT `compliance`
FOR TABLES
MATCH COLUMNS has_tag('pii') AS pii_col
ON COLUMN pii_col;
```

This follows the documented `CREATE POLICY` grammar: `ON { METASTORE | CATALOG | SCHEMA | TABLE }`, a body of `ROW FILTER fn`
or `COLUMN MASK fn`, `TO principals [EXCEPT principals]`, `FOR TABLES`, optional `WHEN condition` (table-level tags),
`MATCH COLUMNS condition [AS alias]`, and `ON COLUMN alias` for masks. To create one you need `MANAGE` on the securable (or
ownership) and `EXECUTE` on the function. Tag functions `has_tag()` and `has_tag_value()` work in `WHEN` (checking tags on the
table, including inherited from catalog/schema) and `MATCH COLUMNS` (checking tags on the column itself). The statement
above wasn't parseable by `sqlglot` and I couldn't run it; treat it as documented syntax, not tested code. ABAC can also
grant and deny privileges (GRANT policies; DENY policies are in Beta per the documentation).

## Performance and safety rules (from the docs)

- The engine **always chooses the secure plan** over the optimized plan when they conflict, so filters and masks can slow
  queries. Keep functions small: simple `CASE` over lookups; few arguments; few distinct masks per table.
- Use **deterministic, non-throwing** expressions. An expression that can error (ANSI division by zero) blocks optimizations
  because the error could leak information about values before filtering. Prefer `try_divide`.
- Prefer SQL over Python UDFs.
- **Type mismatch trap.** If a function parameter is `INT` but the column is `STRING`, Databricks casts implicitly. With
  ANSI mode **off**, uncastable values silently become NULL, which can make a filter return all rows, or a mask hide the
  wrong ones. Keep ANSI mode on, and match types exactly.
- Drop the filter **before** dropping the function: otherwise the table can become inaccessible (`ALTER TABLE ... DROP ROW
  FILTER` repairs it).
- `REPLACE TABLE` retains existing row filters and masks (when columns match), a safety feature that can also surprise you.
- Only one distinct row filter can resolve per user and table, so combine rules with `AND` in the function.

## How It Actually Works

When a query reads a protected table, the planner replaces the table scan with the scan wrapped in the filter or mask
expressions, *before* the user's own predicates and projections can see raw values. That rewrite happens in the engine
for every query on the table, in any language, which is why a user can't bypass it by writing `SELECT *` or using a different
tool. Because the function runs as part of the plan, anything it calls (group membership checks, lookups) is evaluated per
row or per batch; hence the performance guidance. Fine-grained access also affects what compute can run it: the
documentation ties it to standard (Lakeguard-isolated) compute or serverless so users' code cannot read unfiltered files
directly (check the current compute requirements for your runtime).

## Common mistakes

- **Fail-open functions**: unknown users or NULL lookups returning the real data.
- **Masking in a view but granting the base table** to the same people.
- **Per-table copies of the same rule** instead of ABAC.
- **Complicated functions** with joins and Python, then wondering why queries slow down.
- **Implicit cast bugs** from mismatched parameter types with ANSI mode off.
- **Dropping the function first**, leaving the table inaccessible.
- **Forgetting derived data**: a copy of a masked table's unmasked output (an extract written to a file) is no longer protected.

## Exercise

1. Extend the local analogue so that salary is masked to a range (`50k-60k`) for non-admins, and a second mask rule
   shows the real number to users with role `hr`.
2. Add a unit test that proves the filter fails closed for a user not in `grants`, and for a NULL `region`.
3. Write the ABAC policy (documented syntax) to mask every column tagged `pii` in `prod.hr` for the group `analysts` except
   `hr`, and the row filter policy that restricts `prod.sales` tables tagged `regional` by region.
4. Choose, for a table with 10 sensitive columns used by 30 teams, between per-column masks, ABAC and a dynamic view,
   and justify the choice.
