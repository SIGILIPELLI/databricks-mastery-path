---
description: "Unity Catalog privileges: USE CATALOG and USE SCHEMA, inheritance, ownership vs MANAGE, GRANT and REVOKE, with a runnable model of the rules."
---

# 02 · Privileges, Ownership & Grants

Unity Catalog access control looks simple (`GRANT SELECT ON TABLE ...`) until the first time someone with `SELECT` gets
"permission denied", or a catalog owner can't read a table they apparently own. The rules are consistent once you know
three ideas: **usage privileges are prerequisites**, **privileges inherit downward**, and **ownership does not**.

!!! note "What was run"
    Unity Catalog isn't available locally, so no `GRANT` below was executed. The rules are summarized from the Databricks
    documentation page on Unity Catalog permissions concepts, and the `GRANT`, `REVOKE` and `SHOW GRANTS` syntax from the SQL
    reference (October 2026); `sqlglot` can't validate this DDL (it falls back to treating it as an opaque command), so I
    checked it by hand against those pages. What I *did* run is a small Python model, written by me from the documented rules,
    that lets you step through access decisions on a laptop. **It is a teaching aid, not Databricks code**, and
    covers only the rules listed, so it can't tell you what a real workspace will do in edge cases.

## Principals and privileges

A **principal** is a user, a group or a service principal. Grant to **groups**, not individuals: membership changes without
touching grants, and `SHOW GRANTS` stays readable.

```sql
GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.sales TO `analysts`;
GRANT MODIFY ON TABLE prod.sales.orders TO `etl-service`;
REVOKE SELECT ON TABLE prod.sales.orders FROM `interns`;
SHOW GRANTS ON TABLE prod.sales.orders;
SHOW GRANTS `analysts` ON SCHEMA prod.sales;
```

Syntax from the reference: `GRANT privilege_types ON securable_object TO principal`, the mirror `REVOKE ... FROM`, and
`SHOW GRANTS [principal] ON securable_object`. The documentation notes that a `REVOKE` succeeds even if the privilege
was never granted: it guarantees the privilege is absent, so it is safe in idempotent scripts.

Common privileges: `SELECT` (read tables/views), `MODIFY` (write; a composite whose children are `INSERT`, `UPDATE` and
`DELETE`), `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `CREATE SCHEMA`, `EXECUTE` (functions), `READ VOLUME` and
`WRITE VOLUME`, `BROWSE`, `MANAGE`, `APPLY TAG`. Use the privileges reference page for the full list: it changes.

## Rule 1: usage privileges are prerequisites

To read a table you need **all three**: `USE CATALOG` on its catalog, `USE SCHEMA` on its schema and `SELECT` on the table.
The documentation states plainly that `SELECT` alone is not sufficient. The design lets catalog and schema owners
draw boundaries: a table owner can't share data outside the area the parent owners approved, because only they can grant
usage privileges.

| Operation | Needs |
|---|---|
| Read a table or view | `USE CATALOG` + `USE SCHEMA` + `SELECT` |
| Write a table | `USE CATALOG` + `USE SCHEMA` + `MODIFY` |
| Create a table | `USE CATALOG` + `USE SCHEMA` + `CREATE TABLE` (on the schema, or inherited from the catalog) |
| Create a schema | `USE CATALOG` + `CREATE SCHEMA` on the catalog |
| Run a function | `USE CATALOG` + `USE SCHEMA` + `EXECUTE` |
| Read volume files | `USE CATALOG` + `USE SCHEMA` + `READ VOLUME` |

## Rule 2: privileges inherit downward

A privilege granted on a **catalog** applies to every schema, table, view, volume and function in it, including ones created
later. A privilege on a **schema** applies to everything in that schema. So `GRANT SELECT ON CATALOG prod TO analysts`
means "read everything in prod, now and in the future", which is convenient and also the most common over-grant.
Denials (from ABAC deny policies) inherit the same way. One exception: privileges granted on the **metastore** do not
inherit to data objects (they control metastore-level operations such as `CREATE CATALOG`), except `READ METADATA`.

## Rule 3: ownership doesn't inherit, but gives control

Every securable has an owner, initially its creator. The owner can do everything on that object, grant and revoke
privileges, transfer ownership and drop it, and automatically gets the ability to *manage* all child objects. But
ownership **does not make you the owner of children**, and an owner of a catalog doesn't hold `SELECT` on the tables
inside until it is granted (or the owner uses the manage ability to grant it to themselves). Databricks also doesn't
list `ALL PRIVILEGES` for owners in `SHOW GRANTS`, so owners are easy to miss when auditing.

`MANAGE` is like ownership for permissions: it lets someone grant and revoke privileges, transfer ownership and drop the
object, without being the owner. `ALL PRIVILEGES` deliberately **excludes** `MANAGE` (and `READ METADATA`, `EXTERNAL USE
SCHEMA`, `EXTERNAL USE LOCATION`) to prevent accidental escalation.

`BROWSE` lets users discover that an object exists and request access, without access to data; the documentation
recommends granting it on catalogs to all account users so data is discoverable.

Make owners groups or service principals, not individuals: when a person leaves, objects owned by their account become a
problem.

## Step through the rules: a runnable model

This model encodes the rules above (usage prerequisites, inheritance from catalog/schema, ownership that does not inherit,
`ALL PRIVILEGES` excluding `MANAGE`). Run it and watch each decision change as grants are added.

```python
"""A teaching model of Unity Catalog's documented privilege rules (NOT Databricks code, no real API)."""
from dataclasses import dataclass, field

@dataclass
class Obj:
    kind: str; name: str; parent: "Obj|None" = None; owner: str = "admin"
    grants: dict = field(default_factory=dict)   # principal -> set(privileges)
    def path(self): return (self.parent.path() + "." if self.parent else "") + self.name
    def chain(self):  # object, then ancestors
        o, out = self, []
        while o: out.append(o); o = o.parent
        return out

def groups_of(user, membership): return {user} | membership.get(user, set())

def has(obj, principals, priv):
    """Explicit/inherited grant of `priv` on obj (grants on ancestors inherit down, except metastore)."""
    for o in obj.chain():
        if o.kind == "metastore" and o is not obj: continue
        for p in principals:
            gs = o.grants.get(p, set())
            if priv in gs or "ALL PRIVILEGES" in gs and priv not in ("MANAGE", "READ METADATA"): return True
    return False

def can_select(table, user, membership):
    pr = groups_of(user, membership); cat, sch = table.parent.parent, table.parent
    usage = (has(cat, pr, "USE CATALOG") or cat.owner in pr) and (has(sch, pr, "USE SCHEMA") or sch.owner in pr or cat.owner in pr)
    data = has(table, pr, "SELECT") or table.owner in pr
    return usage and data, dict(use_catalog=has(cat, pr, "USE CATALOG") or cat.owner in pr, use_schema=has(sch, pr, "USE SCHEMA") or sch.owner in pr or cat.owner in pr, select=data)

ms = Obj("metastore", "ms"); cat = Obj("catalog", "prod", ms, owner="platform"); sch = Obj("schema", "sales", cat, owner="platform")
orders = Obj("table", "orders", sch, owner="dana")
members = {"ana": {"analysts"}, "ben": {"analysts", "interns"}, "dana": set(), "platform": set()}
def show(label, user):
    ok, parts = can_select(orders, user, members); print(f"{label:<62} -> {'ALLOW' if ok else 'DENY '} {parts}")

show("1. nothing granted, user ana", "ana")
orders.grants["analysts"] = {"SELECT"}
show("2. SELECT on table only (no usage privileges)", "ana")
cat.grants["analysts"] = {"USE CATALOG"}
show("3. + USE CATALOG", "ana")
sch.grants["analysts"] = {"USE SCHEMA"}
show("4. + USE SCHEMA  (all three now present)", "ana")
orders.grants.clear(); cat.grants["analysts"] = {"USE CATALOG", "SELECT"}; sch.grants.clear()
show("5. SELECT + USE CATALOG on catalog, no USE SCHEMA", "ana")
sch.grants["analysts"] = {"USE SCHEMA"}
show("6. + USE SCHEMA: SELECT inherited from the catalog", "ana")
show("7. table owner dana (owner of table, no usage grants)", "dana")
cat.grants["dana"] = {"USE CATALOG"}; sch.grants["dana"] = {"USE SCHEMA"}
show("8. dana gets usage privileges", "dana")
show("9. platform (owner of catalog) reads the table", "platform")
cat.grants["analysts"] = {"USE CATALOG", "ALL PRIVILEGES"}
print("ALL PRIVILEGES includes MANAGE?", has(cat, {"analysts"}, "MANAGE"))
```

Output of running it:

```text
1. nothing granted, user ana                                   -> DENY  {'use_catalog': False, 'use_schema': False, 'select': False}
2. SELECT on table only (no usage privileges)                  -> DENY  {'use_catalog': False, 'use_schema': False, 'select': True}
3. + USE CATALOG                                               -> DENY  {'use_catalog': True, 'use_schema': False, 'select': True}
4. + USE SCHEMA  (all three now present)                       -> ALLOW {'use_catalog': True, 'use_schema': True, 'select': True}
5. SELECT + USE CATALOG on catalog, no USE SCHEMA              -> DENY  {'use_catalog': True, 'use_schema': False, 'select': True}
6. + USE SCHEMA: SELECT inherited from the catalog             -> ALLOW {'use_catalog': True, 'use_schema': True, 'select': True}
7. table owner dana (owner of table, no usage grants)          -> DENY  {'use_catalog': False, 'use_schema': False, 'select': True}
8. dana gets usage privileges                                  -> ALLOW {'use_catalog': True, 'use_schema': True, 'select': True}
9. platform (owner of catalog) reads the table                 -> DENY  {'use_catalog': True, 'use_schema': True, 'select': False}
ALL PRIVILEGES includes MANAGE? False
```

Rows 7 and 9 are the surprising ones. In row 7 even the *table's owner* can't read it without usage privileges on the
parents (the documentation says owners don't need usage privileges only for exercising `MANAGE`-type metadata abilities,
not for reading data). In row 9 the catalog's owner has full control over the catalog and can manage its children, yet
reading a table still needs `SELECT`, which they can grant themselves.

## A practical grant design

1. Create groups by **role** (`analysts`, `data-engineers`, `finance-readers`), synchronized from your identity provider.
2. Grant `USE CATALOG` and `BROWSE` broadly; grant `USE SCHEMA` + `SELECT` at schema level for read-only roles; grant
   `CREATE TABLE`/`MODIFY` only to engineering and service principals.
3. Keep grants at the **highest sensible level** to avoid thousands of table-level grants, but resist catalog-wide
   `SELECT` in catalogs that mix sensitive and non-sensitive schemas.
4. Run pipelines as **service principals**, not people.
5. Audit with `SHOW GRANTS` and the `information_schema` privilege views, plus the audit log (Lesson 05).

## How It Actually Works

Each grant is a row in the metastore: (securable, principal, privilege). On a query, compute asks UC to resolve the
principal's effective groups, walk from the table up to the metastore collecting grants and owners, and apply the rules:
the three-part requirement for reads, inheritance from containers, and special cases such as ownership and `MANAGE`.
Because evaluation happens in UC, one change takes effect for all compute immediately; there are no cached permissions to
flush on clusters. Row filters, column masks and ABAC policies (Lesson 04) are evaluated *after* the basic privileges
let the query through.

## Common mistakes

- **`GRANT SELECT` on a table but no usage privileges** on its parents.
- **Granting `SELECT` on a catalog** that contains sensitive schemas.
- **Granting to individuals** instead of groups.
- **Using `ALL PRIVILEGES` as a shortcut** and assuming it covers `MANAGE`.
- **Individual owners**, so access dies with the person.
- **Assuming ownership of a catalog gives data access** to everything inside.
- **Not auditing**: owners don't appear as `ALL PRIVILEGES` in `SHOW GRANTS`.

## Exercise

1. Add a new group `interns` to the model with `SELECT` on a second table and show it can't read the first table.
2. Extend the model with `BROWSE` and show a user who can see the table's metadata but not read it.
3. Design the grants for three roles (analyst, engineer, service principal) over catalog `prod` with schemas `bronze`,
   `silver`, `gold`. Write the exact `GRANT` statements.
4. In the documentation, find what `EXTERNAL USE SCHEMA` protects against, and why `ALL PRIVILEGES` excludes it.
