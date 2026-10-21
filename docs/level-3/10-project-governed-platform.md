---
description: "Level 3 project: describe a governed platform as a spec, generate Unity Catalog SQL from it, and lint it with tests for ownership and privileges."
---

# 10 · Project — Design a Governed Data Platform

You can't run Unity Catalog on a laptop, but you *can* treat your governance design as code: a declarative spec of catalogs,
schemas, groups and grants; a generator that turns it into SQL; and a **linter with tests** that catches the mistakes from
this level before anyone runs a `GRANT`. That is also how teams review access changes in practice: as a pull request on a
spec, not as clicks in a UI.

!!! note "What was run, and what wasn't"
    The Python below (generator, linter and tests) was run locally with Python 3.12, and all output in this lesson is
    copied from that run. **The generated SQL was not executed against Unity Catalog**; it follows the syntax in the
    Databricks SQL reference (Lessons 01 to 04). The lint rules are *my encoding of the rules documented in this level*
    (usage privileges, inheritance, ownership, `ALL PRIVILEGES` excluding `MANAGE`, no overlapping external locations),
    so passing the linter means "consistent with those rules", not "will succeed on a workspace". The bucket names, group names and
    catalog layout are invented.

## The design being encoded

A small company, one production catalog `prod`, with a medallion layout (Level 2 · 09):

| Schema | Content | Who may read | Who may write |
|---|---|---|---|
| `prod.bronze` | Raw data | Data engineers, the ETL service principal | The ETL service principal |
| `prod.silver` | Cleaned, contains PII | Data engineers only | Data engineers |
| `prod.gold` | Aggregates for the business | Analysts, data engineers | Data engineers |

Principles: grant to **groups** and **service principals**, never to individuals; owners are groups; analysts can *discover*
the catalog (`BROWSE`) but only read `gold`; managed storage is set at catalog level inside an external location; external
locations never overlap.

## The code

`uc_platform.py` holds the spec, the SQL generator and the linter:

```python
"""Governance-as-code: turn a platform spec into Unity Catalog SQL and lint it against the rules from Level 3.
Pure Python (no Databricks needed). The generated SQL is NOT executed here; it follows the documented syntax."""
import re, sys

SPEC = {
    "env": "prod",
    "locations": {"raw": "s3://acme-raw/landing", "lake": "s3://acme-lake/prod"},
    "catalogs": {"prod": {"owner": "platform-admins", "managed_location": "s3://acme-lake/prod/uc"}},
    "schemas": {   # catalog.schema -> settings
        "prod.bronze": {"owner": "data-engineers", "sensitive": False},
        "prod.silver": {"owner": "data-engineers", "sensitive": True},
        "prod.gold":   {"owner": "data-engineers", "sensitive": False},
    },
    "groups": {"analysts", "data-engineers", "platform-admins", "etl-service"},
    "grants": [   # (principal, privilege(s), securable kind, securable name)
        ("analysts", ["USE CATALOG", "BROWSE"], "CATALOG", "prod"),
        ("analysts", ["USE SCHEMA", "SELECT"], "SCHEMA", "prod.gold"),
        ("data-engineers", ["USE CATALOG", "USE SCHEMA", "CREATE TABLE", "SELECT", "MODIFY"], "CATALOG", "prod"),
        ("etl-service", ["USE CATALOG"], "CATALOG", "prod"),
        ("etl-service", ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"], "SCHEMA", "prod.bronze"),
    ],
}

def generate(spec):
    out = []
    for c, o in spec["catalogs"].items():
        out.append(f"CREATE CATALOG IF NOT EXISTS {c} MANAGED LOCATION '{o['managed_location']}';")
        out.append(f"ALTER CATALOG {c} OWNER TO `{o['owner']}`;")
    for s, o in spec["schemas"].items():
        out.append(f"CREATE SCHEMA IF NOT EXISTS {s};")
        out.append(f"ALTER SCHEMA {s} OWNER TO `{o['owner']}`;")
    for who, privs, kind, name in spec["grants"]:
        out.append(f"GRANT {', '.join(privs)} ON {kind} {name} TO `{who}`;")
    return out

def contains(a, b): a = a.rstrip("/") + "/"; return (b.rstrip("/") + "/").startswith(a)

def lint(spec):
    problems = []
    # 1. owners are groups, not people
    for kind, items in (("catalog", spec["catalogs"]), ("schema", spec["schemas"])):
        for name, o in items.items():
            if o["owner"] not in spec["groups"] or "@" in o["owner"]:
                problems.append(f"OWNER: {kind} {name} is owned by {o['owner']!r}, which is not a known group")
    # 2. grants go to known groups only
    for who, privs, kind, name in spec["grants"]:
        if who not in spec["groups"]: problems.append(f"PRINCIPAL: grant to unknown principal {who!r}")
    # 3. usage prerequisites: any SELECT/MODIFY/CREATE TABLE on schema or catalog needs USE CATALOG (+ USE SCHEMA at/under schema)
    def privs_of(p, kind, name):
        return {x for w, ps, k, n in spec["grants"] if w == p and k == kind and n == name for x in ps}
    for p in {g[0] for g in spec["grants"]}:
        for s in spec["schemas"]:
            cat = s.split(".")[0]
            eff = set()
            for w, ps, k, n in spec["grants"]:
                if w != p: continue
                if (k == "CATALOG" and n == cat) or (k == "SCHEMA" and n == s): eff |= set(ps)
            data = eff & {"SELECT", "MODIFY", "CREATE TABLE"}
            has_cat = "USE CATALOG" in privs_of(p, "CATALOG", cat)
            has_sch = "USE SCHEMA" in eff
            if data and not has_cat: problems.append(f"USAGE: {p} has {sorted(data)} on {s} but no USE CATALOG on {cat}")
            if data and not has_sch: problems.append(f"USAGE: {p} has {sorted(data)} on {s} but no USE SCHEMA (granted on schema or inherited from catalog)")
    # 4. catalog-wide SELECT must not reach a sensitive schema for non-engineering groups
    for who, privs, kind, name in spec["grants"]:
        if kind == "CATALOG" and "SELECT" in privs and who not in ("data-engineers", "platform-admins"):
            sens = [s for s, o in spec["schemas"].items() if s.startswith(name + ".") and o["sensitive"]]
            if sens: problems.append(f"INHERIT: catalog-level SELECT for {who} reaches sensitive schema(s) {sens}")
    # 5. ALL PRIVILEGES shortcut
    for who, privs, kind, name in spec["grants"]:
        if "ALL PRIVILEGES" in privs: problems.append(f"ALLPRIV: {who} granted ALL PRIVILEGES on {name}; it excludes MANAGE, list explicit privileges")
    # 6. managed locations must sit inside an external location and external locations must not overlap
    locs = list(spec["locations"].items())
    for i, (n1, u1) in enumerate(locs):
        for n2, u2 in locs[i+1:]:
            if contains(u1, u2) or contains(u2, u1): problems.append(f"OVERLAP: external locations {n1!r} and {n2!r} overlap")
    for c, o in spec["catalogs"].items():
        if not any(contains(u, o["managed_location"]) for u in spec["locations"].values()):
            problems.append(f"LOCATION: catalog {c} managed location {o['managed_location']} is not inside any external location")
    return problems

if __name__ == "__main__":
    print("-- generated SQL --"); print("\n".join(generate(SPEC)))
    print("\n-- lint --"); probs = lint(SPEC)
    print("\n".join(probs) if probs else "OK"); sys.exit(1 if probs else 0)
```

`test_platform.py` mutates the spec in eight ways and asserts the linter's verdict for each:

```python
import copy
from uc_platform import SPEC, lint

def mutate(fn):
    s = copy.deepcopy(SPEC); fn(s); return lint(s)

cases = {
 "baseline spec is clean": lambda s: None,
 "person owns a schema": lambda s: s["schemas"]["prod.gold"].update(owner="dana@acme.com"),
 "analysts get SELECT on whole catalog": lambda s: s["grants"].append(("analysts", ["SELECT"], "CATALOG", "prod")),
 "SELECT on schema but no USE CATALOG": lambda s: s["grants"].remove(("etl-service", ["USE CATALOG"], "CATALOG", "prod")),
 "ALL PRIVILEGES shortcut": lambda s: s["grants"].append(("etl-service", ["ALL PRIVILEGES"], "SCHEMA", "prod.gold")),
 "overlapping external locations": lambda s: s["locations"].update(raw_all="s3://acme-raw"),
 "catalog storage outside any location": lambda s: s["catalogs"]["prod"].update(managed_location="s3://other/uc"),
 "grant to unknown principal": lambda s: s["grants"].append(("contractors", ["USE CATALOG"], "CATALOG", "prod")),
}
failed = 0
for name, fn in cases.items():
    probs = mutate(fn)
    expect_clean = name.startswith("baseline")
    ok = (not probs) if expect_clean else bool(probs)
    failed += not ok
    print(f"[{'PASS' if ok else 'FAIL'}] {name}")
    for p in probs: print("        ->", p)
print("\nall tests passed" if not failed else f"\n{failed} FAILED")
```

## Output

Running `python uc_platform.py`:

```text
-- generated SQL --
CREATE CATALOG IF NOT EXISTS prod MANAGED LOCATION 's3://acme-lake/prod/uc';
ALTER CATALOG prod OWNER TO `platform-admins`;
CREATE SCHEMA IF NOT EXISTS prod.bronze;
ALTER SCHEMA prod.bronze OWNER TO `data-engineers`;
CREATE SCHEMA IF NOT EXISTS prod.silver;
ALTER SCHEMA prod.silver OWNER TO `data-engineers`;
CREATE SCHEMA IF NOT EXISTS prod.gold;
ALTER SCHEMA prod.gold OWNER TO `data-engineers`;
GRANT USE CATALOG, BROWSE ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.gold TO `analysts`;
GRANT USE CATALOG, USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON CATALOG prod TO `data-engineers`;
GRANT USE CATALOG ON CATALOG prod TO `etl-service`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA prod.bronze TO `etl-service`;

-- lint --
OK
```

Running `python test_platform.py`:

```text
[PASS] baseline spec is clean
[PASS] person owns a schema
        -> OWNER: schema prod.gold is owned by 'dana@acme.com', which is not a known group
[PASS] analysts get SELECT on whole catalog
        -> USAGE: analysts has ['SELECT'] on prod.bronze but no USE SCHEMA (granted on schema or inherited from catalog)
        -> USAGE: analysts has ['SELECT'] on prod.silver but no USE SCHEMA (granted on schema or inherited from catalog)
        -> INHERIT: catalog-level SELECT for analysts reaches sensitive schema(s) ['prod.silver']
[PASS] SELECT on schema but no USE CATALOG
        -> USAGE: etl-service has ['CREATE TABLE', 'MODIFY', 'SELECT'] on prod.bronze but no USE CATALOG on prod
[PASS] ALL PRIVILEGES shortcut
        -> ALLPRIV: etl-service granted ALL PRIVILEGES on prod.gold; it excludes MANAGE, list explicit privileges
[PASS] overlapping external locations
        -> OVERLAP: external locations 'raw' and 'raw_all' overlap
[PASS] catalog storage outside any location
        -> LOCATION: catalog prod managed location s3://other/uc is not inside any external location
[PASS] grant to unknown principal
        -> PRINCIPAL: grant to unknown principal 'contractors'

all tests passed
```

## Reading the results

- The **catalog-wide SELECT** mutation triggers two different rules, which shows why linting beats eyeballing. The
  `INHERIT` rule catches the real danger (inheritance reaches `silver`, which holds PII). The two `USAGE` messages are a
  quirk of my linter: analysts hold `SELECT` on the whole catalog, which covers `bronze` and `silver` as well, but they have
  `USE SCHEMA` only on `gold`, so those schemas would *not* actually be readable. The linter is conservative: it flags a privilege
  that can't take effect, because an unusable grant usually means a mistake in intent.
- The `USE CATALOG` mutation reproduces the lesson 02 trap in a form you can review in a pull request.
- The overlap check uses a trailing-separator comparison, so `landing-archive` isn't mistaken for a child of `landing`
  (Lesson 03).

## What this project doesn't do

It doesn't read actual state from a workspace (a real tool would compare the spec to `SHOW GRANTS` output or the
`information_schema` privilege views and report drift), doesn't model row filters, masks or ABAC policies (Lesson 04), doesn't
create storage credentials or external locations (they need cloud-side setup), and doesn't model group membership from your
identity provider. In production this role is played by Terraform's Databricks provider, Declarative Automation Bundles
for workspace objects (Level 4 · 01), or the Databricks SDK; check their current docs, as I did not run them.

## Extensions

1. Add `owners_must_be_groups` for **tables** and a rule that no securable has both `MODIFY` for analysts and `SELECT` only
   for engineers (a likely inversion).
2. Add a `masks` section: columns tagged `pii` must have either a mask policy for `analysts` or no analyst access to the schema.
   Generate the `CREATE POLICY` statements from Lesson 04.
3. Add drift detection: parse a pasted `SHOW GRANTS` output into the same structure and diff it against the spec.
4. Add a rule that environments (`dev`, `prod`) never share a managed location.

## How It Actually Works

The pattern is *desired-state configuration*: the spec states what should exist, the generator produces idempotent statements
(`IF NOT EXISTS`, `GRANT` and `REVOKE` which are both safe to repeat), and the linter is a policy engine
that rejects specs violating organization rules before they reach a workspace. Tools like Terraform add the missing piece (state):
they compare the desired spec to what exists and compute the minimal changes. Everything in this lesson would still be true
if the generator emitted Terraform instead of SQL.

## Common mistakes

- **Making grants by hand** in a UI with no review or history.
- **A linter nobody maintains**: encode each incident as a new rule and test.
- **Generating `REVOKE` blindly** from a spec that is incomplete and removing legitimate access.
- **Treating a clean lint as proof of security**: it checks consistency with rules, not correctness of the rules.
- **No separation between environments** in the spec.

## Exercise

1. Run both scripts. Then add a `finance` schema with PII and a `finance-readers` group; make the linter pass.
2. Write two new tests: one where a schema is owned by a service principal not listed in `groups`, one where a group has
   `MODIFY` on a schema but no `SELECT`. Decide what the linter should say.
3. Add the Delta Sharing/OpenSharing share from Lesson 08 to the spec (a share and a recipient) and a lint rule that shares may
   only include tables from `gold`.
4. Write down three governance rules your organization has that this linter cannot check, and how you would check them.
