---
description: "Spark SQL on Databricks: GROUP BY ALL, window frames and tie traps, QUALIFY, PIVOT, arrays, ANSI mode and try_ functions, with local runs."
---

# 04 · Spark SQL, Built-in Functions & Window Functions

SQL is a first-class citizen on Databricks. Notebooks can mix SQL and Python, SQL warehouses speak only SQL, and
DataFrame code is compiled by the same optimizer. This lesson covers the SQL you'll write daily plus the traps that
produce wrong-but-plausible numbers.

!!! note "What was run"
    Queries below ran on **local Spark 4.2.0** against a small temp view, and output blocks are copied from those
    runs. Databricks SQL adds platform-specific syntax and functions on top of Spark's, and some statements shown
    elsewhere in the course (for example Unity Catalog `GRANT`) need a workspace. Where I say "Databricks also
    supports...", that comes from the documentation, not a run.

Setup used throughout (the same data works as a table in a workspace):

```sql
CREATE OR REPLACE TEMP VIEW sales AS SELECT * FROM VALUES
  (1,'2026-01-01','DE','book',12.0), (2,'2026-01-01','US','pen',3.0),  (3,'2026-01-02','DE','pen',3.5),
  (4,'2026-01-02','US','book',15.0), (5,'2026-01-03','DE','book',11.0), (6,'2026-01-03','DE','lamp',40.0),
  (7,'2026-01-04','US','lamp',42.0)
AS t(id, day, country, item, amount);
```

## Aggregation conveniences

```sql
SELECT country, item, SUM(amount) AS total FROM sales GROUP BY ALL ORDER BY 1, 2;
```

`GROUP BY ALL` groups by every non-aggregate expression in `SELECT`, so you don't repeat the list.

```text
|DE     |book|23.0 |
|DE     |lamp|40.0 |
|DE     |pen |3.5  |
|US     |book|15.0 |
|US     |lamp|42.0 |
|US     |pen |3.0  |
```

`FILTER` restricts one aggregate without a `CASE`:

```sql
SELECT country, COUNT(*) FILTER (WHERE item = 'book') AS books, COUNT(*) AS n
FROM sales GROUP BY country ORDER BY 1;
```

```text
|DE     |2    |4  |
|US     |1    |3  |
```

`PIVOT` turns row values into columns (the list of values must be written out):

```sql
SELECT * FROM (SELECT country, item, amount FROM sales)
PIVOT (SUM(amount) FOR item IN ('book', 'pen', 'lamp')) ORDER BY country;
```

```text
|country|book|pen|lamp|
|DE     |23.0|3.5|40.0|
|US     |15.0|3.0|42.0|
```

## Window functions

A window function computes a value per row using related rows, **without collapsing them** the way `GROUP BY` does.

```sql
SELECT day, country, amount,
       SUM(amount) OVER (PARTITION BY country ORDER BY day
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running,
       LAG(amount) OVER (PARTITION BY country ORDER BY day)                AS prev,
       RANK()      OVER (PARTITION BY country ORDER BY amount DESC)        AS rk
FROM sales ORDER BY country, day;
```

Local output:

```text
|day       |country|amount|running|prev|rk |
|2026-01-01|DE     |12.0  |12.0   |NULL|2  |
|2026-01-02|DE     |3.5   |15.5   |12.0|4  |
|2026-01-03|DE     |40.0  |66.5   |11.0|1  |
|2026-01-03|DE     |11.0  |26.5   |3.5 |3  |
|2026-01-01|US     |3.0   |3.0    |NULL|3  |
|2026-01-02|US     |15.0  |18.0   |3.0 |2  |
|2026-01-04|US     |42.0  |60.0   |15.0|1  |
```

Look at the two `DE` rows on `2026-01-03`. Their `running` values (66.5 and 26.5) and `prev` values (11.0 and 3.5)
depend on which of the two tied rows Spark happened to sort first. Here the 11.0 row came first for the window
calculation (it shows `running` 26.5), even though the final `ORDER BY country, day` listed the 40.0 row first. If you
need deterministic results, **add a tie-breaker** to the window's `ORDER BY` (`ORDER BY day, id`). A running total or
`LAG` over a sort key with ties is one of the most common silent bugs in analytics SQL.

Two more details visible in the output: `RANK()` leaves gaps after ties while `DENSE_RANK()` doesn't, and an explicit
`ROWS` frame states exactly which rows are summed. Without a frame, a window with `ORDER BY` defaults to a *range*
frame, which treats tied rows as peers and gives all of them the same running value. Writing the frame removes
the ambiguity.

### Top row per group: QUALIFY

Databricks SQL and Spark 4 support `QUALIFY`, which filters on window results without a subquery (this ran on local
Spark 4.2.0):

```sql
SELECT country, item, amount FROM sales
QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY amount DESC) = 1
ORDER BY country;
```

```text
|DE     |lamp|40.0  |
|US     |lamp|42.0  |
```

If you must support an engine without `QUALIFY`, wrap the window in a subquery and filter in the outer `WHERE`.

## Arrays, structs and higher-order functions

Lakehouse data is often nested. Spark SQL has first-class array/map/struct types:

```sql
SELECT country, collect_list(item) AS items, array_distinct(collect_list(item)) AS uniq
FROM sales GROUP BY country ORDER BY 1;
-- DE  [book, pen, book, lamp]   [book, pen, lamp]

SELECT transform(array(1,2,3), a -> a * 10) AS scaled;       -- [10, 20, 30]
SELECT explode(sequence(1, 3)) AS n;                          -- one row each for 1, 2, 3
SELECT from_json('{"a":1,"b":[1,2]}', 'a INT, b ARRAY<INT>') AS j;   -- {1, [1, 2]}
```

(Those values were produced by the local run; for the first query the order inside `collect_list` is not
guaranteed and can change between runs because it depends on partition order.) **Higher-order functions** such as
`transform`, `filter`, `exists` and `aggregate` apply a lambda to each array element inside the engine, avoiding an
`explode` and re-aggregate.

Dates and strings:

```sql
SELECT date_trunc('MONTH', DATE'2026-01-17')                   AS m,    -- 2026-01-01 00:00:00
       date_add(DATE'2026-01-31', 1)                           AS d,    -- 2026-02-01
       datediff(DATE'2026-03-01', DATE'2026-02-01')            AS dd,   -- 28
       to_date('03/04/2026', 'dd/MM/yyyy')                     AS p;    -- 2026-04-03
SELECT regexp_extract('order-4821-DE', '(\\d+)', 1)            AS n,    -- 4821
       concat_ws('-', 'a', NULL, 'c')                          AS c,    -- a-c
       lpad('7', 3, '0')                                       AS z;    -- 007
```

Always pass an explicit format to `to_date`: `03/04/2026` is 3 April or 4 March depending on the pattern you give.

## ANSI mode: errors instead of silent NULLs

Spark 4 enables ANSI SQL semantics by default (`spark.sql.ansi.enabled` printed `true` in the local run). Invalid
operations then **fail loudly**:

```sql
SELECT 1/0;                    -- [DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate ...
SELECT CAST('abc' AS INT);     -- [CAST_INVALID_INPUT] The value 'abc' ... cannot be cast to "INT" ...
SELECT try_cast('abc' AS INT); -- NULL
SELECT try_divide(1, 0);       -- NULL
```

Older Spark versions (3.x) returned `NULL` for both, so migrating code can start raising errors it never raised
before. Databricks Runtime behaviour depends on the runtime version and workspace/compute settings, so check the
setting on your compute with `SET spark.sql.ansi.enabled` rather than assuming. Use `try_cast` and `try_divide` where
bad data is expected, and let errors surface where it isn't.

## Parameters and pipe syntax

Never build SQL by string concatenation. Spark supports named parameter markers when you pass `args`:

```python
spark.sql("SELECT * FROM sales WHERE country = :c AND amount > :min", args={"c": "DE", "min": 10})
```

(Running `SELECT :x + 1` without supplying `args` fails with `[UNBOUND_SQL_PARAMETER]`, as the local run showed.)
Spark 4 also accepts **SQL pipe syntax**, which reads top to bottom:

```sql
FROM sales |> WHERE country = 'DE' |> AGGREGATE SUM(amount) AS t GROUP BY item |> ORDER BY item
```

```text
|book|23.0|
|lamp|40.0|
|pen |3.5 |
```

## How It Actually Works

`spark.sql(...)` and the DataFrame API produce the *same* kind of logical plan, so there is no inherent speed
difference between them. A window function becomes a plan node that first **shuffles rows so each `PARTITION BY` key
lands on one task**, then **sorts within each partition by the `ORDER BY`**, then streams through the sorted rows
applying the frame. That is why a window with no `PARTITION BY` is dangerous: all rows go to one task. And it
explains the tie problem above: sorting is stable only with respect to the keys you gave it, so tied rows come out
in whatever order the shuffle delivered them.

`GROUP BY` works differently: a hash aggregate computes partial results per task, shuffles by group key, and merges
the partials. `FILTER` clauses and `PIVOT` are rewritten into conditional aggregates before execution.

## Common mistakes

- **Window `ORDER BY` with ties** and no tie-breaker (see above).
- **`ROW_NUMBER` vs `RANK`** for "top N": `RANK` returns more than N rows when there are ties.
- **A window with no `PARTITION BY`** on a big table: single-task bottleneck.
- **Assuming `collect_list` preserves order.** It doesn't, unless you sort afterward (`sort_array`).
- **Relying on implicit casts.** ANSI mode turns bad casts into errors; decide between `CAST` and `try_cast` on purpose.
- **Ambiguous date parsing** without an explicit pattern.

## Exercise

1. Write a query returning each country's best-selling *item by revenue* using `QUALIFY`, then rewrite it with a
   subquery. Check they match.
2. Change the running-total query's window `ORDER BY` to `day, id` and confirm the two tied `DE` rows now give a
   stable answer on repeated runs.
3. Compute each country's revenue share of the total with a window over the whole result set
   (`SUM(...) OVER ()`), rounded to 2 decimals.
4. Predict what `SELECT 5 / 0` and `SELECT try_divide(5, 0)` do on your compute, then check them. If the first returns
   `NULL` instead of an error, find which setting explains it.
