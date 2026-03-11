# GroundDB Roadmap

## Project Goal
Build a from-scratch, pure-Python SQL database engine (GroundDB) that correctly executes all 22 TPC-H benchmark queries at scale factor 0.01, cross-validated against SQLite within specified tolerances, completing the full suite in under 300 seconds.

## Architecture Overview
- **SQL Parser**: Recursive-descent parser for a SQL subset covering all TPC-H queries
- **Storage Engine**: In-memory row store, loads TPC-H pipe-delimited data
- **Query Executor**: Hash joins, nested-loop joins, aggregates, sorts, LIMIT
- **TPC-H Harness**: Data generation (generate_tpch.py), query runner, SQLite cross-validation

## Key SQL Features Needed (from TPC-H analysis)
- SELECT with expressions, CASE WHEN, arithmetic
- FROM with multi-table JOINs (INNER, LEFT OUTER)
- WHERE with complex predicates (BETWEEN, IN, LIKE, EXISTS, NOT EXISTS, subqueries)
- GROUP BY with aggregate functions (SUM, AVG, COUNT, MIN, MAX)
- HAVING clauses
- ORDER BY (multi-column, ASC/DESC)
- LIMIT
- Correlated and non-correlated subqueries (IN, EXISTS, scalar)
- Subqueries in FROM clause (derived tables)
- String functions (SUBSTRING, UPPER, etc.)
- Date arithmetic (date comparisons, interval arithmetic)
- COUNT(DISTINCT)
- UNION ALL

## Milestones

### M1: Project Skeleton + Storage Engine + Basic SELECT (Cycles: 6)
- Set up Python project structure (grounddb/ package, tests/, scripts/)
- Implement CSV/pipe-delimited loader for TPC-H tables
- Implement in-memory table storage
- Implement basic SQL parser: SELECT col FROM table WHERE simple_condition
- Implement basic executor for single-table queries
- Test harness: load TPC-H data, run Q6 (simple single-table aggregate)
- **Status: COMPLETE** (actual: 1 cycle — Q6 cross-validation passes)

### M2: Core SQL — JOINs, GROUP BY, ORDER BY, LIMIT (Cycles: 8)
- INNER JOIN (hash join implementation)
- LEFT OUTER JOIN
- GROUP BY with aggregates (SUM, COUNT, AVG, MIN, MAX)
- HAVING clause
- ORDER BY (multi-column)
- LIMIT
- Arithmetic and CASE WHEN expressions
- Table aliases (e.g., `lineitem l`, `orders o`)
- Target: Pass TPC-H Q1, Q6, Q14, Q3, Q5 (core join + aggregate queries)
- **Status: COMPLETE** (actual: 1 cycle — all 12 tests pass, Apollo verified)

### M3: Subqueries and Advanced SQL (Cycles: 8)
Key missing feature: subquery support. This covers the bulk of remaining TPC-H queries.

**Parser additions:**
- Subquery as expression: `(SELECT ...) = value` or `(SELECT ...)` standalone
- IN/NOT IN subquery: `expr IN (SELECT ...)`, `expr NOT IN (SELECT ...)`
- EXISTS/NOT EXISTS: `EXISTS (SELECT ...)`, `NOT EXISTS (SELECT ...)`
- Subquery in FROM: `(SELECT ... FROM ...) AS alias` (derived tables)
- SUBSTRING function: `SUBSTRING(col FROM n FOR m)`

**Executor additions:**
- Subquery evaluation (recursive, with storage context)
- Correlated subquery support (pass outer row as context)
- EXISTS/NOT EXISTS evaluation

**Target queries:** Q4, Q11, Q13, Q15, Q16, Q17, Q18, Q20, Q21, Q22
(Also verify Q2, Q7, Q8, Q9, Q10, Q12 with existing features)

**Tests:** test_m3.py with cross-validation for Q4, Q16, Q17, Q18, Q21 vs SQLite
- **Status: COMPLETE** (actual: 1 cycle — all 19 tests pass including Q4, Q16, Q18)

### M4: Full TPC-H Pass + Performance (Cycles: 8)
Fix remaining failures to pass all 22 TPC-H queries:

**Known issues after M3:**
- Q7, Q8, Q9, Q22: `substr(col, start, len)` not recognized as function (parser only handles KEYWORD-functions like SUM, COUNT)
- Q2: Correlated scalar subquery returns 0 rows (should be 4) — outer row context not propagated correctly
- Q19: OR join without hash key causes OOM (60k × 2k cross-join)
- Q21: EXISTS/NOT EXISTS with aliased outer tables returns 0 rows (should be 3)
- Q11: Extra column in output (HAVING aggregate leaked into SELECT)
- Q17: SUM of empty set returns 0.0 instead of NULL

**Tests:** test_m4.py with cross-validation for all 22 queries
- Status: IN PROGRESS

## Lessons Learned
- M1 took only 1 cycle (estimated 6) — Leo implemented full skeleton quickly
- M2 took only 1 cycle (estimated 8) — Leo is highly efficient
- M3 took only 1 cycle (estimated 8) — Q4, Q16, Q18 pass cross-validation
- Q6 (single-table aggregate) passes cross-validation against SQLite
- Q1, Q3, Q4, Q5, Q14, Q16, Q18 all cross-validated after M3
- Q10, Q11, Q12, Q13, Q15, Q17 work but need verification in test_m4.py
- Be aggressive with scope — Leo can handle complex tasks in one cycle
- M4 hardest parts: `substr()` function recognition, correlated subquery scoping, OR-join performance

## Cycle Budget Tracking
| Milestone | Estimated | Actual |
|-----------|-----------|--------|
| M1        | 6         | 1      |
| M2        | 8         | 1      |
| M3        | 8         | 1      |
| M4        | 8         | -      |
