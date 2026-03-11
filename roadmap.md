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
- Correlated and non-correlated subqueries
- String functions (SUBSTRING, UPPER, etc.)
- Date arithmetic (date comparisons, interval arithmetic)

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
- **Status: IN PROGRESS**

### M3: Subqueries and Advanced SQL (Cycles: 10)
- Non-correlated subqueries (IN, EXISTS, scalar subqueries)
- Correlated subqueries (EXISTS with correlation, NOT EXISTS)
- String functions (SUBSTRING, UPPER, LOWER, TRIM)
- Date comparisons and arithmetic (interval arithmetic)
- Multi-level subqueries
- UNION / INTERSECT
- Target: Pass TPC-H Q2, Q4, Q11, Q15, Q17, Q18, Q20, Q21, Q22
- Status: PENDING

### M4: Full TPC-H Pass + Performance (Cycles: 10)
- Pass all 22 TPC-H queries correctly
- Optimize for 300-second total runtime
- Performance improvements: better join ordering, column pruning
- Final validation: cross-validate all 22 queries against SQLite
- Status: PENDING

## Lessons Learned
- M1 took only 1 cycle (estimated 6) — Leo implemented full skeleton quickly
- Q6 (single-table aggregate) passes cross-validation against SQLite
- Parser already has KEYWORD stubs for JOIN/subqueries but executor doesn't implement them yet
- M2 estimates may also be aggressive — be ready to break down if needed

## Cycle Budget Tracking
| Milestone | Estimated | Actual |
|-----------|-----------|--------|
| M1        | 6         | 1      |
| M2        | 8         | -      |
| M3        | 10        | -      |
| M4        | 10        | -      |
