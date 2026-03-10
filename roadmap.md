# GroundDB Roadmap

## Project Goal
Build a from-scratch, pure-Python SQL database engine (GroundDB) that correctly executes all 22 TPC-H benchmark queries at scale factor 0.01, cross-validated against SQLite within specified tolerances, completing the full suite in under 300 seconds.

## Architecture Overview
- **SQL Parser**: Recursive-descent parser for a SQL subset covering all TPC-H queries
- **Storage Engine**: In-memory columnar/row store, loads TPC-H pipe-delimited data
- **Query Planner**: Logical plan builder with predicate pushdown, join ordering
- **Query Executor**: Hash joins, nested-loop joins, aggregates, sorts, LIMIT
- **TPC-H Harness**: Data generation (dbgen), query runner, SQLite cross-validation

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
- Status: PENDING

### M2: Core SQL — JOINs, GROUP BY, ORDER BY, LIMIT (Cycles: 8)
- INNER JOIN (hash join implementation)
- LEFT OUTER JOIN
- GROUP BY with aggregates (SUM, COUNT, AVG, MIN, MAX)
- HAVING clause
- ORDER BY (multi-column)
- LIMIT
- Arithmetic and CASE WHEN expressions
- Target: Pass TPC-H Q1, Q6, Q14 (simpler queries)
- Status: PENDING

### M3: Subqueries and Advanced SQL (Cycles: 10)
- Non-correlated subqueries (IN, EXISTS, scalar subqueries)
- Correlated subqueries (EXISTS with correlation)
- String functions (SUBSTRING, UPPER, LOWER, TRIM)
- Date comparisons and arithmetic
- Multi-level subqueries
- Target: Pass TPC-H Q2, Q4, Q11, Q15, Q17, Q20, Q22
- Status: PENDING

### M4: Full TPC-H Pass + Performance (Cycles: 10)
- Pass all 22 TPC-H queries correctly
- Optimize for 300-second total runtime
- Performance improvements: better join ordering, column pruning
- Final validation: cross-validate all 22 queries against SQLite
- Status: PENDING

## Lessons Learned
- (none yet — project just started)

## Cycle Budget Tracking
| Milestone | Estimated | Actual |
|-----------|-----------|--------|
| M1        | 6         | -      |
| M2        | 8         | -      |
| M3        | 10        | -      |
| M4        | 10        | -      |
