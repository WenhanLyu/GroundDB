# Project Specification

## What do you want to build?

GroundDB is a from-scratch, pure-Python SQL database engine with no external database or SQL parsing libraries. It targets analytical query workloads, implementing its own SQL parser, query planner, storage engine, and query executor.

## How do you consider the project is success?

Correctly execute all 22 TPC-H benchmark queries at scale factor 0.01. Generate test data with the official TPC-H dbgen tool. Cross-validate results against SQLite on the same data — numeric columns must match within ±0.01 absolute tolerance, string and date columns must match exactly, and row ordering must match for queries with ORDER BY. The full 22-query suite must complete within 300 seconds.
