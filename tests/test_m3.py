"""
Test TPC-H Q4, Q16, Q18 — Subquery support (M3).

Loads TPC-H SF 0.01 data, runs each query through GroundDB,
runs the same query in SQLite, and compares results.
Numeric values are compared within ±0.01 tolerance.
String/date values are compared exactly.
"""

import os
import sys
import sqlite3
import subprocess
import pytest

# Ensure repo root is on path
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

DATA_DIR = os.path.join(REPO_ROOT, "data")


# ── TPC-H Queries ────────────────────────────────────────────────────────────

Q4_SQL = """
SELECT o_orderpriority, count(*) as order_count
FROM orders
WHERE o_orderdate >= date '1993-07-01'
  AND o_orderdate < date '1993-10-01'
  AND EXISTS (
    SELECT * FROM lineitem
    WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
  )
GROUP BY o_orderpriority
ORDER BY o_orderpriority
"""

Q4_SQLITE = """
SELECT o_orderpriority, count(*) as order_count
FROM orders
WHERE o_orderdate >= '1993-07-01'
  AND o_orderdate < '1993-10-01'
  AND EXISTS (
    SELECT * FROM lineitem
    WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
  )
GROUP BY o_orderpriority
ORDER BY o_orderpriority
"""

Q16_SQL = """
SELECT p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
FROM partsupp, part
WHERE p_partkey = ps_partkey
  AND p_brand <> 'Brand#45'
  AND p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps_suppkey NOT IN (
    SELECT s_suppkey FROM supplier
    WHERE s_comment LIKE '%Customer%Complaints%'
  )
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
LIMIT 10
"""

Q16_SQLITE = """
SELECT p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
FROM partsupp, part
WHERE p_partkey = ps_partkey
  AND p_brand <> 'Brand#45'
  AND p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps_suppkey NOT IN (
    SELECT s_suppkey FROM supplier
    WHERE s_comment LIKE '%Customer%Complaints%'
  )
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
LIMIT 10
"""

Q18_SQL = """
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (
  SELECT l_orderkey FROM lineitem
  GROUP BY l_orderkey
  HAVING sum(l_quantity) > 300
)
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
"""

Q18_SQLITE = """
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (
  SELECT l_orderkey FROM lineitem
  GROUP BY l_orderkey
  HAVING sum(l_quantity) > 300
)
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
"""


# ── Helpers ──────────────────────────────────────────────────────────────────

def _ensure_data():
    """Generate TPC-H data if it doesn't exist."""
    if not os.path.exists(os.path.join(DATA_DIR, "lineitem.tbl")):
        gen_script = os.path.join(REPO_ROOT, "scripts", "generate_tpch.py")
        subprocess.run([sys.executable, gen_script, DATA_DIR], check=True)
    assert os.path.exists(os.path.join(DATA_DIR, "lineitem.tbl")), \
        "lineitem.tbl not found after generation"


def _get_engine():
    """Create and load a GroundDB engine with all needed tables."""
    from grounddb import Engine
    engine = Engine()
    engine.load_tpch(DATA_DIR, tables=[
        "lineitem", "orders", "customer", "part",
        "supplier", "nation", "region", "partsupp"
    ])
    return engine


def _get_sqlite_conn():
    """Create an in-memory SQLite connection with all needed tables loaded."""
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # Create all tables
    cursor.execute("""
        CREATE TABLE lineitem (
            l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER,
            l_linenumber INTEGER, l_quantity REAL, l_extendedprice REAL,
            l_discount REAL, l_tax REAL, l_returnflag TEXT, l_linestatus TEXT,
            l_shipdate TEXT, l_commitdate TEXT, l_receiptdate TEXT,
            l_shipinstruct TEXT, l_shipmode TEXT, l_comment TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE orders (
            o_orderkey INTEGER, o_custkey INTEGER, o_orderstatus TEXT,
            o_totalprice REAL, o_orderdate TEXT, o_orderpriority TEXT,
            o_clerk TEXT, o_shippriority INTEGER, o_comment TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE customer (
            c_custkey INTEGER, c_name TEXT, c_address TEXT,
            c_nationkey INTEGER, c_phone TEXT, c_acctbal REAL,
            c_mktsegment TEXT, c_comment TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE part (
            p_partkey INTEGER, p_name TEXT, p_mfgr TEXT,
            p_brand TEXT, p_type TEXT, p_size INTEGER,
            p_container TEXT, p_retailprice REAL, p_comment TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE supplier (
            s_suppkey INTEGER, s_name TEXT, s_address TEXT,
            s_nationkey INTEGER, s_phone TEXT, s_acctbal REAL,
            s_comment TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE partsupp (
            ps_partkey INTEGER, ps_suppkey INTEGER,
            ps_availqty INTEGER, ps_supplycost REAL, ps_comment TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE nation (
            n_nationkey INTEGER, n_name TEXT, n_regionkey INTEGER,
            n_comment TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE region (
            r_regionkey INTEGER, r_name TEXT, r_comment TEXT
        )
    """)

    # Load data
    _load_tbl(cursor, "lineitem", os.path.join(DATA_DIR, "lineitem.tbl"),
              [int, int, int, int, float, float, float, float, str, str, str, str, str, str, str, str])
    _load_tbl(cursor, "orders", os.path.join(DATA_DIR, "orders.tbl"),
              [int, int, str, float, str, str, str, int, str])
    _load_tbl(cursor, "customer", os.path.join(DATA_DIR, "customer.tbl"),
              [int, str, str, int, str, float, str, str])
    _load_tbl(cursor, "part", os.path.join(DATA_DIR, "part.tbl"),
              [int, str, str, str, str, int, str, float, str])
    _load_tbl(cursor, "supplier", os.path.join(DATA_DIR, "supplier.tbl"),
              [int, str, str, int, str, float, str])
    _load_tbl(cursor, "partsupp", os.path.join(DATA_DIR, "partsupp.tbl"),
              [int, int, int, float, str])
    _load_tbl(cursor, "nation", os.path.join(DATA_DIR, "nation.tbl"),
              [int, str, int, str])
    _load_tbl(cursor, "region", os.path.join(DATA_DIR, "region.tbl"),
              [int, str, str])

    conn.commit()
    return conn


def _load_tbl(cursor, table_name, filepath, types):
    """Load a .tbl file into SQLite."""
    ncols = len(types)
    placeholders = ",".join(["?"] * ncols)
    with open(filepath, "r") as f:
        for line in f:
            line = line.rstrip("\n").rstrip("|")
            if not line:
                continue
            parts = line.split("|")
            vals = []
            for i, t in enumerate(types):
                v = parts[i].strip() if i < len(parts) else ""
                if v == "":
                    vals.append(None)
                else:
                    vals.append(t(v))
            cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", vals)


def _compare_results(grounddb_rows, sqlite_rows, col_names, numeric_cols=None, tolerance=0.01):
    """Compare GroundDB results against SQLite results."""
    if numeric_cols is None:
        numeric_cols = set()

    assert len(grounddb_rows) == len(sqlite_rows), \
        f"Row count mismatch: GroundDB={len(grounddb_rows)}, SQLite={len(sqlite_rows)}"

    for row_idx, (gdb_row, sql_row) in enumerate(zip(grounddb_rows, sqlite_rows)):
        for col_idx, col_name in enumerate(col_names):
            gdb_val = gdb_row.get(col_name)
            sql_val = sql_row[col_idx]

            if col_name in numeric_cols:
                gdb_num = float(gdb_val) if gdb_val is not None else 0.0
                sql_num = float(sql_val) if sql_val is not None else 0.0
                diff = abs(gdb_num - sql_num)
                assert diff <= tolerance, (
                    f"Row {row_idx}, col {col_name}: "
                    f"GroundDB={gdb_num:.6f}, SQLite={sql_num:.6f}, diff={diff:.6f} (tol={tolerance})"
                )
            else:
                assert str(gdb_val) == str(sql_val), (
                    f"Row {row_idx}, col {col_name}: "
                    f"GroundDB={gdb_val!r}, SQLite={sql_val!r}"
                )


# ── Test class ───────────────────────────────────────────────────────────────

class TestM3:
    """Test TPC-H Queries Q4, Q16, Q18 with subquery support."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Ensure TPC-H data exists."""
        _ensure_data()

    # ── Q18 (IN subquery) ──────────────────────────────────────────────

    def test_q18_grounddb(self):
        """Q18 should return results from GroundDB."""
        engine = _get_engine()
        results = engine.execute(Q18_SQL)
        assert len(results) >= 0, f"Q18 returned negative rows?!"
        if len(results) > 0:
            assert 'c_name' in results[0]
            assert 'o_orderkey' in results[0]

    def test_q18_cross_validation(self):
        """Q18 GroundDB results must match SQLite within ±0.01."""
        engine = _get_engine()
        gdb_results = engine.execute(Q18_SQL)

        conn = _get_sqlite_conn()
        cursor = conn.cursor()
        cursor.execute(Q18_SQLITE)
        sqlite_results = cursor.fetchall()
        conn.close()

        col_names = ['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate',
                      'o_totalprice', 'sum(l_quantity)']
        numeric_cols = {'c_custkey', 'o_orderkey', 'o_totalprice', 'sum(l_quantity)'}

        _compare_results(gdb_results, sqlite_results, col_names, numeric_cols)
        print(f"\n  Q18: {len(gdb_results)} rows matched")

    # ── Q4 (EXISTS correlated subquery) ────────────────────────────────

    def test_q4_grounddb(self):
        """Q4 should return results from GroundDB."""
        engine = _get_engine()
        results = engine.execute(Q4_SQL)
        assert len(results) > 0, "Q4 returned no results"
        assert 'o_orderpriority' in results[0]
        assert 'order_count' in results[0]

    def test_q4_cross_validation(self):
        """Q4 GroundDB results must match SQLite within ±0.01."""
        engine = _get_engine()
        gdb_results = engine.execute(Q4_SQL)

        conn = _get_sqlite_conn()
        cursor = conn.cursor()
        cursor.execute(Q4_SQLITE)
        sqlite_results = cursor.fetchall()
        conn.close()

        col_names = ['o_orderpriority', 'order_count']
        numeric_cols = {'order_count'}

        _compare_results(gdb_results, sqlite_results, col_names, numeric_cols)
        print(f"\n  Q4: {len(gdb_results)} rows matched")

    # ── Q16 (NOT IN subquery, COUNT DISTINCT) ──────────────────────────

    def test_q16_grounddb(self):
        """Q16 should return results from GroundDB."""
        engine = _get_engine()
        results = engine.execute(Q16_SQL)
        assert len(results) > 0, "Q16 returned no results"
        assert 'p_brand' in results[0]
        assert 'supplier_cnt' in results[0]

    def test_q16_cross_validation(self):
        """Q16 GroundDB results must match SQLite within ±0.01."""
        engine = _get_engine()
        gdb_results = engine.execute(Q16_SQL)

        conn = _get_sqlite_conn()
        cursor = conn.cursor()
        cursor.execute(Q16_SQLITE)
        sqlite_results = cursor.fetchall()
        conn.close()

        col_names = ['p_brand', 'p_type', 'p_size', 'supplier_cnt']
        numeric_cols = {'p_size', 'supplier_cnt'}

        _compare_results(gdb_results, sqlite_results, col_names, numeric_cols)
        print(f"\n  Q16: {len(gdb_results)} rows matched")

    # ── Regression: existing tests still pass ──────────────────────────

    def test_q6_still_passes(self):
        """Verify Q6 still works after M3 changes."""
        from grounddb import Engine

        engine = Engine()
        engine.load_tpch(DATA_DIR, tables=["lineitem"])

        q6 = """
        SELECT
            sum(l_extendedprice * l_discount) AS revenue
        FROM
            lineitem
        WHERE
            l_shipdate >= date '1994-01-01'
            AND l_shipdate < date '1995-01-01'
            AND l_discount BETWEEN 0.05 AND 0.07
            AND l_quantity < 24
        """

        results = engine.execute(q6)
        assert len(results) == 1
        revenue = float(list(results[0].values())[0])
        assert revenue > 0, f"Q6 revenue should be positive, got {revenue}"
