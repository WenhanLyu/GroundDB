"""
Test TPC-H Q1, Q3, Q5, Q14 — Multi-table JOIN support (M2).

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

Q1_SQL = """
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus
"""

Q1_SQLITE = """
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
FROM
    lineitem
WHERE
    l_shipdate <= '1998-09-02'
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus
"""

Q3_SQL = """
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < date '1995-03-15'
    AND l_shipdate > date '1995-03-15'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10
"""

Q3_SQLITE = """
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < '1995-03-15'
    AND l_shipdate > '1995-03-15'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10
"""

Q5_SQL = """
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= date '1994-01-01'
    AND o_orderdate < date '1995-01-01'
GROUP BY
    n_name
ORDER BY
    revenue DESC
"""

Q5_SQLITE = """
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= '1994-01-01'
    AND o_orderdate < '1995-01-01'
GROUP BY
    n_name
ORDER BY
    revenue DESC
"""

Q14_SQL = """
SELECT
    100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= date '1995-09-01'
    AND l_shipdate < date '1995-10-01'
"""

Q14_SQLITE = """
SELECT
    100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= '1995-09-01'
    AND l_shipdate < '1995-10-01'
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
        "supplier", "nation", "region"
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
    """Compare GroundDB results against SQLite results.

    Args:
        grounddb_rows: list of dicts from GroundDB
        sqlite_rows: list of tuples from SQLite
        col_names: ordered list of column names (matching SQLite column order)
        numeric_cols: set of column names that should be compared numerically
        tolerance: max allowed difference for numeric comparisons
    """
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

class TestM2:
    """Test TPC-H Queries Q1, Q3, Q5, Q14 with multi-table JOIN support."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Ensure TPC-H data exists."""
        _ensure_data()

    def test_q1_grounddb(self):
        """Q1 should return 4 result rows from GroundDB."""
        engine = _get_engine()
        results = engine.execute(Q1_SQL)
        assert len(results) == 4, f"Expected 4 rows, got {len(results)}"
        # Check first row has expected keys
        assert 'l_returnflag' in results[0]
        assert 'sum_qty' in results[0]
        assert 'count_order' in results[0]

    def test_q1_cross_validation(self):
        """Q1 GroundDB results must match SQLite within ±0.01."""
        engine = _get_engine()
        gdb_results = engine.execute(Q1_SQL)

        conn = _get_sqlite_conn()
        cursor = conn.cursor()
        cursor.execute(Q1_SQLITE)
        sqlite_results = cursor.fetchall()
        conn.close()

        col_names = [
            'l_returnflag', 'l_linestatus', 'sum_qty', 'sum_base_price',
            'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price',
            'avg_disc', 'count_order'
        ]
        numeric_cols = {'sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge',
                        'avg_qty', 'avg_price', 'avg_disc', 'count_order'}

        _compare_results(gdb_results, sqlite_results, col_names, numeric_cols)
        print(f"\n  Q1: {len(gdb_results)} rows matched")

    def test_q3_grounddb(self):
        """Q3 should return up to 10 result rows from GroundDB."""
        engine = _get_engine()
        results = engine.execute(Q3_SQL)
        assert len(results) > 0, "Q3 returned no results"
        assert len(results) <= 10, f"Q3 returned more than 10 rows: {len(results)}"
        assert 'l_orderkey' in results[0]
        assert 'revenue' in results[0]

    def test_q3_cross_validation(self):
        """Q3 GroundDB results must match SQLite within ±0.01."""
        engine = _get_engine()
        gdb_results = engine.execute(Q3_SQL)

        conn = _get_sqlite_conn()
        cursor = conn.cursor()
        cursor.execute(Q3_SQLITE)
        sqlite_results = cursor.fetchall()
        conn.close()

        col_names = ['l_orderkey', 'revenue', 'o_orderdate', 'o_shippriority']
        numeric_cols = {'l_orderkey', 'revenue', 'o_shippriority'}

        _compare_results(gdb_results, sqlite_results, col_names, numeric_cols)
        print(f"\n  Q3: {len(gdb_results)} rows matched")

    def test_q5_grounddb(self):
        """Q5 should return result rows from GroundDB."""
        engine = _get_engine()
        results = engine.execute(Q5_SQL)
        assert len(results) > 0, "Q5 returned no results"
        assert 'n_name' in results[0]
        assert 'revenue' in results[0]

    def test_q5_cross_validation(self):
        """Q5 GroundDB results must match SQLite within ±0.01."""
        engine = _get_engine()
        gdb_results = engine.execute(Q5_SQL)

        conn = _get_sqlite_conn()
        cursor = conn.cursor()
        cursor.execute(Q5_SQLITE)
        sqlite_results = cursor.fetchall()
        conn.close()

        col_names = ['n_name', 'revenue']
        numeric_cols = {'revenue'}

        _compare_results(gdb_results, sqlite_results, col_names, numeric_cols)
        print(f"\n  Q5: {len(gdb_results)} rows matched")

    def test_q14_grounddb(self):
        """Q14 should return 1 result row from GroundDB."""
        engine = _get_engine()
        results = engine.execute(Q14_SQL)
        assert len(results) == 1, f"Expected 1 row, got {len(results)}"
        assert 'promo_revenue' in results[0]
        assert results[0]['promo_revenue'] > 0

    def test_q14_cross_validation(self):
        """Q14 GroundDB results must match SQLite within ±0.01."""
        engine = _get_engine()
        gdb_results = engine.execute(Q14_SQL)

        conn = _get_sqlite_conn()
        cursor = conn.cursor()
        cursor.execute(Q14_SQLITE)
        sqlite_results = cursor.fetchall()
        conn.close()

        col_names = ['promo_revenue']
        numeric_cols = {'promo_revenue'}

        _compare_results(gdb_results, sqlite_results, col_names, numeric_cols)
        print(f"\n  Q14: promo_revenue matched: {gdb_results[0]['promo_revenue']:.4f}")

    def test_q6_still_passes(self):
        """Verify Q6 still works after M2 changes."""
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
