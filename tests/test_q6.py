"""
Test TPC-H Q6 (Forecasting Revenue Change Query).

Loads TPC-H SF 0.01 lineitem data, runs Q6 through GroundDB,
runs the same query in SQLite, and compares results within ±0.01 tolerance.
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

# TPC-H Q6
Q6_SQL = """
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


def _ensure_data():
    """Generate TPC-H data if it doesn't exist."""
    if not os.path.exists(os.path.join(DATA_DIR, "lineitem.tbl")):
        gen_script = os.path.join(REPO_ROOT, "scripts", "generate_tpch.py")
        subprocess.run([sys.executable, gen_script, DATA_DIR], check=True)
    assert os.path.exists(os.path.join(DATA_DIR, "lineitem.tbl")), \
        "lineitem.tbl not found after generation"


def _run_q6_grounddb() -> float:
    """Run Q6 through GroundDB and return the revenue value."""
    from grounddb import Engine

    engine = Engine()
    engine.load_tpch(DATA_DIR, tables=["lineitem"])

    results = engine.execute(Q6_SQL)
    assert len(results) == 1, f"Expected 1 result row, got {len(results)}"
    row = results[0]

    # Get the revenue value (might be keyed by alias)
    revenue = None
    for key, val in row.items():
        revenue = val
        break

    assert revenue is not None, "Q6 returned None"
    return float(revenue)


def _run_q6_sqlite() -> float:
    """Run Q6 through SQLite and return the revenue value."""
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # Create lineitem table
    cursor.execute("""
        CREATE TABLE lineitem (
            l_orderkey INTEGER,
            l_partkey INTEGER,
            l_suppkey INTEGER,
            l_linenumber INTEGER,
            l_quantity REAL,
            l_extendedprice REAL,
            l_discount REAL,
            l_tax REAL,
            l_returnflag TEXT,
            l_linestatus TEXT,
            l_shipdate TEXT,
            l_commitdate TEXT,
            l_receiptdate TEXT,
            l_shipinstruct TEXT,
            l_shipmode TEXT,
            l_comment TEXT
        )
    """)

    # Load data from same .tbl file
    lineitem_path = os.path.join(DATA_DIR, "lineitem.tbl")
    with open(lineitem_path, "r") as f:
        for line in f:
            line = line.rstrip("\n").rstrip("|")
            if not line:
                continue
            parts = line.split("|")
            cursor.execute(
                "INSERT INTO lineitem VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    int(parts[0]),
                    int(parts[1]),
                    int(parts[2]),
                    int(parts[3]),
                    float(parts[4]),
                    float(parts[5]),
                    float(parts[6]),
                    float(parts[7]),
                    parts[8],
                    parts[9],
                    parts[10],
                    parts[11],
                    parts[12],
                    parts[13],
                    parts[14],
                    parts[15] if len(parts) > 15 else "",
                )
            )

    conn.commit()

    # Run Q6 — SQLite uses string comparison for dates, which matches our approach
    sqlite_q6 = """
        SELECT sum(l_extendedprice * l_discount) AS revenue
        FROM lineitem
        WHERE l_shipdate >= '1994-01-01'
          AND l_shipdate < '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24
    """

    cursor.execute(sqlite_q6)
    row = cursor.fetchone()
    conn.close()

    assert row is not None and row[0] is not None, "SQLite Q6 returned None"
    return float(row[0])


class TestTPCHQ6:
    """Test TPC-H Query 6: Forecasting Revenue Change."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Ensure TPC-H data exists."""
        _ensure_data()

    def test_q6_grounddb_returns_result(self):
        """GroundDB should return a single numeric result for Q6."""
        revenue = _run_q6_grounddb()
        assert isinstance(revenue, (int, float))
        assert revenue > 0, f"Q6 revenue should be positive, got {revenue}"

    def test_q6_sqlite_returns_result(self):
        """SQLite baseline should return a result for Q6."""
        revenue = _run_q6_sqlite()
        assert isinstance(revenue, (int, float))
        assert revenue > 0, f"SQLite Q6 revenue should be positive, got {revenue}"

    def test_q6_cross_validation(self):
        """GroundDB Q6 result must match SQLite within ±0.01."""
        grounddb_revenue = _run_q6_grounddb()
        sqlite_revenue = _run_q6_sqlite()

        diff = abs(grounddb_revenue - sqlite_revenue)
        print(f"\n  GroundDB revenue: {grounddb_revenue:.6f}")
        print(f"  SQLite revenue:   {sqlite_revenue:.6f}")
        print(f"  Difference:       {diff:.6f}")

        assert diff <= 0.01, (
            f"Q6 revenue mismatch: GroundDB={grounddb_revenue:.6f}, "
            f"SQLite={sqlite_revenue:.6f}, diff={diff:.6f} (tolerance=0.01)"
        )
