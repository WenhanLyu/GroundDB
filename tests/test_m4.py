"""
Test all 22 TPC-H queries — M4 cross-validation.

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
import time

# Ensure repo root is on path
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

DATA_DIR = os.path.join(REPO_ROOT, "data")


# ── TPC-H Queries (GroundDB format) ────────────────────────────────────────

QUERIES_GDB = {}
QUERIES_SQLITE = {}

# ─── Q1 ───
QUERIES_GDB['Q1'] = """
SELECT l_returnflag, l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
FROM lineitem
WHERE l_shipdate <= date '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
"""
QUERIES_SQLITE['Q1'] = QUERIES_GDB['Q1'].replace("date '1998-09-02'", "'1998-09-02'")

# ─── Q2 ───
QUERIES_GDB['Q2'] = """
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = 15
  AND p_type LIKE '%BRASS'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'EUROPE'
  AND ps_supplycost = (
    SELECT min(ps_supplycost)
    FROM partsupp ps2, supplier s2, nation n2, region r2
    WHERE p_partkey = ps2.ps_partkey
      AND s2.s_suppkey = ps2.ps_suppkey
      AND s2.s_nationkey = n2.n_nationkey
      AND n2.n_regionkey = r2.r_regionkey
      AND r2.r_name = 'EUROPE'
  )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100
"""
QUERIES_SQLITE['Q2'] = QUERIES_GDB['Q2']

# ─── Q3 ───
QUERIES_GDB['Q3'] = """
SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue,
  o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < date '1995-03-15'
  AND l_shipdate > date '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
"""
QUERIES_SQLITE['Q3'] = QUERIES_GDB['Q3'].replace("date '1995-03-15'", "'1995-03-15'")

# ─── Q4 ───
QUERIES_GDB['Q4'] = """
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
QUERIES_SQLITE['Q4'] = QUERIES_GDB['Q4'].replace("date '1993-07-01'", "'1993-07-01'").replace("date '1993-10-01'", "'1993-10-01'")

# ─── Q5 ───
QUERIES_GDB['Q5'] = """
SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= date '1994-01-01'
  AND o_orderdate < date '1995-01-01'
GROUP BY n_name
ORDER BY revenue DESC
"""
QUERIES_SQLITE['Q5'] = QUERIES_GDB['Q5'].replace("date '1994-01-01'", "'1994-01-01'").replace("date '1995-01-01'", "'1995-01-01'")

# ─── Q6 ───
QUERIES_GDB['Q6'] = """
SELECT sum(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= date '1994-01-01'
  AND l_shipdate < date '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24
"""
QUERIES_SQLITE['Q6'] = QUERIES_GDB['Q6'].replace("date '1994-01-01'", "'1994-01-01'").replace("date '1995-01-01'", "'1995-01-01'")

# ─── Q7 ───
QUERIES_GDB['Q7'] = """
SELECT
  n1.n_name as supp_nation,
  n2.n_name as cust_nation,
  extract(year from l_shipdate) as l_year,
  sum(l_extendedprice * (1 - l_discount)) as revenue
FROM supplier, lineitem, orders, customer, nation n1, nation n2
WHERE s_suppkey = l_suppkey
  AND o_orderkey = l_orderkey
  AND c_custkey = o_custkey
  AND s_nationkey = n1.n_nationkey
  AND c_nationkey = n2.n_nationkey
  AND (
    (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
    OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
  )
  AND l_shipdate BETWEEN date '1995-01-01' AND date '1996-12-31'
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year
"""
QUERIES_SQLITE['Q7'] = """
SELECT
  n1.n_name as supp_nation,
  n2.n_name as cust_nation,
  CAST(strftime('%Y', l_shipdate) AS INTEGER) as l_year,
  sum(l_extendedprice * (1 - l_discount)) as revenue
FROM supplier, lineitem, orders, customer, nation n1, nation n2
WHERE s_suppkey = l_suppkey
  AND o_orderkey = l_orderkey
  AND c_custkey = o_custkey
  AND s_nationkey = n1.n_nationkey
  AND c_nationkey = n2.n_nationkey
  AND (
    (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
    OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
  )
  AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year
"""

# ─── Q8 ───
QUERIES_GDB['Q8'] = """
SELECT
  extract(year from o_orderdate) as o_year,
  sum(case when n2.n_name = 'BRAZIL' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as mkt_share
FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
WHERE p_partkey = l_partkey
  AND s_suppkey = l_suppkey
  AND l_orderkey = o_orderkey
  AND o_custkey = c_custkey
  AND c_nationkey = n1.n_nationkey
  AND n1.n_regionkey = r_regionkey
  AND r_name = 'AMERICA'
  AND s_nationkey = n2.n_nationkey
  AND o_orderdate BETWEEN date '1995-01-01' AND date '1996-12-31'
  AND p_type = 'ECONOMY ANODIZED STEEL'
GROUP BY o_year
ORDER BY o_year
"""
QUERIES_SQLITE['Q8'] = """
SELECT
  CAST(strftime('%Y', o_orderdate) AS INTEGER) as o_year,
  sum(case when n2.n_name = 'BRAZIL' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as mkt_share
FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
WHERE p_partkey = l_partkey
  AND s_suppkey = l_suppkey
  AND l_orderkey = o_orderkey
  AND o_custkey = c_custkey
  AND c_nationkey = n1.n_nationkey
  AND n1.n_regionkey = r_regionkey
  AND r_name = 'AMERICA'
  AND s_nationkey = n2.n_nationkey
  AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
  AND p_type = 'ECONOMY ANODIZED STEEL'
GROUP BY o_year
ORDER BY o_year
"""

# ─── Q9 ───
QUERIES_GDB['Q9'] = """
SELECT n_name as nation, extract(year from o_orderdate) as o_year,
  sum(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as sum_profit
FROM part, supplier, lineitem, partsupp, orders, nation
WHERE s_suppkey = l_suppkey
  AND ps_suppkey = l_suppkey
  AND ps_partkey = l_partkey
  AND p_partkey = l_partkey
  AND o_orderkey = l_orderkey
  AND s_nationkey = n_nationkey
  AND p_name LIKE '%green%'
GROUP BY nation, o_year
ORDER BY nation, o_year DESC
"""
QUERIES_SQLITE['Q9'] = """
SELECT n_name as nation, CAST(strftime('%Y', o_orderdate) AS INTEGER) as o_year,
  sum(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as sum_profit
FROM part, supplier, lineitem, partsupp, orders, nation
WHERE s_suppkey = l_suppkey
  AND ps_suppkey = l_suppkey
  AND ps_partkey = l_partkey
  AND p_partkey = l_partkey
  AND o_orderkey = l_orderkey
  AND s_nationkey = n_nationkey
  AND p_name LIKE '%green%'
GROUP BY nation, o_year
ORDER BY nation, o_year DESC
"""

# ─── Q10 ───
QUERIES_GDB['Q10'] = """
SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue,
  c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= date '1993-10-01'
  AND o_orderdate < date '1994-01-01'
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20
"""
QUERIES_SQLITE['Q10'] = QUERIES_GDB['Q10'].replace("date '1993-10-01'", "'1993-10-01'").replace("date '1994-01-01'", "'1994-01-01'")

# ─── Q11 ───
QUERIES_GDB['Q11'] = """
SELECT ps_partkey, sum(ps_supplycost * ps_availqty) as value
FROM partsupp, supplier, nation
WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING sum(ps_supplycost * ps_availqty) > (
  SELECT sum(ps_supplycost * ps_availqty) * 0.0001
  FROM partsupp, supplier, nation
  WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
)
ORDER BY value DESC
LIMIT 10
"""
QUERIES_SQLITE['Q11'] = QUERIES_GDB['Q11']

# ─── Q12 ───
QUERIES_GDB['Q12'] = """
SELECT l_shipmode,
  sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
  sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
FROM orders, lineitem
WHERE o_orderkey = l_orderkey
  AND l_shipmode IN ('MAIL', 'SHIP')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= date '1994-01-01'
  AND l_receiptdate < date '1995-01-01'
GROUP BY l_shipmode
ORDER BY l_shipmode
"""
QUERIES_SQLITE['Q12'] = QUERIES_GDB['Q12'].replace("date '1994-01-01'", "'1994-01-01'").replace("date '1995-01-01'", "'1995-01-01'")

# ─── Q13 ───
QUERIES_GDB['Q13'] = """
SELECT c_count, count(*) as custdist
FROM (
  SELECT c_custkey, count(o_orderkey) as c_count
  FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
  GROUP BY c_custkey
) as c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC
LIMIT 10
"""
QUERIES_SQLITE['Q13'] = QUERIES_GDB['Q13']

# ─── Q14 ───
QUERIES_GDB['Q14'] = """
SELECT 100.00 * sum(case when p_type LIKE 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM lineitem, part
WHERE l_partkey = p_partkey
  AND l_shipdate >= date '1995-09-01'
  AND l_shipdate < date '1995-10-01'
"""
QUERIES_SQLITE['Q14'] = QUERIES_GDB['Q14'].replace("date '1995-09-01'", "'1995-09-01'").replace("date '1995-10-01'", "'1995-10-01'")

# ─── Q15 ───
QUERIES_GDB['Q15'] = """
SELECT s_suppkey, s_name, s_address, s_phone, sum(l_extendedprice * (1 - l_discount)) as total_revenue
FROM supplier, lineitem
WHERE s_suppkey = l_suppkey
  AND l_shipdate >= date '1996-01-01'
  AND l_shipdate < date '1996-04-01'
GROUP BY s_suppkey, s_name, s_address, s_phone
ORDER BY total_revenue DESC
LIMIT 1
"""
QUERIES_SQLITE['Q15'] = QUERIES_GDB['Q15'].replace("date '1996-01-01'", "'1996-01-01'").replace("date '1996-04-01'", "'1996-04-01'")

# ─── Q16 ───
QUERIES_GDB['Q16'] = """
SELECT p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
FROM partsupp, part
WHERE p_partkey = ps_partkey
  AND p_brand <> 'Brand#45'
  AND p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps_suppkey NOT IN (
    SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%'
  )
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
LIMIT 10
"""
QUERIES_SQLITE['Q16'] = QUERIES_GDB['Q16']

# ─── Q17 ───
QUERIES_GDB['Q17'] = """
SELECT sum(l_extendedprice) / 7.0 as avg_yearly
FROM lineitem, part
WHERE p_partkey = l_partkey
  AND p_brand = 'Brand#23'
  AND p_container = 'MED BOX'
  AND l_quantity < (
    SELECT 0.2 * avg(l_quantity)
    FROM lineitem
    WHERE l_partkey = p_partkey
  )
"""
QUERIES_SQLITE['Q17'] = QUERIES_GDB['Q17']

# ─── Q18 ───
QUERIES_GDB['Q18'] = """
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
QUERIES_SQLITE['Q18'] = QUERIES_GDB['Q18']

# ─── Q19 ───
QUERIES_GDB['Q19'] = """
SELECT sum(l_extendedprice * (1 - l_discount)) as revenue
FROM lineitem, part
WHERE
  (
    p_partkey = l_partkey
    AND p_brand = 'Brand#12'
    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND l_quantity >= 1 AND l_quantity <= 11
    AND p_size BETWEEN 1 AND 5
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  )
  OR
  (
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    AND l_quantity >= 10 AND l_quantity <= 20
    AND p_size BETWEEN 1 AND 10
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  )
  OR
  (
    p_partkey = l_partkey
    AND p_brand = 'Brand#34'
    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    AND l_quantity >= 20 AND l_quantity <= 30
    AND p_size BETWEEN 1 AND 15
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  )
"""
QUERIES_SQLITE['Q19'] = QUERIES_GDB['Q19']

# ─── Q20 ───
QUERIES_GDB['Q20'] = """
SELECT s_name, s_address
FROM supplier, nation
WHERE s_suppkey IN (
  SELECT ps_suppkey
  FROM partsupp
  WHERE ps_partkey IN (
    SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'
  )
  AND ps_availqty > (
    SELECT 0.5 * sum(l_quantity)
    FROM lineitem
    WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
      AND l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01'
  )
)
AND s_nationkey = n_nationkey
AND n_name = 'CANADA'
ORDER BY s_name
"""
QUERIES_SQLITE['Q20'] = QUERIES_GDB['Q20'].replace("date '1994-01-01'", "'1994-01-01'").replace("date '1995-01-01'", "'1995-01-01'")

# ─── Q21 ───
QUERIES_GDB['Q21'] = """
SELECT s_name, count(*) as numwait
FROM supplier, lineitem l1, orders, nation
WHERE s_suppkey = l1.l_suppkey
  AND o_orderkey = l1.l_orderkey
  AND o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND EXISTS (
    SELECT * FROM lineitem l2
    WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey
  )
  AND NOT EXISTS (
    SELECT * FROM lineitem l3
    WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey
      AND l3.l_receiptdate > l3.l_commitdate
  )
  AND s_nationkey = n_nationkey
  AND n_name = 'SAUDI ARABIA'
GROUP BY s_name
ORDER BY numwait DESC, s_name
LIMIT 100
"""
QUERIES_SQLITE['Q21'] = QUERIES_GDB['Q21']

# ─── Q22 ───
QUERIES_GDB['Q22'] = """
SELECT substr(c_phone, 1, 2) as cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal
FROM customer
WHERE substr(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  AND c_acctbal > (
    SELECT avg(c_acctbal) FROM customer
    WHERE c_acctbal > 0.00
      AND substr(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  )
  AND NOT EXISTS (
    SELECT * FROM orders WHERE o_custkey = c_custkey
  )
GROUP BY cntrycode
ORDER BY cntrycode
"""
QUERIES_SQLITE['Q22'] = QUERIES_GDB['Q22']


# ── Expected row counts at SF 0.01 ────────────────────────────────────────

EXPECTED_ROWS = {
    'Q1': 4, 'Q2': 4, 'Q3': 10, 'Q4': 5, 'Q5': 4,
    'Q6': 1, 'Q7': 4, 'Q8': 2, 'Q9': 0, 'Q10': 20,
    'Q11': 10, 'Q12': 2, 'Q13': 10, 'Q14': 1, 'Q15': 1,
    'Q16': 10, 'Q17': 1, 'Q18': 0, 'Q19': 1, 'Q20': 0,
    'Q21': 3, 'Q22': 0,
}

# ── Column metadata for each query ────────────────────────────────────────

QUERY_COLS = {
    'Q1': {
        'names': ['l_returnflag', 'l_linestatus', 'sum_qty', 'sum_base_price',
                   'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order'],
        'numeric': {'sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge',
                    'avg_qty', 'avg_price', 'avg_disc', 'count_order'},
    },
    'Q2': {
        'names': ['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr',
                   's_address', 's_phone', 's_comment'],
        'numeric': {'s_acctbal', 'p_partkey'},
    },
    'Q3': {
        'names': ['l_orderkey', 'revenue', 'o_orderdate', 'o_shippriority'],
        'numeric': {'l_orderkey', 'revenue', 'o_shippriority'},
    },
    'Q4': {
        'names': ['o_orderpriority', 'order_count'],
        'numeric': {'order_count'},
    },
    'Q5': {
        'names': ['n_name', 'revenue'],
        'numeric': {'revenue'},
    },
    'Q6': {
        'names': ['revenue'],
        'numeric': {'revenue'},
    },
    'Q7': {
        'names': ['supp_nation', 'cust_nation', 'l_year', 'revenue'],
        'numeric': {'l_year', 'revenue'},
    },
    'Q8': {
        'names': ['o_year', 'mkt_share'],
        'numeric': {'o_year', 'mkt_share'},
    },
    'Q9': {
        'names': ['nation', 'o_year', 'sum_profit'],
        'numeric': {'o_year', 'sum_profit'},
    },
    'Q10': {
        'names': ['c_custkey', 'c_name', 'revenue', 'c_acctbal', 'n_name',
                   'c_address', 'c_phone', 'c_comment'],
        'numeric': {'c_custkey', 'revenue', 'c_acctbal'},
    },
    'Q11': {
        'names': ['ps_partkey', 'value'],
        'numeric': {'ps_partkey', 'value'},
    },
    'Q12': {
        'names': ['l_shipmode', 'high_line_count', 'low_line_count'],
        'numeric': {'high_line_count', 'low_line_count'},
    },
    'Q13': {
        'names': ['c_count', 'custdist'],
        'numeric': {'c_count', 'custdist'},
    },
    'Q14': {
        'names': ['promo_revenue'],
        'numeric': {'promo_revenue'},
    },
    'Q15': {
        'names': ['s_suppkey', 's_name', 's_address', 's_phone', 'total_revenue'],
        'numeric': {'s_suppkey', 'total_revenue'},
    },
    'Q16': {
        'names': ['p_brand', 'p_type', 'p_size', 'supplier_cnt'],
        'numeric': {'p_size', 'supplier_cnt'},
    },
    'Q17': {
        'names': ['avg_yearly'],
        'numeric': {'avg_yearly'},
    },
    'Q18': {
        'names': ['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate',
                   'o_totalprice', 'sum(l_quantity)'],
        'numeric': {'c_custkey', 'o_orderkey', 'o_totalprice', 'sum(l_quantity)'},
    },
    'Q19': {
        'names': ['revenue'],
        'numeric': {'revenue'},
    },
    'Q20': {
        'names': ['s_name', 's_address'],
        'numeric': set(),
    },
    'Q21': {
        'names': ['s_name', 'numwait'],
        'numeric': {'numwait'},
    },
    'Q22': {
        'names': ['cntrycode', 'numcust', 'totacctbal'],
        'numeric': {'numcust', 'totacctbal'},
    },
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _ensure_data():
    """Generate TPC-H data if it doesn't exist."""
    if not os.path.exists(os.path.join(DATA_DIR, "lineitem.tbl")):
        gen_script = os.path.join(REPO_ROOT, "scripts", "generate_tpch.py")
        subprocess.run([sys.executable, gen_script, DATA_DIR], check=True)
    assert os.path.exists(os.path.join(DATA_DIR, "lineitem.tbl")), \
        "lineitem.tbl not found after generation"


_engine_cache = None

def _get_engine():
    """Create and load a GroundDB engine with all needed tables."""
    global _engine_cache
    if _engine_cache is not None:
        return _engine_cache
    from grounddb import Engine
    engine = Engine()
    engine.load_tpch(DATA_DIR, tables=[
        "lineitem", "orders", "customer", "part",
        "supplier", "nation", "region", "partsupp"
    ])
    _engine_cache = engine
    return engine


_sqlite_conn_cache = None

def _get_sqlite_conn():
    """Create an in-memory SQLite connection with all needed tables loaded."""
    global _sqlite_conn_cache
    if _sqlite_conn_cache is not None:
        return _sqlite_conn_cache

    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

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
    _sqlite_conn_cache = conn
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

            # Both None
            if gdb_val is None and sql_val is None:
                continue

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


def _run_query_test(query_name):
    """Run a single TPC-H query cross-validation test."""
    engine = _get_engine()
    gdb_sql = QUERIES_GDB[query_name]
    sqlite_sql = QUERIES_SQLITE[query_name]
    cols = QUERY_COLS[query_name]
    expected = EXPECTED_ROWS[query_name]

    # Run GroundDB
    gdb_results = engine.execute(gdb_sql)

    # Run SQLite
    conn = _get_sqlite_conn()
    cursor = conn.cursor()
    cursor.execute(sqlite_sql)
    sqlite_results = cursor.fetchall()

    # Check row count
    assert len(gdb_results) == expected, \
        f"{query_name}: Expected {expected} rows, got {len(gdb_results)}"

    # Cross-validate
    if expected > 0:
        _compare_results(gdb_results, sqlite_results, cols['names'], cols['numeric'])


# ── Test class ───────────────────────────────────────────────────────────────

class TestM4:
    """Test all 22 TPC-H queries with cross-validation."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Ensure TPC-H data exists."""
        _ensure_data()

    def test_q1(self):
        _run_query_test('Q1')

    def test_q2(self):
        _run_query_test('Q2')

    def test_q3(self):
        _run_query_test('Q3')

    def test_q4(self):
        _run_query_test('Q4')

    def test_q5(self):
        _run_query_test('Q5')

    def test_q6(self):
        _run_query_test('Q6')

    def test_q7(self):
        _run_query_test('Q7')

    def test_q8(self):
        _run_query_test('Q8')

    def test_q9(self):
        _run_query_test('Q9')

    def test_q10(self):
        _run_query_test('Q10')

    def test_q11(self):
        _run_query_test('Q11')

    def test_q12(self):
        _run_query_test('Q12')

    def test_q13(self):
        _run_query_test('Q13')

    def test_q14(self):
        _run_query_test('Q14')

    def test_q15(self):
        _run_query_test('Q15')

    def test_q16(self):
        _run_query_test('Q16')

    def test_q17(self):
        _run_query_test('Q17')

    def test_q18(self):
        _run_query_test('Q18')

    def test_q19(self):
        _run_query_test('Q19')

    def test_q20(self):
        _run_query_test('Q20')

    def test_q21(self):
        _run_query_test('Q21')

    def test_q22(self):
        _run_query_test('Q22')

    # ── Regression: existing tests still pass ──────────────────────────

    def test_existing_q6_still_passes(self):
        """Verify Q6 still works after M4 changes."""
        from grounddb import Engine
        engine = Engine()
        engine.load_tpch(DATA_DIR, tables=["lineitem"])
        q6 = """
        SELECT sum(l_extendedprice * l_discount) AS revenue
        FROM lineitem
        WHERE l_shipdate >= date '1994-01-01'
            AND l_shipdate < date '1995-01-01'
            AND l_discount BETWEEN 0.05 AND 0.07
            AND l_quantity < 24
        """
        results = engine.execute(q6)
        assert len(results) == 1
        revenue = float(list(results[0].values())[0])
        assert revenue > 0, f"Q6 revenue should be positive, got {revenue}"
