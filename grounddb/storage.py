"""
In-memory table storage for GroundDB.

Loads pipe-delimited TPC-H data files into memory.
Each table is stored as a list of rows (dicts) with typed columns.
"""

import os
from typing import List, Dict, Any, Optional


# ── TPC-H table schemas ─────────────────────────────────────────────────────
# Column name -> type ('int', 'float', 'str', 'date')

TPCH_SCHEMAS = {
    "lineitem": [
        ("l_orderkey", "int"),
        ("l_partkey", "int"),
        ("l_suppkey", "int"),
        ("l_linenumber", "int"),
        ("l_quantity", "float"),
        ("l_extendedprice", "float"),
        ("l_discount", "float"),
        ("l_tax", "float"),
        ("l_returnflag", "str"),
        ("l_linestatus", "str"),
        ("l_shipdate", "date"),
        ("l_commitdate", "date"),
        ("l_receiptdate", "date"),
        ("l_shipinstruct", "str"),
        ("l_shipmode", "str"),
        ("l_comment", "str"),
    ],
    "orders": [
        ("o_orderkey", "int"),
        ("o_custkey", "int"),
        ("o_orderstatus", "str"),
        ("o_totalprice", "float"),
        ("o_orderdate", "date"),
        ("o_orderpriority", "str"),
        ("o_clerk", "str"),
        ("o_shippriority", "int"),
        ("o_comment", "str"),
    ],
    "customer": [
        ("c_custkey", "int"),
        ("c_name", "str"),
        ("c_address", "str"),
        ("c_nationkey", "int"),
        ("c_phone", "str"),
        ("c_acctbal", "float"),
        ("c_mktsegment", "str"),
        ("c_comment", "str"),
    ],
    "supplier": [
        ("s_suppkey", "int"),
        ("s_name", "str"),
        ("s_address", "str"),
        ("s_nationkey", "int"),
        ("s_phone", "str"),
        ("s_acctbal", "float"),
        ("s_comment", "str"),
    ],
    "part": [
        ("p_partkey", "int"),
        ("p_name", "str"),
        ("p_mfgr", "str"),
        ("p_brand", "str"),
        ("p_type", "str"),
        ("p_size", "int"),
        ("p_container", "str"),
        ("p_retailprice", "float"),
        ("p_comment", "str"),
    ],
    "partsupp": [
        ("ps_partkey", "int"),
        ("ps_suppkey", "int"),
        ("ps_availqty", "int"),
        ("ps_supplycost", "float"),
        ("ps_comment", "str"),
    ],
    "nation": [
        ("n_nationkey", "int"),
        ("n_name", "str"),
        ("n_regionkey", "int"),
        ("n_comment", "str"),
    ],
    "region": [
        ("r_regionkey", "int"),
        ("r_name", "str"),
        ("r_comment", "str"),
    ],
}


class Table:
    """In-memory table: list of rows with column metadata."""

    def __init__(self, name: str, columns: List[str], column_types: Dict[str, str]):
        self.name = name
        self.columns = columns  # ordered list of column names
        self.column_types = column_types  # col_name -> type string
        self.rows: List[Dict[str, Any]] = []

    def add_row(self, row: Dict[str, Any]):
        self.rows.append(row)

    def __len__(self):
        return len(self.rows)

    def __repr__(self):
        return f"Table({self.name!r}, {len(self.rows)} rows, cols={self.columns})"


class Storage:
    """In-memory storage engine — manages loaded tables."""

    def __init__(self):
        self.tables: Dict[str, Table] = {}

    def load_table(self, name: str, filepath: str, schema: Optional[List[tuple]] = None):
        """Load a pipe-delimited file into a table.
        
        Args:
            name: Table name
            filepath: Path to the .tbl file
            schema: List of (column_name, type_str) pairs. If None, looks up TPCH_SCHEMAS.
        """
        if schema is None:
            schema = TPCH_SCHEMAS.get(name)
            if schema is None:
                raise ValueError(f"Unknown table {name!r} and no schema provided")

        columns = [col for col, _ in schema]
        column_types = {col: typ for col, typ in schema}
        table = Table(name, columns, column_types)

        with open(filepath, "r") as f:
            for line in f:
                line = line.rstrip("\n").rstrip("|")
                if not line:
                    continue
                parts = line.split("|")
                if len(parts) < len(columns):
                    # Pad with empty strings
                    parts.extend([""] * (len(columns) - len(parts)))
                row = {}
                for i, (col, typ) in enumerate(schema):
                    val = parts[i].strip() if i < len(parts) else ""
                    row[col] = _cast_value(val, typ)
                table.add_row(row)

        self.tables[name] = table
        return table

    def get_table(self, name: str) -> Table:
        """Get a loaded table by name."""
        if name not in self.tables:
            raise KeyError(f"Table {name!r} not loaded")
        return self.tables[name]

    def load_tpch_directory(self, data_dir: str, tables: Optional[List[str]] = None):
        """Load all (or specified) TPC-H tables from a directory of .tbl files."""
        if tables is None:
            tables = list(TPCH_SCHEMAS.keys())
        for tbl_name in tables:
            filepath = os.path.join(data_dir, f"{tbl_name}.tbl")
            if os.path.exists(filepath):
                self.load_table(tbl_name, filepath)


def _cast_value(val: str, typ: str) -> Any:
    """Cast a string value to the appropriate Python type."""
    if val == "" or val is None:
        return None
    if typ == "int":
        return int(val)
    elif typ == "float":
        return float(val)
    elif typ == "date":
        # Store dates as strings in YYYY-MM-DD format for lexicographic comparison
        return val
    else:
        return val
