"""
Main entry point for GroundDB — the Engine class.
"""

from typing import List, Dict, Any, Optional
from .parser import parse_sql
from .storage import Storage
from .executor import execute_select


class Engine:
    """GroundDB SQL engine.
    
    Usage:
        engine = Engine()
        engine.load_tpch("data/")
        results = engine.execute("SELECT * FROM lineitem LIMIT 10")
    """

    def __init__(self):
        self.storage = Storage()

    def load_tpch(self, data_dir: str, tables: Optional[List[str]] = None):
        """Load TPC-H data from a directory of .tbl files.
        
        Args:
            data_dir: Path to directory containing .tbl files
            tables: Optional list of table names to load. If None, loads all.
        """
        self.storage.load_tpch_directory(data_dir, tables)

    def load_table(self, name: str, filepath: str, schema=None):
        """Load a single table from a pipe-delimited file."""
        self.storage.load_table(name, filepath, schema)

    def execute(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results as a list of dicts.
        
        Args:
            sql: SQL SELECT query string
            
        Returns:
            List of result rows, each as a dict of {column_name: value}
        """
        stmt = parse_sql(sql)
        return execute_select(stmt, self.storage)
