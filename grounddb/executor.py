"""
Query executor for GroundDB.

Evaluates parsed AST against in-memory tables.
"""

from typing import List, Dict, Any, Optional
from .parser import (
    ASTNode, SelectStatement, ColumnRef, NumberLiteral, StringLiteral,
    DateLiteral, BinaryOp, UnaryOp, BetweenExpr, FunctionCall, StarExpr
)
from .storage import Storage, Table


def execute_select(stmt: SelectStatement, storage: Storage) -> List[Dict[str, Any]]:
    """Execute a SELECT statement and return a list of result rows (as dicts)."""

    # 1. Get the source table
    table = storage.get_table(stmt.from_table)
    rows = table.rows

    # 2. Apply WHERE filter
    if stmt.where is not None:
        rows = [row for row in rows if _eval_expr(stmt.where, row)]

    # 3. Check if we have aggregates (with no GROUP BY, it's a single-group aggregate)
    has_aggregates = _has_aggregate(stmt)

    if has_aggregates and not stmt.group_by:
        # Single-group aggregate
        result_row = _compute_aggregates(stmt.columns, rows)
        return [result_row]
    elif stmt.group_by:
        # Group by
        groups = _group_rows(stmt.group_by, rows)
        results = []
        for group_key, group_rows in groups.items():
            row = _compute_aggregates(stmt.columns, group_rows, group_rows[0] if group_rows else {})
            results.append(row)

        # Apply HAVING
        if stmt.having is not None:
            results = [r for r in results if _eval_expr(stmt.having, r)]

        # ORDER BY
        if stmt.order_by:
            results = _sort_rows(results, stmt.order_by)

        # LIMIT
        if stmt.limit is not None:
            results = results[:stmt.limit]

        return results
    else:
        # No aggregates — simple projection
        results = []
        for row in rows:
            result_row = {}
            for expr, alias in stmt.columns:
                if isinstance(expr, StarExpr):
                    result_row.update(row)
                else:
                    col_name = alias or _expr_name(expr)
                    result_row[col_name] = _eval_expr(expr, row)
            results.append(result_row)

        # DISTINCT
        if stmt.distinct:
            seen = set()
            unique_results = []
            for r in results:
                key = tuple(sorted(r.items()))
                if key not in seen:
                    seen.add(key)
                    unique_results.append(r)
            results = unique_results

        # ORDER BY
        if stmt.order_by:
            results = _sort_rows(results, stmt.order_by)

        # LIMIT
        if stmt.limit is not None:
            results = results[:stmt.limit]

        return results


def _has_aggregate(stmt: SelectStatement) -> bool:
    """Check if any select expression contains an aggregate function."""
    for expr, alias in stmt.columns:
        if _contains_aggregate(expr):
            return True
    return False


def _contains_aggregate(node: ASTNode) -> bool:
    """Recursively check if an expression contains an aggregate call."""
    if isinstance(node, FunctionCall) and node.name in ("SUM", "AVG", "COUNT", "MIN", "MAX"):
        return True
    if isinstance(node, BinaryOp):
        return _contains_aggregate(node.left) or _contains_aggregate(node.right)
    if isinstance(node, UnaryOp):
        return _contains_aggregate(node.operand)
    return False


def _eval_expr(node: ASTNode, row: Dict[str, Any]) -> Any:
    """Evaluate an expression node against a row."""

    if isinstance(node, ColumnRef):
        name = node.name
        if name in row:
            return row[name]
        # Try with table prefix
        if node.table:
            full_name = f"{node.table}.{node.name}"
            if full_name in row:
                return row[full_name]
        # Case-insensitive lookup
        for key in row:
            if key.lower() == name.lower():
                return row[key]
        raise KeyError(f"Column {name!r} not found in row. Available: {list(row.keys())}")

    if isinstance(node, NumberLiteral):
        return node.value

    if isinstance(node, StringLiteral):
        return node.value

    if isinstance(node, DateLiteral):
        return node.value

    if isinstance(node, BinaryOp):
        # Short-circuit for AND/OR
        if node.op == "AND":
            left = _eval_expr(node.left, row)
            if not left:
                return False
            return bool(_eval_expr(node.right, row))
        if node.op == "OR":
            left = _eval_expr(node.left, row)
            if left:
                return True
            return bool(_eval_expr(node.right, row))

        left = _eval_expr(node.left, row)
        right = _eval_expr(node.right, row)

        if node.op == "+":
            return _numeric(left) + _numeric(right)
        if node.op == "-":
            return _numeric(left) - _numeric(right)
        if node.op == "*":
            return _numeric(left) * _numeric(right)
        if node.op == "/":
            r = _numeric(right)
            if r == 0:
                return None
            return _numeric(left) / r
        if node.op == "=":
            return left == right
        if node.op in ("<>", "!="):
            return left != right
        if node.op == "<":
            return _compare(left, right) < 0
        if node.op == ">":
            return _compare(left, right) > 0
        if node.op == "<=":
            return _compare(left, right) <= 0
        if node.op == ">=":
            return _compare(left, right) >= 0
        if node.op == "LIKE":
            return _like_match(str(left), str(right))
        if node.op == "IS NULL":
            return left is None
        if node.op == "IS NOT NULL":
            return left is not None
        if node.op == "IN":
            # right is a list of AST nodes
            values = [_eval_expr(v, row) for v in right]
            return left in values
        if node.op == "||":
            return str(left) + str(right)

        raise ValueError(f"Unknown binary op: {node.op}")

    if isinstance(node, UnaryOp):
        if node.op == "NOT":
            return not _eval_expr(node.operand, row)
        if node.op == "-":
            return -_numeric(_eval_expr(node.operand, row))
        raise ValueError(f"Unknown unary op: {node.op}")

    if isinstance(node, BetweenExpr):
        val = _eval_expr(node.expr, row)
        low = _eval_expr(node.low, row)
        high = _eval_expr(node.high, row)
        return _compare(val, low) >= 0 and _compare(val, high) <= 0

    if isinstance(node, FunctionCall):
        # For aggregate functions used in expressions (e.g., in HAVING),
        # the result should already be in the row
        name_lower = node.name.lower()
        if name_lower in row:
            return row[name_lower]
        # Try constructing the function name from the expression
        func_key = _expr_name(node)
        if func_key in row:
            return row[func_key]
        raise KeyError(f"Aggregate result {func_key!r} not found in row")

    if isinstance(node, StarExpr):
        return None

    raise TypeError(f"Cannot evaluate node type: {type(node).__name__}")


def _numeric(val) -> float:
    """Coerce a value to numeric."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    return float(val)


def _compare(a, b) -> int:
    """Compare two values, handling None and mixed types."""
    if a is None and b is None:
        return 0
    if a is None:
        return -1
    if b is None:
        return 1
    if a < b:
        return -1
    if a > b:
        return 1
    return 0


def _like_match(text: str, pattern: str) -> bool:
    """Simple SQL LIKE pattern matching (% and _ wildcards)."""
    import re as _re
    # Escape regex special chars, then convert SQL wildcards
    regex = ""
    for ch in pattern:
        if ch == "%":
            regex += ".*"
        elif ch == "_":
            regex += "."
        else:
            regex += _re.escape(ch)
    return bool(_re.fullmatch(regex, text, _re.IGNORECASE))


def _expr_name(node: ASTNode) -> str:
    """Generate a name for an expression (used for column naming)."""
    if isinstance(node, ColumnRef):
        if node.table:
            return f"{node.table}.{node.name}"
        return node.name
    if isinstance(node, FunctionCall):
        args_str = ", ".join(_expr_name(a) for a in node.args)
        return f"{node.name.lower()}({args_str})"
    if isinstance(node, BinaryOp):
        return f"{_expr_name(node.left)} {node.op} {_expr_name(node.right)}"
    if isinstance(node, StarExpr):
        return "*"
    if isinstance(node, NumberLiteral):
        return str(node.value)
    if isinstance(node, StringLiteral):
        return f"'{node.value}'"
    if isinstance(node, DateLiteral):
        return f"date '{node.value}'"
    if isinstance(node, UnaryOp):
        return f"{node.op}({_expr_name(node.operand)})"
    return "?"


def _compute_aggregates(columns: list, rows: List[dict], 
                         sample_row: Optional[dict] = None) -> dict:
    """Compute aggregate expressions over a group of rows."""
    result = {}
    for expr, alias in columns:
        col_name = alias or _expr_name(expr)
        if _contains_aggregate(expr):
            result[col_name] = _eval_aggregate_expr(expr, rows)
        else:
            # Non-aggregate expression: take from first row
            if rows:
                result[col_name] = _eval_expr(expr, rows[0])
            else:
                result[col_name] = None
    return result


def _eval_aggregate_expr(node: ASTNode, rows: List[dict]) -> Any:
    """Evaluate an expression that contains aggregates over a set of rows."""

    if isinstance(node, FunctionCall) and node.name in ("SUM", "AVG", "COUNT", "MIN", "MAX"):
        if node.name == "COUNT":
            if node.args and isinstance(node.args[0], StarExpr):
                return len(rows)
            else:
                vals = [_eval_expr(node.args[0], row) for row in rows]
                vals = [v for v in vals if v is not None]
                if node.distinct:
                    vals = list(set(vals))
                return len(vals)

        # For other aggregates, evaluate the argument for each row
        vals = [_eval_expr(node.args[0], row) for row in rows]
        vals = [_numeric(v) for v in vals if v is not None]

        if node.distinct:
            vals = list(set(vals))

        if not vals:
            return None

        if node.name == "SUM":
            return sum(vals)
        elif node.name == "AVG":
            return sum(vals) / len(vals)
        elif node.name == "MIN":
            return min(vals)
        elif node.name == "MAX":
            return max(vals)

    if isinstance(node, BinaryOp):
        # If one or both sides contain aggregates, evaluate them
        left = _eval_aggregate_expr(node.left, rows) if _contains_aggregate(node.left) else _eval_expr(node.left, rows[0] if rows else {})
        right = _eval_aggregate_expr(node.right, rows) if _contains_aggregate(node.right) else _eval_expr(node.right, rows[0] if rows else {})

        if node.op == "+":
            return _numeric(left) + _numeric(right)
        if node.op == "-":
            return _numeric(left) - _numeric(right)
        if node.op == "*":
            return _numeric(left) * _numeric(right)
        if node.op == "/":
            r = _numeric(right)
            if r == 0:
                return None
            return _numeric(left) / r

    # Non-aggregate — shouldn't reach here
    if rows:
        return _eval_expr(node, rows[0])
    return None


def _group_rows(group_by_exprs: list, rows: List[dict]) -> dict:
    """Group rows by the group-by expressions. Returns {key_tuple: [rows]}."""
    groups = {}
    for row in rows:
        key = tuple(_eval_expr(expr, row) for expr in group_by_exprs)
        if key not in groups:
            groups[key] = []
        groups[key].append(row)
    return groups


def _sort_rows(rows: List[dict], order_by: list) -> List[dict]:
    """Sort rows by ORDER BY specification."""
    import functools

    def compare(a, b):
        for expr, direction in order_by:
            va = _eval_expr(expr, a)
            vb = _eval_expr(expr, b)
            cmp = _compare(va, vb)
            if direction == "DESC":
                cmp = -cmp
            if cmp != 0:
                return cmp
        return 0

    return sorted(rows, key=functools.cmp_to_key(compare))
