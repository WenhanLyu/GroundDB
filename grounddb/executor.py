"""
Query executor for GroundDB.

Evaluates parsed AST against in-memory tables.
Supports single-table queries and multi-table JOINs (comma-join, INNER JOIN, LEFT OUTER JOIN).
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from .parser import (
    ASTNode, SelectStatement, ColumnRef, NumberLiteral, StringLiteral,
    DateLiteral, IntervalLiteral, CaseExpr, BinaryOp, UnaryOp, BetweenExpr,
    FunctionCall, StarExpr
)
from .storage import Storage, Table


def execute_select(stmt: SelectStatement, storage: Storage) -> List[Dict[str, Any]]:
    """Execute a SELECT statement and return a list of result rows (as dicts)."""

    # Determine if this is a multi-table query
    is_multi_table = len(stmt.from_tables) > 1 or len(stmt.joins) > 0

    if is_multi_table:
        rows = _execute_multi_table(stmt, storage)
    else:
        # Single table query (backward compatible path)
        table = storage.get_table(stmt.from_table)
        rows = table.rows

    # 2. Apply WHERE filter
    if stmt.where is not None:
        if is_multi_table:
            # For multi-table comma-join: WHERE already partially consumed for join predicates
            # We apply the full WHERE on the joined rows; join predicates are idempotent
            rows = [row for row in rows if _eval_expr(stmt.where, row)]
        else:
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


def _execute_multi_table(stmt: SelectStatement, storage: Storage) -> List[Dict[str, Any]]:
    """Execute multi-table FROM clause using hash joins."""

    # Build alias map: alias -> table_name
    alias_map = {}  # alias -> table_name
    for table_name, alias in stmt.from_tables:
        key = alias if alias else table_name
        alias_map[key] = table_name

    for join_info in stmt.joins:
        key = join_info['alias'] if join_info['alias'] else join_info['table']
        alias_map[key] = join_info['table']

    # Load tables and prefix rows with aliases
    table_rows = {}  # alias -> list of prefixed rows
    for table_name, alias in stmt.from_tables:
        key = alias if alias else table_name
        table = storage.get_table(table_name)
        prefixed = []
        for row in table.rows:
            new_row = {}
            for col, val in row.items():
                new_row[f"{key}.{col}"] = val
                new_row[col] = val  # also store bare name
            prefixed.append(new_row)
        table_rows[key] = prefixed

    if len(stmt.from_tables) > 1:
        # Comma-separated FROM: extract equi-join predicates from WHERE
        rows = _comma_join(stmt, table_rows, alias_map, storage)
    else:
        # Start with first table's rows
        first_alias = stmt.from_tables[0][1] or stmt.from_tables[0][0]
        rows = table_rows[first_alias]

    # Process explicit JOINs
    for join_info in stmt.joins:
        table_name = join_info['table']
        alias = join_info['alias'] if join_info['alias'] else table_name
        join_type = join_info['type']
        on_expr = join_info['on']

        table = storage.get_table(table_name)
        right_rows = []
        for row in table.rows:
            new_row = {}
            for col, val in row.items():
                new_row[f"{alias}.{col}"] = val
                new_row[col] = val
            right_rows.append(new_row)

        # Try to extract equi-join key from ON expression
        left_key, right_key = _extract_equi_key(on_expr)
        if left_key and right_key:
            rows = _hash_join(rows, right_rows, left_key, right_key, join_type)
        else:
            # Nested loop join with ON condition
            rows = _nested_loop_join(rows, right_rows, on_expr, join_type)

    return rows


def _comma_join(stmt, table_rows, alias_map, storage):
    """Handle comma-separated FROM with equi-join extraction from WHERE."""
    aliases = [alias if alias else tname for tname, alias in stmt.from_tables]

    # Start with first table
    result = table_rows[aliases[0]]

    if stmt.where is None:
        # Pure cross product (rare but possible)
        for i in range(1, len(aliases)):
            new_result = []
            for left_row in result:
                for right_row in table_rows[aliases[i]]:
                    merged = {**left_row, **right_row}
                    new_result.append(merged)
            result = new_result
        return result

    # Extract all equi-join predicates from WHERE
    equi_preds = _extract_all_equi_predicates(stmt.where)

    # For each subsequent table, find an equi-join predicate and use hash join
    joined_aliases = {aliases[0]}

    for i in range(1, len(aliases)):
        next_alias = aliases[i]
        # Find equi-join predicate connecting next_alias to any already-joined alias
        join_key = _find_join_key(equi_preds, next_alias, joined_aliases, alias_map)

        if join_key:
            left_key_name, right_key_name = join_key
            result = _hash_join(result, table_rows[next_alias],
                                left_key_name, right_key_name, 'INNER')
        else:
            # Cross product fallback
            new_result = []
            for left_row in result:
                for right_row in table_rows[next_alias]:
                    merged = {**left_row, **right_row}
                    new_result.append(merged)
            result = new_result

        joined_aliases.add(next_alias)

    return result


def _extract_all_equi_predicates(node):
    """Extract all equi-join predicates (col1 = col2) from a WHERE clause."""
    preds = []
    if isinstance(node, BinaryOp):
        if node.op == "AND":
            preds.extend(_extract_all_equi_predicates(node.left))
            preds.extend(_extract_all_equi_predicates(node.right))
        elif node.op == "=":
            if isinstance(node.left, ColumnRef) and isinstance(node.right, ColumnRef):
                preds.append((node.left, node.right))
    return preds


def _find_join_key(equi_preds, next_alias, joined_aliases, alias_map):
    """Find an equi-join predicate connecting next_alias to already-joined tables."""
    for left_col, right_col in equi_preds:
        # Determine which alias each column belongs to
        left_alias = _resolve_column_alias(left_col, joined_aliases, next_alias, alias_map)
        right_alias = _resolve_column_alias(right_col, joined_aliases, next_alias, alias_map)

        if left_alias is None or right_alias is None:
            continue

        # One must be in joined_aliases, other must be next_alias
        if left_alias in joined_aliases and right_alias == next_alias:
            left_key = f"{left_alias}.{left_col.name}" if left_col.table else left_col.name
            right_key = f"{right_alias}.{right_col.name}" if right_col.table else right_col.name
            return (left_key, right_key)
        elif right_alias in joined_aliases and left_alias == next_alias:
            left_key = f"{right_alias}.{right_col.name}" if right_col.table else right_col.name
            right_key = f"{left_alias}.{left_col.name}" if left_col.table else left_col.name
            return (left_key, right_key)

    # Try matching by column name prefix convention (e.g., c_custkey matches customer alias)
    for left_col, right_col in equi_preds:
        l_table = left_col.table
        r_table = right_col.table

        if l_table and r_table:
            if l_table in joined_aliases and r_table == next_alias:
                return (f"{l_table}.{left_col.name}", f"{r_table}.{right_col.name}")
            elif r_table in joined_aliases and l_table == next_alias:
                return (f"{r_table}.{right_col.name}", f"{l_table}.{left_col.name}")
        elif l_table is None and r_table is None:
            # Try to figure out by column name prefix
            l_alias = _guess_alias_from_column(left_col.name, joined_aliases, next_alias)
            r_alias = _guess_alias_from_column(right_col.name, joined_aliases, next_alias)
            if l_alias and r_alias:
                if l_alias in joined_aliases and r_alias == next_alias:
                    return (left_col.name, right_col.name)
                elif r_alias in joined_aliases and l_alias == next_alias:
                    return (right_col.name, left_col.name)

    return None


def _resolve_column_alias(col, joined_aliases, next_alias, alias_map):
    """Resolve which alias a ColumnRef belongs to."""
    if col.table:
        return col.table
    # Try to guess from column name prefix
    all_aliases = joined_aliases | {next_alias}
    return _guess_alias_from_column(col.name, joined_aliases, next_alias)


def _guess_alias_from_column(col_name, joined_aliases, next_alias):
    """Guess which alias a column belongs to based on naming conventions."""
    all_aliases = list(joined_aliases) + [next_alias]
    # TPC-H convention: column prefix like l_ for lineitem, o_ for orders, etc.
    prefix = col_name.split("_")[0] + "_" if "_" in col_name else None
    if prefix:
        for alias in all_aliases:
            # Check if column prefix matches first letter of table/alias
            if alias[0] == col_name[0]:
                return alias
    return None


def _extract_equi_key(on_expr):
    """Extract left and right key names from a simple equi-join ON expression."""
    if isinstance(on_expr, BinaryOp) and on_expr.op == "=":
        left = on_expr.left
        right = on_expr.right
        if isinstance(left, ColumnRef) and isinstance(right, ColumnRef):
            left_key = f"{left.table}.{left.name}" if left.table else left.name
            right_key = f"{right.table}.{right.name}" if right.table else right.name
            return (left_key, right_key)
    return (None, None)


def _hash_join(left_rows, right_rows, left_key, right_key, join_type='INNER'):
    """Perform a hash join between left and right row sets.

    Args:
        left_rows: list of dicts (left side)
        right_rows: list of dicts (right side)
        left_key: column name to use as key from left rows
        right_key: column name to use as key from right rows
        join_type: 'INNER' or 'LEFT OUTER'

    Returns:
        list of merged row dicts
    """
    # Build hash table from right rows
    hash_table = {}
    right_cols = set()
    for row in right_rows:
        key_val = row.get(right_key)
        if key_val is not None:
            if key_val not in hash_table:
                hash_table[key_val] = []
            hash_table[key_val].append(row)
        right_cols.update(row.keys())

    result = []
    for left_row in left_rows:
        key_val = left_row.get(left_key)
        matches = hash_table.get(key_val, []) if key_val is not None else []

        if matches:
            for right_row in matches:
                merged = {**left_row, **right_row}
                result.append(merged)
        elif join_type == 'LEFT OUTER':
            # No match - include left row with NULL right columns
            merged = dict(left_row)
            for col in right_cols:
                if col not in merged:
                    merged[col] = None
            result.append(merged)

    return result


def _nested_loop_join(left_rows, right_rows, on_expr, join_type='INNER'):
    """Perform a nested loop join with an arbitrary ON condition."""
    right_cols = set()
    for row in right_rows:
        right_cols.update(row.keys())

    result = []
    for left_row in left_rows:
        matched = False
        for right_row in right_rows:
            merged = {**left_row, **right_row}
            if _eval_expr(on_expr, merged):
                result.append(merged)
                matched = True
        if not matched and join_type == 'LEFT OUTER':
            merged = dict(left_row)
            for col in right_cols:
                if col not in merged:
                    merged[col] = None
            result.append(merged)

    return result


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
    if isinstance(node, CaseExpr):
        for cond, result in node.when_clauses:
            if _contains_aggregate(cond) or _contains_aggregate(result):
                return True
        if node.else_expr and _contains_aggregate(node.else_expr):
            return True
    return False


def _eval_expr(node: ASTNode, row: Dict[str, Any]) -> Any:
    """Evaluate an expression node against a row."""

    if isinstance(node, ColumnRef):
        # Try direct name first
        name = node.name
        if node.table:
            # Prefer alias.colname lookup
            full_name = f"{node.table}.{node.name}"
            if full_name in row:
                return row[full_name]
        if name in row:
            return row[name]
        # Try with table prefix if table is set
        if node.table:
            full_name = f"{node.table}.{node.name}"
            if full_name in row:
                return row[full_name]
        # Case-insensitive lookup
        name_lower = name.lower()
        for key in row:
            if key.lower() == name_lower:
                return row[key]
        # Try all prefixed versions
        suffix = f".{name}"
        for key in row:
            if key.endswith(suffix):
                return row[key]
        raise KeyError(f"Column {name!r} (table={node.table!r}) not found in row. Available: {list(row.keys())[:20]}")

    if isinstance(node, NumberLiteral):
        return node.value

    if isinstance(node, StringLiteral):
        return node.value

    if isinstance(node, DateLiteral):
        return node.value

    if isinstance(node, IntervalLiteral):
        return node  # Return the node itself for arithmetic

    if isinstance(node, CaseExpr):
        return _eval_case(node, row)

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

        # Handle date - interval arithmetic
        if node.op == "-" and isinstance(left, str) and isinstance(right, IntervalLiteral):
            return _date_subtract_interval(left, right)
        if node.op == "+" and isinstance(left, str) and isinstance(right, IntervalLiteral):
            return _date_add_interval(left, right)

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


def _eval_case(node: CaseExpr, row: Dict[str, Any]) -> Any:
    """Evaluate a CASE WHEN expression."""
    for condition, result in node.when_clauses:
        if _eval_expr(condition, row):
            return _eval_expr(result, row)
    if node.else_expr is not None:
        return _eval_expr(node.else_expr, row)
    return None


def _date_subtract_interval(date_str: str, interval: 'IntervalLiteral') -> str:
    """Subtract an interval from a date string, return new date string."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if interval.unit == "DAY":
        dt = dt - timedelta(days=interval.n)
    elif interval.unit == "MONTH":
        # Subtract months
        month = dt.month - interval.n
        year = dt.year
        while month <= 0:
            month += 12
            year -= 1
        dt = dt.replace(year=year, month=month)
    elif interval.unit == "YEAR":
        dt = dt.replace(year=dt.year - interval.n)
    return dt.strftime("%Y-%m-%d")


def _date_add_interval(date_str: str, interval: 'IntervalLiteral') -> str:
    """Add an interval to a date string, return new date string."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if interval.unit == "DAY":
        dt = dt + timedelta(days=interval.n)
    elif interval.unit == "MONTH":
        month = dt.month + interval.n
        year = dt.year
        while month > 12:
            month -= 12
            year += 1
        dt = dt.replace(year=year, month=month)
    elif interval.unit == "YEAR":
        dt = dt.replace(year=dt.year + interval.n)
    return dt.strftime("%Y-%m-%d")


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
    if isinstance(node, CaseExpr):
        return "case_expr"
    if isinstance(node, IntervalLiteral):
        return f"interval_{node.n}_{node.unit}"
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
