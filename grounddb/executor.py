"""
Query executor for GroundDB.

Evaluates parsed AST against in-memory tables.
Supports single-table queries, multi-table JOINs (comma-join, INNER JOIN, LEFT OUTER JOIN),
and subqueries (EXISTS, IN subquery, scalar subquery, correlated subqueries, derived tables).
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from .parser import (
    ASTNode, SelectStatement, ColumnRef, NumberLiteral, StringLiteral,
    DateLiteral, IntervalLiteral, CaseExpr, BinaryOp, UnaryOp, BetweenExpr,
    FunctionCall, StarExpr, SubqueryExpr, InSubqueryExpr, ExistsExpr
)
from .storage import Storage, Table


def execute_select(stmt: SelectStatement, storage: Storage, outer_row: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Execute a SELECT statement and return a list of result rows (as dicts).

    Args:
        stmt: Parsed SELECT statement AST
        storage: Storage engine with loaded tables
        outer_row: Optional row context from outer query (for correlated subqueries)
    """

    # Check for derived tables in from_tables and materialize them
    materialized_tables = {}  # alias -> list of rows (for derived tables)
    real_from_tables = []
    for table_ref, alias in stmt.from_tables:
        if isinstance(table_ref, SelectStatement):
            # Execute the subquery and store result as a virtual table
            sub_results = execute_select(table_ref, storage)
            materialized_tables[alias] = sub_results
            real_from_tables.append((alias, alias))
        else:
            real_from_tables.append((table_ref, alias))
    stmt_from_tables_original = stmt.from_tables
    stmt.from_tables = real_from_tables

    # Determine if this is a multi-table query
    is_multi_table = len(stmt.from_tables) > 1 or len(stmt.joins) > 0

    if is_multi_table:
        rows = _execute_multi_table(stmt, storage, materialized_tables)
    else:
        # Single table query
        table_ref = stmt.from_tables[0][0]
        alias = stmt.from_tables[0][1]
        if table_ref in materialized_tables:
            rows = materialized_tables[table_ref]
        else:
            table = storage.get_table(table_ref)
            rows = table.rows

    # Merge outer_row into each row for correlated subquery support
    if outer_row is not None:
        rows = [{**outer_row, **row} for row in rows]

    # 2. Apply WHERE filter
    if stmt.where is not None:
        # Pre-materialize uncorrelated subqueries to avoid re-executing per row
        _materialize_uncorrelated_subqueries(stmt.where, storage)
        rows = [row for row in rows if _eval_expr(stmt.where, row, storage)]

    # 3. Check if we have aggregates (with no GROUP BY, it's a single-group aggregate)
    has_aggregates = _has_aggregate(stmt)

    # Restore original from_tables
    stmt.from_tables = stmt_from_tables_original

    if has_aggregates and not stmt.group_by:
        # Single-group aggregate
        result_row = _compute_aggregates(stmt.columns, rows, storage=storage)
        return [result_row]
    elif stmt.group_by:
        # Group by
        groups = _group_rows(stmt.group_by, rows, storage)
        results = []
        for group_key, group_rows in groups.items():
            row = _compute_aggregates(stmt.columns, group_rows, group_rows[0] if group_rows else {}, storage=storage)
            # Also compute any aggregates referenced in HAVING that aren't in SELECT
            if stmt.having is not None:
                _add_having_aggregates(stmt.having, row, group_rows, storage)
            results.append(row)

        # Apply HAVING
        if stmt.having is not None:
            results = [r for r in results if _eval_expr(stmt.having, r, storage)]

        # ORDER BY
        if stmt.order_by:
            results = _sort_rows(results, stmt.order_by, storage)

        # LIMIT
        if stmt.limit is not None:
            results = results[:stmt.limit]

        # Remove any extra columns added for HAVING evaluation that aren't in SELECT
        select_keys = set()
        for expr, alias in stmt.columns:
            if isinstance(expr, StarExpr):
                select_keys = None
                break
            select_keys.add(alias or _expr_name(expr))
        if select_keys is not None:
            cleaned = []
            for r in results:
                cleaned.append({k: v for k, v in r.items() if k in select_keys})
            results = cleaned

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
                    result_row[col_name] = _eval_expr(expr, row, storage)
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
            results = _sort_rows(results, stmt.order_by, storage)

        # LIMIT
        if stmt.limit is not None:
            results = results[:stmt.limit]

        return results


def _execute_multi_table(stmt: SelectStatement, storage: Storage, materialized_tables: Optional[Dict] = None) -> List[Dict[str, Any]]:
    """Execute multi-table FROM clause using hash joins."""
    if materialized_tables is None:
        materialized_tables = {}

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
        if table_name in materialized_tables:
            raw_rows = materialized_tables[table_name]
        else:
            table = storage.get_table(table_name)
            raw_rows = table.rows
        prefixed = []
        for row in raw_rows:
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

        if table_name in materialized_tables:
            raw_rows = materialized_tables[table_name]
        else:
            table = storage.get_table(table_name)
            raw_rows = table.rows
        right_rows = []
        for row in raw_rows:
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
            rows = _nested_loop_join(rows, right_rows, on_expr, join_type, storage)

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
    # TPC-H convention: column prefix maps to table name
    _TABLE_COL_PREFIX = {
        'l_': 'lineitem', 'o_': 'orders', 'c_': 'customer',
        's_': 'supplier', 'p_': 'part', 'ps_': 'partsupp',
        'n_': 'nation', 'r_': 'region',
    }
    # Try longer prefixes first (ps_ before p_)
    for prefix in sorted(_TABLE_COL_PREFIX.keys(), key=len, reverse=True):
        if col_name.startswith(prefix):
            table = _TABLE_COL_PREFIX[prefix]
            if table in all_aliases:
                return table
            # Also check aliases that might map to this table
            for alias in all_aliases:
                if alias == table:
                    return alias
            break
    # Fallback: first letter match
    if "_" in col_name:
        col_prefix = col_name[0]
        for alias in all_aliases:
            if alias[0] == col_prefix:
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


def _nested_loop_join(left_rows, right_rows, on_expr, join_type='INNER', storage=None):
    """Perform a nested loop join with an arbitrary ON condition."""
    right_cols = set()
    for row in right_rows:
        right_cols.update(row.keys())

    result = []
    for left_row in left_rows:
        matched = False
        for right_row in right_rows:
            merged = {**left_row, **right_row}
            if _eval_expr(on_expr, merged, storage):
                result.append(merged)
                matched = True
        if not matched and join_type == 'LEFT OUTER':
            merged = dict(left_row)
            for col in right_cols:
                if col not in merged:
                    merged[col] = None
            result.append(merged)

    return result


def _is_correlated(subquery: SelectStatement) -> bool:
    """Check if a subquery references columns not defined in its own tables.
    
    A simple heuristic: if all column refs in the WHERE clause either have a table 
    prefix matching one of the subquery's own tables, or have no table prefix and 
    the column name starts with a letter matching one of the subquery's tables,
    then it's uncorrelated. Otherwise assume correlated.
    """
    # Get the subquery's own table names/aliases
    own_tables = set()
    for table_ref, alias in subquery.from_tables:
        if isinstance(table_ref, str):
            own_tables.add(table_ref)
        if alias:
            own_tables.add(alias)
    for join_info in subquery.joins:
        own_tables.add(join_info.get('table', ''))
        if join_info.get('alias'):
            own_tables.add(join_info['alias'])
    
    # Check WHERE clause for external references
    if subquery.where is not None:
        if _has_external_refs(subquery.where, own_tables):
            return True
    return False


def _has_external_refs(node: ASTNode, own_tables: set) -> bool:
    """Check if an expression references columns from tables not in own_tables."""
    if isinstance(node, ColumnRef):
        if node.table:
            return node.table not in own_tables
        # No table prefix: use naming convention to guess
        # TPC-H: column prefix like l_ for lineitem, o_ for orders, etc.
        return not _column_belongs_to_tables(node.name, own_tables)
    if isinstance(node, BinaryOp):
        return _has_external_refs(node.left, own_tables) or _has_external_refs(node.right, own_tables)
    if isinstance(node, UnaryOp):
        return _has_external_refs(node.operand, own_tables)
    if isinstance(node, BetweenExpr):
        return (_has_external_refs(node.expr, own_tables) or 
                _has_external_refs(node.low, own_tables) or 
                _has_external_refs(node.high, own_tables))
    if isinstance(node, FunctionCall):
        return any(_has_external_refs(arg, own_tables) for arg in node.args if isinstance(arg, ASTNode))
    if isinstance(node, CaseExpr):
        for cond, result in node.when_clauses:
            if _has_external_refs(cond, own_tables) or _has_external_refs(result, own_tables):
                return True
        if node.else_expr and _has_external_refs(node.else_expr, own_tables):
            return True
    return False


def _column_belongs_to_tables(col_name: str, tables: set) -> bool:
    """Check if a column name belongs to any of the given tables using naming conventions."""
    # TPC-H naming: table prefix maps to column prefix
    # lineitem -> l_, orders -> o_, customer -> c_, etc.
    _TABLE_PREFIX_MAP = {
        'lineitem': 'l_', 'orders': 'o_', 'customer': 'c_',
        'supplier': 's_', 'part': 'p_', 'partsupp': 'ps_',
        'nation': 'n_', 'region': 'r_',
    }
    for table in tables:
        prefix = _TABLE_PREFIX_MAP.get(table)
        if prefix and col_name.startswith(prefix):
            return True
    # Fallback: first letter match
    if '_' in col_name:
        col_prefix = col_name[0]
        for table in tables:
            if table[0] == col_prefix:
                return True
    return False


def _materialize_uncorrelated_subqueries(node: ASTNode, storage: Storage):
    """Walk the WHERE tree and pre-materialize uncorrelated subquery results.
    Also optimize correlated EXISTS/NOT EXISTS by pre-computing lookup sets."""
    if isinstance(node, InSubqueryExpr):
        if not _is_correlated(node.subquery):
            # Execute subquery once and cache results
            sub_results = execute_select(node.subquery, storage)
            values = set()
            has_null = False
            for sub_row in sub_results:
                v = next(iter(sub_row.values()))
                if v is None:
                    has_null = True
                else:
                    values.add(v)
            node._cached_values = values
            node._cached_has_null = has_null
    elif isinstance(node, ExistsExpr):
        # Try to optimize correlated EXISTS by pre-computing a lookup set
        _try_optimize_exists(node, storage)
    elif isinstance(node, BinaryOp):
        _materialize_uncorrelated_subqueries(node.left, storage)
        _materialize_uncorrelated_subqueries(node.right, storage)
    elif isinstance(node, UnaryOp):
        _materialize_uncorrelated_subqueries(node.operand, storage)


def _try_optimize_exists(node: ExistsExpr, storage: Storage):
    """Try to optimize a correlated EXISTS subquery.
    
    Pattern: EXISTS (SELECT * FROM table WHERE inner_col = outer_col AND filter_conditions)
    We pre-compute the set of inner_col values that satisfy filter_conditions,
    then at evaluation time just check set membership.
    """
    subquery = node.subquery
    if not subquery.where:
        return
    if subquery.group_by or subquery.having:
        return
    if len(subquery.from_tables) != 1:
        return
    
    table_ref = subquery.from_tables[0][0]
    if isinstance(table_ref, SelectStatement):
        return
    
    # Get the subquery's table
    try:
        table = storage.get_table(table_ref)
    except KeyError:
        return
    
    # Analyze WHERE: look for exactly one equi-join predicate with an outer column
    # and possibly additional filter conditions on inner columns
    equi_preds, filter_preds = _analyze_correlated_where(subquery.where, table_ref, subquery.from_tables)
    
    if len(equi_preds) != 1:
        return
    
    inner_col_name, outer_col_ref = equi_preds[0]
    
    # Pre-compute: for each row in inner table, check filter conditions
    # and collect inner_col values into a set
    qualifying_values = set()
    for row in table.rows:
        # Check filter predicates
        passes = True
        for fp in filter_preds:
            if not _eval_expr(fp, row, storage):
                passes = False
                break
        if passes:
            val = row.get(inner_col_name)
            if val is not None:
                qualifying_values.add(val)
    
    # Cache on the node
    node._optimized = True
    node._qualifying_values = qualifying_values
    node._outer_col_ref = outer_col_ref


def _analyze_correlated_where(node, table_name, from_tables):
    """Analyze a WHERE clause to find correlated equi-join and filter predicates.
    
    Returns (equi_preds, filter_preds) where:
    - equi_preds: list of (inner_col_name, outer_ColumnRef) pairs
    - filter_preds: list of ASTNode filter conditions on inner columns only
    """
    own_tables = set()
    for t, a in from_tables:
        if isinstance(t, str):
            own_tables.add(t)
        if a:
            own_tables.add(a)
    
    equi_preds = []
    filter_preds = []
    
    _collect_and_terms(node, equi_preds, filter_preds, own_tables)
    
    return equi_preds, filter_preds


def _collect_and_terms(node, equi_preds, filter_preds, own_tables):
    """Collect AND-ed terms into equi-join and filter categories."""
    if isinstance(node, BinaryOp) and node.op == "AND":
        _collect_and_terms(node.left, equi_preds, filter_preds, own_tables)
        _collect_and_terms(node.right, equi_preds, filter_preds, own_tables)
        return
    
    if isinstance(node, BinaryOp) and node.op == "=":
        left, right = node.left, node.right
        if isinstance(left, ColumnRef) and isinstance(right, ColumnRef):
            l_external = _has_external_refs(left, own_tables)
            r_external = _has_external_refs(right, own_tables)
            l_internal = not l_external
            r_internal = not r_external
            
            if l_internal and r_external:
                equi_preds.append((left.name, right))
                return
            elif r_internal and l_external:
                equi_preds.append((right.name, left))
                return
    
    # Check if this is a pure inner predicate (no external refs)
    if not _has_external_refs(node, own_tables):
        filter_preds.append(node)
    else:
        # Mixed or external predicate — can't optimize
        filter_preds.append(node)


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


def _eval_expr(node: ASTNode, row: Dict[str, Any], storage: Optional[Storage] = None) -> Any:
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
        return _eval_case(node, row, storage)

    if isinstance(node, SubqueryExpr):
        # Scalar subquery: execute and return first column of first row
        if storage is None:
            raise ValueError("Cannot evaluate subquery without storage")
        sub_results = execute_select(node.subquery, storage, outer_row=row)
        if not sub_results:
            return None
        first_row = sub_results[0]
        # Return first column value
        return next(iter(first_row.values()))

    if isinstance(node, InSubqueryExpr):
        # expr IN (SELECT ...) or expr NOT IN (SELECT ...)
        if storage is None:
            raise ValueError("Cannot evaluate IN subquery without storage")
        val = _eval_expr(node.expr, row, storage)
        
        # Use cached values if available (pre-materialized uncorrelated subquery)
        if hasattr(node, '_cached_values'):
            sub_values = node._cached_values
            has_null = node._cached_has_null
        else:
            sub_results = execute_select(node.subquery, storage, outer_row=row)
            # Collect all values from first column of subquery results
            sub_values = set()
            has_null = False
            for sub_row in sub_results:
                v = next(iter(sub_row.values()))
                if v is None:
                    has_null = True
                else:
                    sub_values.add(v)
        
        if node.negated:
            # NOT IN: if val is NULL, result is NULL/False
            if val is None:
                return False
            if val in sub_values:
                return False
            if has_null:
                return False
            return True
        else:
            # IN
            if val is None:
                return False
            return val in sub_values

    if isinstance(node, ExistsExpr):
        # EXISTS (SELECT ...) or NOT EXISTS (SELECT ...)
        if storage is None:
            raise ValueError("Cannot evaluate EXISTS without storage")
        
        # Use optimized lookup if available
        if hasattr(node, '_optimized') and node._optimized:
            outer_val = _eval_expr(node._outer_col_ref, row, storage)
            exists = outer_val in node._qualifying_values
        else:
            sub_results = execute_select(node.subquery, storage, outer_row=row)
            exists = len(sub_results) > 0
        
        if node.negated:
            return not exists
        return exists

    if isinstance(node, BinaryOp):
        # Short-circuit for AND/OR
        if node.op == "AND":
            left = _eval_expr(node.left, row, storage)
            if not left:
                return False
            return bool(_eval_expr(node.right, row, storage))
        if node.op == "OR":
            left = _eval_expr(node.left, row, storage)
            if left:
                return True
            return bool(_eval_expr(node.right, row, storage))

        # Handle IN specially (right side is a list of AST nodes)
        if node.op == "IN":
            left = _eval_expr(node.left, row, storage)
            values = [_eval_expr(v, row, storage) for v in node.right]
            return left in values

        left = _eval_expr(node.left, row, storage)
        right = _eval_expr(node.right, row, storage)

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
            if left is None:
                return False
            return _like_match(str(left), str(right))
        if node.op == "NOT LIKE":
            if left is None:
                return False
            return not _like_match(str(left), str(right))
        if node.op == "IS NULL":
            return left is None
        if node.op == "IS NOT NULL":
            return left is not None
        if node.op == "||":
            return str(left) + str(right)

        raise ValueError(f"Unknown binary op: {node.op}")

    if isinstance(node, UnaryOp):
        if node.op == "NOT":
            return not _eval_expr(node.operand, row, storage)
        if node.op == "-":
            return -_numeric(_eval_expr(node.operand, row, storage))
        raise ValueError(f"Unknown unary op: {node.op}")

    if isinstance(node, BetweenExpr):
        val = _eval_expr(node.expr, row, storage)
        low = _eval_expr(node.low, row, storage)
        high = _eval_expr(node.high, row, storage)
        return _compare(val, low) >= 0 and _compare(val, high) <= 0

    if isinstance(node, FunctionCall):
        # SUBSTRING function
        if node.name == "SUBSTRING":
            if len(node.args) >= 3:
                s = _eval_expr(node.args[0], row, storage)
                start = int(_eval_expr(node.args[1], row, storage))
                length = int(_eval_expr(node.args[2], row, storage))
                if s is None:
                    return None
                s = str(s)
                # SQL SUBSTRING is 1-indexed
                return s[start-1:start-1+length]
            elif len(node.args) == 2:
                s = _eval_expr(node.args[0], row, storage)
                start = int(_eval_expr(node.args[1], row, storage))
                if s is None:
                    return None
                s = str(s)
                return s[start-1:]
            else:
                return None

        # For aggregate functions used in expressions (e.g., in HAVING),
        # the result should already be in the row
        if node.name in ("SUM", "AVG", "COUNT", "MIN", "MAX"):
            name_lower = node.name.lower()
            if name_lower in row:
                return row[name_lower]
            # Try constructing the function name from the expression
            func_key = _expr_name(node)
            if func_key in row:
                return row[func_key]
            raise KeyError(f"Aggregate result {func_key!r} not found in row")

        # SUBSTR / SUBSTRING function (identifier-based)
        if node.name in ("SUBSTR", "SUBSTRING"):
            if len(node.args) >= 3:
                s = _eval_expr(node.args[0], row, storage)
                start = int(_eval_expr(node.args[1], row, storage))
                length = int(_eval_expr(node.args[2], row, storage))
                if s is None:
                    return None
                s = str(s)
                return s[start-1:start-1+length]
            elif len(node.args) == 2:
                s = _eval_expr(node.args[0], row, storage)
                start = int(_eval_expr(node.args[1], row, storage))
                if s is None:
                    return None
                s = str(s)
                return s[start-1:]
            return None

        # EXTRACT_YEAR, EXTRACT_MONTH, EXTRACT_DAY
        if node.name.startswith("EXTRACT_"):
            unit = node.name.split("_", 1)[1]
            val = _eval_expr(node.args[0], row, storage)
            if val is None:
                return None
            from datetime import datetime as _dt
            if isinstance(val, str):
                dt = _dt.strptime(val[:10], "%Y-%m-%d")
            else:
                dt = val
            if unit == "YEAR":
                return dt.year
            elif unit == "MONTH":
                return dt.month
            elif unit == "DAY":
                return dt.day
            return None

        # COALESCE function
        if node.name == "COALESCE":
            for arg in node.args:
                v = _eval_expr(arg, row, storage)
                if v is not None:
                    return v
            return None

        # UPPER function
        if node.name == "UPPER":
            v = _eval_expr(node.args[0], row, storage)
            return str(v).upper() if v is not None else None

        # LOWER function
        if node.name == "LOWER":
            v = _eval_expr(node.args[0], row, storage)
            return str(v).lower() if v is not None else None

        # TRIM function
        if node.name == "TRIM":
            v = _eval_expr(node.args[0], row, storage)
            return str(v).strip() if v is not None else None

        # CAST function (simplified)
        if node.name == "CAST":
            v = _eval_expr(node.args[0], row, storage)
            return v  # simplified: just return the value

        # Other functions: try looking up in row
        name_lower = node.name.lower()
        if name_lower in row:
            return row[name_lower]
        func_key = _expr_name(node)
        if func_key in row:
            return row[func_key]
        raise KeyError(f"Function result {func_key!r} not found in row")

    if isinstance(node, StarExpr):
        return None

    raise TypeError(f"Cannot evaluate node type: {type(node).__name__}")


def _eval_case(node: CaseExpr, row: Dict[str, Any], storage: Optional[Storage] = None) -> Any:
    """Evaluate a CASE WHEN expression."""
    for condition, result in node.when_clauses:
        if _eval_expr(condition, row, storage):
            return _eval_expr(result, row, storage)
    if node.else_expr is not None:
        return _eval_expr(node.else_expr, row, storage)
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
    if isinstance(node, SubqueryExpr):
        return "subquery"
    if isinstance(node, InSubqueryExpr):
        return "in_subquery"
    if isinstance(node, ExistsExpr):
        return "exists_subquery"
    return "?"


def _add_having_aggregates(node: ASTNode, result_row: dict, group_rows: List[dict], storage: Optional[Storage] = None):
    """Pre-compute aggregate expressions from HAVING clause and add them to the result row."""
    if isinstance(node, FunctionCall) and node.name in ("SUM", "AVG", "COUNT", "MIN", "MAX"):
        func_key = _expr_name(node)
        if func_key not in result_row:
            result_row[func_key] = _eval_aggregate_expr(node, group_rows, storage)
    elif isinstance(node, BinaryOp):
        _add_having_aggregates(node.left, result_row, group_rows, storage)
        _add_having_aggregates(node.right, result_row, group_rows, storage)
    elif isinstance(node, UnaryOp):
        _add_having_aggregates(node.operand, result_row, group_rows, storage)


def _compute_aggregates(columns: list, rows: List[dict],
                         sample_row: Optional[dict] = None, storage: Optional[Storage] = None) -> dict:
    """Compute aggregate expressions over a group of rows."""
    result = {}
    for expr, alias in columns:
        col_name = alias or _expr_name(expr)
        if _contains_aggregate(expr):
            result[col_name] = _eval_aggregate_expr(expr, rows, storage)
        else:
            # Non-aggregate expression: take from first row
            if rows:
                result[col_name] = _eval_expr(expr, rows[0], storage)
            else:
                result[col_name] = None
    return result


def _eval_aggregate_expr(node: ASTNode, rows: List[dict], storage: Optional[Storage] = None) -> Any:
    """Evaluate an expression that contains aggregates over a set of rows."""

    if isinstance(node, FunctionCall) and node.name in ("SUM", "AVG", "COUNT", "MIN", "MAX"):
        if node.name == "COUNT":
            if node.args and isinstance(node.args[0], StarExpr):
                return len(rows)
            else:
                vals = [_eval_expr(node.args[0], row, storage) for row in rows]
                vals = [v for v in vals if v is not None]
                if node.distinct:
                    vals = list(set(vals))
                return len(vals)

        # For other aggregates, evaluate the argument for each row
        vals = [_eval_expr(node.args[0], row, storage) for row in rows]
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
        left = _eval_aggregate_expr(node.left, rows, storage) if _contains_aggregate(node.left) else _eval_expr(node.left, rows[0] if rows else {}, storage)
        right = _eval_aggregate_expr(node.right, rows, storage) if _contains_aggregate(node.right) else _eval_expr(node.right, rows[0] if rows else {}, storage)

        # Propagate NULL: if either operand is None (from empty aggregate), result is None
        if left is None or right is None:
            return None

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
        return _eval_expr(node, rows[0], storage)
    return None


def _group_rows(group_by_exprs: list, rows: List[dict], storage: Optional[Storage] = None) -> dict:
    """Group rows by the group-by expressions. Returns {key_tuple: [rows]}."""
    groups = {}
    for row in rows:
        key = tuple(_eval_expr(expr, row, storage) for expr in group_by_exprs)
        if key not in groups:
            groups[key] = []
        groups[key].append(row)
    return groups


def _sort_rows(rows: List[dict], order_by: list, storage: Optional[Storage] = None) -> List[dict]:
    """Sort rows by ORDER BY specification."""
    import functools

    def compare(a, b):
        for expr, direction in order_by:
            va = _eval_expr(expr, a, storage)
            vb = _eval_expr(expr, b, storage)
            cmp = _compare(va, vb)
            if direction == "DESC":
                cmp = -cmp
            if cmp != 0:
                return cmp
        return 0

    return sorted(rows, key=functools.cmp_to_key(compare))
