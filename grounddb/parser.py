"""
Tokenizer and recursive-descent SQL parser for GroundDB.

Supports a SQL subset sufficient for TPC-H queries:
- SELECT with expressions, aggregates, aliases
- FROM single or multiple tables (comma-join, INNER JOIN, LEFT OUTER JOIN)
- WHERE with AND/OR, comparisons, BETWEEN, IN, date literals
- GROUP BY, HAVING, ORDER BY, LIMIT
- CASE WHEN ... THEN ... ELSE ... END
- INTERVAL literals and date arithmetic
- Subqueries: EXISTS, IN (SELECT ...), scalar subqueries, derived tables
"""

import re
from typing import List, Optional, Any, Dict, Tuple


# ── Token types ──────────────────────────────────────────────────────────────

class TokenType:
    KEYWORD = "KEYWORD"
    IDENTIFIER = "IDENTIFIER"
    NUMBER = "NUMBER"
    STRING = "STRING"
    OPERATOR = "OPERATOR"
    LPAREN = "LPAREN"
    RPAREN = "RPAREN"
    COMMA = "COMMA"
    STAR = "STAR"
    DOT = "DOT"
    EOF = "EOF"


class Token:
    __slots__ = ("type", "value")

    def __init__(self, type: str, value: str):
        self.type = type
        self.value = value

    def __repr__(self):
        return f"Token({self.type}, {self.value!r})"


# ── Keywords ─────────────────────────────────────────────────────────────────

KEYWORDS = {
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "AS",
    "GROUP", "BY", "HAVING", "ORDER", "ASC", "DESC", "LIMIT",
    "BETWEEN", "IN", "LIKE", "IS", "NULL", "EXISTS",
    "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "ON", "CROSS",
    "CASE", "WHEN", "THEN", "ELSE", "END",
    "SUM", "AVG", "COUNT", "MIN", "MAX",
    "DATE", "INTERVAL",
    "DISTINCT",
    "TRUE", "FALSE",
    "CAST",
    "SUBSTRING", "UPPER", "LOWER", "TRIM",
    "DAY", "MONTH", "YEAR",
    "FOR",
}


# ── Tokenizer ────────────────────────────────────────────────────────────────

def tokenize(sql: str) -> List[Token]:
    """Tokenize a SQL string into a list of Tokens."""
    tokens = []
    i = 0
    n = len(sql)

    while i < n:
        # Skip whitespace
        if sql[i].isspace():
            i += 1
            continue

        # Skip single-line comments
        if sql[i:i+2] == "--":
            while i < n and sql[i] != "\n":
                i += 1
            continue

        # Multi-character operators
        two_char = sql[i:i+2] if i + 1 < n else ""
        if two_char in (">=", "<=", "<>", "!=", "||"):
            tokens.append(Token(TokenType.OPERATOR, two_char))
            i += 2
            continue

        # Single-character operators
        if sql[i] in (">", "<", "=", "+", "-", "/"):
            tokens.append(Token(TokenType.OPERATOR, sql[i]))
            i += 1
            continue

        if sql[i] == "*":
            tokens.append(Token(TokenType.STAR, "*"))
            i += 1
            continue

        if sql[i] == "(":
            tokens.append(Token(TokenType.LPAREN, "("))
            i += 1
            continue

        if sql[i] == ")":
            tokens.append(Token(TokenType.RPAREN, ")"))
            i += 1
            continue

        if sql[i] == ",":
            tokens.append(Token(TokenType.COMMA, ","))
            i += 1
            continue

        if sql[i] == ".":
            tokens.append(Token(TokenType.DOT, "."))
            i += 1
            continue

        # String literal
        if sql[i] == "'":
            j = i + 1
            while j < n and sql[j] != "'":
                j += 1
            value = sql[i+1:j]
            tokens.append(Token(TokenType.STRING, value))
            i = j + 1
            continue

        # Number literal (integer or decimal)
        if sql[i].isdigit() or (sql[i] == "." and i + 1 < n and sql[i+1].isdigit()):
            j = i
            while j < n and (sql[j].isdigit() or sql[j] == "."):
                j += 1
            tokens.append(Token(TokenType.NUMBER, sql[i:j]))
            i = j
            continue

        # Identifier or keyword
        if sql[i].isalpha() or sql[i] == "_":
            j = i
            while j < n and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            word = sql[i:j]
            if word.upper() in KEYWORDS:
                tokens.append(Token(TokenType.KEYWORD, word.upper()))
            else:
                tokens.append(Token(TokenType.IDENTIFIER, word.lower()))
            i = j
            continue

        # Unknown character — skip
        i += 1

    tokens.append(Token(TokenType.EOF, ""))
    return tokens


# ── AST node types ───────────────────────────────────────────────────────────

class ASTNode:
    """Base class for AST nodes."""
    pass


class SelectStatement(ASTNode):
    def __init__(self):
        self.columns = []       # list of (expr, alias)
        self.from_table = None  # table name string (first table, for backward compat)
        self.from_tables = []   # list of (table_name_or_subquery, alias) pairs
        self.joins = []         # list of {'type': 'INNER'|'LEFT OUTER', 'table': str, 'alias': str|None, 'on': ASTNode}
        self.where = None       # expression node
        self.group_by = []      # list of expression nodes
        self.having = None      # expression node
        self.order_by = []      # list of (expr, direction)
        self.limit = None       # integer or None
        self.distinct = False


class ColumnRef(ASTNode):
    """Reference to a column: possibly table.column or just column."""
    def __init__(self, name: str, table: str = None):
        self.name = name
        self.table = table

    def __repr__(self):
        if self.table:
            return f"ColumnRef({self.table}.{self.name})"
        return f"ColumnRef({self.name})"


class NumberLiteral(ASTNode):
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return f"NumberLiteral({self.value})"


class StringLiteral(ASTNode):
    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return f"StringLiteral({self.value!r})"


class DateLiteral(ASTNode):
    """DATE 'YYYY-MM-DD' literal — stored as string for lexicographic comparison."""
    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return f"DateLiteral({self.value!r})"


class IntervalLiteral(ASTNode):
    """INTERVAL 'N' DAY|MONTH|YEAR literal."""
    def __init__(self, n: int, unit: str):
        self.n = n
        self.unit = unit.upper()

    def __repr__(self):
        return f"IntervalLiteral({self.n}, {self.unit!r})"


class CaseExpr(ASTNode):
    """CASE WHEN cond THEN val ... ELSE val END."""
    def __init__(self, when_clauses: list, else_expr: ASTNode = None):
        self.when_clauses = when_clauses  # list of (condition, result) pairs
        self.else_expr = else_expr

    def __repr__(self):
        return f"CaseExpr(whens={len(self.when_clauses)}, has_else={self.else_expr is not None})"


class BinaryOp(ASTNode):
    def __init__(self, op: str, left: ASTNode, right: ASTNode):
        self.op = op
        self.left = left
        self.right = right

    def __repr__(self):
        return f"BinaryOp({self.op}, {self.left}, {self.right})"


class UnaryOp(ASTNode):
    def __init__(self, op: str, operand: ASTNode):
        self.op = op
        self.operand = operand


class BetweenExpr(ASTNode):
    def __init__(self, expr: ASTNode, low: ASTNode, high: ASTNode):
        self.expr = expr
        self.low = low
        self.high = high


class FunctionCall(ASTNode):
    def __init__(self, name: str, args: list, distinct: bool = False):
        self.name = name.upper()
        self.args = args
        self.distinct = distinct


class StarExpr(ASTNode):
    """Represents SELECT * or COUNT(*)."""
    pass


class SubqueryExpr(ASTNode):
    """A scalar subquery: (SELECT ...) used as a value."""
    def __init__(self, subquery: SelectStatement):
        self.subquery = subquery

    def __repr__(self):
        return f"SubqueryExpr(...)"


class InSubqueryExpr(ASTNode):
    """expr IN (SELECT ...) or expr NOT IN (SELECT ...)."""
    def __init__(self, expr: ASTNode, subquery: SelectStatement, negated: bool = False):
        self.expr = expr
        self.subquery = subquery
        self.negated = negated

    def __repr__(self):
        neg = "NOT " if self.negated else ""
        return f"InSubqueryExpr({neg}IN)"


class ExistsExpr(ASTNode):
    """EXISTS (SELECT ...) or NOT EXISTS (SELECT ...)."""
    def __init__(self, subquery: SelectStatement, negated: bool = False):
        self.subquery = subquery
        self.negated = negated

    def __repr__(self):
        neg = "NOT " if self.negated else ""
        return f"ExistsExpr({neg}EXISTS)"


class DerivedTable(ASTNode):
    """A subquery used as a table in FROM clause: (SELECT ...) AS alias."""
    def __init__(self, subquery: SelectStatement, alias: str):
        self.subquery = subquery
        self.alias = alias


# ── Parser ───────────────────────────────────────────────────────────────────

class Parser:
    """Recursive-descent SQL parser."""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.pos = 0

    def peek(self) -> Token:
        return self.tokens[self.pos]

    def peek_ahead(self, offset: int = 1) -> Token:
        idx = self.pos + offset
        if idx < len(self.tokens):
            return self.tokens[idx]
        return self.tokens[-1]  # EOF

    def advance(self) -> Token:
        tok = self.tokens[self.pos]
        self.pos += 1
        return tok

    def expect(self, type: str = None, value: str = None) -> Token:
        tok = self.peek()
        if type and tok.type != type:
            raise SyntaxError(f"Expected token type {type} but got {tok.type} ({tok.value!r}) at pos {self.pos}")
        if value and tok.value.upper() != value.upper():
            raise SyntaxError(f"Expected {value!r} but got {tok.value!r} at pos {self.pos}")
        return self.advance()

    def match_keyword(self, keyword: str) -> bool:
        tok = self.peek()
        if tok.type == TokenType.KEYWORD and tok.value.upper() == keyword.upper():
            self.advance()
            return True
        return False

    def check_keyword(self, keyword: str) -> bool:
        tok = self.peek()
        return tok.type == TokenType.KEYWORD and tok.value.upper() == keyword.upper()

    def _is_subquery_start(self) -> bool:
        """Check if current position is '(' followed by SELECT."""
        if self.peek().type == TokenType.LPAREN:
            next_tok = self.peek_ahead(1)
            if next_tok.type == TokenType.KEYWORD and next_tok.value == "SELECT":
                return True
        return False

    def parse(self) -> SelectStatement:
        """Parse a SELECT statement."""
        return self.parse_select()

    def parse_select(self) -> SelectStatement:
        stmt = SelectStatement()
        self.expect(TokenType.KEYWORD, "SELECT")

        # DISTINCT
        if self.match_keyword("DISTINCT"):
            stmt.distinct = True

        # Columns
        stmt.columns = self.parse_select_list()

        # FROM
        self.expect(TokenType.KEYWORD, "FROM")
        self._parse_from_clause(stmt)

        # WHERE
        if self.check_keyword("WHERE"):
            self.advance()
            stmt.where = self.parse_expression()

        # GROUP BY
        if self.check_keyword("GROUP"):
            self.advance()
            self.expect(TokenType.KEYWORD, "BY")
            stmt.group_by = self.parse_expression_list()

        # HAVING
        if self.check_keyword("HAVING"):
            self.advance()
            stmt.having = self.parse_expression()

        # ORDER BY
        if self.check_keyword("ORDER"):
            self.advance()
            self.expect(TokenType.KEYWORD, "BY")
            stmt.order_by = self.parse_order_list()

        # LIMIT
        if self.check_keyword("LIMIT"):
            self.advance()
            tok = self.expect(TokenType.NUMBER)
            stmt.limit = int(tok.value)

        return stmt

    def _parse_from_clause(self, stmt: SelectStatement):
        """Parse FROM clause with support for multiple tables, JOINs, and derived tables."""
        # Parse first table + optional alias
        table_ref, alias = self._parse_table_ref()
        stmt.from_tables = [(table_ref, alias)]
        if isinstance(table_ref, str):
            stmt.from_table = table_ref  # backward compat
        else:
            stmt.from_table = alias  # derived table: use alias as table name

        # Check for comma-separated tables or JOINs
        while True:
            tok = self.peek()

            # Comma-separated tables
            if tok.type == TokenType.COMMA:
                self.advance()
                tref, talias = self._parse_table_ref()
                stmt.from_tables.append((tref, talias))
                continue

            # INNER JOIN / JOIN
            if self.check_keyword("JOIN") or self.check_keyword("INNER"):
                join_type = "INNER"
                if self.check_keyword("INNER"):
                    self.advance()
                self.expect(TokenType.KEYWORD, "JOIN")
                tref, talias = self._parse_table_ref()
                self.expect(TokenType.KEYWORD, "ON")
                on_expr = self.parse_expression()
                stmt.joins.append({
                    'type': join_type,
                    'table': tref,
                    'alias': talias,
                    'on': on_expr,
                })
                continue

            # LEFT [OUTER] JOIN
            if self.check_keyword("LEFT"):
                self.advance()
                if self.check_keyword("OUTER"):
                    self.advance()
                self.expect(TokenType.KEYWORD, "JOIN")
                tref, talias = self._parse_table_ref()
                self.expect(TokenType.KEYWORD, "ON")
                on_expr = self.parse_expression()
                stmt.joins.append({
                    'type': 'LEFT OUTER',
                    'table': tref,
                    'alias': talias,
                    'on': on_expr,
                })
                continue

            # CROSS JOIN
            if self.check_keyword("CROSS"):
                self.advance()
                self.expect(TokenType.KEYWORD, "JOIN")
                tref, talias = self._parse_table_ref()
                stmt.from_tables.append((tref, talias))
                continue

            break

    def _parse_table_ref(self):
        """Parse a table reference: table_name [alias], table_name AS alias,
        or (SELECT ...) AS alias (derived table).
        Returns (table_name_or_SelectStatement, alias)."""
        # Check for derived table: (SELECT ...)
        if self._is_subquery_start():
            self.advance()  # consume LPAREN
            subquery = self.parse_select()
            self.expect(type=TokenType.RPAREN)
            # Must have an alias
            alias = None
            if self.match_keyword("AS"):
                alias = self.advance().value.lower()
            elif self.peek().type == TokenType.IDENTIFIER:
                alias = self.advance().value.lower()
            if alias is None:
                raise SyntaxError("Derived table (subquery in FROM) requires an alias")
            return (subquery, alias)

        tok = self.advance()
        table_name = tok.value.lower()
        alias = None

        # Check for alias
        if self.match_keyword("AS"):
            alias = self.advance().value.lower()
        elif self.peek().type == TokenType.IDENTIFIER:
            # Check it's not a keyword that starts next clause
            next_val = self.peek().value.upper()
            if next_val not in ("WHERE", "GROUP", "ORDER", "LIMIT", "HAVING",
                                "JOIN", "INNER", "LEFT", "RIGHT", "OUTER",
                                "CROSS", "ON"):
                alias = self.advance().value.lower()

        return (table_name, alias)

    def parse_select_list(self) -> list:
        """Parse comma-separated select expressions with optional aliases."""
        items = []
        items.append(self.parse_select_item())
        while self.peek().type == TokenType.COMMA:
            self.advance()
            items.append(self.parse_select_item())
        return items

    def parse_select_item(self) -> tuple:
        """Parse one select expression with optional alias. Returns (expr, alias)."""
        expr = self.parse_expression()
        alias = None
        if self.match_keyword("AS"):
            alias = self.advance().value
        elif self.peek().type == TokenType.IDENTIFIER and not self.check_keyword("FROM"):
            alias = self.advance().value
        return (expr, alias)

    def parse_table_name(self) -> str:
        tok = self.advance()
        return tok.value.lower()

    def parse_expression_list(self) -> list:
        exprs = [self.parse_expression()]
        while self.peek().type == TokenType.COMMA:
            self.advance()
            exprs.append(self.parse_expression())
        return exprs

    def parse_order_list(self) -> list:
        items = []
        expr = self.parse_expression()
        direction = "ASC"
        if self.match_keyword("ASC"):
            direction = "ASC"
        elif self.match_keyword("DESC"):
            direction = "DESC"
        items.append((expr, direction))
        while self.peek().type == TokenType.COMMA:
            self.advance()
            expr = self.parse_expression()
            direction = "ASC"
            if self.match_keyword("ASC"):
                direction = "ASC"
            elif self.match_keyword("DESC"):
                direction = "DESC"
            items.append((expr, direction))
        return items

    # ── Expression parsing (precedence climbing) ─────────────────────────

    def parse_expression(self) -> ASTNode:
        return self.parse_or_expr()

    def parse_or_expr(self) -> ASTNode:
        left = self.parse_and_expr()
        while self.check_keyword("OR"):
            self.advance()
            right = self.parse_and_expr()
            left = BinaryOp("OR", left, right)
        return left

    def parse_and_expr(self) -> ASTNode:
        left = self.parse_not_expr()
        while self.check_keyword("AND"):
            self.advance()
            right = self.parse_not_expr()
            left = BinaryOp("AND", left, right)
        return left

    def parse_not_expr(self) -> ASTNode:
        if self.check_keyword("NOT"):
            self.advance()
            # Check for NOT EXISTS
            if self.check_keyword("EXISTS"):
                self.advance()
                self.expect(type=TokenType.LPAREN)
                subquery = self.parse_select()
                self.expect(type=TokenType.RPAREN)
                return ExistsExpr(subquery, negated=True)
            # Check for NOT IN at this level (for standalone NOT IN after parse_comparison)
            operand = self.parse_not_expr()
            return UnaryOp("NOT", operand)
        # Check for EXISTS
        if self.check_keyword("EXISTS"):
            self.advance()
            self.expect(type=TokenType.LPAREN)
            subquery = self.parse_select()
            self.expect(type=TokenType.RPAREN)
            return ExistsExpr(subquery, negated=False)
        return self.parse_comparison()

    def parse_comparison(self) -> ASTNode:
        left = self.parse_addition()

        # BETWEEN
        if self.check_keyword("BETWEEN"):
            self.advance()
            low = self.parse_addition()
            self.expect(TokenType.KEYWORD, "AND")
            high = self.parse_addition()
            return BetweenExpr(left, low, high)

        # NOT BETWEEN / NOT IN / NOT LIKE
        if self.check_keyword("NOT"):
            saved = self.pos
            self.advance()
            if self.check_keyword("BETWEEN"):
                self.advance()
                low = self.parse_addition()
                self.expect(TokenType.KEYWORD, "AND")
                high = self.parse_addition()
                return UnaryOp("NOT", BetweenExpr(left, low, high))
            if self.check_keyword("IN"):
                self.advance()
                self.expect(type=TokenType.LPAREN)
                # Check if this is a subquery: (SELECT ...)
                if self.check_keyword("SELECT"):
                    subquery = self.parse_select()
                    self.expect(type=TokenType.RPAREN)
                    return InSubqueryExpr(left, subquery, negated=True)
                else:
                    values = self.parse_expression_list()
                    self.expect(type=TokenType.RPAREN)
                    return UnaryOp("NOT", BinaryOp("IN", left, values))
            if self.check_keyword("LIKE"):
                self.advance()
                right = self.parse_addition()
                return BinaryOp("NOT LIKE", left, right)
            self.pos = saved

        # IN
        if self.check_keyword("IN"):
            self.advance()
            self.expect(type=TokenType.LPAREN)
            # Check if this is a subquery: (SELECT ...)
            if self.check_keyword("SELECT"):
                subquery = self.parse_select()
                self.expect(type=TokenType.RPAREN)
                return InSubqueryExpr(left, subquery, negated=False)
            else:
                values = self.parse_expression_list()
                self.expect(type=TokenType.RPAREN)
                return BinaryOp("IN", left, values)

        # IS NULL / IS NOT NULL
        if self.check_keyword("IS"):
            self.advance()
            if self.match_keyword("NOT"):
                self.expect(TokenType.KEYWORD, "NULL")
                return BinaryOp("IS NOT NULL", left, None)
            self.expect(TokenType.KEYWORD, "NULL")
            return BinaryOp("IS NULL", left, None)

        # Comparison operators
        tok = self.peek()
        if tok.type == TokenType.OPERATOR and tok.value in ("=", "<>", "!=", "<", ">", "<=", ">="):
            op = self.advance().value
            right = self.parse_addition()
            return BinaryOp(op, left, right)

        # LIKE
        if self.check_keyword("LIKE"):
            self.advance()
            right = self.parse_addition()
            return BinaryOp("LIKE", left, right)

        return left

    def parse_addition(self) -> ASTNode:
        left = self.parse_multiplication()
        while self.peek().type == TokenType.OPERATOR and self.peek().value in ("+", "-"):
            op = self.advance().value
            right = self.parse_multiplication()
            left = BinaryOp(op, left, right)
        return left

    def parse_multiplication(self) -> ASTNode:
        left = self.parse_unary()
        while (self.peek().type == TokenType.OPERATOR and self.peek().value in ("/",)) or \
              (self.peek().type == TokenType.STAR):
            if self.peek().type == TokenType.STAR:
                self.advance()
                op = "*"
            else:
                op = self.advance().value
            right = self.parse_unary()
            left = BinaryOp(op, left, right)
        return left

    def parse_unary(self) -> ASTNode:
        if self.peek().type == TokenType.OPERATOR and self.peek().value == "-":
            self.advance()
            operand = self.parse_primary()
            return UnaryOp("-", operand)
        return self.parse_primary()

    def parse_primary(self) -> ASTNode:
        tok = self.peek()

        # INTERVAL literal: INTERVAL 'N' DAY|MONTH|YEAR
        if tok.type == TokenType.KEYWORD and tok.value == "INTERVAL":
            self.advance()
            n_str = self.expect(TokenType.STRING).value
            n = int(n_str)
            # Expect DAY, MONTH, or YEAR
            unit_tok = self.peek()
            if unit_tok.type == TokenType.KEYWORD and unit_tok.value in ("DAY", "MONTH", "YEAR"):
                self.advance()
                return IntervalLiteral(n, unit_tok.value)
            else:
                # Default to DAY if no unit specified
                return IntervalLiteral(n, "DAY")

        # DATE literal: DATE 'YYYY-MM-DD'
        if tok.type == TokenType.KEYWORD and tok.value == "DATE":
            self.advance()
            date_str = self.expect(TokenType.STRING).value
            return DateLiteral(date_str)

        # EXISTS (SELECT ...)
        if tok.type == TokenType.KEYWORD and tok.value == "EXISTS":
            self.advance()
            self.expect(type=TokenType.LPAREN)
            subquery = self.parse_select()
            self.expect(type=TokenType.RPAREN)
            return ExistsExpr(subquery, negated=False)

        # SUBSTRING function with FROM/FOR syntax
        if tok.type == TokenType.KEYWORD and tok.value == "SUBSTRING":
            func_name = self.advance().value
            self.expect(type=TokenType.LPAREN)
            # Parse first argument (the string expression)
            arg1 = self.parse_expression()
            args = [arg1]
            # Check for FROM keyword (SUBSTRING(expr FROM n FOR m))
            if self.check_keyword("FROM"):
                self.advance()
                arg2 = self.parse_expression()
                args.append(arg2)
                if self.check_keyword("FOR"):
                    self.advance()
                    arg3 = self.parse_expression()
                    args.append(arg3)
            else:
                # Comma-separated args
                while self.peek().type == TokenType.COMMA:
                    self.advance()
                    args.append(self.parse_expression())
            self.expect(type=TokenType.RPAREN)
            return FunctionCall(func_name, args)

        # Aggregate / function call
        if tok.type == TokenType.KEYWORD and tok.value in ("SUM", "AVG", "COUNT", "MIN", "MAX",
                                                            "UPPER", "LOWER", "TRIM", "CAST"):
            func_name = self.advance().value
            self.expect(type=TokenType.LPAREN)

            distinct = False
            if self.match_keyword("DISTINCT"):
                distinct = True

            if self.peek().type == TokenType.STAR:
                self.advance()
                args = [StarExpr()]
            else:
                args = self.parse_expression_list()
            self.expect(type=TokenType.RPAREN)
            return FunctionCall(func_name, args, distinct)

        # Subquery in parens: (SELECT ...)
        if self._is_subquery_start():
            self.advance()  # consume LPAREN
            subquery = self.parse_select()
            self.expect(type=TokenType.RPAREN)
            return SubqueryExpr(subquery)

        # Parenthesized expression
        if tok.type == TokenType.LPAREN:
            self.advance()
            expr = self.parse_expression()
            self.expect(type=TokenType.RPAREN)
            return expr

        # Number literal
        if tok.type == TokenType.NUMBER:
            self.advance()
            val = tok.value
            if "." in val:
                return NumberLiteral(float(val))
            else:
                return NumberLiteral(int(val))

        # String literal
        if tok.type == TokenType.STRING:
            self.advance()
            return StringLiteral(tok.value)

        # Star
        if tok.type == TokenType.STAR:
            self.advance()
            return StarExpr()

        # CASE WHEN
        if tok.type == TokenType.KEYWORD and tok.value == "CASE":
            return self.parse_case()

        # Identifier: could be function call like substr(...), coalesce(...), extract(...)
        # or table.column reference
        if tok.type == TokenType.IDENTIFIER:
            name = self.advance().value
            # Check for function call: identifier followed by LPAREN
            if self.peek().type == TokenType.LPAREN:
                self.expect(type=TokenType.LPAREN)
                # Special handling for EXTRACT(YEAR/MONTH/DAY FROM expr)
                if name.lower() == "extract":
                    unit_tok = self.peek()
                    if unit_tok.type == TokenType.KEYWORD and unit_tok.value in ("YEAR", "MONTH", "DAY"):
                        unit = self.advance().value
                        self.expect(TokenType.KEYWORD, "FROM")
                        arg = self.parse_expression()
                        self.expect(type=TokenType.RPAREN)
                        return FunctionCall("EXTRACT_" + unit, [arg])
                args = []
                if self.peek().type != TokenType.RPAREN:
                    args = self.parse_expression_list()
                self.expect(type=TokenType.RPAREN)
                return FunctionCall(name, args)
            if self.peek().type == TokenType.DOT:
                self.advance()
                col = self.advance().value
                return ColumnRef(col, table=name)
            return ColumnRef(name)

        raise SyntaxError(f"Unexpected token: {tok} at pos {self.pos}")

    def parse_case(self) -> ASTNode:
        """Parse CASE WHEN ... THEN ... ELSE ... END."""
        self.expect(TokenType.KEYWORD, "CASE")
        when_clauses = []
        else_expr = None

        while self.check_keyword("WHEN"):
            self.advance()
            condition = self.parse_expression()
            self.expect(TokenType.KEYWORD, "THEN")
            result = self.parse_expression()
            when_clauses.append((condition, result))

        if self.check_keyword("ELSE"):
            self.advance()
            else_expr = self.parse_expression()

        self.expect(TokenType.KEYWORD, "END")
        return CaseExpr(when_clauses, else_expr)


def parse_sql(sql: str) -> SelectStatement:
    """Parse a SQL string into an AST."""
    tokens = tokenize(sql)
    parser = Parser(tokens)
    return parser.parse()
