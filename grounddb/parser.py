"""
Tokenizer and recursive-descent SQL parser for GroundDB.

Supports a SQL subset sufficient for TPC-H queries:
- SELECT with expressions, aggregates, aliases
- FROM single table (extendable to JOINs)
- WHERE with AND/OR, comparisons, BETWEEN, IN, date literals
- GROUP BY, HAVING, ORDER BY, LIMIT
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
        self.from_table = None  # table name string
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


# ── Parser ───────────────────────────────────────────────────────────────────

class Parser:
    """Recursive-descent SQL parser."""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.pos = 0

    def peek(self) -> Token:
        return self.tokens[self.pos]

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
        stmt.from_table = self.parse_table_name()

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
            operand = self.parse_not_expr()
            return UnaryOp("NOT", operand)
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

        # NOT BETWEEN
        if self.check_keyword("NOT"):
            saved = self.pos
            self.advance()
            if self.check_keyword("BETWEEN"):
                self.advance()
                low = self.parse_addition()
                self.expect(TokenType.KEYWORD, "AND")
                high = self.parse_addition()
                return UnaryOp("NOT", BetweenExpr(left, low, high))
            self.pos = saved

        # IN
        if self.check_keyword("IN"):
            self.advance()
            self.expect(type=TokenType.LPAREN)
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

        # DATE literal: DATE 'YYYY-MM-DD'
        if tok.type == TokenType.KEYWORD and tok.value == "DATE":
            self.advance()
            date_str = self.expect(TokenType.STRING).value
            return DateLiteral(date_str)

        # Aggregate / function call
        if tok.type == TokenType.KEYWORD and tok.value in ("SUM", "AVG", "COUNT", "MIN", "MAX",
                                                            "SUBSTRING", "UPPER", "LOWER", "TRIM", "CAST"):
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

        # Identifier (possibly table.column)
        if tok.type == TokenType.IDENTIFIER:
            name = self.advance().value
            if self.peek().type == TokenType.DOT:
                self.advance()
                col = self.advance().value
                return ColumnRef(col, table=name)
            return ColumnRef(name)

        raise SyntaxError(f"Unexpected token: {tok} at pos {self.pos}")

    def parse_case(self) -> ASTNode:
        """Parse CASE WHEN ... THEN ... ELSE ... END."""
        self.expect(TokenType.KEYWORD, "CASE")
        # For now, simple CASE WHEN support
        # We'll represent it as a special node but it's not needed for Q6
        raise NotImplementedError("CASE WHEN not yet implemented")


def parse_sql(sql: str) -> SelectStatement:
    """Parse a SQL string into an AST."""
    tokens = tokenize(sql)
    parser = Parser(tokens)
    return parser.parse()
