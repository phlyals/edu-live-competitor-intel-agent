#!/usr/bin/env python3
"""Small database compatibility layer used by Runtime V3.

The first V3 implementation was SQLite-specific.  Runtime V3 Final uses
PostgreSQL as its source of truth, but the public surface intentionally stays
close to sqlite3 so the worker state-machine code remains deterministic and
auditable during the migration.  This module is deliberately narrow: it does
not emulate database semantics, it only normalises placeholders, row access,
and schema-script execution.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable

import psycopg
from psycopg import sql


class Row(dict):
    """A dict row which also supports sqlite-style numeric indexing."""

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)

    def __iter__(self):
        # ``dict(row)`` is used throughout V3; normal dict iteration preserves
        # that behaviour while numeric access is handled by __getitem__.
        return super().__iter__()


def row_factory(cursor: Any) -> Row:
    columns = [str(desc.name) for desc in cursor.description or ()]
    values = cursor.fetchone()
    if values is None:
        return Row()
    return Row(zip(columns, values))


def _replace_qmarks(query: str) -> str:
    """Replace sqlite '?' placeholders without touching quoted text."""
    result: list[str] = []
    quote: str | None = None
    i = 0
    while i < len(query):
        char = query[i]
        if quote:
            result.append(char)
            if char == quote:
                if i + 1 < len(query) and query[i + 1] == quote:
                    result.append(query[i + 1])
                    i += 1
                else:
                    quote = None
            i += 1
            continue
        if char in ("'", '"'):
            quote = char
            result.append(char)
        elif char == "?":
            result.append("__V3_PARAM__")
        else:
            result.append(char)
        i += 1
    return "".join(result)


def _escape_literal_percent(query: str) -> str:
    # psycopg interprets every percent in a query as parameter syntax.  SQL
    # LIKE patterns and legacy identifiers contain literal percent signs.
    out: list[str] = []
    i = 0
    while i < len(query):
        if query[i] == "%":
            out.append("%%")
        else:
            out.append(query[i])
        i += 1
    return "".join(out)


def translate_sql(query: str) -> str:
    query = query.strip()
    if not query:
        return query
    if query.upper() == "BEGIN IMMEDIATE":
        return "BEGIN"
    if query.upper().startswith("PRAGMA "):
        return "SELECT 1 AS ignored_pragma"
    query = re.sub(r"^INSERT\s+OR\s+IGNORE\s+INTO\s+", "INSERT INTO ", query, flags=re.I)
    if query.upper().startswith("INSERT INTO ") and "ON CONFLICT" not in query.upper():
        # SQLite's INSERT OR IGNORE has already lost the marker.  The caller
        # passes a marker through _translate_insert_ignore when necessary.
        pass
    query = re.sub(
        r"strftime\(\s*'%Y-%m-%dT%H:%M:%fZ'\s*,\s*'now'\s*\)",
        "to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')",
        query,
        flags=re.I,
    )
    query = _replace_qmarks(query)
    return _escape_literal_percent(query).replace("__V3_PARAM__", "%s")


def _translate_insert_ignore(query: str) -> str:
    if not re.match(r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\s+", query, flags=re.I):
        return query
    query = re.sub(r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\s+", "INSERT INTO ", query, flags=re.I)
    if re.search(r"\bON\s+CONFLICT\b", query, flags=re.I):
        return query
    return query.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"


class CursorProxy:
    def __init__(self, cursor: Any):
        self._cursor = cursor
        self.rowcount = cursor.rowcount

    def fetchone(self):
        values = self._cursor.fetchone()
        if values is None:
            return None
        columns = [str(desc.name) for desc in self._cursor.description or ()]
        return Row(zip(columns, values))

    def fetchall(self):
        values = self._cursor.fetchall()
        columns = [str(desc.name) for desc in self._cursor.description or ()]
        return [Row(zip(columns, row)) for row in values]

    def __iter__(self):
        return iter(self.fetchall())


class PostgresConnection:
    def __init__(self, dsn: str):
        self._conn = psycopg.connect(dsn, autocommit=False)
        self._conn.execute("SET TIME ZONE 'UTC'")

    def execute(self, query: str, params: Iterable[Any] | None = None) -> CursorProxy:
        query = _translate_insert_ignore(query)
        query = translate_sql(query)
        cursor = self._conn.execute(query, tuple(params or ()))
        return CursorProxy(cursor)

    def executescript(self, script: str) -> None:
        # SQLite INTEGER is a signed 64-bit value.  PostgreSQL INTEGER is only
        # 32-bit, while media byte counts and some platform counters exceed
        # that range, so preserve SQLite's effective width in the final schema.
        script = re.sub(r"\bINTEGER\b", "BIGINT", script, flags=re.I)
        statements = []
        current: list[str] = []
        quote: str | None = None
        for char in script:
            if quote:
                current.append(char)
                if char == quote:
                    quote = None
                continue
            if char in ("'", '"'):
                quote = char
                current.append(char)
            elif char == ";":
                statement = "".join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
            else:
                current.append(char)
        statement = "".join(current).strip()
        if statement:
            statements.append(statement)
        for statement in statements:
            self.execute(statement)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if exc_type:
                self.rollback()
            else:
                self.commit()
        finally:
            self.close()
        return False


def connect(dsn: str) -> PostgresConnection:
    return PostgresConnection(dsn)
