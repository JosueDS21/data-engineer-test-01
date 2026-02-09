"""
Database execution helpers. Execute SQL and return results.
Used by the ETL pipeline for all reads and writes.
"""
from typing import Any


def run_sql(conn, sql: str, params: tuple | list | None = None) -> None:
    """Execute a single statement (e.g. DELETE, INSERT, UPDATE). Commits are caller's responsibility."""
    params = params or ()
    h = conn.cursor()
    try:
        h.execute(sql, params)
    finally:
        h.close()


def query_one(conn, sql: str, params: tuple | list | None = None) -> tuple | None:
    """Execute a SELECT and return the first row as a tuple, or None."""
    params = params or ()
    h = conn.cursor()
    try:
        h.execute(sql, params)
        return h.fetchone()
    finally:
        h.close()


def query_all(conn, sql: str, params: tuple | list | None = None) -> list:
    """Execute a SELECT and return all rows as a list of tuples."""
    params = params or ()
    h = conn.cursor()
    try:
        h.execute(sql, params)
        return h.fetchall()
    finally:
        h.close()


def run_many(conn, sql: str, rows: list[tuple]) -> None:
    """Execute the same parameterized statement for each row (e.g. bulk INSERT)."""
    h = conn.cursor()
    try:
        for row in rows:
            h.execute(sql, row)
    finally:
        h.close()


def run_insert_return_id(conn, sql: str, params: tuple | list) -> int:
    """Execute INSERT and return the generated identity (SCOPE_IDENTITY() for SQL Server). Caller commits."""
    h = conn.cursor()
    try:
        h.execute(sql, params)
        h.execute("SELECT SCOPE_IDENTITY()")
        row = h.fetchone()
        return int(row[0]) if row and row[0] else 0
    finally:
        h.close()
