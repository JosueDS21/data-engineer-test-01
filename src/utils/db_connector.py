"""Database connection helper - uses env vars (no hardcoded credentials).
Supports: SQL Server (pyodbc), PostgreSQL (psycopg2), DuckDB (file-based fallback).
"""
import os
from contextlib import contextmanager

try:
    import pyodbc
    HAS_PYODBC = True
except ImportError:
    HAS_PYODBC = False

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False


def get_connection_string() -> str:
    """Build connection string from environment."""
    db_type = os.getenv("DB_TYPE", "sqlserver").lower()
    if db_type in ("sqlserver", "mssql"):
        driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={os.getenv('DB_HOST', 'localhost')},{os.getenv('DB_PORT', '1433')};"
            f"DATABASE={os.getenv('DB_NAME', 'airbnb')};"
            f"UID={os.getenv('DB_USER', 'sa')};"
            f"PWD={os.getenv('DB_PASSWORD', '')}"
        )
    if db_type in ("postgresql", "postgres"):
        return (
            f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', '')}"
            f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'airbnb')}"
        )
    db_path = os.getenv("DB_PATH", "output/warehouse.duckdb")
    return f"duckdb:///{db_path}"


def _get_schema_path() -> str:
    """Return path to schema SQL file for current DB_TYPE."""
    root = os.getenv("PROJECT_ROOT", os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    db_type = os.getenv("DB_TYPE", "sqlserver").lower()
    if db_type in ("sqlserver", "mssql"):
        return os.path.join(root, "sql", "schema_sqlserver.sql")
    return os.path.join(root, "sql", "schema.sql")


@contextmanager
def get_connection():
    """Yield a database connection. Default: SQL Server (DB_TYPE=sqlserver). Use DB_TYPE=duckdb for local file."""
    db_type = os.getenv("DB_TYPE", "sqlserver").lower()
    if db_type in ("sqlserver", "mssql") and HAS_PYODBC:
        conn = pyodbc.connect(get_connection_string())
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    elif db_type in ("postgresql", "postgres") and HAS_PSYCOPG2:
        conn = psycopg2.connect(get_connection_string())
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    elif HAS_DUCKDB:
        db_path = os.getenv("DB_PATH", "output/warehouse.duckdb")
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        conn = duckdb.connect(db_path)
        try:
            yield conn
        finally:
            conn.close()
    else:
        raise RuntimeError(
            "Set DB_TYPE=sqlserver (need pyodbc), postgres (need psycopg2), or duckdb. "
            "For SQL Server: pip install pyodbc"
        )


def execute_sql_file(conn, path: str | None = None) -> None:
    """Execute a SQL file (schema). If path is None, uses schema for current DB_TYPE."""
    path = path or _get_schema_path()
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    if hasattr(conn, "executescript"):
        conn.executescript(sql)
    else:
        from src.utils.db_helpers import run_sql
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                run_sql(conn, stmt)
