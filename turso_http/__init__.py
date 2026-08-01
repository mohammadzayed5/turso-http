"""turso-http — a pure-Python, thread-safe DB-API 2.0 client for Turso.

Speaks Turso's hrana `/v2/pipeline` HTTP protocol using only `requests`.
Nothing to compile, no native runtime, no tokio — so it works cleanly under
gunicorn's threaded worker and other embedded-Python contexts where the
official `libsql` package deadlocks on shutdown.

    from turso_http import connect
    conn = connect(
        url="libsql://your-db.turso.io",   # or "https://..."
        auth_token="…",                    # optional
    )
    cur = conn.cursor()
    cur.execute("SELECT ? + ?", (1, 2))
    print(cur.fetchone())   # (3,)

The API is intentionally sqlite3-shaped so it's a drop-in for code that
was written against sqlite3.
"""
from .client import (
    Connection,
    Cursor,
    Error,
    IntegrityError,
    InterfaceError,
    OperationalError,
    connect,
)

__all__ = [
    "Connection",
    "Cursor",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "OperationalError",
    "connect",
]

__version__ = "0.1.0"
