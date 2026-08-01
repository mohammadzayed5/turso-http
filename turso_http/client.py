"""DB-API 2.0 client for Turso's hrana /v2/pipeline HTTP endpoint."""
from __future__ import annotations

import base64
from typing import Any, Iterable, Optional, Sequence, Tuple

import requests


class Error(Exception):
    """Base class for all turso-http errors."""


class InterfaceError(Error):
    """Client-side misuse (bad URL, closed connection, etc.)."""


class OperationalError(Error):
    """Server-side or network error executing a statement."""


class IntegrityError(Error):
    """UNIQUE / FOREIGN KEY / CHECK constraint violation."""


_WRITE_KEYWORDS = frozenset(
    {"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "REPLACE"}
)


def _to_value(v: Any) -> dict:
    """Encode a Python value as a hrana Value."""
    if v is None:
        return {"type": "null"}
    if isinstance(v, bool):
        return {"type": "integer", "value": "1" if v else "0"}
    if isinstance(v, int):
        return {"type": "integer", "value": str(v)}
    if isinstance(v, float):
        return {"type": "float", "value": v}
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"type": "blob", "base64": base64.b64encode(bytes(v)).decode("ascii")}
    return {"type": "text", "value": str(v)}


def _from_value(v: dict) -> Any:
    """Decode a hrana Value into its Python equivalent."""
    t = v.get("type")
    if t == "null":
        return None
    if t == "integer":
        return int(v["value"])
    if t == "float":
        return float(v["value"])
    if t == "text":
        return v["value"]
    if t == "blob":
        return base64.b64decode(v["base64"])
    return v.get("value")


def _resolve_endpoint(url: str) -> str:
    """Turn a Turso `libsql://…` or `https://…` URL into the pipeline URL."""
    if "://" in url:
        scheme, host = url.split("://", 1)
    else:
        scheme, host = "https", url
    if scheme not in ("libsql", "http", "https"):
        raise InterfaceError(f"unsupported scheme: {scheme!r}")
    https_scheme = "http" if scheme == "http" else "https"
    return f"{https_scheme}://{host.rstrip('/')}/v2/pipeline"


def _classify(msg: str) -> Error:
    """Map a server error message onto a DB-API 2.0 exception subclass."""
    lower = (msg or "").lower()
    if "constraint" in lower or "unique" in lower or "foreign key" in lower:
        return IntegrityError(msg)
    return OperationalError(msg)


class Cursor:
    """DB-API 2.0 cursor. Rows are returned as plain tuples."""

    arraysize = 1

    def __init__(self, conn: "Connection") -> None:
        self._conn = conn
        self.description: Optional[list] = None
        self._rows: list[tuple] = []
        self._row_idx = 0
        self.lastrowid: Optional[int] = None
        self.rowcount: int = -1
        self._closed = False

    def _check_open(self) -> None:
        if self._closed:
            raise InterfaceError("cursor is closed")
        if self._conn._closed:
            raise InterfaceError("connection is closed")

    def execute(self, sql: str, params: Sequence[Any] = ()) -> "Cursor":
        """Execute a single statement. `params` is bound positionally with `?`."""
        self._check_open()
        result = self._conn._execute_stmt(sql, params or ())
        cols = result.get("cols") or []
        self.description = [
            (c.get("name"), None, None, None, None, None, None) for c in cols
        ]
        self._rows = [
            tuple(_from_value(v) for v in row) for row in (result.get("rows") or [])
        ]
        self._row_idx = 0
        lir = result.get("last_insert_rowid")
        self.lastrowid = int(lir) if lir is not None else None
        arc = result.get("affected_row_count")
        self.rowcount = int(arc) if arc is not None else -1
        return self

    def executemany(self, sql: str, seq_of_params: Iterable[Sequence[Any]]) -> "Cursor":
        for params in seq_of_params:
            self.execute(sql, params)
        return self

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        if self._row_idx >= len(self._rows):
            return None
        row = self._rows[self._row_idx]
        self._row_idx += 1
        return row

    def fetchmany(self, size: Optional[int] = None) -> list:
        n = self.arraysize if size is None else size
        end = min(self._row_idx + max(n, 0), len(self._rows))
        rows = self._rows[self._row_idx:end]
        self._row_idx = end
        return rows

    def fetchall(self) -> list:
        remaining = self._rows[self._row_idx:]
        self._row_idx = len(self._rows)
        return remaining

    def close(self) -> None:
        self._closed = True

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


class Connection:
    """DB-API 2.0 connection over Turso's /v2/pipeline HTTP endpoint.

    A single instance is NOT thread-safe — treat it like a sqlite3 connection
    and use one per thread (or one per request, if you're in Flask/gunicorn).
    The underlying `requests.Session` is cheap to create.
    """

    def __init__(
        self,
        url: str,
        auth_token: Optional[str] = None,
        *,
        timeout: float = 30.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._endpoint = _resolve_endpoint(url)
        self._timeout = timeout
        self._session = session or requests.Session()
        headers = {"Content-Type": "application/json"}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        self._session.headers.update(headers)
        self._baton: Optional[str] = None
        self._in_transaction = False
        self._closed = False

    def cursor(self) -> Cursor:
        if self._closed:
            raise InterfaceError("connection is closed")
        return Cursor(self)

    def execute(self, sql: str, params: Sequence[Any] = ()) -> Cursor:
        """Shortcut for `conn.cursor().execute(sql, params)`."""
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self) -> None:
        if self._in_transaction:
            self._pipeline([{"type": "execute", "stmt": {"sql": "COMMIT"}}])
            self._in_transaction = False

    def rollback(self) -> None:
        if self._in_transaction:
            try:
                self._pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}])
            except Error:
                pass
            self._in_transaction = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.rollback()
        finally:
            if self._baton is not None:
                try:
                    self._pipeline([{"type": "close"}])
                except Error:
                    pass
            self._session.close()

    def _pipeline(self, requests_list: list) -> dict:
        payload = {"baton": self._baton, "requests": requests_list}
        try:
            resp = self._session.post(
                self._endpoint, json=payload, timeout=self._timeout
            )
        except requests.RequestException as e:
            raise OperationalError(f"network error: {e}") from e
        if resp.status_code != 200:
            raise OperationalError(f"HTTP {resp.status_code}: {resp.text[:500]}")
        data = resp.json()
        self._baton = data.get("baton")
        for r in data.get("results", []):
            if r.get("type") != "ok":
                err = r.get("error") or {}
                raise _classify(err.get("message") or str(err) or "unknown error")
        return data

    def _execute_stmt(self, sql: str, params: Sequence[Any]) -> dict:
        args = [_to_value(p) for p in params]
        stmt = {"sql": sql, "args": args}

        first_kw = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
        is_write = first_kw in _WRITE_KEYWORDS

        pipeline_requests = []
        if is_write and not self._in_transaction:
            pipeline_requests.append({"type": "execute", "stmt": {"sql": "BEGIN"}})
            self._in_transaction = True
        pipeline_requests.append({"type": "execute", "stmt": stmt})

        data = self._pipeline(pipeline_requests)
        return data["results"][-1]["response"]["result"]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()


def connect(
    url: str,
    auth_token: Optional[str] = None,
    *,
    timeout: float = 30.0,
) -> Connection:
    """Open a new connection. See `Connection` for parameter details."""
    return Connection(url, auth_token=auth_token, timeout=timeout)
