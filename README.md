# turso-http

A pure-Python, DB-API 2.0 client for [Turso](https://turso.tech) that speaks
its hrana `/v2/pipeline` HTTP protocol using only [`requests`](https://requests.readthedocs.io/).

No native runtime. No embedded tokio. Just HTTP.

## Why this exists

The official [`libsql`](https://pypi.org/project/libsql/) Python package wraps
a Rust client that runs an embedded tokio runtime for every connection. When
that connection is opened inside a threaded server — `gunicorn -k gthread`,
`uwsgi --threads`, `waitress`, etc. — tokio can panic during shutdown with:

```
thread 'tokio-runtime-worker' panicked at library/std/src/sys/pal/unix/thread.rs:310:9:
failed to join thread: Resource deadlock avoided (os error 35)
```

…and take the whole worker down with it. I hit this in production, wrote it
up [in a blog post](./POST_DRAFT.md), and — since Turso's wire protocol is
just JSON over HTTPS — decided the simplest fix was to bypass the Rust
client entirely.

This library is the result. It:

- Talks to `POST https://<host>/v2/pipeline` directly.
- Exposes the same shape as [`sqlite3`](https://docs.python.org/3/library/sqlite3.html)
  (`connect`, `cursor`, `execute`, `fetchone`, `fetchall`, `lastrowid`,
  `rowcount`, `commit`, `rollback`, context managers).
- Maps constraint errors onto `IntegrityError` so code written against
  `sqlite3.IntegrityError` keeps working.
- Has no C or Rust dependencies — pip-install and go.

## Install

```bash
pip install turso-http
```

Requires Python 3.9+.

## Usage

```python
from turso_http import connect

conn = connect(
    url="libsql://your-db.turso.io",          # or "https://…"
    auth_token="<TURSO_AUTH_TOKEN>",           # optional
)

# Reads
cur = conn.cursor()
cur.execute("SELECT name, wins FROM players WHERE user_id = ?", (42,))
for name, wins in cur:
    print(name, wins)

# Writes are wrapped in an implicit BEGIN…COMMIT
cur.execute("INSERT INTO players (name, user_id) VALUES (?, ?)", ("Alice", 42))
print(cur.lastrowid)
conn.commit()

conn.close()
```

Or use the connection as a context manager — commits on clean exit,
rolls back on exception, always closes the underlying session:

```python
with connect(url, auth_token=token) as conn:
    conn.execute("UPDATE players SET wins = wins + 1 WHERE id = ?", (7,))
```

### Migrating from `sqlite3`

The surface is close enough that most `sqlite3` code moves over as-is:

```python
# before
import sqlite3
conn = sqlite3.connect("app.db")

# after
from turso_http import connect
conn = connect(TURSO_URL, TURSO_AUTH_TOKEN)
```

Rows are returned as plain tuples (like default `sqlite3`, before you set
`conn.row_factory = sqlite3.Row`). Column names are on `cursor.description`.

### Under Flask + gunicorn

The whole reason this library exists:

```python
# app.py
from flask import Flask, g
from turso_http import connect

app = Flask(__name__)

def get_db():
    if "db" not in g:
        g.db = connect(TURSO_URL, TURSO_AUTH_TOKEN)
    return g.db

@app.teardown_appcontext
def _close(_):
    db = g.pop("db", None)
    if db is not None:
        db.close()
```

```bash
gunicorn -k gthread -w 1 --threads 4 app:app
```

This is exactly the pattern that deadlocks with the official `libsql` client.
Here, it doesn't — because there's no tokio runtime to shut down.

See [`examples/flask_gunicorn.py`](examples/flask_gunicorn.py) for a full
sketch.

## What's supported

- `Connection`, `Cursor`, and the DB-API 2.0 exception hierarchy
  (`Error`, `InterfaceError`, `OperationalError`, `IntegrityError`).
- `?`-style positional parameter binding.
- Value types: `NULL`, `INTEGER`, `REAL`, `TEXT`, `BLOB`, `BOOLEAN` (as
  `INTEGER`).
- Implicit transactions — writes are wrapped in `BEGIN` on first write,
  committed on `conn.commit()` or clean context-manager exit.
- Baton threading — hrana's session token is echoed on every subsequent
  request, so the server can keep the client on the same replica.
- Context-manager `close()` that shuts the hrana stream via `{type: "close"}`
  when a baton exists.

## What's not (yet)

- No connection pooling. Open one connection per thread (or per request
  in Flask/gunicorn); `requests.Session` is cheap.
- No named parameters (`:name`) — hrana supports them, this library
  currently only exposes positional binding.
- No sync replicas / embedded reads (that's a different Turso feature that
  needs the Rust client).
- No streaming results — `fetchmany` and `fetchall` operate over the rows
  returned in one pipeline response.

Contributions welcome for any of the above.

## Thread safety

A single `Connection` is not thread-safe — treat it like a `sqlite3.Connection`
and don't share one across threads. Open one per worker / thread / request.

## Development

```bash
git clone https://github.com/mohammadzayed5/turso-http
cd turso-http
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

## License

MIT. See [LICENSE](LICENSE).

## Related

- [Turso](https://turso.tech) — hosted libsql / SQLite
- [libsql](https://github.com/tursodatabase/libsql) — the official Rust /
  Python / JS clients
- [hrana protocol reference](https://github.com/tursodatabase/libsql/blob/main/docs/HRANA_3_SPEC.md)
