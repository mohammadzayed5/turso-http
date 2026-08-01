# Rewriting Turso's Python client because the official one deadlocks under gunicorn

*Draft — for editing before publishing on my portfolio.*

## The symptom

I ship a small iOS + web app called Mini Golf Score Tracker. The backend
is Flask, deployed to Render's free tier. I'd just migrated it off local
SQLite — which was getting wiped on every deploy because Render's free
disk is ephemeral — onto [Turso](https://turso.tech), a hosted libsql
service that gives you a real SQLite over HTTP.

Migration went in. The first few requests worked. And then every request
hung for two minutes and returned `502 Bad Gateway`. Render's logs showed
the worker was being killed by gunicorn's own timeout:

```
[CRITICAL] WORKER TIMEOUT (pid:34)
```

But the piece that told me something was actually wrong — not just slow —
was buried above it:

```
thread 'tokio-runtime-worker' panicked at library/std/src/sys/pal/unix/thread.rs:310:9:
failed to join thread: Resource deadlock avoided (os error 35)
```

That's not a Python error. That's Rust panicking inside a runtime the
official [`libsql`](https://pypi.org/project/libsql/) Python package has
embedded in its C extension.

## The diagnosis

Here's the shape of the setup. Flask, gunicorn with the threaded worker,
handling requests concurrently:

```
gunicorn -k gthread -w 1 --threads 4 --timeout 120 wsgi:app
```

The `libsql` Python package is a thin `pyo3` wrapper over the Rust
libsql client. That Rust client uses tokio to do its async I/O. Every
`libsql.connect(...)` in Python spawns a tokio runtime under the hood.
When you `.close()` that connection (or when Python GC's it), tokio has
to shut its runtime down, which means joining its worker threads.

Under gunicorn's `gthread` worker, Python is running our handlers in a
thread pool. So the sequence is:

1. Request comes in → gunicorn picks a worker thread.
2. Handler calls `libsql.connect(...)`. That call spawns a tokio runtime
   that owns its own worker threads.
3. Handler runs a query. Fine.
4. Handler returns. `close()` fires (or Python's tracemalloc frees it
   later on another gunicorn thread). Tokio tries to `join` its workers.
5. Because of how tokio's shutdown interacts with a foreign OS thread
   ([`EDEADLK`](https://man7.org/linux/man-pages/man3/pthread_join.3p.html)
   from `pthread_join` when you'd deadlock yourself), it panics.
6. The panic escapes the `pyo3` boundary and takes the whole Python
   worker with it. Gunicorn respawns. Next request hits the same trap.

This is [a known](https://github.com/tursodatabase/libsql/issues) class
of problem with Rust-runtime-under-Python — the Rust side assumes it
owns the process, Python's request-per-thread model violates that
assumption, and the shutdown path is where the mismatch actually lands.

Pinning to `libsql==0.1.8` (an older version I'd been running earlier
in dev) didn't help — same panic. This isn't a regression, it's a
fundamental incompatibility with the deployment shape.

## The decision

I had three options:

1. **Move off gunicorn's threaded worker to `sync`.** Would work, but a
   sync worker with one request in flight at a time on a free-tier box
   is bad. And it treats the symptom, not the cause.
2. **Move off Turso.** Would work, but the whole reason I migrated was
   to get durable storage. Going back to sqlite-on-ephemeral-disk is
   worse than where I started.
3. **Bypass the Rust client entirely.**

Turso's wire protocol — hrana — is
[documented](https://github.com/tursodatabase/libsql/blob/main/docs/HRANA_3_SPEC.md).
The v2 version is JSON over HTTPS at `POST /v2/pipeline`. It's a batch
of `execute` / `close` requests in, a batch of `ok` / `error` results
out. There's a `baton` field the server hands you to keep you on the
same replica for follow-up requests.

I checked the endpoint with `curl`:

```
$ curl -s https://<db>.turso.io/v2/pipeline
{"error":"unauthorized"}
```

279ms round-trip, ~40 lines of protocol to implement. That's it.

Option 3.

## The design

The goal was a drop-in replacement for the `sqlite3.Connection` shape
that most of the codebase was written against — cursor / execute /
fetchone / fetchall / lastrowid / rowcount / commit / rollback. Not a
full new abstraction, just enough of DB-API 2.0 that
`api/store.py` didn't need to change.

The core is small enough to inline here:

```python
class Connection:
    def __init__(self, url, auth_token):
        self._endpoint = _resolve_endpoint(url)   # libsql:// → https://<host>/v2/pipeline
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        })
        self._baton = None
        self._in_transaction = False

    def _execute_stmt(self, sql, params):
        stmt = {"sql": sql, "args": [_to_value(p) for p in params]}
        is_write = sql.lstrip().split(None, 1)[0].upper() in _WRITE_KEYWORDS

        pipeline = []
        if is_write and not self._in_transaction:
            pipeline.append({"type": "execute", "stmt": {"sql": "BEGIN"}})
            self._in_transaction = True
        pipeline.append({"type": "execute", "stmt": stmt})

        data = self._pipeline(pipeline)
        return data["results"][-1]["response"]["result"]

    def commit(self):
        if self._in_transaction:
            self._pipeline([{"type": "execute", "stmt": {"sql": "COMMIT"}}])
            self._in_transaction = False
```

Four design decisions worth calling out:

**Implicit transactions.** SQLite's default behavior is autocommit per
statement, but Turso's HTTP protocol executes each `/v2/pipeline` call
as its own hrana stream. To make `store.py`'s `conn.commit()` mean what
it meant against `sqlite3`, I open a `BEGIN` on the first write, and
`COMMIT` when the caller says to. Reads are unaffected.

**Batons.** hrana returns a `baton` string on the first response. Passing
it back on subsequent requests keeps you on the same replica, so
`INSERT` → `SELECT last_insert_rowid()` sees your own writes. Storing
one on the connection is all that takes.

**DB-API 2.0 error mapping.** hrana returns constraint failures as
plain error messages. The rest of the codebase catches
`sqlite3.IntegrityError` for `UNIQUE` violations on the users table. I
map any message containing `"constraint"` / `"unique"` /
`"foreign key"` to a new `IntegrityError`, so nothing upstream needs to
change.

**No pooling.** Each Flask request opens its own `Connection`, which
opens its own `requests.Session`. Sessions are cheap; the underlying
HTTPS connection reuse happens at the `requests`/`urllib3` layer. And
because there's no runtime to spin up, there's no runtime to tear down
— which is the entire point.

## Verification

Locally, I mocked the four scenarios that mattered most: a `SELECT`, an
`INSERT` followed by `commit`, value round-tripping (`NULL`/`INT`/`REAL`/
`TEXT`/`BLOB`), and constraint errors mapping to `IntegrityError`.

Then, in production: created a game before the deploy, triggered a
manual redeploy of the Render service with build cache cleared, refreshed
the app after it came back. The game was still there. That's the single
test that mattered — the whole point of the Turso migration was that a
deploy shouldn't wipe data. It didn't.

## What I'd do differently

Two things.

**Sub in a proper error hierarchy from the start.** I initially raised a
single `TursoError` from the wrapper and pattern-matched on message text
upstream. The DB-API 2.0 exception classes exist for a reason; using
them meant the surrounding code needed zero changes.

**Log the panic earlier.** I spent longer than I should have on
"gunicorn worker timeout" as the symptom before I scrolled up enough
lines to see the tokio panic. The panic was the actual signal; the
timeout was the coincidental thing that killed the request. A better
habit is to grep prod logs for `panic` / `EDEADLK` / `join thread`
before staring at HTTP-level errors.

## Takeaway

If you're wrapping a Rust library that owns a tokio runtime from Python,
and Python is running under a threaded server, the shutdown path is
where the two runtimes will collide. Sometimes the fix isn't to make
the wrapper smarter — it's to notice that the wire protocol is 40
lines of JSON, and skip the wrapper.

The library is [turso-http on
GitHub](https://github.com/mohammadzayed5/turso-http). It shipped Mini
Golf Score Tracker's v1.1 to the App Store on the first review pass.

*—Mohammad*
