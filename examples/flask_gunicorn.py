"""Minimal Flask + gunicorn sketch using turso-http.

Run locally:
    export TURSO_DATABASE_URL="libsql://<yours>.turso.io"
    export TURSO_AUTH_TOKEN="<your-token>"
    gunicorn -k gthread -w 1 --threads 4 flask_gunicorn:app

This is the exact worker configuration that deadlocks with the official
`libsql` Python package. With `turso-http` it just works, because there's
no tokio runtime to shut down.
"""
import os

from flask import Flask, g, jsonify

from turso_http import connect

TURSO_URL = os.environ["TURSO_DATABASE_URL"]
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

app = Flask(__name__)


def get_db():
    if "db" not in g:
        g.db = connect(TURSO_URL, TURSO_TOKEN)
    return g.db


@app.teardown_appcontext
def close_db(_):
    db = g.pop("db", None)
    if db is not None:
        db.close()


@app.route("/health")
def health():
    cur = get_db().cursor()
    cur.execute("SELECT 1")
    return jsonify({"ok": True, "value": cur.fetchone()[0]})


@app.route("/players")
def players():
    cur = get_db().cursor()
    cur.execute("SELECT id, name, wins FROM players ORDER BY wins DESC LIMIT 10")
    cols = [c[0] for c in cur.description]
    return jsonify([dict(zip(cols, row)) for row in cur.fetchall()])
