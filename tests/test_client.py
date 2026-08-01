"""Unit tests for turso_http. Turso is mocked at the HTTP layer."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from turso_http import (
    Connection,
    IntegrityError,
    OperationalError,
    connect,
)
from turso_http.client import _from_value, _resolve_endpoint, _to_value


def _mock_response(status: int = 200, body: dict | None = None):
    resp = MagicMock()
    resp.status_code = status
    resp.text = json.dumps(body or {})
    resp.json.return_value = body or {}
    return resp


def _ok(cols=None, rows=None, last_insert_rowid=None, affected_row_count=None):
    """Build one `{type: "ok"}` result entry."""
    result: dict = {"cols": cols or [], "rows": rows or []}
    if last_insert_rowid is not None:
        result["last_insert_rowid"] = str(last_insert_rowid)
    if affected_row_count is not None:
        result["affected_row_count"] = affected_row_count
    return {"type": "ok", "response": {"type": "execute", "result": result}}


class TestValueCoding:
    def test_none(self):
        assert _to_value(None) == {"type": "null"}
        assert _from_value({"type": "null"}) is None

    def test_bool(self):
        assert _to_value(True) == {"type": "integer", "value": "1"}
        assert _to_value(False) == {"type": "integer", "value": "0"}

    def test_int(self):
        assert _to_value(42) == {"type": "integer", "value": "42"}
        assert _from_value({"type": "integer", "value": "42"}) == 42

    def test_float(self):
        assert _to_value(1.5) == {"type": "float", "value": 1.5}
        assert _from_value({"type": "float", "value": 1.5}) == 1.5

    def test_text(self):
        assert _to_value("hi") == {"type": "text", "value": "hi"}
        assert _from_value({"type": "text", "value": "hi"}) == "hi"

    def test_blob(self):
        v = _to_value(b"\x00\x01\x02")
        assert v["type"] == "blob"
        assert _from_value(v) == b"\x00\x01\x02"


class TestEndpointResolution:
    def test_libsql_becomes_https(self):
        assert (
            _resolve_endpoint("libsql://db.turso.io")
            == "https://db.turso.io/v2/pipeline"
        )

    def test_https_kept(self):
        assert (
            _resolve_endpoint("https://db.turso.io")
            == "https://db.turso.io/v2/pipeline"
        )

    def test_http_kept(self):
        assert (
            _resolve_endpoint("http://localhost:8080")
            == "http://localhost:8080/v2/pipeline"
        )

    def test_trailing_slash_stripped(self):
        assert (
            _resolve_endpoint("libsql://db.turso.io/")
            == "https://db.turso.io/v2/pipeline"
        )


class TestSelect:
    def test_scalar_select(self):
        body = {
            "baton": "b1",
            "results": [
                _ok(
                    cols=[{"name": "x"}],
                    rows=[[{"type": "integer", "value": "3"}]],
                )
            ],
        }
        with patch("requests.Session.post", return_value=_mock_response(body=body)):
            conn = connect("libsql://db.turso.io", "tok")
            cur = conn.cursor()
            cur.execute("SELECT ? + ?", (1, 2))
            assert cur.fetchone() == (3,)
            assert cur.fetchone() is None
            assert cur.description[0][0] == "x"


class TestInsertAndCommit:
    def test_insert_wraps_in_begin_and_commit(self):
        insert_body = {
            "baton": "b1",
            "results": [_ok(), _ok(last_insert_rowid=17, affected_row_count=1)],
        }
        commit_body = {"baton": "b2", "results": [_ok()]}
        mock_post = MagicMock(
            side_effect=[
                _mock_response(body=insert_body),
                _mock_response(body=commit_body),
            ]
        )
        with patch("requests.Session.post", mock_post):
            conn = connect("libsql://db.turso.io", "tok")
            cur = conn.cursor()
            cur.execute("INSERT INTO t (name) VALUES (?)", ("hi",))
            assert cur.lastrowid == 17
            assert cur.rowcount == 1
            assert conn._in_transaction is True

            conn.commit()
            assert conn._in_transaction is False

        # First call must include a BEGIN then the INSERT.
        first_payload = mock_post.call_args_list[0].kwargs["json"]
        stmts = [r["stmt"]["sql"] for r in first_payload["requests"]]
        assert stmts == ["BEGIN", "INSERT INTO t (name) VALUES (?)"]

        # Second call must be COMMIT alone.
        second_payload = mock_post.call_args_list[1].kwargs["json"]
        stmts2 = [r["stmt"]["sql"] for r in second_payload["requests"]]
        assert stmts2 == ["COMMIT"]


class TestErrorClassification:
    def test_unique_constraint_is_integrity_error(self):
        body = {
            "baton": "b1",
            "results": [
                {
                    "type": "error",
                    "error": {"message": "UNIQUE constraint failed: users.username"},
                }
            ],
        }
        with patch("requests.Session.post", return_value=_mock_response(body=body)):
            conn = connect("libsql://db.turso.io", "tok")
            with pytest.raises(IntegrityError):
                conn.cursor().execute("INSERT INTO users VALUES (?)", ("dup",))

    def test_generic_error_is_operational_error(self):
        body = {
            "baton": "b1",
            "results": [
                {"type": "error", "error": {"message": "syntax error near WHERE"}}
            ],
        }
        with patch("requests.Session.post", return_value=_mock_response(body=body)):
            conn = connect("libsql://db.turso.io", "tok")
            with pytest.raises(OperationalError):
                conn.cursor().execute("SELECT")


class TestBatonThreading:
    def test_baton_is_echoed_on_subsequent_calls(self):
        first = {"baton": "abc", "results": [_ok()]}
        second = {"baton": "def", "results": [_ok()]}
        mock_post = MagicMock(
            side_effect=[
                _mock_response(body=first),
                _mock_response(body=second),
            ]
        )
        with patch("requests.Session.post", mock_post):
            conn = connect("libsql://db.turso.io", "tok")
            conn.cursor().execute("SELECT 1")
            conn.cursor().execute("SELECT 2")

        assert mock_post.call_args_list[0].kwargs["json"]["baton"] is None
        assert mock_post.call_args_list[1].kwargs["json"]["baton"] == "abc"


class TestContextManager:
    def test_commit_on_clean_exit(self):
        body = {"baton": "b1", "results": [_ok(), _ok(affected_row_count=1)]}
        commit_body = {"baton": "b2", "results": [_ok()]}
        close_body = {"baton": None, "results": []}
        mock_post = MagicMock(
            side_effect=[
                _mock_response(body=body),
                _mock_response(body=commit_body),
                _mock_response(body=close_body),
            ]
        )
        with patch("requests.Session.post", mock_post):
            with connect("libsql://db.turso.io", "tok") as conn:
                conn.cursor().execute("INSERT INTO t VALUES (?)", (1,))
        commit_sql = mock_post.call_args_list[1].kwargs["json"]["requests"][0]["stmt"][
            "sql"
        ]
        assert commit_sql == "COMMIT"

    def test_rollback_on_exception(self):
        body = {"baton": "b1", "results": [_ok(), _ok(affected_row_count=1)]}
        rollback_body = {"baton": "b2", "results": [_ok()]}
        close_body = {"baton": None, "results": []}
        mock_post = MagicMock(
            side_effect=[
                _mock_response(body=body),
                _mock_response(body=rollback_body),
                _mock_response(body=close_body),
            ]
        )
        with patch("requests.Session.post", mock_post):
            with pytest.raises(RuntimeError):
                with connect("libsql://db.turso.io", "tok") as conn:
                    conn.cursor().execute("INSERT INTO t VALUES (?)", (1,))
                    raise RuntimeError("boom")
        rollback_sql = mock_post.call_args_list[1].kwargs["json"]["requests"][0][
            "stmt"
        ]["sql"]
        assert rollback_sql == "ROLLBACK"
