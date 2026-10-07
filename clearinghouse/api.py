"""A small real HTTP adapter; the accounting service has no web-framework dependency."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hmac
import json
import sqlite3
from typing import Any
from urllib.parse import unquote, urlsplit

from .ledger import Ledger
from .models import ConflictError, NotFoundError, ValidationError


MAX_BODY_BYTES = 65_536


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def create_server(ledger: Ledger, host: str = "127.0.0.1", port: int = 8080, token: str | None = None) -> ThreadingHTTPServer:
    if host not in ("127.0.0.1", "localhost") and not token:
        raise ValidationError("a non-loopback bind requires CLEARINGHOUSE_API_TOKEN")
    if token is not None and (len(token) < 24 or not token.isascii() or any(c.isspace() for c in token)):
        raise ValidationError("CLEARINGHOUSE_API_TOKEN must contain at least 24 ASCII characters without whitespace")

    class Handler(BaseHTTPRequestHandler):
        server_version = "Clearinghouse/0.1"

        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(10)

        def log_message(self, format: str, *args: Any) -> None:
            # Do not log payment data or authentication headers.
            return

        def _send(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def _authorized(self) -> bool:
            if token and not hmac.compare_digest(self.headers.get("Authorization", "").encode("utf-8"), f"Bearer {token}".encode("utf-8")):
                self._send(401, {"error": "unauthorized"})
                return False
            return True

        def _json_body(self) -> Any:
            if self.headers.get("Transfer-Encoding"):
                raise ValidationError("chunked transfer encoding is not supported")
            if self.headers.get_content_type() != "application/json":
                raise ValidationError("Content-Type must be application/json")
            try:
                length = int(self.headers.get("Content-Length", ""))
            except ValueError:
                raise ValidationError("Content-Length is required") from None
            if not 0 < length <= MAX_BODY_BYTES:
                raise ValidationError(f"request body must be 1–{MAX_BODY_BYTES} bytes")
            try:
                body = self.rfile.read(length)
                if len(body) != length:
                    raise ValidationError("incomplete request body")
                return json.loads(body, object_pairs_hook=_unique_json_object)
            except ValidationError:
                raise
            except (UnicodeDecodeError, ValueError, RecursionError):
                raise ValidationError("invalid JSON") from None

        def _dispatch(self, method: str) -> None:
            if not self._authorized():
                return
            path = unquote(urlsplit(self.path).path)
            try:
                if method == "GET" and path == "/healthz":
                    with ledger.connection() as conn:
                        conn.execute("SELECT 1").fetchone()
                    self._send(200, {"status": "ok", "storage": "sqlite-wal"})
                elif method == "GET" and path.startswith("/v1/events/"):
                    self._send(200, ledger.event(path.removeprefix("/v1/events/")))
                elif method == "GET" and path.startswith("/v1/payments/"):
                    self._send(200, ledger.payment(path.removeprefix("/v1/payments/")))
                elif method == "GET" and path == "/v1/trial-balance":
                    self._send(200, {"accounts": ledger.trial_balance()})
                elif method == "POST" and path == "/v1/events":
                    result = ledger.ingest(self._json_body(), self.headers.get("Idempotency-Key", ""))
                    self._send(202 if result["status"] == "pending" else 200, result)
                elif method == "POST" and path.startswith("/v1/journals/") and path.endswith("/reverse"):
                    journal_id = path.removeprefix("/v1/journals/").removesuffix("/reverse")
                    body = self._json_body()
                    if not isinstance(body, dict) or set(body) != {"reason"}:
                        raise ValidationError("reversal body must contain only reason")
                    self._send(200, ledger.reverse(journal_id, body["reason"], self.headers.get("Idempotency-Key", "")))
                else:
                    self._send(404, {"error": "route_not_found"})
            except ValidationError as exc:
                self._send(400, {"error": "invalid_request", "message": str(exc)})
            except ConflictError as exc:
                self._send(409, {"error": "conflict", "message": str(exc)})
            except NotFoundError as exc:
                self._send(404, {"error": "not_found", "message": str(exc)})
            except sqlite3.OperationalError:
                self._send(503, {"error": "storage_unavailable", "message": "retry with the same idempotency key"})

        def do_GET(self) -> None:
            self._dispatch("GET")

        def do_POST(self) -> None:
            self._dispatch("POST")

    return ThreadingHTTPServer((host, port), Handler)
