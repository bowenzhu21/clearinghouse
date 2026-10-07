from http.client import HTTPConnection
from pathlib import Path
import json
import tempfile
from threading import Thread
import unittest

from clearinghouse.api import create_server
from clearinghouse.ledger import Ledger
from clearinghouse.models import ValidationError
from tests.test_ledger import event


class APITest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.ledger = Ledger(Path(self.temp.name) / "ledger.db")
        self.token = "unit-test-token-24-characters"
        self.server = create_server(self.ledger, port=0, token=self.token)
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop)

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, method, path, body=None, headers=None):
        conn = HTTPConnection("127.0.0.1", self.server.server_port, timeout=3)
        all_headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        all_headers.update(headers or {})
        conn.request(method, path, body=json.dumps(body) if body is not None else None, headers=all_headers)
        result = conn.getresponse()
        status, response = result.status, json.loads(result.read())
        conn.close()
        return status, response

    def test_ingest_read_conflict_reverse_and_balance_over_http(self):
        status, first = self.request("POST", "/v1/events", event(), {"Idempotency-Key": "key"})
        self.assertEqual(200, status)
        self.assertEqual((200, first), self.request("POST", "/v1/events", event(), {"Idempotency-Key": "key"}))
        self.assertEqual(409, self.request("POST", "/v1/events", event(amount=2), {"Idempotency-Key": "key"})[0])
        self.assertEqual(10000, self.request("GET", "/v1/payments/pay_1")[1]["net_cents"])
        self.assertEqual(first, self.request("GET", "/v1/events/evt_1")[1])
        self.assertEqual(200, self.request("POST", f"/v1/journals/{first['journal_id']}/reverse", {"reason": "accounting correction"}, {"Idempotency-Key": "reverse"})[0])
        self.assertTrue(all(account["balance_cents"] == 0 for account in self.request("GET", "/v1/trial-balance")[1]["accounts"]))

    def test_http_pending_and_current_status(self):
        status, result = self.request("POST", "/v1/events", event("refund", kind="payment.refunded", amount=100), {"Idempotency-Key": "refund"})
        self.assertEqual((202, "pending"), (status, result["status"]))
        self.request("POST", "/v1/events", event(), {"Idempotency-Key": "capture"})
        self.assertEqual("posted", self.request("GET", "/v1/events/refund")[1]["status"])

    def test_auth_validation_missing_route_and_health(self):
        self.assertEqual(401, self.request("GET", "/healthz", headers={"Authorization": "Bearer wrong"})[0])
        self.assertEqual(401, self.request("GET", "/healthz", headers={"Authorization": "Bearer café"})[0])
        self.assertEqual(200, self.request("GET", "/healthz")[0])
        self.assertEqual(400, self.request("POST", "/v1/events", event())[0])
        self.assertEqual(400, self.request("POST", "/v1/events", event(amount=True), {"Idempotency-Key": "key"})[0])
        self.assertEqual(404, self.request("GET", "/v1/events/missing")[0])
        self.assertEqual(404, self.request("GET", "/unknown")[0])

    def test_malformed_and_oversized_body_rejected(self):
        for body in (b"{broken", b"x" * 65537, b'{"amount_cents":' + b'9' * 5000 + b'}', json.dumps(event()).replace('"amount_cents": 10000', '"amount_cents": 100, "amount_cents": 200').encode()):
            conn = HTTPConnection("127.0.0.1", self.server.server_port, timeout=3)
            conn.request("POST", "/v1/events", body=body, headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json", "Idempotency-Key": "key"})
            response = conn.getresponse()
            self.assertEqual(400, response.status)
            response.read()
            conn.close()

    def test_non_loopback_needs_authentication(self):
        with self.assertRaises(ValidationError):
            create_server(self.ledger, "0.0.0.0", 0)
        with self.assertRaises(ValidationError):
            create_server(self.ledger, port=0, token="short")


if __name__ == "__main__":
    unittest.main()
