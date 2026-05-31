"""In-process fake of the Runfile Ingest API for tests.

A stdlib threaded HTTP server emulating the SDK-facing endpoints
(``/v1/data-keys``, ``/v1/batches``, ``/v1/policies/current``, ``/v1/health``).
It records requests and supports forcing error statuses, so the SDK core can be
exercised end-to-end over real HTTP without touching prod.
"""

from __future__ import annotations

import base64
import json
import os
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


@dataclass
class RecordedRequest:
    method: str
    path: str
    headers: dict[str, str]
    body: Any


@dataclass
class FakeIngest:
    """Controllable fake Ingest API. Use via the ``serve()`` context manager."""

    requests: list[RecordedRequest] = field(default_factory=list)
    mint_count: int = 0
    batch_count: int = 0
    #: Queue of HTTP statuses to return from POST /v1/data-keys before succeeding.
    datakey_fail_statuses: list[int] = field(default_factory=list)
    #: Queue of HTTP statuses to return from POST /v1/batches before succeeding.
    batch_fail_statuses: list[int] = field(default_factory=list)
    #: When True, the next /v1/batches returns 207 with one rejected item.
    batch_return_207: bool = False
    policy_version: str = "3.2.1"
    _server: ThreadingHTTPServer | None = None
    _thread: threading.Thread | None = None

    @property
    def base_url(self) -> str:
        assert self._server is not None, "server not started"
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def start(self) -> "FakeIngest":
        handler = _make_handler(self)
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()


def _make_handler(state: FakeIngest) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: Any) -> None:  # silence test noise
            pass

        def _read_json(self) -> Any:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b""
            return json.loads(raw) if raw else None

        def _send(self, status: int, body: dict[str, Any]) -> None:
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _record(self, body: Any) -> None:
            state.requests.append(
                RecordedRequest(
                    method=self.command,
                    path=self.path,
                    headers={k.lower(): v for k, v in self.headers.items()},
                    body=body,
                )
            )

        def do_GET(self) -> None:  # noqa: N802
            self._record(None)
            if self.path == "/v1/health":
                self._send(200, {"status": "healthy", "region": "eu-west-2",
                                 "api_version": "1.0.0", "schema_versions_supported": ["1.0"]})
            elif self.path == "/v1/policies/current":
                self._send(200, {
                    "policy_version": state.policy_version,
                    "classification_rules": [],
                    "fetched_at": "2026-05-31T00:00:00.000Z",
                    "ttl_seconds": 300,
                })
            else:
                self._send(404, {"error_code": "bad_request", "error_message": "unknown route"})

        def do_POST(self) -> None:  # noqa: N802
            body = self._read_json()
            self._record(body)
            if self.path == "/v1/data-keys":
                if state.datakey_fail_statuses:
                    status = state.datakey_fail_statuses.pop(0)
                    self._send(status, {"error_code": "service_unavailable",
                                        "error_message": "forced failure"})
                    return
                state.mint_count += 1
                self._send(200, {
                    "key_id": f"dk_{state.mint_count:026d}",
                    "plaintext": base64.b64encode(os.urandom(32)).decode(),
                    "wrapped": base64.b64encode(b"wrapped-key-blob").decode(),
                    "algorithm": "aes-256-gcm",
                })
            elif self.path == "/v1/batches":
                if state.batch_fail_statuses:
                    status = state.batch_fail_statuses.pop(0)
                    self._send(status, {"error_code": "service_unavailable",
                                        "error_message": "forced failure"})
                    return
                batch_id = (body or {}).get("batch_id", "b_unknown")
                items = (body or {}).get("items", [])
                if state.batch_return_207:
                    state.batch_return_207 = False
                    state.batch_count += 1
                    self._send(207, {
                        "batch_id": batch_id,
                        "accepted_count": max(len(items) - 1, 0),
                        "rejected_count": 1,
                        "accepted_items": [],
                        "rejected_items": [{"type": "event", "id": "x",
                                            "error_code": "schema_validation_failed",
                                            "error_message": "forced"}],
                        "received_at": "2026-05-31T00:00:00.000Z",
                    })
                    return
                state.batch_count += 1
                self._send(200, {
                    "batch_id": batch_id,
                    "accepted_count": len(items),
                    "accepted_items": [],
                    "received_at": "2026-05-31T00:00:00.000Z",
                })
            else:
                self._send(404, {"error_code": "bad_request", "error_message": "unknown route"})

    return Handler
