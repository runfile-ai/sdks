"""Background flusher: drain the buffer, chain + encrypt + ship mixed batches.

This is where the off-hot-path work happens (sdk-design.md):

  drain (capture order) → compute the event hash chain in (segment_index,
  local_seq) order per run → classify + redact → AES-256-GCM encrypt payloads →
  assemble a mixed BatchSubmission → validate via Pydantic → POST /v1/batches
  with a stable Idempotency-Key and exponential backoff.

The hash chain commits to ``payload_ref`` (per the canonical projection), so it
can only be computed here, after encryption — not on the hot path. The SDK's
``prev_event_hash_intent`` is its local belief; the server recomputes the
canonical value and flags a benign ``chain_break`` where they diverge (notably
the first event of a run, whose true predecessor is the server-synthesised
``run_create`` event).

Chain state (``last_event_hash`` per run) persists across drains because a run
spans many flushes.
"""

from __future__ import annotations

import base64
import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

import httpx
from runfile_schemas.ingest import (
    EventItem,
    RunCreateItem,
    RunEndItem,
    RunUpdateItem,
)

from ._constants import SCHEMA_VERSION, SDK_NAME, sdk_version
from ._hashing import ZERO_SENTINEL, compute_event_hash
from ._ids import generate_batch_id
from .buffer import BufferedEvent, BufferedItem, BufferedRunItem
from .classifier import CLASSIFIER_VERSION, Redactor

if TYPE_CHECKING:
    from .client import RunfileClient

# One API key == one tenant, so a fixed local cache key is sufficient; the real
# tenant is resolved server-side from the bearer key.
_SELF_TENANT = "self"

# Wire limit: 100 items per batch (the 5 MB cap is enforced separately later).
_MAX_ITEMS_PER_BATCH = 100

_RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


@dataclass
class RetryConfig:
    base_ms: int = 200
    cap_ms: int = 30_000
    max_attempts: int = 8
    sleep: Callable[[float], None] = time.sleep
    jitter: Callable[[], float] = lambda: 1.0  # overridable in tests; 1.0 = no jitter


@dataclass
class DrainResult:
    batches_sent: int = 0
    items_accepted: int = 0
    items_rejected: int = 0
    items_invalid: int = 0  # failed LOCAL validation but shipped raw (chain-preserving)
    items_spooled: int = 0  # retries exhausted → persisted to disk for a later drain
    items_dropped: int = 0  # terminal 4xx, or spool full (data lost)


@dataclass
class Flusher:
    client: "RunfileClient"
    interval_seconds: float = 2.0
    retry: RetryConfig = field(default_factory=RetryConfig)
    redactor: Redactor = field(default_factory=Redactor)

    _last_event_hash: dict[str, str] = field(default_factory=dict)
    _drain_lock: threading.Lock = field(default_factory=threading.Lock)
    _stop: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = None

    # ---- thread lifecycle ------------------------------------------------- #

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop, name="runfile-flusher", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_seconds + 5)
            self._thread = None
        # Best-effort final drain with a SINGLE attempt — never a long backoff
        # storm against a possibly-unreachable endpoint during shutdown.
        # (Durable retry across restarts is the on-disk spool — next slice.)
        saved = self.retry
        self.retry = RetryConfig(max_attempts=1, sleep=lambda _s: None)
        try:
            self.flush_now()
        finally:
            self.retry = saved

    def _loop(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self.client.refresh_policy_if_stale()
                self.flush_now()
            except Exception:  # never let the flusher thread die
                pass

    # ---- draining --------------------------------------------------------- #

    def flush_now(self) -> DrainResult:
        """Synchronously drain and ship everything currently buffered."""
        with self._drain_lock:
            result = DrainResult()
            # Re-send anything spooled by a prior failed drain first (FIFO).
            self._drain_spool(result)
            items = self.client.buffer.take_all()
            if not items:
                return result
            wire_items = [self._to_wire_item(item, result) for item in items]
            for chunk in _chunks(wire_items, _MAX_ITEMS_PER_BATCH):
                self._ship_batch(chunk, result)
            return result

    def _drain_spool(self, result: DrainResult) -> None:
        """Re-send spooled batches (ciphertext only) with their original keys."""
        for entry in self.client.spool.entries():
            try:
                self._post_with_retry(entry.body, entry.idempotency_key)
            except _ShipError:
                break  # still failing — leave this and the rest for the next drain
            else:
                # Any returned status (200/207, or terminal 4xx) means "done with
                # this entry": delivered, or permanently rejected. Drop it.
                self.client.spool.delete(entry.path)
                result.batches_sent += 1

    def _to_wire_item(self, item: BufferedItem, result: DrainResult) -> dict[str, Any]:
        """Turn a buffered item into a wire batch item, chaining events.

        On local validation failure we ship the raw item and flag it rather than
        drop it: dropping an event would tear the run's hash chain (the silent
        corruption the design forbids). The server is the validation authority.
        """
        if isinstance(item, BufferedRunItem):
            wire = self._validate_item(item.item)
            if wire is None:
                result.items_invalid += 1
                return item.item
            return wire

        assert isinstance(item, BufferedEvent)
        event = dict(item.event)
        if item.raw_payload is not None:
            event["payload_ref"] = self._build_payload_ref(item)

        run_id = item.run_id
        prev = self._last_event_hash.get(run_id, ZERO_SENTINEL)
        event["prev_event_hash_intent"] = prev

        wire_item = self._validate_item({"type": "event", "event": event})
        if wire_item is None:
            result.items_invalid += 1
            wire_item = {"type": "event", "event": event}  # ship raw — keep the chain
        # Hash the *shipped* event (the exact bytes) so the SDK's chain matches
        # what the server hashes. event_hash commits to prev_event_hash (canonical
        # projection drops prev_event_hash_intent) + payload_ref.
        event_hash = compute_event_hash({**wire_item["event"], "prev_event_hash": prev})
        self._last_event_hash[run_id] = event_hash
        return wire_item

    def _build_payload_ref(self, item: BufferedEvent) -> dict[str, Any]:
        # Redact (the client-side PII boundary) before encryption.
        redaction = self.redactor.apply(item.raw_payload, self.client.current_policy())
        plaintext, content_type = _serialize(redaction.value)

        from .encrypt import aes_gcm_encrypt  # local import: avoid import cycle

        data_key = self.client.datakeys.get_or_fetch(_SELF_TENANT, item.run.agent_identity)
        enc = aes_gcm_encrypt(plaintext, bytes(data_key.plaintext))
        digest = hashlib.sha256(enc.ciphertext).hexdigest()
        payload_ref: dict[str, Any] = {
            "sha256": f"sha256:{digest}",
            "size_bytes": len(enc.ciphertext),
            "encryption": {
                "algorithm": "aes-256-gcm",
                "data_key_wrapped": data_key.wrapped,
                "key_id": data_key.key_id,
                "nonce": base64.b64encode(enc.nonce).decode(),
            },
            "content_type": content_type,
            "ciphertext_base64": base64.b64encode(enc.ciphertext).decode(),
        }
        if redaction.redacted_classes or redaction.tokenized_classes:
            payload_ref["redaction_applied"] = {
                "redacted_classes": redaction.redacted_classes,
                "tokenized_classes": redaction.tokenized_classes,
                "classifier_version": CLASSIFIER_VERSION,
            }
        return payload_ref

    @staticmethod
    def _validate_item(item: dict[str, Any]) -> dict[str, Any] | None:
        """Validate one batch item against its schema model; drop if invalid.

        Local fail-fast (the hybrid model): hot path builds dicts, the flusher
        validates with the real Pydantic models before shipping.
        """
        model = {
            "event": EventItem,
            "run_create": RunCreateItem,
            "run_update": RunUpdateItem,
            "run_end": RunEndItem,
        }.get(item.get("type", ""))
        if model is None:
            return None
        try:
            validated = model.model_validate(item)
        except Exception:
            return None
        # exclude_unset (not exclude_none): keeps required-nullable fields the SDK
        # set (e.g. parent_event_id=null) while dropping truly-unset optionals.
        dumped: dict[str, Any] = validated.model_dump(mode="json", exclude_unset=True)
        return dumped

    # ---- shipping --------------------------------------------------------- #

    def _ship_batch(self, items: list[dict[str, Any]], result: DrainResult) -> None:
        if not items:
            return
        body = {"batch_id": generate_batch_id(), "items": items}
        idempotency_key = _uuid4()
        try:
            status, payload = self._post_with_retry(body, idempotency_key)
        except _ShipError:
            # Transient failure, retries exhausted → persist (ciphertext only) for
            # a later drain rather than lose it. If the spool is full, drop + flag.
            if self.client.spool.write(idempotency_key, body):
                result.items_spooled += len(items)
            else:
                result.items_dropped += len(items)
            return
        if status == 200:
            result.batches_sent += 1
            result.items_accepted += len(items)
        elif status == 207:
            result.batches_sent += 1
            accepted = len(payload.get("accepted_items", []))
            rejected = len(payload.get("rejected_items", []))
            result.items_accepted += accepted
            result.items_rejected += rejected
        else:  # terminal 4xx (400/401/403/413/422): not retryable, data dropped
            result.items_dropped += len(items)

    def _post_with_retry(
        self, body: dict[str, Any], idempotency_key: str
    ) -> tuple[int, dict[str, Any]]:
        headers = {
            "Authorization": f"Bearer {self.client.api_key}",
            "Idempotency-Key": idempotency_key,  # stable across retries of this batch
            "Runfile-Schema-Version": SCHEMA_VERSION,
            "Runfile-SDK-Name": SDK_NAME,
            "Runfile-SDK-Version": sdk_version(),
            "Content-Type": "application/json",
        }
        url = f"{self.client.base_url}/v1/batches"
        last_exc: Exception | None = None
        for attempt in range(self.retry.max_attempts):
            try:
                resp = self.client._http.post(url, headers=headers, json=body)
            except httpx.HTTPError as exc:
                last_exc = exc
            else:
                if resp.status_code in _RETRYABLE_STATUSES:
                    last_exc = _ShipError(f"retryable status {resp.status_code}")
                else:
                    try:
                        payload = resp.json()
                    except Exception:
                        payload = {}
                    return resp.status_code, payload
            if attempt < self.retry.max_attempts - 1:
                self.retry.sleep(self._backoff_seconds(attempt))
        raise _ShipError(f"exhausted retries: {last_exc}")

    def _backoff_seconds(self, attempt: int) -> float:
        millis = min(self.retry.base_ms * (2**attempt), self.retry.cap_ms)
        jitter: float = self.retry.jitter()
        return float(millis * jitter / 1000.0)


class _ShipError(RuntimeError):
    pass


def _serialize(payload: Any) -> tuple[bytes, str]:
    """Serialize a redacted payload to bytes + a wire content_type."""
    if isinstance(payload, bytes):
        return payload, "application/json"
    if isinstance(payload, str):
        return payload.encode("utf-8"), "text/plain"
    return json.dumps(payload, separators=(",", ":")).encode("utf-8"), "application/json"


def _chunks(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _uuid4() -> str:
    import uuid

    return str(uuid.uuid4())
