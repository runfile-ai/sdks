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
import random
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
from ._ids import generate_batch_id, generate_parallel_group_id
from .buffer import BufferedEvent, BufferedItem, BufferedRunItem
from .classifier import CLASSIFIER_VERSION
from .datakey import DataKeyError

if TYPE_CHECKING:
    from .client import RunfileClient

# One API key == one tenant, so a fixed local cache key is sufficient; the real
# tenant is resolved server-side from the bearer key.
_SELF_TENANT = "self"

# Wire limits: 100 items OR 5 MB per batch, whichever comes first.
_MAX_ITEMS_PER_BATCH = 100
_MAX_BATCH_BYTES = 5 * 1024 * 1024
# Headroom for the batch envelope ({"batch_id":"b_…","items":[…]}) + safety so we
# stay under API Gateway's 5 MB body limit (413) even after JSON whitespace.
_BATCH_ENVELOPE_OVERHEAD = 1024

_RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


#: ±25% backoff jitter (spec) to avoid synchronized retries (thundering herd).
_JITTER = 0.25


def _default_jitter() -> float:
    """A random multiplier in [0.75, 1.25] applied to each backoff delay."""
    return 1.0 + random.uniform(-_JITTER, _JITTER)


@dataclass
class RetryConfig:
    base_ms: int = 200
    cap_ms: int = 30_000
    max_attempts: int = 8
    sleep: Callable[[float], None] = time.sleep
    # ±25% jitter by default; injectable (e.g. a constant) for deterministic tests.
    jitter: Callable[[], float] = _default_jitter


@dataclass
class DrainResult:
    batches_sent: int = 0
    items_accepted: int = 0
    items_rejected: int = 0
    items_invalid: int = 0  # failed LOCAL validation but shipped raw (chain-preserving)
    items_spooled: int = 0  # retries exhausted → persisted to disk for a later drain
    items_deferred: int = 0  # couldn't encrypt (no data key) → held in memory, retried
    items_dropped: int = 0  # terminal 4xx, or spool full (data lost)


@dataclass
class Flusher:
    client: "RunfileClient"
    interval_seconds: float = 2.0
    retry: RetryConfig = field(default_factory=RetryConfig)
    max_items_per_batch: int = _MAX_ITEMS_PER_BATCH
    max_batch_bytes: int = _MAX_BATCH_BYTES

    _last_event_hash: dict[str, str] = field(default_factory=dict)
    _drain_lock: threading.Lock = field(default_factory=threading.Lock)
    _stop: threading.Event = field(default_factory=threading.Event)
    # Set by capture when the buffer crosses flush_threshold (size trigger) and by
    # stop(); wakes the loop before the interval elapses.
    _wake: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = None
    # Sticky: a 401/403 means the key is bad — stop hammering the API; keep
    # capturing + spooling encrypted batches for after the key is fixed.
    _auth_failed: bool = False

    # ---- thread lifecycle ------------------------------------------------- #

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop, name="runfile-flusher", daemon=True
        )
        self._thread.start()

    def notify(self) -> None:
        """Wake the background flusher to drain now (size-based trigger).

        Non-blocking: capture calls this when the buffer hits flush_threshold so a
        burst drains promptly instead of waiting out the interval. No-op if the
        background thread isn't running (e.g. tests flush manually).
        """
        self._wake.set()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()  # interrupt the interval wait so the loop exits promptly
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
        # Drain on the interval (2s) OR when woken by the size trigger (100 items).
        while not self._stop.is_set():
            self._wake.wait(timeout=self.interval_seconds)
            self._wake.clear()
            if self._stop.is_set():
                break
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
            # Skip while auth is broken — re-posting would just 401 again.
            if not self._auth_failed:
                self._drain_spool(result)
            items = self.client.buffer.take_all()
            if not items:
                return result
            _assign_parallel_groups(items)
            wire_items: list[dict[str, Any]] = []
            for idx, item in enumerate(items):
                try:
                    wire_items.append(self._to_wire_item(item, result))
                except DataKeyError as exc:
                    # Can't encrypt without a data key. NEVER spool plaintext and
                    # NEVER drop — hold the unprocessed items in memory and retry
                    # on the next drain (data-key failure-mode #2). Defer this item
                    # and everything after it so per-run order / chaining is kept.
                    if exc.status in (401, 403):
                        self._on_auth_failure(f"/v1/data-keys {exc.status}")
                    deferred = items[idx:]
                    self.client.buffer.requeue_front(deferred)
                    result.items_deferred += len(deferred)
                    break
            for chunk in self._chunk_batches(wire_items):
                self._ship_batch(chunk, result)
            return result

    def _chunk_batches(self, items: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
        """Split items into batches under both the item-count and 5 MB byte caps.

        Greedy/FIFO: preserves order (so each run's items stay in sequence). A
        single item larger than the byte budget is isolated in its own batch — it
        will 413 server-side, but only that one item is lost, not its neighbours.
        """
        budget = self.max_batch_bytes - _BATCH_ENVELOPE_OVERHEAD
        batches: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        current_bytes = 0
        for item in items:
            # Measure with default (spaced) JSON so we over- rather than
            # under-estimate the bytes the HTTP client will actually send.
            item_bytes = len(json.dumps(item).encode("utf-8")) + 1  # +1 for the comma
            too_many = len(current) >= self.max_items_per_batch
            too_big = current_bytes + item_bytes > budget
            if current and (too_many or too_big):
                batches.append(current)
                current = []
                current_bytes = 0
            current.append(item)
            current_bytes += item_bytes
        if current:
            batches.append(current)
        return batches

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
        redaction = self.client.redactor.apply(item.raw_payload, self.client.current_policy())
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

        # Auth is known-broken: don't POST (it would 401). Persist the encrypted
        # batch for after the key is fixed.
        if self._auth_failed:
            self._spool_or_drop(idempotency_key, body, len(items), result)
            return

        try:
            status, payload = self._post_with_retry(body, idempotency_key)
        except _ShipError:
            # Transient failure, retries exhausted → persist (ciphertext only) for
            # a later drain rather than lose it. If the spool is full, drop + flag.
            self._spool_or_drop(idempotency_key, body, len(items), result)
            return

        if status == 200:
            result.batches_sent += 1
            result.items_accepted += len(items)
        elif status == 207:
            result.batches_sent += 1
            result.items_accepted += len(payload.get("accepted_items", []))
            result.items_rejected += len(payload.get("rejected_items", []))
        elif status in (401, 403):
            # Bad/revoked key or wrong scope: surface it, stop shipping, and keep
            # the (encrypted) batch spooled for after it's fixed — never drop it.
            self._on_auth_failure(f"/v1/batches {status}")
            self._spool_or_drop(idempotency_key, body, len(items), result)
        else:  # 400 / 413 / 422: bad data, won't fix on retry → drop
            result.items_dropped += len(items)

    def _spool_or_drop(
        self, idempotency_key: str, body: dict[str, Any], count: int, result: DrainResult
    ) -> None:
        if self.client.spool.write(idempotency_key, body):
            result.items_spooled += count
        else:
            result.items_dropped += count  # spool full — data lost

    def _on_auth_failure(self, detail: str) -> None:
        if not self._auth_failed:
            self.client.emit_diagnostic("auth_failure", detail=detail, severity="error")
        self._auth_failed = True

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
        base_ms = min(self.retry.base_ms * (2**attempt), self.retry.cap_ms)
        jittered_ms: float = base_ms * self.retry.jitter()
        clamped_ms: float = min(jittered_ms, float(self.retry.cap_ms))  # hard ceiling
        return clamped_ms / 1000.0


class _ShipError(RuntimeError):
    pass


def _assign_parallel_groups(items: list[BufferedItem]) -> None:
    """Tag concurrently-dispatched tool calls with a shared ``parallel_group_id``.

    Concurrency is read structurally, not guessed: within a run, a maximal run of
    **≥2 consecutive ``tool_call`` events that share an issuing ``llm_call`` with no
    ``tool_result`` between them** was dispatched in parallel — a *sequential* call
    only ever happens after its predecessor's result, so two calls back-to-back
    must be concurrent. The adapter can't assign this at capture time (the first
    call is already buffered before the second arrives); the flusher can, because
    it sees the drained events together. Each grouped call's matching
    ``tool_result`` (linked via ``parent_event_id``) inherits the same group.

    Sound by construction (never groups sequential calls, never fabricates a
    group). Only limitation: a parallel batch split across two drains isn't
    grouped — the earlier calls already shipped — which degrades to "ungrouped"
    (honest), never to a wrong group. Mutates event dicts in place before hashing,
    so the group id is part of the committed ``event_hash``. Events already carrying
    a ``parallel_group_id`` (e.g. set by the adapter) are left untouched.
    """
    pending: dict[str, list[dict[str, Any]]] = {}  # run_id -> open consecutive tool_calls
    call_group: dict[str, str] = {}  # tool_call event_id -> assigned group_id

    def close(run_id: str) -> None:
        batch = pending.get(run_id)
        if batch and len(batch) >= 2:
            group_id = generate_parallel_group_id()
            for ev in batch:
                ev["parallel_group_id"] = group_id
                call_group[ev["event_id"]] = group_id
        pending[run_id] = []

    for item in items:
        if not isinstance(item, BufferedEvent):
            continue  # run_create/update/end don't participate
        event = item.event
        run_id = event.get("run_id", "")
        kind = event.get("action", {}).get("kind")
        if kind == "tool_call":
            if "parallel_group_id" in event:
                continue  # already grouped (adapter co-resident path) — leave it
            batch = pending.setdefault(run_id, [])
            # A different issuer means a new turn → the prior batch is closed.
            if batch and event.get("parent_event_id") != batch[0].get("parent_event_id"):
                close(run_id)
            pending.setdefault(run_id, []).append(event)
        elif kind == "tool_result":
            close(run_id)  # results close the open call batch (and assign groups)
            group_id = call_group.get(event.get("parent_event_id") or "")
            if group_id is not None and "parallel_group_id" not in event:
                event["parallel_group_id"] = group_id
        else:
            close(run_id)  # llm_call / lifecycle / other ends a concurrent batch
    for run_id in list(pending):
        close(run_id)


def _serialize(payload: Any) -> tuple[bytes, str]:
    """Serialize a redacted payload to bytes + a wire content_type.

    Total: ``default=str`` coerces anything not natively JSON-serializable
    (datetimes, custom objects) so serialization never raises — otherwise an
    unhandled error here would lose the already-drained buffer.
    """
    if isinstance(payload, bytes):
        return payload, "application/json"
    if isinstance(payload, str):
        return payload.encode("utf-8"), "text/plain"
    return json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"), "application/json"


def _uuid4() -> str:
    import uuid

    return str(uuid.uuid4())
