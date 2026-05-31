"""On-disk spool for durability across process restarts.

When a batch can't ship (transient failure, retries exhausted), the flusher
writes it here and moves on; a later drain re-sends it. Files hold ONLY
already-redacted, already-encrypted batch bodies — ciphertext, wrapped data
keys, and event metadata, plus the batch's Idempotency-Key so a re-send dedupes
server-side. Plaintext payloads and plaintext data keys are NEVER written here.

Bounded (default 1 GB). On overflow the spool refuses the write (the caller drops
the batch and emits a diagnostic) rather than evicting already-durable data —
the flusher never silently loses a *partial* run to make room; it loses a clean,
flagged whole batch.

The spool only manages files; the flusher owns the HTTP transport.
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_SPOOL_DIR = Path.home() / ".runfile" / "spool"
DEFAULT_MAX_BYTES = 1024 * 1024 * 1024  # 1 GB


@dataclass
class SpooledBatch:
    path: Path
    idempotency_key: str
    body: dict[str, Any]


class Spool:
    """Append-only, bounded, file-backed queue of un-shipped (encrypted) batches."""

    def __init__(
        self,
        *,
        directory: Path = DEFAULT_SPOOL_DIR,
        max_bytes: int = DEFAULT_MAX_BYTES,
    ) -> None:
        self.directory = Path(directory)
        self.max_bytes = max_bytes
        self._lock = threading.Lock()
        self._seq = 0

    def write(self, idempotency_key: str, body: dict[str, Any]) -> bool:
        """Persist a batch (ciphertext only). Returns False if the spool is full."""
        payload = json.dumps({"idempotency_key": idempotency_key, "body": body}).encode()
        with self._lock:
            self.directory.mkdir(parents=True, exist_ok=True)
            if self._size_bytes() + len(payload) > self.max_bytes:
                return False
            # Monotonic, zero-padded name preserves FIFO order across a process.
            self._seq += 1
            name = f"{self._seq:020d}.json"
            tmp = self.directory / (name + ".tmp")
            tmp.write_bytes(payload)
            tmp.rename(self.directory / name)  # atomic publish
            return True

    def entries(self) -> list[SpooledBatch]:
        """Spooled batches in FIFO order."""
        if not self.directory.exists():
            return []
        out: list[SpooledBatch] = []
        for path in sorted(self.directory.glob("*.json")):
            try:
                data = json.loads(path.read_bytes())
            except (OSError, ValueError):
                continue
            out.append(
                SpooledBatch(path=path, idempotency_key=data["idempotency_key"], body=data["body"])
            )
        return out

    def delete(self, path: Path) -> None:
        with self._lock:
            try:
                os.remove(path)
            except OSError:
                pass

    def _size_bytes(self) -> int:
        if not self.directory.exists():
            return 0
        return sum(p.stat().st_size for p in self.directory.glob("*.json"))

    def size_bytes(self) -> int:
        with self._lock:
            return self._size_bytes()
