"""On-disk spool for durability across process restarts.

Append-only files under ``~/.runfile/spool/`` holding ONLY already-redacted,
already-encrypted batches — ciphertext, wrapped data keys, and event metadata.
Plaintext payloads and plaintext data keys are NEVER written here. A drain
reader ships spooled batches when the network is healthy. Bounded (default
1 GB); on overflow, drop whole runs atomically (never mid-run events).

Skeleton: signatures in place; bodies TODO.
"""

from __future__ import annotations

from pathlib import Path

DEFAULT_SPOOL_DIR = Path.home() / ".runfile" / "spool"
DEFAULT_MAX_BYTES = 1024 * 1024 * 1024  # 1 GB


class Spool:
    def __init__(
        self,
        *,
        directory: Path = DEFAULT_SPOOL_DIR,
        max_bytes: int = DEFAULT_MAX_BYTES,
    ) -> None:
        self._dir = directory
        self._max_bytes = max_bytes

    def write(self, encrypted_batch: bytes) -> None:
        """Append an encrypted batch (ciphertext only) for later retry."""
        raise NotImplementedError

    async def drain(self) -> None:
        """Read spooled batches and ship them when the network is available."""
        raise NotImplementedError
