"""Irreversible stream sealing: freeze the applied event log behind a SHA-256 digest.

A seal request carries a client-assigned ``seal_id`` (a **global** idempotency
key) and an ``expected_next_sequence`` optimistic-concurrency token. A stream
can be sealed only when it:

1. already exists (submitting an event implicitly creates a stream; sealing
   never does);
2. has no pending (buffered behind a sequence gap) events;
3. is currently at exactly ``expected_next_sequence``.

When all preconditions hold, every applied event is read in sequence order and
each ``[sequence, event_id, delta]`` triple is encoded as a compact,
whitespace-free, Unicode-preserving JSON line (``json.dumps(..., separators=(",",
":"), ensure_ascii=False)``). The lines are joined with ``"\n"`` and hashed
with SHA-256; the digest, event count and final sequence are stored in the
``seals`` ledger together with ``sealed_at`` (POSIX milliseconds) — all inside
one ``BEGIN IMMEDIATE`` transaction. Any failure rolls the whole operation
back and leaves no seal record behind.

A sealed stream is frozen forever: ordinary event submission, transfers and
reversals may no longer *append* events to it. Requests for operations that
already completed before sealing are still idempotent replays and keep
returning their stored original results. Querying, consuming (claim/ack) and
using the stream as a fork source continue to work unchanged.

A retried request with the same ``seal_id`` returns the stored original seal
(HTTP 200); the same ``seal_id`` against a different stream, a second
``seal_id`` for an already sealed stream, pending events or a version mismatch
are all ``409``. Concurrent requests serialise on the write lock, so exactly
one seal row is ever written per stream.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass

from . import db


class SealError(Exception):
    """Raised when a seal request is invalid or clashes (HTTP 409)."""

    def __init__(self, code: str, message: str, status_code: int = 409):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


@dataclass(frozen=True)
class Seal:
    stream_id: str
    seal_id: str
    next_sequence: int
    event_count: int
    sha256: str
    sealed_at: int
    replayed: bool

    def as_dict(self) -> dict:
        return {
            "stream_id": self.stream_id,
            "seal_id": self.seal_id,
            "next_sequence": self.next_sequence,
            "event_count": self.event_count,
            "sha256": self.sha256,
            "sealed_at": self.sealed_at,
        }


def canonical_payload(rows) -> str:
    """Render applied events as newline-joined canonical JSON lines.

    ``rows`` is an iterable of ``(sequence, event_id, delta)`` triples already
    ordered by ascending sequence. Each triple is compact JSON with no
    whitespace, non-ASCII characters emitted verbatim (``ensure_ascii=False``)
    so the digest covers the original Unicode event ids byte-for-byte. There is
    no trailing newline.
    """
    return "\n".join(
        json.dumps(
            [sequence, event_id, delta],
            separators=(",", ":"),
            ensure_ascii=False,
        )
        for sequence, event_id, delta in rows
    )


def is_sealed(conn: sqlite3.Connection, stream_id: str) -> bool:
    """Return True when ``stream_id`` already has a seal row (caller holds txn)."""
    return conn.execute(
        "SELECT 1 FROM seals WHERE stream_id = ?", (stream_id,)
    ).fetchone() is not None


def create_seal(
    stream_id: str, seal_id: str, expected_next_sequence: int
) -> Seal | None:
    """Seal ``stream_id`` at ``expected_next_sequence`` with a SHA-256 digest.

    Returns the created/existing :class:`Seal`, or ``None`` when the stream
    does not exist (HTTP 404). Raises :class:`SealError` for pending events
    (``stream_has_pending_events``), a version mismatch
    (``sequence_mismatch``), a repeated seal id on another stream
    (``seal_replay_conflict``) or a second seal on the same stream
    (``stream_already_sealed``), all HTTP 409.
    """
    with db.write_transaction() as conn:
        stream = conn.execute(
            "SELECT stream_id, balance, next_sequence FROM streams WHERE stream_id = ?",
            (stream_id,),
        ).fetchone()
        if stream is None:
            return None

        # seal_id is the global idempotency key: an identical request replays
        # the stored seal; the same id on another stream is a 409.
        by_id = conn.execute(
            "SELECT stream_id, seal_id, sequence, event_count, sha256, sealed_at "
            "FROM seals WHERE seal_id = ?",
            (seal_id,),
        ).fetchone()
        if by_id is not None:
            if by_id["stream_id"] != stream_id:
                raise SealError(
                    "seal_replay_conflict",
                    f"seal_id {seal_id!r} already seals a different stream "
                    f"{by_id['stream_id']!r}",
                )
            return Seal(
                stream_id=by_id["stream_id"],
                seal_id=by_id["seal_id"],
                next_sequence=by_id["sequence"],
                event_count=by_id["event_count"],
                sha256=by_id["sha256"],
                sealed_at=by_id["sealed_at"],
                replayed=True,
            )

        # A different seal_id for an already sealed stream can never reseal it.
        existing = conn.execute(
            "SELECT seal_id FROM seals WHERE stream_id = ?", (stream_id,)
        ).fetchone()
        if existing is not None:
            raise SealError(
                "stream_already_sealed",
                f"stream {stream_id!r} is already sealed by seal_id "
                f"{existing['seal_id']!r}; sealing is irreversible",
            )

        # Only a fully caught-up log is sealable: pending (gap-buffered)
        # events would change the canonical byte stream once their gap closes.
        pending = conn.execute(
            "SELECT 1 FROM events WHERE stream_id = ? AND applied = 0 LIMIT 1",
            (stream_id,),
        ).fetchone()
        if pending is not None:
            raise SealError(
                "stream_has_pending_events",
                f"stream {stream_id!r} has buffered events behind a sequence "
                "gap; close the gap before sealing",
            )

        # Optimistic concurrency: the caller must name the exact version.
        next_sequence = stream["next_sequence"]
        if next_sequence != expected_next_sequence:
            raise SealError(
                "sequence_mismatch",
                f"expected_next_sequence {expected_next_sequence} does not "
                f"match current next_sequence {next_sequence}",
            )

        rows = conn.execute(
            "SELECT sequence, event_id, delta FROM events "
            "WHERE stream_id = ? AND applied = 1 ORDER BY sequence ASC",
            (stream_id,),
        ).fetchall()
        triples = [(row["sequence"], row["event_id"], row["delta"]) for row in rows]
        # Applied events occupy exactly [0, next_sequence) with no gaps after
        # the pending check above, so the count equals the final sequence.
        payload = canonical_payload(triples)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        sealed_at = int(time.time() * 1000)

        conn.execute(
            "INSERT INTO seals (stream_id, seal_id, sequence, event_count, "
            "sha256, sealed_at) VALUES (?, ?, ?, ?, ?, ?)",
            (stream_id, seal_id, next_sequence, len(triples), digest, sealed_at),
        )

        return Seal(
            stream_id=stream_id,
            seal_id=seal_id,
            next_sequence=next_sequence,
            event_count=len(triples),
            sha256=digest,
            sealed_at=sealed_at,
            replayed=False,
        )
