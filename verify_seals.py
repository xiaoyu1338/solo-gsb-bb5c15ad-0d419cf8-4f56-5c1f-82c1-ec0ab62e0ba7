"""Stdlib-only smoke test for irreversible stream sealing (run without FastAPI)."""

import hashlib
import json
import os
import tempfile
import threading
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "seals.db")

from app import consumers, db, forks, repo, reversals, seals, transfers  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


def expected_digest(triples):
    payload = "\n".join(
        json.dumps(t, separators=(",", ":"), ensure_ascii=False) for t in triples
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# Seed: applied events 0..2 with a Unicode event id.
repo.submit_event("acct", "e0", 0, 100)
repo.submit_event("acct", "e1", 1, -30)
repo.submit_event("acct", "事件-2", 2, 20)  # balance 90, next_sequence 3
triples = [(0, "e0", 100), (1, "e1", -30), (2, "事件-2", 20)]

# 1. happy path: digest matches the canonical, whitespace-free Unicode JSON lines
s = seals.create_seal("acct", "seal-1", 3)
want = expected_digest(triples)
check("seal created", not s.replayed, str(s.as_dict()))
check(
    "seal fields/digest",
    s.next_sequence == 3
    and s.event_count == 3
    and s.sha256 == want
    and s.sealed_at > 0,
    str((s.sha256, want)),
)
# The canonical line for the Unicode event keeps the characters verbatim.
check(
    "canonical line keeps unicode, no whitespace",
    seals.canonical_payload(triples).split("\n")[2] == '[2,"事件-2",20]',
    seals.canonical_payload(triples).split("\n")[2],
)

# 2. idempotent replay: same request returns the original seal
s2 = seals.create_seal("acct", "seal-1", 3)
check("replay flagged", s2.replayed)
check(
    "replay returns original result",
    s2.sha256 == s.sha256
    and s2.next_sequence == 3
    and s2.event_count == 3
    and s2.sealed_at == s.sealed_at,
)
# A replay with a (now impossible to meet) wrong version is still the same
# request: seal_id wins and the stored seal is returned unchanged.
s2b = seals.create_seal("acct", "seal-1", 999)
check("replay ignores version param once sealed", s2b.replayed and s2b.sha256 == s.sha256)

# 3. same seal_id on a different stream -> 409 seal_replay_conflict
repo.submit_event("other", "o0", 0, 1)
try:
    seals.create_seal("other", "seal-1", 1)
    check("seal id drift rejected", False)
except seals.SealError as e:
    check(
        "seal id drift -> seal_replay_conflict",
        e.code == "seal_replay_conflict" and e.status_code == 409,
        str((e.code, e.status_code)),
    )

# 4. a second seal_id on the sealed stream -> 409 stream_already_sealed
try:
    seals.create_seal("acct", "seal-2", 3)
    check("second seal rejected", False)
except seals.SealError as e:
    check(
        "second seal -> stream_already_sealed",
        e.code == "stream_already_sealed" and e.status_code == 409,
        str((e.code, e.status_code)),
    )

# 5. sealing is frozen: new events rejected; replays still work
try:
    repo.submit_event("acct", "e3", 3, 5)
    check("new event on sealed stream rejected", False)
except repo.ConflictError as e:
    check(
        "new event -> stream_sealed",
        e.code == "stream_sealed",
        str(e.code),
    )
state, replayed = repo.submit_event("acct", "e0", 0, 100)
check("existing event replay on sealed stream ok", replayed and state.balance == 90)
check("sealed stream unchanged", state.next_sequence == 3)

# 6. transfers: new transfer touching a sealed stream -> 409; replay still ok
repo.submit_event("bank", "b0", 0, 500)
txn = transfers.create_transfer("txn-before", "bank", "other", 10)
check("transfer before sealing completed", not txn.replayed)
# seal "other" (now balance 10, next_sequence 2)
s_other = seals.create_seal("other", "seal-other", 2)
check("other sealed", not s_other.replayed and s_other.event_count == 2)
try:
    transfers.create_transfer("txn-after", "bank", "other", 5)
    check("new transfer into sealed stream rejected", False)
except transfers.TransferError as e:
    check(
        "new transfer -> stream_sealed",
        e.code == "stream_sealed" and e.status_code == 409,
        str((e.code, e.status_code)),
    )
try:
    transfers.create_transfer("txn-from-sealed", "other", "bank", 1)
    check("debit from sealed stream rejected", False)
except transfers.TransferError as e:
    check("debit sealed side -> stream_sealed", e.code == "stream_sealed", str(e.code))
rt = transfers.create_transfer("txn-before", "bank", "other", 10)
check(
    "completed transfer replays against sealed stream",
    rt.replayed
    and rt.source_sequence == txn.source_sequence
    and rt.target_sequence == txn.target_sequence,
)
# The rejected transfer neither debited bank nor created a ledger row.
with db.read_connection() as conn:
    check(
        "rejected transfer not recorded",
        conn.execute(
            "SELECT 1 FROM transfers WHERE transfer_id = 'txn-after'"
        ).fetchone()
        is None,
    )

# 7. reversals: new reversal on sealed stream -> 409; replay still ok
repo.submit_event("rev-src", "r0", 0, 40)
rev = reversals.create_reversal("rev-src", "r0", "rev-before")
check("reversal before sealing completed", not rev.replayed)
s_rev = seals.create_seal("rev-src", "seal-rev", 2)
check("rev-src sealed", not s_rev.replayed)
try:
    reversals.create_reversal("rev-src", "reversal:rev-before", "rev-after")
    check("new reversal on sealed stream rejected", False)
except reversals.ReversalError as e:
    check("new reversal -> stream_sealed", e.code == "stream_sealed", str(e.code))
rr = reversals.create_reversal("rev-src", "r0", "rev-before")
check(
    "completed reversal replays on sealed stream",
    rr.replayed and rr.sequence == rev.sequence and rr.delta == rev.delta,
)

# 8. sealed streams remain queryable, consumable and usable as fork sources
state = repo.get_stream_state("acct")
check("sealed stream queryable", state.balance == 90 and state.next_sequence == 3)
claim = consumers.claim("acct", "w", 100)
check(
    "sealed stream consumable",
    [(ev.sequence, ev.event_id, ev.delta) for ev in claim.events] == triples,
    str(claim),
)
check("sealed stream ack", consumers.ack("acct", "w", claim.lease_token) == 3)
f = forks.create_fork("acct", "fork-sealed", "acct-clone", 3)
check(
    "sealed stream is a fork source",
    f.balance == 90 and f.next_sequence == 3 and not f.replayed,
    str(f),
)
clone_claim = consumers.claim("acct-clone", "w", 100)
check("fork copied sealed log", len(clone_claim.events) == 3)
# Forks never seal the target; the clone keeps accepting events.
ok, _ = repo.submit_event("acct-clone", "c3", 3, 7)
check("forked target stays appendable", ok.next_sequence == 4 and ok.balance == 97)

# 9. pending events block sealing; closing the gap allows it
repo.submit_event("pend", "p1", 1, 5)  # buffered behind gap at 0
try:
    seals.create_seal("pend", "seal-pend", 1)
    check("seal with pending rejected", False)
except seals.SealError as e:
    check(
        "pending -> stream_has_pending_events",
        e.code == "stream_has_pending_events" and e.status_code == 409,
        str((e.code, e.status_code)),
    )
# Failed seal left no ledger row and the same seal_id is still usable.
with db.read_connection() as conn:
    check(
        "failed seal wrote nothing",
        conn.execute("SELECT 1 FROM seals WHERE seal_id = 'seal-pend'").fetchone()
        is None,
    )
repo.submit_event("pend", "p0", 0, 10)  # catch-up -> balance 15, next 2
sp = seals.create_seal("pend", "seal-pend", 2)
check(
    "seal after gap closes",
    not sp.replayed
    and sp.event_count == 2
    and sp.sha256 == expected_digest([(0, "p0", 10), (1, "p1", 5)]),
)

# 10. version mismatch -> 409 sequence_mismatch
repo.submit_event("ver", "v0", 0, 1)
for bad in (0, 2):
    try:
        seals.create_seal("ver", f"seal-ver-{bad}", bad)
        check(f"mismatch {bad} rejected", False)
    except seals.SealError as e:
        check(
            f"mismatch {bad} -> sequence_mismatch",
            e.code == "sequence_mismatch" and e.status_code == 409,
        )
# Correct version with a fresh seal_id then succeeds; failed ids were not burned.
sv = seals.create_seal("ver", "seal-ver-2", 1)
check("seal at exact version", not sv.replayed and sv.sha256 == expected_digest([(0, "v0", 1)]))

# 11. missing stream -> None (HTTP 404 at the API layer)
check("seal missing stream -> None", seals.create_seal("ghost", "g", 0) is None)

# 12. empty (existing) stream seals to SHA-256 of the empty byte string
# Create an empty stream via a fork with through_sequence 0.
fe = forks.create_fork("acct", "fork-empty", "empty", 0)
check("empty fork source", fe.balance == 0 and fe.next_sequence == 0)
se_ = seals.create_seal("empty", "seal-empty", 0)
empty_digest = hashlib.sha256(b"").hexdigest()
check(
    "empty stream digest",
    not se_.replayed and se_.event_count == 0 and se_.sha256 == empty_digest,
    str((se_.sha256, empty_digest)),
)

# 13. single-line payload has no trailing newline (verify digest construction)
one_line_digest = hashlib.sha256('[0,"x",1]'.encode("utf-8")).hexdigest()
check(
    "canonical payload no trailing newline",
    seals.canonical_payload([(0, "x", 1)]) == '[0,"x",1]'
    and hashlib.sha256(seals.canonical_payload([(0, "x", 1)]).encode("utf-8")).hexdigest()
    == one_line_digest,
)

# 14. concurrent identical seal requests: exactly one commit, rest replay
repo.submit_event("race", "q0", 0, 1)
results, errors = [], []


def do_seal():
    try:
        results.append(seals.create_seal("race", "seal-race", 1))
    except Exception:
        errors.append(traceback.format_exc())


threads = [threading.Thread(target=do_seal) for _ in range(20)]
for th in threads:
    th.start()
for th in threads:
    th.join()
check("concurrent seals no errors", not errors, str(errors[:1]))
check(
    "concurrent seals one commit",
    len(results) == 20
    and sum(1 for x in results if not x.replayed) == 1
    and sum(1 for x in results if x.replayed) == 19
    and len({x.sha256 for x in results}) == 1,
)

# 15. concurrent distinct seal_ids on one stream: one wins, others 409
repo.submit_event("race2", "q0", 0, 1)
wins, codes, derrors = [], [], []


def do_distinct(n):
    try:
        seals.create_seal("race2", f"seal-many-{n}", 1)
        wins.append(n)
    except seals.SealError as e:
        codes.append(e.code)
    except Exception:
        derrors.append(traceback.format_exc())


threads = [threading.Thread(target=do_distinct, args=(n,)) for n in range(20)]
for th in threads:
    th.start()
for th in threads:
    th.join()
check("distinct seals no unexpected errors", not derrors, str(derrors[:1]))
check("exactly one seal winner", len(wins) == 1, str(wins))
check(
    "losers all stream_already_sealed",
    len(codes) == 19 and set(codes) == {"stream_already_sealed"},
    str(set(codes)),
)

# 16. persistence: the ledger row survives exactly as stored
with db.read_connection() as conn:
    row = conn.execute(
        "SELECT stream_id, seal_id, sequence, event_count, sha256, sealed_at "
        "FROM seals WHERE stream_id = 'acct'"
    ).fetchone()
    check(
        "ledger persisted",
        tuple(row)[:5] == ("acct", "seal-1", 3, 3, want) and row["sealed_at"] == s.sealed_at,
        str(tuple(row) if row else None),
    )
    # Exactly one seal row per sealed stream.
    check(
        "one seal row per stream",
        conn.execute(
            "SELECT COUNT(*) FROM seals WHERE stream_id = 'acct'"
        ).fetchone()[0]
        == 1,
    )

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL SEAL CHECKS PASSED")
