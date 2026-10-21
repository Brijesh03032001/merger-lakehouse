"""Small dependency-free SCD2 reference model used by CI tests."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping


def _hash(row: Mapping[str, object], tracked: tuple[str, ...]) -> str:
    payload = "||".join("__NULL__" if row.get(name) is None else str(row[name]) for name in tracked)
    return hashlib.sha256(payload.encode()).hexdigest()


def merge_history(
    history: Iterable[Mapping[str, object]],
    incoming: Iterable[Mapping[str, object]],
    *,
    key: str,
    tracked: tuple[str, ...],
    effective_at: str,
) -> list[dict[str, object]]:
    """Return SCD2 history after applying a deterministic source snapshot."""
    result = [dict(row) for row in history]
    current = {row[key]: row for row in result if row["is_current"]}

    for raw in incoming:
        row = dict(raw)
        attribute_hash = _hash(row, tracked)
        active = current.get(row[key])
        if active and active["attribute_hash"] == attribute_hash:
            continue
        if active:
            active["is_current"] = False
            active["effective_to"] = effective_at
        version = {
            key: row[key],
            **{name: row.get(name) for name in tracked},
            "attribute_hash": attribute_hash,
            "effective_from": effective_at,
            "effective_to": None,
            "is_current": True,
        }
        result.append(version)
        current[row[key]] = version
    return result

