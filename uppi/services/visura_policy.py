from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from uppi.services.db_repo import VisuraState


@dataclass(frozen=True)
class VisuraDecision:
    should_download: bool
    reason: str


def should_download_visura(
    *,
    force_update: bool,
    ttl_days: Optional[int],
    db_state: Optional[VisuraState],
    minio_exists: bool,
    now: Optional[datetime] = None,
) -> VisuraDecision:
    if force_update:
        return VisuraDecision(True, "force_update_visura")

    if db_state is None:
        return VisuraDecision(True, "missing_db_record")

    if not minio_exists:
        return VisuraDecision(True, "missing_minio_object")

    if ttl_days is None or ttl_days == 0:
        return VisuraDecision(False, "ttl_disabled")

    if db_state.fetched_at is None:
        return VisuraDecision(True, "missing_fetched_at")

    now = now or datetime.utcnow()
    if now - db_state.fetched_at > timedelta(days=ttl_days):
        return VisuraDecision(True, "ttl_expired")

    return VisuraDecision(False, "fresh_enough")
