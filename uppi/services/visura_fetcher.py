from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from uppi.domain.models import VisuraMetadata


@dataclass(frozen=True)
class VisuraDecision:
    should_download: bool
    reason: str


def should_download_visura(
    *,
    force_update: bool,
    ttl_days: Optional[int],
    db_state: Optional[VisuraMetadata],
    storage_exists: bool,
    now: Optional[datetime] = None,
) -> VisuraDecision:
    if force_update:
        return VisuraDecision(True, "force_update")

    if db_state is None:
        return VisuraDecision(True, "db_missing")

    if not storage_exists:
        return VisuraDecision(True, "storage_missing")

    if ttl_days:
        reference = db_state.fetched_at
        if reference is None:
            return VisuraDecision(True, "missing_fetched_at")

        now_dt = now or datetime.now(timezone.utc)
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)

        if now_dt - reference >= timedelta(days=ttl_days):
            return VisuraDecision(True, "ttl_expired")

    return VisuraDecision(False, "cache_ok")
