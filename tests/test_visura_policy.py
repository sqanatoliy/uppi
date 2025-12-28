from datetime import datetime, timedelta, timezone

from uppi.domain.models import VisuraMetadata
from uppi.services.visura_fetcher import should_download_visura


def test_should_download_when_missing():
    decision = should_download_visura(
        force_update=False,
        ttl_days=None,
        db_state=None,
        storage_exists=False,
    )
    assert decision.should_download is True
    assert decision.reason == "db_missing"


def test_should_download_when_ttl_expired():
    fetched_at = datetime.now(timezone.utc) - timedelta(days=10)
    db_state = VisuraMetadata(
        cf="ABC",
        pdf_bucket="visure",
        pdf_object="visure/ABC.pdf",
        checksum_sha256=None,
        fetched_at=fetched_at,
    )
    decision = should_download_visura(
        force_update=False,
        ttl_days=7,
        db_state=db_state,
        storage_exists=True,
    )
    assert decision.should_download is True
    assert decision.reason == "ttl_expired"


def test_should_skip_when_cache_ok():
    fetched_at = datetime.now(timezone.utc) - timedelta(days=2)
    db_state = VisuraMetadata(
        cf="ABC",
        pdf_bucket="visure",
        pdf_object="visure/ABC.pdf",
        checksum_sha256=None,
        fetched_at=fetched_at,
    )
    decision = should_download_visura(
        force_update=False,
        ttl_days=7,
        db_state=db_state,
        storage_exists=True,
    )
    assert decision.should_download is False
    assert decision.reason == "cache_ok"
