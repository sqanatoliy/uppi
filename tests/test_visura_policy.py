from datetime import datetime, timedelta

from uppi.services.db_repo import VisuraState
from uppi.services.visura_policy import should_download_visura


def test_should_download_force_update():
    decision = should_download_visura(
        force_update=True,
        ttl_days=30,
        db_state=None,
        minio_exists=False,
    )
    assert decision.should_download is True
    assert decision.reason == "force_update_visura"


def test_should_download_missing_state():
    decision = should_download_visura(
        force_update=False,
        ttl_days=30,
        db_state=None,
        minio_exists=False,
    )
    assert decision.should_download is True
    assert decision.reason == "missing_db_record"


def test_should_download_ttl_expired():
    state = VisuraState(
        cf="CF",
        pdf_bucket="bucket",
        pdf_object="obj",
        fetched_at=datetime.utcnow() - timedelta(days=40),
    )
    decision = should_download_visura(
        force_update=False,
        ttl_days=30,
        db_state=state,
        minio_exists=True,
    )
    assert decision.should_download is True
    assert decision.reason == "ttl_expired"


def test_should_download_fresh():
    state = VisuraState(
        cf="CF",
        pdf_bucket="bucket",
        pdf_object="obj",
        fetched_at=datetime.utcnow() - timedelta(days=5),
    )
    decision = should_download_visura(
        force_update=False,
        ttl_days=30,
        db_state=state,
        minio_exists=True,
    )
    assert decision.should_download is False
    assert decision.reason == "fresh_enough"
