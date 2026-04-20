"""Unit tests for bootstrap/config modules."""

from unittest.mock import MagicMock, patch

from celery_app import celery_app
from celery_app import celery_config
from app import main as app_main


def test_celery_app_is_initialized():
    assert celery_app is not None
    assert celery_config.celery_app.main == "curriculum_parser"


def test_celery_config_has_expected_defaults():
    assert celery_config.celery_app.conf.task_default_retry_delay == 60
    assert celery_config.celery_app.conf.task_max_retries == 3
    assert celery_config.celery_app.conf.worker_prefetch_multiplier == 1


@patch("app.main.Base")
@patch("app.main.engine")
def test_create_tables_calls_metadata_create_all(mock_engine, mock_base):
    app_main.create_tables()
    mock_base.metadata.create_all.assert_called_once_with(bind=mock_engine)


@patch("app.main.Session")
def test_create_test_lecturer_skips_if_exists(mock_session_cls):
    mock_db = MagicMock()
    mock_session_cls.return_value.__enter__.return_value = mock_db
    mock_db.query.return_value.filter.return_value.first.return_value = object()

    app_main.create_test_lecturer()

    mock_db.add.assert_not_called()
    mock_db.commit.assert_not_called()


@patch("app.main.Session")
def test_create_test_lecturer_inserts_when_missing(mock_session_cls):
    mock_db = MagicMock()
    mock_session_cls.return_value.__enter__.return_value = mock_db
    mock_db.query.return_value.filter.return_value.first.return_value = None

    app_main.create_test_lecturer()

    mock_db.add.assert_called_once()
    mock_db.commit.assert_called_once()
