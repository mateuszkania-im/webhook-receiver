from pathlib import Path

import pytest
from pydantic import ValidationError

from tests.fakes import make_settings
from webhook_receiver.config import Settings


def test_inconsistent_settings_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="processing_min_s"):
        make_settings(tmp_path, processing_min_s=3.0, processing_max_s=2.0)
    with pytest.raises(ValidationError, match="heartbeat_s"):
        make_settings(tmp_path, heartbeat_s=30.0, lease_s=30.0)
    with pytest.raises(ValidationError, match="poll_interval_s"):
        make_settings(tmp_path, poll_interval_s=15.0)
    with pytest.raises(ValidationError, match="processing_timeout_s"):
        make_settings(tmp_path, processing_max_s=90.0)


def test_env_example_also_runs_without_docker() -> None:
    example = Path(__file__).parents[2] / ".env.example"
    settings = Settings(_env_file=example)
    assert not settings.db_path.is_absolute()
    assert settings.api_key is not None
