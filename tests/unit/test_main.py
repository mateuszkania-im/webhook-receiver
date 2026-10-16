import signal
from pathlib import Path

import pytest

from webhook_receiver import __main__ as entry

LIVE_KEY = "live-7Hq2LmZx9V"


def test_invalid_configuration_exits_2_without_printing_the_value(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WEBHOOK_API_KEY", LIVE_KEY)
    with pytest.raises(SystemExit) as exited:
        entry.main()
    err = capsys.readouterr().err
    assert exited.value.code == 2
    assert "api_key: Value should have at least 16 items after validation" in err
    assert LIVE_KEY not in err
    assert "input_value" not in err


def test_sigterm_reraised_after_graceful_shutdown_exits_zero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("uvicorn.run", lambda app, **kwargs: signal.raise_signal(signal.SIGTERM))
    with pytest.raises(SystemExit) as exited:
        entry.main()
    assert exited.value.code == 0
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL
