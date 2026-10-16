import signal
import sys
from types import FrameType

import uvicorn
from pydantic import ValidationError

from webhook_receiver.api.errors import validation_details
from webhook_receiver.app import create_app
from webhook_receiver.config import Settings

# Below Docker's default 10 s stop period, so shutdown finishes before SIGKILL.
GRACEFUL_SHUTDOWN_S = 8


def main() -> None:
    try:
        settings = Settings()
    except ValidationError as exc:
        print(f"invalid configuration:\n{_describe(exc)}", file=sys.stderr)
        raise SystemExit(2) from exc
    previous = signal.signal(signal.SIGTERM, _exit_after_graceful_shutdown)
    try:
        uvicorn.run(
            create_app(settings),
            host=settings.host,
            port=settings.port,
            workers=1,
            timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_S,
            log_config=None,
            access_log=False,
        )
    finally:
        signal.signal(signal.SIGTERM, previous)


# Pydantic's default message echoes the rejected input, which can be the API key, so
# only locations and messages are printed.
def _describe(exc: ValidationError) -> str:
    errors = validation_details(exc.errors(include_input=False, include_url=False))["errors"]
    return "\n".join(f"{error['loc'] or 'settings'}: {error['message']}" for error in errors)


# uvicorn re-raises SIGTERM after its graceful shutdown; without this handler the
# process would then die with -15 instead of exiting 0 as the spec requires.
def _exit_after_graceful_shutdown(_signum: int, _frame: FrameType | None) -> None:
    raise SystemExit(0)


if __name__ == "__main__":
    main()
