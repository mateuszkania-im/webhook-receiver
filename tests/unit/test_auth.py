import pytest
from pydantic import SecretStr

from tests.fakes import API_KEY
from webhook_receiver.api.auth import check_api_key
from webhook_receiver.api.errors import ApiError

EXPECTED = SecretStr(API_KEY)


def test_matching_bearer_key_or_no_configured_key_passes() -> None:
    check_api_key(EXPECTED, f"Bearer {API_KEY}")
    check_api_key(EXPECTED, f"bearer {API_KEY}")
    check_api_key(None, None)


def test_missing_or_wrong_credentials_are_unauthorized() -> None:
    for header in (None, f"Basic {API_KEY}", "Bearer wrong-key-0123456789", "Bearer zażółć"):
        with pytest.raises(ApiError) as caught:
            check_api_key(EXPECTED, header)
        assert (caught.value.status, caught.value.code) == (401, "unauthorized")
