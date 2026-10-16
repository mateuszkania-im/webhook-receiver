import hmac

from pydantic import SecretStr

from webhook_receiver.api.errors import ApiError


def check_api_key(expected: SecretStr | None, authorization: str | None) -> None:
    if expected is None:
        return
    scheme, _, token = (authorization or "").partition(" ")
    valid = hmac.compare_digest(token.encode(), expected.get_secret_value().encode())
    if scheme.lower() != "bearer" or not valid:
        raise ApiError(401, "unauthorized", "send the API key as Authorization: Bearer <api_key>")
