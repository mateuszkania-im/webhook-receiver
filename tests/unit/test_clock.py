from webhook_receiver.clock import to_rfc3339


def test_to_rfc3339_uses_utc_and_milliseconds() -> None:
    assert to_rfc3339(1_760_529_603_512) == "2025-10-15T12:00:03.512Z"
    assert to_rfc3339(0) == "1970-01-01T00:00:00.000Z"
