import hashlib

from webhook_receiver.idempotency import payload_hash


def test_hash_is_sha256_of_sorted_compact_utf8_json() -> None:
    canonical = '{"event_type":"e","payload":{"a":1,"name":"Zażółć"}}'
    expected = hashlib.sha256(canonical.encode()).hexdigest()
    assert payload_hash("e", {"name": "Zażółć", "a": 1}) == expected


def test_integer_and_float_are_different_payloads() -> None:
    assert payload_hash("e", {"amount": 1}) != payload_hash("e", {"amount": 1.0})
