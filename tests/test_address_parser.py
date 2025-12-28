import pytest

from uppi.parsers.address_parser import parse_address


@pytest.mark.parametrize(
    "raw,expected",
    [
        (
            "VIALE DELLA RIVIERA n. 285 Scala U Interno 1 Piano 1",
            {
                "via_type": "VIALE",
                "via_name": "DELLA RIVIERA",
                "civico": "285",
                "scala": "U",
                "interno": "1",
                "piano": "1",
            },
        ),
        (
            "Via Roma 10",
            {
                "via_type": "VIA",
                "via_name": "Roma",
                "civico": "10",
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),
        (
            "Piazza Garibaldi SNC",
            {
                "via_type": "PIAZZA",
                "via_name": "Garibaldi",
                "civico": "SNC",
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),
    ],
)

def test_parse_address(raw, expected):
    parsed = parse_address(raw)
    assert parsed.via_type == expected["via_type"]
    assert parsed.via_name == expected["via_name"]
    assert parsed.civico == expected["civico"]
    assert parsed.scala == expected["scala"]
    assert parsed.interno == expected["interno"]
    assert parsed.piano == expected["piano"]
    assert parsed.indirizzo_raw
