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
                "via_num": "285",
                "scala": "U",
                "interno": "1",
                "piano": "1",
            },
        ),
        (
            "VIA XX SETTEMBRE 15",
            {
                "via_type": "VIA",
                "via_name": "XX SETTEMBRE",
                "via_num": "15",
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),
        (
            "PIAZZA GARIBALDI SNC",
            {
                "via_type": "PIAZZA",
                "via_name": "GARIBALDI",
                "via_num": None,
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),
    ],
)

def test_parse_address_components(raw, expected):
    parsed = parse_address(raw)

    assert parsed.via_type == expected["via_type"]
    assert parsed.via_name == expected["via_name"]
    assert parsed.via_num == expected["via_num"]
    assert parsed.scala == expected["scala"]
    assert parsed.interno == expected["interno"]
    assert parsed.piano == expected["piano"]
    assert parsed.indirizzo_raw == raw
