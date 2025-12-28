from uppi.config.clients import parse_client_row


def test_parse_client_row_defaults_and_force_flag():
    raw = {
        "LOCATORE_CF": "AAAAAA11A11A111A",
        "FORCE_UPDATE_VISURA": "yes",
        "IMMOBILE_VIA": "Via Roma",
        "A1": "X",
    }

    client = parse_client_row(raw)

    assert client.locatore_cf == "AAAAAA11A11A111A"
    assert client.force_update_visura is True
    assert client.comune == "PESCARA"
    assert client.tipo_catasto == "F"
    assert client.ufficio_label == "PESCARA Territorio"
    assert client.immobile_via == "Via Roma"
    assert client.a1 == "X"
