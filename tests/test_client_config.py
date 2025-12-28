from uppi.config.clients import ClientConfig


def test_client_config_from_raw_basic():
    raw = {
        "LOCATORE_CF": "ABCDEF12G34H567I",
        "COMUNE": "PESCARA",
        "TIPO_CATASTO": "F",
        "UFFICIO_PROVINCIALE_LABEL": "PESCARA Territorio",
        "FORCE_UPDATE_VISURA": "true",
        "IMMOBILE_VIA": "Via Roma",
        "A1": "X",
        "CUSTOM_FIELD": "value",
    }

    cfg = ClientConfig.from_raw(
        raw,
        default_comune="PESCARA",
        default_tipo_catasto="F",
        default_ufficio_label="PESCARA Territorio",
    )

    assert cfg.locatore_cf == "ABCDEF12G34H567I"
    assert cfg.force_update_visura is True
    assert cfg.immobile_via == "Via Roma"
    assert cfg.elements["a1"] == "X"
    assert cfg.extra["CUSTOM_FIELD"] == "value"

    item = cfg.to_item_dict()
    assert item["locatore_cf"] == "ABCDEF12G34H567I"
    assert item["immobile_via"] == "Via Roma"
    assert item["a1"] == "X"
