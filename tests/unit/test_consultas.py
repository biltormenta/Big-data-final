from datetime import datetime

import pytest

from app.consultas import (ParametroInvalido, a_json, validar_categoria, validar_limite, validar_min_dias,
                           validar_poligono, validar_punto, validar_radio)

CAJA = {"type": "Polygon", "coordinates": [[[-75.2, 39.9], [-75.1, 39.9], [-75.1, 40.0], [-75.2, 40.0], [-75.2, 39.9]]]}


def test_punto_valido_e_invalido():
    assert validar_punto("39.9", "-75.1") == (39.9, -75.1)
    for lat, lon in (("abc", 1), (95, 1), (1, 200), (None, None)):
        with pytest.raises(ParametroInvalido):
            validar_punto(lat, lon)


def test_radio_limite_y_categoria():
    assert validar_radio("500") == 500
    for r in (0, -5, 200000, "x", None):
        with pytest.raises(ParametroInvalido):
            validar_radio(r)
    assert validar_limite(None) == 100 and validar_limite("20") == 20 and validar_limite(10 ** 9) == 5000
    with pytest.raises(ParametroInvalido):
        validar_limite(0)
    assert validar_categoria(None) is None and validar_categoria(" Pizza ") == "Pizza"
    with pytest.raises(ParametroInvalido):
        validar_categoria("x" * 100)


def test_min_dias():
    assert validar_min_dias("5000") == 5000
    assert validar_min_dias(None) > 0
    for v in ("abc", -1):
        with pytest.raises(ParametroInvalido):
            validar_min_dias(v)


def test_poligono_suelto_y_envuelto_en_feature():
    assert validar_poligono(CAJA) == CAJA
    assert validar_poligono({"type": "Feature", "properties": {}, "geometry": CAJA}) == CAJA


def test_poligono_invalido():
    abierto = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1]]]}
    for malo in ({"type": "Point", "coordinates": [0, 0]}, abierto, {"type": "Polygon", "coordinates": []}, "texto"):
        with pytest.raises(ParametroInvalido):
            validar_poligono(malo)


def test_a_json_convierte_fechas_anidadas():
    assert a_json({"vida": {"primera": datetime(2020, 1, 2)}}) == {"vida": {"primera": "2020-01-02T00:00:00"}}
