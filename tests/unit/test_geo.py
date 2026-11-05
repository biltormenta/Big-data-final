import pytest

from app.geo import distancia_km, geohash_caja, geohash_centro, geohash_codificar, geohash_poligono


# valores de referencia conocidos del algoritmo de geohash
def test_geohash_conocidos():
    assert geohash_codificar(57.64911, 10.40744, 11) == "u4pruydqqvj"
    assert geohash_codificar(42.605, -5.603, 5) == "ezs42"


def test_la_celda_contiene_el_punto():
    lat, lon = 39.9526, -75.1652
    gh = geohash_codificar(lat, lon, 5)
    la0, la1, lo0, lo1 = geohash_caja(gh)
    assert la0 <= lat <= la1 and lo0 <= lon <= lo1
    clat, clon = geohash_centro(gh)
    assert geohash_codificar(clat, clon, 5) == gh


def test_poligono_de_la_celda_es_geojson_cerrado():
    anillo = geohash_poligono("dr4e3")["coordinates"][0]
    assert len(anillo) == 5 and anillo[0] == anillo[-1]


def test_distancia_philadelphia_nueva_york():
    # unos 130 km en linea recta
    assert distancia_km(39.9526, -75.1652, 40.7128, -74.0060) == pytest.approx(130, abs=5)
    assert distancia_km(10, 10, 10, 10) == 0
