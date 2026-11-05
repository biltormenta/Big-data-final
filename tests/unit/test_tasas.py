from datetime import datetime

import pytest

from app.tasas import etiqueta_sentimiento, ic_poisson, parse_fecha, tasa_cierre, vida_negocio


# la formula es la misma de la AFR de discos: cierres / (dias-negocio / 365) * 100
def test_formula_de_la_tasa_de_cierre():
    # 10 negocios-año (3650 dias) y 1 cierre -> 1 cierre por cada 10 años = 10 % anual
    assert tasa_cierre(1, 3650) == pytest.approx(10.0)
    # 5 cierres en 36500 dias (100 negocios-año) = 5 % anual
    assert tasa_cierre(5, 36500) == pytest.approx(5.0)
    assert tasa_cierre(0, 1000) == 0


# no es la proporcion de cerrados: con 1 cierre en 2 negocios la tasa cambia si los negocios viven mas o menos
def test_la_tasa_depende_de_los_dias_no_de_la_proporcion():
    corto = tasa_cierre(1, 2 * 365)
    largo = tasa_cierre(1, 2 * 3650)
    assert corto == pytest.approx(10 * largo)


def test_sin_dias_no_hay_tasa():
    assert tasa_cierre(3, 0) is None


def test_intervalo_contiene_la_tasa_y_es_mas_ancho_con_pocos_cierres():
    inf1, sup1 = ic_poisson(2, 36500)
    inf2, sup2 = ic_poisson(200, 3650000)
    assert inf1 < tasa_cierre(2, 36500) < sup1
    assert (sup1 - inf1) / tasa_cierre(2, 36500) > (sup2 - inf2) / tasa_cierre(200, 3650000)
    assert ic_poisson(0, 1000)[0] == 0


def test_vida_cerrado_es_evento_y_abierto_es_censurado():
    a, b = datetime(2015, 1, 1), datetime(2018, 1, 1)
    cerrado = vida_negocio(a, b, 0)
    abierto = vida_negocio(a, b, 1)
    assert cerrado["evento"] == 1 and cerrado["censurado"] is False
    assert abierto["evento"] == 0 and abierto["censurado"] is True
    assert cerrado["dias_negocio"] == (b - a).days == 1096


def test_vida_con_una_sola_resena_cuenta_un_dia():
    f = datetime(2020, 5, 5)
    assert vida_negocio(f, f, 1)["dias_negocio"] == 1


def test_etiquetas_de_sentimiento():
    assert [etiqueta_sentimiento(i) for i in (1, 2, 3, 4, 5)] == ["negativo", "negativo", "neutral", "positivo", "positivo"]


def test_parse_fecha():
    assert parse_fecha("2018-07-07 22:09:11") == datetime(2018, 7, 7, 22, 9, 11)
    assert parse_fecha("no es fecha") is None
