from datetime import datetime, timedelta

import pytest

from app.hotspot import es_hotspot
from app.riesgo import etiqueta_riesgo, features_de_resenas

T = datetime(2021, 1, 19)


def d(dias):
    """Una fecha 'dias' antes (o despues si es negativo) de T."""
    return T - timedelta(days=dias)


# las variables se calculan solo con datos hasta la fecha de referencia: una resena posterior no puede mover nada
def test_features_ignoran_lo_posterior_a_la_fecha():
    base = [(d(400), 5), (d(300), 4), (d(10), 2)]
    con_futuro = base + [(T + timedelta(days=30), 1), (T + timedelta(days=60), 1)]
    assert features_de_resenas(base, T) == features_de_resenas(con_futuro, T)


def test_features_basicas():
    f = features_de_resenas([(d(400), 5), (d(300), 4), (d(10), 2), (d(5), 1)], T)
    assert f["n_resenas"] == 4 and f["estrellas"] == pytest.approx(3.0)
    assert f["sentimiento_medio"] == pytest.approx(0.0)       # +1 +1 -1 -1
    assert f["edad_dias"] == 400 and f["dias_desde_ultima"] == 5


# tendencia: % negativo de los ultimos 6 meses menos el de los 6 meses anteriores
def test_tendencia_del_porcentaje_negativo():
    antes = [(d(300), 5), (d(280), 5)]                      # 0 % negativo
    ahora = [(d(60), 1), (d(30), 1), (d(10), 5), (d(5), 5)]  # 50 % negativo
    assert features_de_resenas(antes + ahora, T)["tendencia_neg"] == pytest.approx(50.0)
    # sin resenas en alguno de los dos periodos no hay tendencia que medir
    assert features_de_resenas(ahora, T)["tendencia_neg"] == 0.0


def test_sin_historia_no_hay_features():
    assert features_de_resenas([(T + timedelta(days=5), 5)], T) is None


# la etiqueta sigue la definicion del enunciado
def test_etiqueta_de_riesgo():
    # cerro y su ultima resena cae entre T y T + 12 meses -> 1
    assert etiqueta_riesgo(0, d(900), T + timedelta(days=100), T) == 1
    # sigue abierto con actividad despues de T -> 0
    assert etiqueta_riesgo(1, d(900), T + timedelta(days=100), T) == 0
    # cerro pero su ultima resena es posterior al plazo de 12 meses: seguia activo despues del plazo -> 0
    assert etiqueta_riesgo(0, d(900), T + timedelta(days=500), T) == 0
    # sin actividad despues de T se excluye; sin historia antes de T tambien
    assert etiqueta_riesgo(0, d(900), d(30), T) is None
    assert etiqueta_riesgo(1, T + timedelta(days=10), T + timedelta(days=100), T) is None


def test_regla_de_hotspot():
    assert es_hotspot(40, 25, 100, 90, umbral_pp=10, min_n=30) is True       # subio 15 puntos
    assert es_hotspot(30, 25, 100, 90, umbral_pp=10, min_n=30) is False      # subio solo 5
    assert es_hotspot(60, 20, 10, 90, umbral_pp=10, min_n=30) is False       # pocas resenas: no cuenta
    assert es_hotspot(60, None, 100, 0, umbral_pp=10, min_n=30) is False     # no hay ventana anterior


# la actividad y el cambio de estrellas de los ultimos 6 meses
def test_actividad_y_cambio_de_estrellas_recientes():
    filas = [(d(300), 5), (d(280), 5), (d(60), 1), (d(30), 1), (d(10), 3)]
    f = features_de_resenas(filas, T)
    assert f["actividad_6m"] == 3
    assert f["delta_estrellas_6m"] == pytest.approx((1 + 1 + 3) / 3 - (5 + 5 + 1 + 1 + 3) / 5)
    assert features_de_resenas([(d(300), 5)], T)["actividad_6m"] == 0


# actualizar el estado con resenas nuevas da lo mismo que recalcular todo desde cero (el streaming depende de esto)
def test_actualizar_estado_equivale_a_recalcular():
    from app.riesgo import actualizar_estado, features_de_estado, resumen_historia
    antes = [(d(500), 4), (d(300), 5), (d(100), 2), (d(20), 4)]
    nuevas = [(T + timedelta(days=10), 1), (T + timedelta(days=40), 2), (T + timedelta(days=70), 5)]
    ref = T + timedelta(days=70)
    incremental = features_de_estado(actualizar_estado(resumen_historia(antes, T), nuevas, ref), ref)
    completo = features_de_resenas(antes + nuevas, ref)
    assert incremental == pytest.approx(completo)
