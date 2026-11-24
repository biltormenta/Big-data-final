import pytest

from app.supervivencia import kaplan_meier, reducir, supervivencia_en


# sin censura ni entrada tardia es el caso de libro: 5 negocios que cierran en los dias 1 a 5
def test_km_sin_censura():
    r = kaplan_meier([1, 2, 3, 4, 5], [1, 1, 1, 1, 1], n_min=1)
    assert [round(p["S"], 4) for p in r["curva"]] == [0.8, 0.6, 0.4, 0.2, 0.0]
    assert r["mediana_dias"] == 3
    assert [p["en_riesgo"] for p in r["curva"]] == [5, 4, 3, 2, 1]


# un negocio abierto (censurado) sale del grupo en riesgo pero NO cuenta como cierre
def test_km_trata_los_abiertos_como_censurados():
    r = kaplan_meier([1, 2, 3], [1, 0, 1], n_min=1)
    assert [round(p["S"], 4) for p in r["curva"]] == [0.6667, 0.0]
    assert r["cierres"] == 2
    # si se tratara el abierto como cierre saldria 2/3 y luego 1/3: la diferencia es justo lo que se evita
    mal = kaplan_meier([1, 2, 3], [1, 1, 1], n_min=1)
    assert mal["curva"][1]["S"] != r["curva"][1]["S"]


# entrada tardia: el negocio C solo empieza a ser observable en el dia 2,5, asi que no cuenta como en riesgo antes
def test_km_con_entrada_tardia():
    r = kaplan_meier([2, 3, 4], [1, 1, 1], entradas=[0, 0, 2.5], n_min=1)
    assert [p["en_riesgo"] for p in r["curva"]] == [2, 2, 1]
    assert [round(p["S"], 4) for p in r["curva"]] == [0.5, 0.25, 0.0]
    # ignorando la entrada tardia el primer paso seria 2/3 en vez de 1/2: se sobrestima la supervivencia
    sin = kaplan_meier([2, 3, 4], [1, 1, 1], n_min=1)
    assert round(sin["curva"][0]["S"], 4) == 0.6667


# con un minimo de negocios en riesgo la curva solo usa los dias donde hay suficientes
def test_km_corta_donde_hay_pocos_en_riesgo():
    duraciones = list(range(1, 11))
    r = kaplan_meier(duraciones, [1] * 10, n_min=5)
    assert r["t_inicio"] == 1 and r["t_fin"] == 6  # en el dia 6 quedan 5; en el 7 solo 4
    assert min(p["en_riesgo"] for p in r["curva"]) == 5


def test_el_intervalo_contiene_la_curva_y_es_mas_ancho_con_menos_datos():
    chico = kaplan_meier([1, 2, 3, 4, 5, 6, 7, 8], [1, 0, 1, 0, 1, 0, 1, 0], n_min=2)
    grande = kaplan_meier(list(range(1, 81)), [1, 0] * 40, n_min=2)
    for p in chico["curva"]:
        assert p["ic_inf"] <= p["S"] <= p["ic_sup"]
    ancho = lambda r: r["curva"][2]["ic_sup"] - r["curva"][2]["ic_inf"]
    assert ancho(chico) > ancho(grande)


def test_supervivencia_en_y_reducir():
    r = kaplan_meier([1, 2, 3, 4, 5], [1, 1, 1, 1, 1], n_min=1)
    assert supervivencia_en(r["curva"], 2.5) == pytest.approx(0.6)
    assert supervivencia_en(r["curva"], 99) is None  # fuera de la parte confiable no se inventa un valor
    larga = [{"t": i} for i in range(500)]
    corta = reducir(larga, 20)
    assert len(corta) <= 20 and corta[0]["t"] == 0 and corta[-1]["t"] == 499
