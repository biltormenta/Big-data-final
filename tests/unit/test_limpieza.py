import json

import pandas as pd

from app.limpieza import categoria_principal, limpiar_negocios, procesar_lineas, rangos, seleccionar_area_familia

NEG = ["business_id", "name", "city", "state", "latitude", "longitude", "stars", "review_count", "is_open", "categories"]


def negocios(filas):
    return pd.DataFrame(filas, columns=NEG)


# de 6 filas solo 2 son buenas; limpiar_negocios cuenta cada descarte en su categoria
def test_limpieza_de_negocios_cuenta_cada_regla():
    df = negocios([
        ["a", "A", "Philadelphia", "PA", 39.95, -75.16, 4.0, 10, 1, "Restaurants, Pizza"],   # bueno
        ["b", "B", "Philadelphia", "PA", None, -75.16, 4.0, 10, 1, "Restaurants"],            # lat nula
        ["c", "C", "Philadelphia", "PA", 139.95, -75.16, 4.0, 10, 1, "Restaurants"],          # lat fuera de rango
        ["d", "D", "Philadelphia", "PA", 39.95, -75.16, 4.0, 10, 1, None],                    # sin categorias
        ["a", "A", "Philadelphia", "PA", 39.95, -75.16, 4.0, 10, 1, "Restaurants, Pizza"],   # duplicado
        ["e", "E", "Philadelphia", "PA", 39.96, -75.17, 3.5, 5, 0, "Restaurants"],            # bueno
    ])
    limpio, c = limpiar_negocios(df)
    assert len(limpio) == 2
    assert (c["leidos"], c["coordenadas_nulas"], c["fuera_de_rango"], c["sin_categorias"], c["duplicados"]) == (6, 1, 1, 1, 1)


def test_area_y_familia_y_categoria_principal():
    df = negocios([
        ["a", "A", "Philadelphia", "PA", 39.95, -75.16, 4.0, 10, 1, "Restaurants, Pizza, Food"],
        ["b", "B", "New York", "NY", 40.71, -74.00, 4.0, 10, 1, "Restaurants"],               # 130 km: fuera del area
        ["c", "C", "Philadelphia", "PA", 39.96, -75.17, 4.0, 10, 1, "Shopping, Fashion"],     # no es restaurante
    ])
    sel, c = seleccionar_area_familia(df)
    assert list(sel["business_id"]) == ["a"]
    assert (c["limpios"], c["en_el_area"], c["en_area_y_familia"]) == (3, 2, 1)
    assert sel.loc[0, "categoria_principal"] == "Pizza" and len(sel.loc[0, "geohash"]) == 5
    assert categoria_principal("Restaurants, Food") == "Restaurants"


# la categoria principal es la mas comun entre las del negocio, no la primera de la lista
def test_categoria_principal_es_la_mas_frecuente():
    df = negocios([
        ["a", "A", "Philadelphia", "PA", 39.95, -75.16, 4.0, 10, 1, "Restaurants, Seafood, Italian"],
        ["b", "B", "Philadelphia", "PA", 39.95, -75.16, 4.0, 10, 1, "Restaurants, Italian"],
        ["c", "C", "Philadelphia", "PA", 39.95, -75.16, 4.0, 10, 1, "Restaurants, Italian, Pizza"],
    ])
    sel, _ = seleccionar_area_familia(df)
    assert list(sel["categoria_principal"]) == ["Italian", "Italian", "Italian"]


def linea(rid, bid, estrellas, texto, fecha):
    return json.dumps({"review_id": rid, "user_id": "u", "business_id": bid, "stars": estrellas, "useful": 0,
                       "funny": 0, "cool": 0, "text": texto, "date": fecha}).encode()


# cada resena mala cae en una sola regla, y las buenas quedan con su etiqueta de sentimiento
def test_limpieza_de_resenas_cuenta_cada_regla():
    validos, area = {"a", "x"}, {"a"}
    lineas = [
        linea("r1", "a", 5, "muy rico", "2019-03-01 10:00:00"),     # buena -> positivo
        linea("r2", "zzz", 5, "texto", "2019-03-01 10:00:00"),      # negocio que no existe
        linea("r3", "x", 5, "texto", "2019-03-01 10:00:00"),        # negocio valido pero fuera del area
        linea("r4", "a", 4, "   ", "2019-03-01 10:00:00"),          # sin texto
        linea("r5", "a", 9, "texto", "2019-03-01 10:00:00"),        # estrellas invalidas
        linea("r6", "a", 4, "texto", "fecha mala"),                 # fecha invalida
        linea("r1", "a", 5, "muy rico", "2019-03-01 10:00:00"),     # duplicada
        linea("r7", "a", 2, "malo", "2020-06-01 08:30:00"),         # buena -> negativo
    ]
    df, st = procesar_lineas(lineas, validos, area)
    assert list(df["review_id"]) == ["r1", "r7"] and list(df["sentimiento"]) == ["positivo", "negativo"]
    assert (st["leidas"], st["sin_negocio"], st["fuera_area"], st["sin_texto"], st["invalida"], st["duplicada"],
            st["validas"]) == (8, 1, 1, 1, 2, 1, 2)
    assert st["fecha_max"] == "2020-06-01 08:30:00"


def test_rangos_cubren_el_archivo_completo():
    r = rangos(100, 30)
    assert r[0] == (0, 30) and r[-1] == (90, 100) and len(r) == 4
