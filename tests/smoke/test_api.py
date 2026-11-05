import datetime
import os

import pytest
import requests
from pymongo import GEOSPHERE, MongoClient

API = os.getenv("API_URL", "http://localhost:5000")
MONGO = os.getenv("MONGO_URI", "mongodb://mongo:27017")
DB = os.getenv("MONGO_DB", "lifemap")

# negocios de prueba en el atlantico (0, -30) para no mezclarse con los de philadelphia; llevan "_prueba": True
LAT0, LON0 = 0.0, -30.0
# (lat, lon, categoria, is_open, dias, resenas, suma_sentimiento, negativas)
CERCA = [(0.0, -30.0, "Pizza", 0, 3650, 10, 4, 2), (0.0003, -30.0003, "Pizza", 1, 3650, 20, 10, 2),
         (-0.0003, -30.0002, "Tacos", 1, 3650, 10, 5, 1)]
LEJOS = [(0.009, -30.0, "Pizza", 1, 3650, 10, 10, 0), (0.0, -29.991, "Tacos", 0, 3650, 10, -4, 6)]
CAJA = {"type": "Polygon", "coordinates": [[[-30.001, -0.001], [-29.999, -0.001], [-29.999, 0.001],
                                            [-30.001, 0.001], [-30.001, -0.001]]]}


@pytest.fixture(scope="module", autouse=True)
def datos_de_prueba():
    db = MongoClient(MONGO)[DB]
    db["negocios"].create_index([("loc", GEOSPHERE)])
    f0 = datetime.datetime(2010, 1, 1)
    docs = []
    for i, (lat, lon, cat, abierto, dias, n, suma, neg) in enumerate(CERCA + LEJOS):
        docs.append({"_id": f"prueba{i}", "_prueba": True, "name": f"n{i}", "categories": ["Restaurants", cat],
                     "categoria_principal": cat, "is_open": abierto, "loc": {"type": "Point", "coordinates": [lon, lat]},
                     "vida": {"primera_resena": f0, "ultima_resena": f0, "dias_negocio": dias,
                              "evento": 1 - abierto, "censurado": bool(abierto)},
                     "resenas": {"n": n, "suma_sentimiento": suma, "negativas": neg}})
    db["negocios"].insert_many(docs)
    # tasas de prueba para /stats/closure: una confiable (dias altos) y una con pocos dias
    db["tasas_categoria"].insert_many([
        {"_prueba": True, "categoria": "PruebaGrande", "negocios": 100, "cierres": 40, "dias_negocio": 500000,
         "tasa_cierre_pct": 29.2, "confiable": True},
        {"_prueba": True, "categoria": "PruebaChica", "negocios": 2, "cierres": 1, "dias_negocio": 700,
         "tasa_cierre_pct": 52.1, "confiable": False}])
    yield
    for col in ("negocios", "tasas_categoria"):
        db[col].delete_many({"_prueba": True})


def test_ping_y_health_informan_cada_servicio():
    assert requests.get(f"{API}/ping").json()["status"] == "ok"
    r = requests.get(f"{API}/health")
    j = r.json()
    # en la pila de pruebas solo corren mongo y la api: spark y dask salen como caidos y el codigo es 503
    assert r.status_code in (200, 503)
    assert {"mongo", "spark", "dask", "api"} <= set(j["servicios"])
    assert j["servicios"]["mongo"]["estado"] == "ok" and j["servicios"]["api"]["estado"] == "ok"
    assert j["commit"]


def test_cercanos_por_radio_y_categoria():
    r = requests.get(f"{API}/businesses/near", params={"lat": LAT0, "lon": LON0, "radio": 200})
    assert r.status_code == 200 and r.json()["count"] == len(CERCA)
    r = requests.get(f"{API}/businesses/near", params={"lat": LAT0, "lon": LON0, "radio": 2000})
    assert r.json()["count"] == len(CERCA) + len(LEJOS)
    r = requests.get(f"{API}/businesses/near", params={"lat": LAT0, "lon": LON0, "radio": 2000, "categoria": "Tacos"})
    assert r.json()["count"] == 2
    r = requests.get(f"{API}/businesses/near", params={"lat": LAT0, "lon": LON0, "radio": 2000, "limit": 1})
    assert r.json()["count"] == 1


def test_dentro_de_poligono():
    r = requests.post(f"{API}/businesses/within", json=CAJA)
    assert r.status_code == 200 and r.json()["count"] == len(CERCA)
    r = requests.post(f"{API}/businesses/within", json={"type": "Feature", "properties": {}, "geometry": CAJA},
                      params={"categoria": "Pizza"})
    assert r.json()["count"] == 2


def test_resumen_de_zona_calcula_sentimiento_y_tasa():
    r = requests.get(f"{API}/zones/summary", params={"lat": LAT0, "lon": LON0, "radio": 200, "min_dias": 1000})
    j = r.json()
    assert r.status_code == 200 and j["negocios"] == 3 and j["resenas"] == 40
    # suma de sentimiento 19 sobre 40 resenas; 5 negativas de 40
    assert j["sentimiento_medio"] == 0.475 and j["pct_negativas"] == 12.5
    # 1 cierre en 3 * 3650 dias = 30 negocios-año -> 3,333 % anual
    assert j["cierres"] == 1 and j["tasa_cierre_pct"] == pytest.approx(3.333, abs=0.001) and j["confiable"] is True
    # con un minimo de dias muy alto la zona queda marcada como no confiable
    r = requests.get(f"{API}/zones/summary", params={"lat": LAT0, "lon": LON0, "radio": 200, "min_dias": 10 ** 7})
    assert r.json()["confiable"] is False
    r = requests.get(f"{API}/zones/summary", params={"lat": LAT0, "lon": LON0, "radio": 200, "categoria": "Tacos"})
    assert r.json()["negocios"] == 1


def test_stats_closure_aplica_minimo_de_dias():
    r = requests.get(f"{API}/stats/closure", params={"por": "categoria", "min_dias": 100000, "limit": 5000})
    nombres = [x["categoria"] for x in r.json()["resultados"]]
    assert r.status_code == 200 and "PruebaGrande" in nombres and "PruebaChica" not in nombres
    r = requests.get(f"{API}/stats/closure", params={"por": "categoria", "min_dias": 0, "limit": 5000})
    assert "PruebaChica" in [x["categoria"] for x in r.json()["resultados"]]


def test_predict_valida_la_entrada():
    assert requests.post(f"{API}/predict", json={}).status_code == 400
    assert requests.post(f"{API}/predict", json={"text": "   "}).status_code == 400
    # texto valido: 200 si el modelo ya esta entrenado, 503 si todavia no (pila de pruebas recien creada)
    r = requests.post(f"{API}/predict", json={"text": "the food was great"})
    assert r.status_code in (200, 503)
    if r.status_code == 200:
        assert r.json()["sentimiento"] in ("negativo", "neutral", "positivo")


@pytest.mark.parametrize("metodo,ruta,params", [
    ("get", "/businesses/near", {"lat": "abc", "lon": 1, "radio": 10}),
    ("get", "/businesses/near", {"lat": 95, "lon": 1, "radio": 10}),
    ("get", "/businesses/near", {"lat": 1, "lon": 1}),
    ("get", "/businesses/near", {"lat": 1, "lon": 1, "radio": -5}),
    ("get", "/zones/summary", {"lat": 1, "lon": 1, "radio": 10, "min_dias": "x"}),
    ("get", "/stats/closure", {"por": "otro"}),
    ("get", "/stats/closure", {"min_dias": -1}),
])
def test_parametros_invalidos_dan_400(metodo, ruta, params):
    assert getattr(requests, metodo)(f"{API}{ruta}", params=params).status_code == 400


def test_poligono_invalido_da_400():
    assert requests.post(f"{API}/businesses/within", json={"type": "Point", "coordinates": [0, 0]}).status_code == 400
    assert requests.post(f"{API}/businesses/within", data="no es json").status_code == 400
