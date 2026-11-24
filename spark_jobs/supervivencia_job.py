import time

import pandas as pd
from pymongo import GEOSPHERE, MongoClient
from pyspark.sql import Window
from pyspark.sql import functions as F

from comun import RESENAS, guardar_json, leer_mongo, sesion
from app import config
from app.supervivencia import kaplan_meier, reducir, supervivencia_en

spark = sesion("lifemap-supervivencia")
t0 = time.time()
db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
K = config.RESENAS_PARA_ENTRAR

# ---------- entrada tardia ----------
# Yelp solo incluye negocios con al menos 5 resenas, asi que un negocio solo es "observable" desde su resena numero 5:
# los que cerraron antes de llegar a 5 resenas ni aparecen. La entrada (en dias desde su primera resena) es el
# tiempo que tardo en llegar a su resena numero 5. Ese tiempo no cuenta como "en riesgo" en la curva.
w = Window.partitionBy("business_id").orderBy("date")
orden = spark.read.parquet(RESENAS).select("business_id", "date").withColumn("rn", F.row_number().over(w))
entradas = (orden.filter(F.col("rn").isin(1, K)).groupBy("business_id")
            .agg(F.min(F.when(F.col("rn") == 1, F.col("date"))).alias("primera"),
                 F.max(F.when(F.col("rn") == K, F.col("date"))).alias("kesima"))
            .withColumn("entrada_dias", ((F.unix_timestamp("kesima") - F.unix_timestamp("primera")) / 86400).cast("int")))

negocios = (leer_mongo(spark, "negocios")
            .select(F.col("_id").alias("business_id"), "categoria_principal", "geohash",
                    F.col("vida.dias_negocio").alias("duracion"), F.col("vida.evento").alias("evento"))
            .join(entradas.select("business_id", "entrada_dias"), "business_id"))
pdf = negocios.toPandas()
print("negocios:", len(pdf), "| entrada tardia media:", round(pdf["entrada_dias"].mean(), 1), "dias")
assert (pdf["duracion"] >= pdf["entrada_dias"]).all(), "hay negocios que cierran antes de entrar"


def ajustar(sub):
    km = kaplan_meier(sub["duracion"], sub["evento"], sub["entrada_dias"], n_min=config.N_MIN_EN_RIESGO)
    # un grupo chico no se interpreta: curva marcada, pero sin conclusiones
    pequeno = km["negocios"] < config.N_MIN_GRUPO or km["cierres"] < config.EVENTOS_MIN_GRUPO or not km["curva"]
    return {
        "negocios": km["negocios"], "cierres": km["cierres"], "censurados": km["negocios"] - km["cierres"],
        "mediana_dias": km["mediana_dias"], "t_inicio": km["t_inicio"], "t_fin": km["t_fin"],
        # supervivencia a 1, 3 y 5 años (None si ese dia queda fuera de la parte con suficientes negocios en riesgo)
        "s_1a": supervivencia_en(km["curva"], 365), "s_3a": supervivencia_en(km["curva"], 1095),
        "s_5a": supervivencia_en(km["curva"], 1825),
        "en_riesgo_inicio": km["curva"][0]["en_riesgo"] if km["curva"] else 0,
        "pequeno": bool(pequeno), "curva": reducir(km["curva"]),
    }


docs = [{"_id": "categoria|TODAS", "tipo": "categoria", "clave": "TODAS", **ajustar(pdf)}]

# ---------- por categoria principal ----------
for cat, sub in pdf.groupby("categoria_principal"):
    if len(sub) >= 30:  # con menos de 30 negocios ni siquiera se ajusta
        docs.append({"_id": f"categoria|{cat}", "tipo": "categoria", "clave": cat, **ajustar(sub)})

# ---------- por zona: cada celda de geohash es un poligono y sus negocios se piden a mongo con $geoWithin ----------
coincide = 0
celdas = list(db[config.COL_TASAS_CELDA].find({}, {"celda": 1, "geom": 1, "loc": 1}))
por_id = pdf.set_index("business_id")
for c in celdas:
    ids = [d["_id"] for d in db[config.COL_NEGOCIOS].find({"loc": {"$geoWithin": {"$geometry": c["geom"]}}}, {"_id": 1})]
    sub = por_id.loc[por_id.index.intersection(ids)].reset_index()
    coincide += int(len(sub) == int((pdf["geohash"] == c["celda"]).sum()))
    if len(sub) >= 30:
        docs.append({"_id": f"zona|{c['celda']}", "tipo": "zona", "clave": c["celda"], "loc": c["loc"], "geom": c["geom"],
                     **ajustar(sub)})
print(f"celdas donde $geoWithin da los mismos negocios que el geohash: {coincide} de {len(celdas)}")

db[config.COL_SUPERVIVENCIA].drop()
db[config.COL_SUPERVIVENCIA].insert_many(docs)
db[config.COL_SUPERVIVENCIA].create_index([("loc", GEOSPHERE)], sparse=True)
db[config.COL_SUPERVIVENCIA].create_index([("tipo", 1), ("clave", 1)])

# ---------- resumen para el informe ----------
def fila(d):
    return {k: d[k] for k in ("clave", "negocios", "cierres", "mediana_dias", "s_1a", "s_3a", "s_5a", "t_inicio", "t_fin", "pequeno")}


cats = [d for d in docs if d["tipo"] == "categoria" and d["clave"] != "TODAS"]
zonas = [d for d in docs if d["tipo"] == "zona"]
fiables = lambda lista: [d for d in lista if not d["pequeno"] and d["s_3a"] is not None]
orden_s = lambda lista: sorted(fiables(lista), key=lambda d: d["s_3a"])
salida = {
    "K_resenas_para_entrar": K, "n_min_en_riesgo": config.N_MIN_EN_RIESGO, "n_min_grupo": config.N_MIN_GRUPO,
    "entrada_tardia_dias": {"media": round(float(pdf["entrada_dias"].mean()), 1), "mediana": float(pdf["entrada_dias"].median()),
                            "max": int(pdf["entrada_dias"].max())},
    "global": fila(docs[0]), "curva_global": docs[0]["curva"],
    "categorias": {"ajustadas": len(cats), "interpretables": len(fiables(cats)), "pequenas": sum(d["pequeno"] for d in cats),
                   "menor_supervivencia_3a": [fila(d) for d in orden_s(cats)[:5]],
                   "mayor_supervivencia_3a": [fila(d) for d in orden_s(cats)[-5:][::-1]]},
    "zonas": {"celdas": len(celdas), "ajustadas": len(zonas), "interpretables": len(fiables(zonas)), "pequenas": sum(d["pequeno"] for d in zonas),
              "geoWithin_igual_a_geohash": coincide,
              "menor_supervivencia_3a": [fila(d) for d in orden_s(zonas)[:5]],
              "mayor_supervivencia_3a": [fila(d) for d in orden_s(zonas)[-5:][::-1]]},
    "curvas_categoria": {d["clave"]: d["curva"] for d in orden_s(cats)[:2] + orden_s(cats)[-2:]},
    "segundos": round(time.time() - t0, 1),
}
guardar_json("supervivencia.json", salida)
db[config.COL_METRICAS].replace_one({"_id": "supervivencia"}, {"_id": "supervivencia", **{k: v for k, v in salida.items() if not k.startswith("curva")}}, upsert=True)
g = salida["global"]
print("global:", g["negocios"], "negocios,", g["cierres"], "cierres; S a 3 años:", g["s_3a"], "| mediana:", g["mediana_dias"])
print("categorias interpretables:", salida["categorias"]["interpretables"], "de", len(cats), "| zonas:", salida["zonas"]["interpretables"], "de", len(zonas))
spark.stop()
