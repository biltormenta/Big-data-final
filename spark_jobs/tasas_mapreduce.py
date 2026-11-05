import os
import time

from pyspark.sql import functions as F

from comun import guardar_json, guardar_mongo, info_negocios, resenas_con_tiempo, sesion
from pymongo import GEOSPHERE, MongoClient
from app import config
from app.geo import geohash_centro, geohash_poligono
from app.tasas import ic_poisson, tasa_cierre

PARTICIONES = int(os.getenv("PARTICIONES", "8"))  # 8 salio la mas rapida en balanceo.py
MIN_DIAS = config.MIN_DIAS_NEGOCIO

spark = sesion("lifemap-tasas")
sc = spark.sparkContext
t0 = time.time()

negocios = info_negocios(spark).cache()
# diccionario pequeño (~17.000 negocios) que viaja a todos los workers una sola vez
info = {r["business_id"]: (r["categoria_principal"], r["geohash"], int(r["evento"]))
        for r in negocios.collect()}
info_b = sc.broadcast(info)
print("negocios:", len(info))

# ---------- paso 1 (MapReduce): primera y ultima resena de cada negocio, a partir de las ~1,3 M de resenas ----------
resenas = resenas_con_tiempo(spark)
vida = (resenas.rdd.map(lambda r: (r["business_id"], (r["t"], r["t"])))
        .reduceByKey(lambda a, b: (min(a[0], b[0]), max(a[1], b[1])), PARTICIONES))


def dias_de(par):
    return max((par[1] - par[0]) // 86400, 1)  # minimo 1 dia, igual que en la ingesta


# ---------- paso 2 (MapReduce): (clave, (cierre, dias_negocio, 1)) y suma con reduceByKey ----------
def reducir(indice):
    pares = vida.map(lambda kv: (info_b.value[kv[0]][indice], (info_b.value[kv[0]][2], dias_de(kv[1]), 1)))
    return dict(pares.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]), PARTICIONES).collect())


por_categoria = reducir(0)
por_celda = reducir(1)

# ---------- validacion contra groupBy de DataFrames (usa los dias que la ingesta calculo con dask) ----------
def con_groupby(columna):
    filas = negocios.groupBy(columna).agg(F.sum("evento").alias("c"), F.sum("dias").alias("d"),
                                          F.count("*").alias("n")).collect()
    return {r[columna]: (int(r["c"]), int(r["d"]), int(r["n"])) for r in filas}


coincide = {"categoria": por_categoria == con_groupby("categoria_principal"),
            "celda": por_celda == con_groupby("geohash")}
print("reduceByKey coincide con groupBy:", coincide)
assert all(coincide.values()), "reduceByKey y groupBy no dan lo mismo"


# ---------- tasas con la formula anualizada, intervalo y minimo de dias-negocio ----------
def fila(clave, v):
    cierres, dias, n = v
    inf, sup = ic_poisson(cierres, dias)
    return {"negocios": int(n), "cierres": int(cierres), "dias_negocio": int(dias),
            # float(): scipy devuelve numpy.float64 y spark no lo acepta en una columna double
            "tasa_cierre_pct": round(float(tasa_cierre(cierres, dias)), 4),
            "ic95_inf": round(float(inf), 4), "ic95_sup": round(float(sup), 4),
            "confiable": dias >= MIN_DIAS, "min_dias": MIN_DIAS}


cat_filas = [{"categoria": k, **fila(k, v)} for k, v in por_categoria.items()]

# porcentaje de resenas negativas por celda (DataFrame: es una cuenta simple, no hace falta RDD)
neg_celda = {r["geohash"]: (r["resenas"], r["negativas"]) for r in
             (resenas.join(F.broadcast(negocios.select("business_id", "geohash")), "business_id")
              .groupBy("geohash")
              .agg(F.count("*").alias("resenas"), F.sum((F.col("sentimiento") == "negativo").cast("int")).alias("negativas"))
              .collect())}
celda_filas = []
for gh, v in por_celda.items():
    n_res, n_neg = neg_celda.get(gh, (0, 0))
    la, lo = geohash_centro(gh)
    celda_filas.append({"celda": gh, **fila(gh, v), "resenas": int(n_res), "negativas": int(n_neg),
                        "pct_negativas": round(n_neg / n_res * 100, 2) if n_res else None,
                        "loc": ("Point", [lo, la]), "geom": ("Polygon", geohash_poligono(gh)["coordinates"])})

# schema explicito: si no, spark vuelve los GeoJSON anidados un "map" y mongo no los reconoce como geometria
COMUN = ("negocios long, cierres long, dias_negocio long, tasa_cierre_pct double, ic95_inf double, ic95_sup double, "
         "confiable boolean, min_dias long")
ESQ_CAT = "categoria string, " + COMUN
ESQ_CELDA = ("celda string, " + COMUN + ", resenas long, negativas long, pct_negativas double, "
             "loc struct<type:string,coordinates:array<double>>, "
             "geom struct<type:string,coordinates:array<array<array<double>>>>")


def a_df(filas, esquema):
    nombres = [c.split()[0] for c in esquema.split(", ") if " " in c and "<" not in c.split()[0]]
    return spark.createDataFrame([tuple(f[n] for n in nombres) for f in filas], esquema)


guardar_mongo(a_df(cat_filas, ESQ_CAT), config.COL_TASAS_CATEGORIA)
guardar_mongo(a_df(celda_filas, ESQ_CELDA), config.COL_TASAS_CELDA)
# las celdas quedan con su geometria y con indice 2dsphere para poder consultarlas por ubicacion
col = MongoClient(config.MONGO_URI)[config.MONGO_DB][config.COL_TASAS_CELDA]
col.create_index([("geom", GEOSPHERE)])
col.create_index("tasa_cierre_pct")
MongoClient(config.MONGO_URI)[config.MONGO_DB][config.COL_TASAS_CATEGORIA].create_index("tasa_cierre_pct")

tot_c = sum(v[0] for v in por_categoria.values())
tot_d = sum(v[1] for v in por_categoria.values())


def top(filas, clave, n=8):
    ok = sorted([f for f in filas if f["confiable"]], key=lambda f: -f["tasa_cierre_pct"])
    campos = ("negocios", "cierres", "dias_negocio", "tasa_cierre_pct", "ic95_inf", "ic95_sup", "resenas", "pct_negativas")
    return [{clave: f[clave], **{k: f[k] for k in campos if k in f}} for f in ok[:n]]


salida = {
    "min_dias_negocio": MIN_DIAS, "particiones": PARTICIONES,
    "coincide_con_groupby": coincide,
    "global": {"negocios": len(info), "cierres": tot_c, "dias_negocio": tot_d,
               "tasa_cierre_pct": round(tasa_cierre(tot_c, tot_d), 4)},
    "categorias": {"total": len(cat_filas), "confiables": sum(f["confiable"] for f in cat_filas),
                   "mayor_tasa": top(cat_filas, "categoria"),
                   "menor_tasa": list(reversed(top(cat_filas, "categoria", 1000)[-5:]))},
    "celdas": {"total": len(celda_filas), "confiables": sum(f["confiable"] for f in celda_filas),
               "mayor_tasa": top(celda_filas, "celda", 5),
               "mayor_pct_negativas": sorted(
                   [{"celda": f["celda"], "pct_negativas": f["pct_negativas"], "resenas": f["resenas"]}
                    for f in celda_filas if f["resenas"] >= 5000], key=lambda f: -f["pct_negativas"])[:5]},
    "segundos": round(time.time() - t0, 1),
}
guardar_json("tasas.json", salida)

# un negocio de ejemplo (uno que cerro) y los indices de la coleccion, para el informe
neg_col = MongoClient(config.MONGO_URI)[config.MONGO_DB][config.COL_NEGOCIOS]
guardar_json("ejemplo_negocio.json", {"documento": neg_col.find_one({"is_open": 0, "resenas.n": {"$gte": 50}}),
                                      "indices": sorted(neg_col.index_information())})
MongoClient(config.MONGO_URI)[config.MONGO_DB][config.COL_METRICAS].replace_one(
    {"_id": "tasas"}, {"_id": "tasas", **salida}, upsert=True)
print("tasa global:", salida["global"])
print("categorias confiables:", salida["categorias"]["confiables"], "de", salida["categorias"]["total"],
      "| celdas confiables:", salida["celdas"]["confiables"], "de", salida["celdas"]["total"])
spark.stop()
