import json
import os
import statistics
import time

from pymongo import MongoClient
from pyspark.sql import functions as F

from comun import guardar_json, info_negocios, resenas_con_tiempo, sesion
from app import config

REPETICIONES = int(os.getenv("REPETICIONES", "3"))
CONFIGURACIONES = [2, 8, 32, 200]

spark = sesion("lifemap-balanceo")
sc = spark.sparkContext

negocios = info_negocios(spark).cache()
info = {r["business_id"]: (r["categoria_principal"], int(r["evento"])) for r in negocios.collect()}
info_b = sc.broadcast(info)
resenas = resenas_con_tiempo(spark).select("business_id", "t").cache()
print("resenas:", resenas.count(), "| negocios:", len(info))


def operacion(P):
    """La misma operacion de tasas_mapreduce.py (de resenas a tasa por categoria) con P particiones."""
    base = resenas.rdd.map(lambda r: (r["business_id"], (r["t"], r["t"]))).repartition(P)
    vida = base.reduceByKey(lambda a, b: (min(a[0], b[0]), max(a[1], b[1])), P)
    tasas = (vida.map(lambda kv: (info_b.value[kv[0]][0],
                                  (info_b.value[kv[0]][1], max((kv[1][1] - kv[1][0]) // 86400, 1), 1)))
             .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]), P))
    return len(tasas.collect())


# ---------- tiempos: 2, 8, 32 y 200 particiones, con repeticiones ----------
operacion(8)  # calentamiento: la primera corrida arranca los procesos de python y no cuenta
tiempos = {}
for P in CONFIGURACIONES:
    medidas = []
    for _ in range(REPETICIONES):
        t = time.time()
        operacion(P)
        medidas.append(time.time() - t)
    tiempos[P] = {"medidas_s": [round(m, 2) for m in medidas], "media_s": round(statistics.mean(medidas), 2),
                  "desv_s": round(statistics.stdev(medidas), 2) if len(medidas) > 1 else 0.0}
    print(f"P={P}: media {tiempos[P]['media_s']} s, desv {tiempos[P]['desv_s']} s, medidas {tiempos[P]['medidas_s']}")

# ---------- sesgo: filas por particion con spark_partition_id(), con clave frente a sin clave ----------
detalle = (resenas.join(F.broadcast(negocios.select("business_id", "categoria_principal", "city")), "business_id"))
N = 200


def filas_por_particion(df):
    contadas = {r["pid"]: r["n"] for r in
                df.withColumn("pid", F.spark_partition_id()).groupBy("pid").agg(F.count("*").alias("n")).collect()}
    todas = [contadas.get(i, 0) for i in range(N)]
    media = sum(todas) / N
    return {"particiones": N, "vacias": todas.count(0), "max": max(todas), "min": min(todas), "media": round(media, 1),
            "max_sobre_media": round(max(todas) / media, 2), "desv": round(statistics.pstdev(todas), 1),
            "ordenadas": sorted(todas, reverse=True)}


def tiempo_reduccion(df):
    """Tiempo de sumar filas por negocio cuando los datos ya vienen repartidos de esa manera."""
    medidas = []
    for _ in range(REPETICIONES):
        t = time.time()
        df.rdd.map(lambda r: (r["business_id"], 1)).reduceByKey(lambda a, b: a + b, N).count()
        medidas.append(time.time() - t)
    return round(statistics.mean(medidas), 2)


sesgo = {}
for nombre, df in {"categoria": detalle.repartition(N, "categoria_principal"),
                   "ciudad": detalle.repartition(N, "city"),
                   "sin_clave": detalle.repartition(N)}.items():
    df = df.cache()
    sesgo[nombre] = filas_por_particion(df)
    sesgo[nombre]["tiempo_s"] = tiempo_reduccion(df)
    s = sesgo[nombre]
    print(f"{nombre}: vacias {s['vacias']}/{N}, max {s['max']} filas ({s['max_sobre_media']}x la media), "
          f"tiempo {s['tiempo_s']} s")
    df.unpersist()

mejor = min(tiempos, key=lambda p: tiempos[p]["media_s"])
salida = {"repeticiones": REPETICIONES, "nucleos": sc.defaultParallelism, "tiempos": tiempos, "mejor": mejor,
          "sesgo": sesgo}
guardar_json("balanceo.json", salida)
MongoClient(config.MONGO_URI)[config.MONGO_DB][config.COL_METRICAS].replace_one(
    {"_id": "balanceo"}, {"_id": "balanceo", **json.loads(json.dumps(salida))}, upsert=True)  # mongo pide llaves de texto
print("mejor configuracion:", mejor, "particiones")
spark.stop()
