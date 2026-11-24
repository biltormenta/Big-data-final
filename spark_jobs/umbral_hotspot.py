import time

from pymongo import MongoClient
from pyspark.sql import Window
from pyspark.sql import functions as F

from comun import RESENAS, guardar_json, info_negocios, sesion
from app import config

# el umbral de hotspot sale de la historia anterior a T: se mira cuanto cambia normalmente el % de resenas negativas
# de una celda entre dos ventanas consecutivas de 30 dias, y se llama hotspot a un salto que casi nunca pasa (percentil 95)
PERCENTIL = 0.95

spark = sesion("lifemap-umbral-hotspot")
t0 = time.time()
T = config.FECHA_T
ventana_s = config.VENTANA_DIAS * 86400

# solo historia (hasta T); la etiqueta sale de las estrellas porque el modelo de sentimiento aun no clasifico nada
hist = (spark.read.parquet(RESENAS).select("business_id", "sentimiento", "date")
        .filter(F.col("date") <= F.lit(T).cast("timestamp"))
        .join(F.broadcast(info_negocios(spark).select("business_id", "geohash")), "business_id"))

# numero de ventana: las mismas ventanas de 30 dias del streaming, alineadas para que una empiece justo en T
# (el numero es negativo hacia el pasado)
v = F.floor((F.unix_timestamp("date") - F.unix_timestamp(F.lit(T).cast("timestamp"))) / ventana_s)
ventanas = (hist.groupBy("geohash", v.alias("v"))
            .agg(F.count("*").alias("n"), F.sum((F.col("sentimiento") == "negativo").cast("int")).alias("neg"))
            .withColumn("pct", F.col("neg") / F.col("n") * 100))

w = Window.partitionBy("geohash").orderBy("v")
pares = (ventanas.withColumn("pct_prev", F.lag("pct").over(w)).withColumn("n_prev", F.lag("n").over(w))
         .withColumn("v_prev", F.lag("v").over(w))
         # solo ventanas consecutivas, y con suficientes resenas en las dos (la misma regla que usa el streaming)
         .filter((F.col("v_prev") == F.col("v") - 1) & (F.col("n") >= config.MIN_RESENAS_VENTANA)
                 & (F.col("n_prev") >= config.MIN_RESENAS_VENTANA))
         .withColumn("delta", F.col("pct") - F.col("pct_prev")))
pares.cache()
n_pares = pares.count()
estadisticas = pares.agg(F.mean("delta").alias("media"), F.stddev("delta").alias("desv")).first()
q = pares.approxQuantile("delta", [0.5, 0.9, PERCENTIL], 0.001)
umbral = round(q[2], 1)

salida = {
    "pares_de_ventanas": n_pares, "ventanas_totales": ventanas.count(), "celdas": ventanas.select("geohash").distinct().count(),
    "min_resenas_por_ventana": config.MIN_RESENAS_VENTANA, "percentil": PERCENTIL,
    "delta_medio_pp": round(estadisticas["media"], 3), "delta_desv_pp": round(estadisticas["desv"], 3),
    "delta_mediana_pp": round(q[0], 3), "delta_p90_pp": round(q[1], 3), "umbral_pp": umbral,
    "hasta": T, "segundos": round(time.time() - t0, 1),
    "regla": "hotspot = % negativo sube >= umbral_pp frente a la ventana anterior, con >= min_resenas en las dos",
}
MongoClient(config.MONGO_URI)[config.MONGO_DB]["config"].replace_one({"_id": "hotspot"}, {"_id": "hotspot", **salida}, upsert=True)
guardar_json("umbral_hotspot.json", salida)
print(f"{n_pares} pares de ventanas consecutivas; el % negativo cambia {salida['delta_medio_pp']} +- {salida['delta_desv_pp']} puntos")
print(f"umbral de hotspot (percentil {int(PERCENTIL * 100)}): {umbral} puntos porcentuales")
spark.stop()
