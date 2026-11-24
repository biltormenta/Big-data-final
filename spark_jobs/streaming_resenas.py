import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pandas as pd
from pymongo import GEOSPHERE, MongoClient, ReplaceOne, UpdateOne
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from comun import leer_mongo, sesion
from app import config
from app.geo import geohash_centro, geohash_poligono
from app.hotspot import es_hotspot
from app.productor import asegurar_topic
from app.riesgo import actualizar_estado, features_de_estado
from app.versiones import verificar_version

PRUEBA = os.getenv("MODO_PRUEBA") == "1"
TOPIC = config.TOPIC_RESENAS
CHECK = os.path.join(config.RUTA_CHECKPOINTS, "prueba" if PRUEBA else "reviews")
T = datetime.fromisoformat(config.FECHA_T)
VENTANA = timedelta(days=config.VENTANA_DIAS)

spark = sesion("lifemap-streaming")
spark.conf.set("spark.sql.shuffle.partitions", "4")  # pocos datos por lote: 16 particiones solo estorban
db = MongoClient(config.MONGO_URI)[config.MONGO_DB]

# ---------- modelos: se CARGAN (nunca se reentrenan aqui) y se comprueba que son exactamente los versionados ----------
v_sent = verificar_version(config.RUTA_MODELO)
v_riesgo = verificar_version(config.RUTA_MODELO_RIESGO)
m_sent = PipelineModel.load(config.RUTA_MODELO)
m_riesgo = PipelineModel.load(config.RUTA_MODELO_RIESGO)
UMBRAL_RIESGO = float(v_riesgo["umbral"])
cfg = db["config"].find_one({"_id": "hotspot"})
UMBRAL_HOTSPOT = float(cfg["umbral_pp"]) if cfg else 10.0
db[config.COL_METRICAS].replace_one({"_id": "modelo_cargado"}, {
    "_id": "modelo_cargado", "sentimiento_version": v_sent["version"], "sentimiento_huella": v_sent["huella"],
    "riesgo_version": v_riesgo["version"], "riesgo_huella": v_riesgo["huella"], "umbral_riesgo": UMBRAL_RIESGO,
    "umbral_hotspot_pp": UMBRAL_HOTSPOT, "cargado_en": datetime.utcnow()}, upsert=True)
print(f"modelos cargados: sentimiento {v_sent['version']}, riesgo {v_riesgo['version']} | umbral riesgo {UMBRAL_RIESGO:.3f}, "
      f"umbral hotspot {UMBRAL_HOTSPOT} pp")

db[config.COL_VENTANAS].create_index([("loc", GEOSPHERE)])
db[config.COL_VENTANAS].create_index("inicio")
db[config.COL_ALERTAS].create_index([("loc", GEOSPHERE)])
db[config.COL_ALERTAS].create_index([("activa", 1), ("prob", -1)])
db[config.COL_RESENAS_STREAM].create_index("business_id")
db[config.COL_RESENAS_STREAM].create_index("date")

# ---------- fuente: kafka ----------
asegurar_topic(config.KAFKA_SERVIDOR, TOPIC)  # si el productor todavia no corrio, el topic se crea (3 particiones) y no hay errores
esquema = StructType([StructField("review_id", StringType()), StructField("business_id", StringType()),
                      StructField("stars", IntegerType()), StructField("text", StringType()), StructField("date", StringType())])
neg = leer_mongo(spark, config.COL_NEGOCIOS).select(F.col("_id").alias("business_id"), "geohash").cache()

fuente = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", config.KAFKA_SERVIDOR).option("subscribe", TOPIC)
          .option("startingOffsets", os.getenv("OFFSETS_INICIALES", "earliest"))
          .option("maxOffsetsPerTrigger", os.getenv("MAX_POR_LOTE", "6000")).option("failOnDataLoss", "false").load())
msgs = (fuente.select(F.from_json(F.col("value").cast("string"), esquema).alias("r")).select("r.*")
        # tiempo del EVENTO: la fecha de la resena, no la hora en que llega a kafka
        .withColumn("date", F.to_timestamp("date")))
etiquetas = F.array(F.lit("negativo"), F.lit("neutral"), F.lit("positivo"))
clas = (m_sent.transform(msgs)
        .withColumn("prob", F.array_max(vector_to_array("probability")))
        .withColumn("etiqueta", F.element_at(etiquetas, F.col("prediction").cast("int") + 1))
        .join(F.broadcast(neg), "business_id", "left")
        .select("review_id", "business_id", "stars", "date", "prediction", "etiqueta", "prob", "geohash"))

estado = {"lote": -1, "fecha_evento": None, "procesadas": 0, "sin_base": 0}


# ---------- consulta A: cada resena clasificada a mongo, y riesgo de los negocios afectados (alertas) ----------
def lote_resenas(df, id_lote):
    filas = df.collect()
    if not filas:
        return
    ahora = datetime.utcnow()
    db[config.COL_RESENAS_STREAM].bulk_write([ReplaceOne({"_id": r["review_id"]}, {
        "_id": r["review_id"], "business_id": r["business_id"], "stars": r["stars"], "date": r["date"],
        "prediccion": int(r["prediction"]), "sentimiento": r["etiqueta"], "probabilidad": float(r["prob"]),
        "geohash": r["geohash"], "procesado_en": ahora, "lote": id_lote}, upsert=True) for r in filas], ordered=False)
    estado["procesadas"] += len(filas)
    estado["fecha_evento"] = max(r["date"] for r in filas)
    estado["lote"] = id_lote
    actualizar_riesgo(df.sparkSession, filas, id_lote, ahora)


def actualizar_riesgo(sesion_spark, filas, id_lote, ahora):
    """Cada micro-lote: actualiza las variables de los negocios que recibieron resenas, los puntua con el modelo
    de riesgo y deja en alerts (con su geometria) los que superan el umbral."""
    nuevas = defaultdict(list)
    for r in filas:
        nuevas[r["business_id"]].append((r["date"], int(r["stars"])))
    fecha_ref = max(r["date"] for r in filas)
    docs = {d["_id"]: d for d in db[config.COL_RIESGO].find({"_id": {"$in": list(nuevas)}})}
    a_puntuar, nuevos_estados = [], {}
    for bid, lista in nuevas.items():
        d = docs.get(bid)
        if d is None:
            estado["sin_base"] += 1  # negocio sin linea base (no estaba activo a T): no se puntua
            continue
        est = actualizar_estado(d["estado"], lista, fecha_ref)
        f = features_de_estado(est, fecha_ref)
        f["competidores"] = d["features"]["competidores"]  # la densidad de competidores no cambia con las resenas
        nuevos_estados[bid] = (est, f)
        a_puntuar.append({"business_id": bid, **f})
    if not a_puntuar:
        return
    res = (m_riesgo.transform(sesion_spark.createDataFrame(pd.DataFrame(a_puntuar)))
           .withColumn("p", vector_to_array("probability")[1]).select("business_id", "p").collect())
    ops_riesgo, ops_alertas = [], []
    for r in res:
        bid, p = r["business_id"], float(r["p"])
        est, f = nuevos_estados[bid]
        d = docs[bid]
        ops_riesgo.append(UpdateOne({"_id": bid}, {"$set": {"prob": p, "alerta": p >= UMBRAL_RIESGO, "fecha_ref": fecha_ref, "features": f,
                                                            "estado": est, "version": v_riesgo["version"], "actualizado_en": ahora}}))
        if p >= UMBRAL_RIESGO:
            ops_alertas.append(UpdateOne({"_id": bid}, {"$set": {
                "business_id": bid, "name": d["name"], "categoria_principal": d["categoria_principal"], "geohash": d["geohash"],
                "loc": d["loc"], "prob": p, "umbral": UMBRAL_RIESGO, "fecha": fecha_ref, "generado_en": ahora, "activa": True,
                "features": f, "version": v_riesgo["version"], "lote": id_lote}}, upsert=True))
        else:  # si estaba en alerta y ya bajo del umbral, la alerta se cierra
            ops_alertas.append(UpdateOne({"_id": bid, "activa": True}, {"$set": {"activa": False, "cerrada_en": ahora, "prob": p}}))
    db[config.COL_RIESGO].bulk_write(ops_riesgo, ordered=False)
    db[config.COL_ALERTAS].bulk_write(ops_alertas, ordered=False)


# ---------- consulta B: ventanas de 30 dias por celda, con watermark de 7 dias sobre la fecha de la resena ----------
# las ventanas de spark parten del 1-ene-1970; startTime las corre para que una empiece exactamente en T
corrimiento = int(T.replace(tzinfo=timezone.utc).timestamp()) % (config.VENTANA_DIAS * 86400)
ventanas = (clas.where(F.col("geohash").isNotNull())
            .withWatermark("date", f"{config.WATERMARK_DIAS} days")
            .groupBy(F.window("date", f"{config.VENTANA_DIAS} days", f"{config.VENTANA_DIAS} days", f"{corrimiento} seconds"), "geohash")
            .agg(F.count("*").alias("n"), F.sum((F.col("prediction") == 0).cast("int")).alias("negativas")))


def evaluar(celda, inicio, ahora, id_lote):
    """Recalcula % negativo, cambio frente a la ventana anterior y si es hotspot (con la regla justificada en umbral_hotspot.py)."""
    actual = db[config.COL_VENTANAS].find_one({"_id": f"{celda}|{inicio:%Y-%m-%d}"})
    if actual is None:
        return
    previa = db[config.COL_VENTANAS].find_one({"_id": f"{celda}|{inicio - VENTANA:%Y-%m-%d}"})
    pct_prev, n_prev = (previa["pct_negativas"], previa["n"]) if previa else (None, 0)
    hot = es_hotspot(actual["pct_negativas"], pct_prev, actual["n"], n_prev, UMBRAL_HOTSPOT, config.MIN_RESENAS_VENTANA)
    db[config.COL_VENTANAS].update_one({"_id": actual["_id"]}, {"$set": {
        "pct_prev": pct_prev, "n_prev": n_prev, "umbral_pp": UMBRAL_HOTSPOT, "hotspot": bool(hot),
        "delta_pp": None if pct_prev is None else round(actual["pct_negativas"] - pct_prev, 2), "evaluado_en": ahora, "lote": id_lote}})


def lote_ventanas(df, id_lote):
    filas = df.collect()
    if not filas:
        return
    ahora = datetime.utcnow()
    tocadas = set()
    for r in filas:
        celda, inicio = r["geohash"], r["window"]["start"]
        la, lo = geohash_centro(celda)
        db[config.COL_VENTANAS].update_one({"_id": f"{celda}|{inicio:%Y-%m-%d}"}, {"$set": {
            "celda": celda, "inicio": inicio, "fin": r["window"]["end"], "n": int(r["n"]), "negativas": int(r["negativas"]),
            "pct_negativas": round(r["negativas"] / r["n"] * 100, 2), "loc": {"type": "Point", "coordinates": [lo, la]},
            "geom": geohash_poligono(celda), "actualizado_en": ahora}}, upsert=True)
        tocadas.add((celda, inicio))
        tocadas.add((celda, inicio + VENTANA))  # la ventana siguiente se compara contra esta: hay que reevaluarla
    for celda, inicio in tocadas:
        evaluar(celda, inicio, ahora, id_lote)


def retraso_total(progreso):
    """Mensajes que hay en el topic y todavia no se procesan (retraso del consumidor)."""
    try:
        src = progreso["sources"][0]
        fin = src["endOffset"][TOPIC]
        from kafka import KafkaConsumer, TopicPartition
        c = KafkaConsumer(bootstrap_servers=config.KAFKA_SERVIDOR)
        partes = [TopicPartition(TOPIC, int(p)) for p in fin]
        ultimos = c.end_offsets(partes)
        c.close()
        return int(sum(ultimos[tp] - int(fin[str(tp.partition)]) for tp in partes))
    except Exception:
        return None


qa = (clas.writeStream.foreachBatch(lote_resenas).option("checkpointLocation", CHECK + "/resenas")
      .trigger(processingTime="5 seconds").start())
qb = (ventanas.writeStream.outputMode("update").foreachBatch(lote_ventanas).option("checkpointLocation", CHECK + "/ventanas")
      .trigger(processingTime="5 seconds").start())
print("streaming en marcha: topic", TOPIC, "| checkpoints en", CHECK)

# latido: cada 5 s deja en mongo el estado del flujo (lo lee /health de la api para mostrar el retraso del consumidor)
while qa.isActive and qb.isActive:
    p = qa.lastProgress or {}
    db[config.COL_METRICAS].replace_one({"_id": "streaming"}, {
        "_id": "streaming", "activo": True, "actualizado_en": datetime.utcnow(), "topic": TOPIC, "lote": estado["lote"],
        "resenas_procesadas": estado["procesadas"], "sin_linea_base": estado["sin_base"],
        "ultima_fecha_evento": estado["fecha_evento"], "filas_por_seg": round(p.get("processedRowsPerSecond") or 0, 1),
        "retraso": retraso_total(p) if p else None, "sentimiento_version": v_sent["version"],
        "riesgo_version": v_riesgo["version"]}, upsert=True)
    time.sleep(5)
for q in (qa, qb):
    if q.exception():
        raise RuntimeError(f"fallo la consulta {q.name}: {q.exception()}")
