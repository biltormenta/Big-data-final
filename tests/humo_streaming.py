"""Prueba de humo del flujo de streaming (la corre Jenkins antes de desplegar).

Publica 100 mensajes en un topic de prueba y verifica que aparecen CLASIFICADOS en MongoDB, que se agregaron en ventanas
y que el modelo que cargo el streaming es exactamente el versionado. Trabaja con modelos de juguete entrenados aqui mismo:
lo que se prueba es el flujo, no la calidad del modelo.

Uso (desde humo_streaming.sh):  python3 tests/humo_streaming.py preparar | esperar | publicar N | verificar N
"""
import random
import sys
import time
from datetime import datetime, timedelta

from pymongo import MongoClient

from app import config
from app.geo import geohash_codificar
from app.riesgo import FEATURES
from app.versiones import escribir_version, leer_version

TOPIC = config.TOPIC_RESENAS  # el .sh lo deja en reviews_test
T = datetime.fromisoformat(config.FECHA_T)
LAT0, LON0 = 0.0, -30.0
GH = geohash_codificar(LAT0, LON0, config.GEOHASH_PRECISION)
NEGOCIOS = [f"humo-{i}" for i in range(5)]
BUENAS = ["great food and friendly service", "amazing pizza delicious and fresh", "loved it excellent staff wonderful place"]
MALAS = ["terrible service cold food awful", "rude staff slow and dirty never again", "disgusting bad horrible experience"]
db = MongoClient(config.MONGO_URI)[config.MONGO_DB]


def preparar():
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression, NaiveBayes
    from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StopWordsRemover, VectorAssembler
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[1]").appName("humo-preparar").config("spark.ui.enabled", "false") \
        .config("spark.driver.memory", "768m").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    for col in (config.COL_NEGOCIOS, config.COL_RESENAS_STREAM, config.COL_VENTANAS, config.COL_ALERTAS, config.COL_RIESGO,
                config.COL_METRICAS, "config"):
        db[col].drop()

    # modelo de sentimiento de juguete: mismo pipeline que el de verdad, entrenado con frases inventadas
    filas = [(random.choice(BUENAS), 2.0) for _ in range(60)] + [(random.choice(MALAS), 0.0) for _ in range(60)] + \
            [("it was okay nothing special average", 1.0) for _ in range(30)]
    entrenamiento = spark.createDataFrame(filas, ["text", "label"])
    pipe = Pipeline(stages=[RegexTokenizer(inputCol="text", outputCol="palabras", pattern="\\W+"),
                            StopWordsRemover(inputCol="palabras", outputCol="limpias"),
                            HashingTF(inputCol="limpias", outputCol="tf", numFeatures=1024),
                            IDF(inputCol="tf", outputCol="features"), NaiveBayes()])
    m = pipe.fit(entrenamiento)
    m.write().overwrite().save(config.RUTA_MODELO)
    escribir_version(config.RUTA_MODELO, {"modelo": "juguete", "fecha_t": config.FECHA_T})

    # modelo de riesgo de juguete: cierra quien lleva mucho sin resenas y tiene sentimiento bajo
    random.seed(1)
    datos = []
    for _ in range(200):
        f = {k: random.random() for k in FEATURES}
        f["dias_desde_ultima"] = random.random() * 400
        f["sentimiento_medio"] = random.random() * 2 - 1
        datos.append({**f, "label": 1.0 if f["dias_desde_ultima"] > 200 and f["sentimiento_medio"] < 0 else 0.0})
    riesgo = Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol="features"), LogisticRegression(maxIter=50)])
    import pandas as pd
    riesgo.fit(spark.createDataFrame(pd.DataFrame(datos))).write().overwrite().save(config.RUTA_MODELO_RIESGO)
    escribir_version(config.RUTA_MODELO_RIESGO, {"modelo": "juguete", "fecha_t": config.FECHA_T, "umbral": 0.5, "features": FEATURES})
    spark.stop()

    # negocios de prueba (con su geohash, como los que carga la ingesta) y su linea base de riesgo
    la, lo = LAT0, LON0
    for i, bid in enumerate(NEGOCIOS):
        loc = {"type": "Point", "coordinates": [lo + i * 0.0002, la]}
        db[config.COL_NEGOCIOS].insert_one({"_id": bid, "name": f"negocio {i}", "categoria_principal": "Pizza", "geohash": GH, "loc": loc})
        db[config.COL_RIESGO].insert_one({
            "_id": bid, "business_id": bid, "name": f"negocio {i}", "categoria_principal": "Pizza", "geohash": GH, "loc": loc,
            "prob": 0.1, "umbral": 0.5, "alerta": False, "fecha_ref": T,
            "features": {"competidores": 3, "n_resenas": 20},
            "estado": {"n": 20, "suma_sentimiento": 5, "suma_estrellas": 70, "primera": T - timedelta(days=900),
                       "ultima": T - timedelta(days=20), "recientes": [[T - timedelta(days=30), 4]]}})
    db["config"].insert_one({"_id": "hotspot", "umbral_pp": 10.0})
    print("preparado: modelos de juguete versionados y", len(NEGOCIOS), "negocios de prueba")


def esperar():
    limite = time.time() + 240
    while time.time() < limite:
        d = db[config.COL_METRICAS].find_one({"_id": "streaming"})
        if d and d.get("activo"):
            print("streaming activo")
            return
        time.sleep(3)
    raise SystemExit("el streaming no arranco en 240 s")


def mensajes(n):
    """n resenas posteriores a T, en orden de fecha: mitad buenas (5 estrellas) y mitad malas (1 estrella)."""
    docs = []
    for i in range(n):
        buena = i % 2 == 0
        docs.append({"_id": f"humo-r{i}", "business_id": NEGOCIOS[i % len(NEGOCIOS)], "stars": 5 if buena else 1,
                     "text": random.choice(BUENAS if buena else MALAS), "date": T + timedelta(days=1 + i * 0.5)})
    return docs


def publicar(n):
    from app.productor import asegurar_topic, crear_productor, publicar as pub
    asegurar_topic(config.KAFKA_SERVIDOR, TOPIC)
    p = crear_productor(config.KAFKA_SERVIDOR)
    print("publicados:", pub(p, TOPIC, mensajes(n)))


def verificar(n):
    col = db[config.COL_RESENAS_STREAM]
    limite = time.time() + 180
    while time.time() < limite and col.count_documents({}) < n:
        time.sleep(3)
    docs = list(col.find({}))
    assert len(docs) == n, f"llegaron {len(docs)} de {n} mensajes clasificados a mongo"

    # cada mensaje quedo clasificado, con su probabilidad y la marca de procesamiento
    for d in docs:
        for campo in ("prediccion", "sentimiento", "probabilidad", "procesado_en", "business_id", "date"):
            assert d.get(campo) is not None, f"a {d['_id']} le falta {campo}"
        assert d["sentimiento"] in ("negativo", "neutral", "positivo") and 0 < d["probabilidad"] <= 1
    aciertos = sum(1 for d in docs if d["sentimiento"] == ("positivo" if d["stars"] == 5 else "negativo"))
    assert aciertos >= 0.9 * n, f"el modelo de juguete acerto solo {aciertos} de {n}"

    # ventanas por celda con su geometria
    v = list(db[config.COL_VENTANAS].find({}))
    assert v and all(x["celda"] == GH and x["geom"]["type"] == "Polygon" and x["n"] > 0 for x in v), "faltan ventanas con geometria"
    assert sum(x["n"] for x in v) == n, f"las ventanas suman {sum(x['n'] for x in v)} y deberian sumar {n}"

    # el riesgo de los negocios afectados se actualizo en el flujo
    actualizados = db[config.COL_RIESGO].count_documents({"actualizado_en": {"$exists": True}})
    assert actualizados == len(NEGOCIOS), f"se actualizaron {actualizados} de {len(NEGOCIOS)} negocios"

    # el modelo que cargo el streaming ES el versionado (misma huella que su version.json), y no se reentreno
    cargado = db[config.COL_METRICAS].find_one({"_id": "modelo_cargado"})
    for nombre, ruta in (("sentimiento", config.RUTA_MODELO), ("riesgo", config.RUTA_MODELO_RIESGO)):
        v_disco = leer_version(ruta)
        assert cargado[f"{nombre}_huella"] == v_disco["huella"], f"el modelo de {nombre} cargado no es el versionado"
        assert cargado[f"{nombre}_version"] == v_disco["version"]

    # el topic tiene 3 particiones y las resenas de un negocio van siempre a la misma (clave business_id)
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=config.KAFKA_SERVIDOR)
    partes = [TopicPartition(TOPIC, p) for p in c.partitions_for_topic(TOPIC)]
    fines = c.end_offsets(partes)
    c.close()
    assert len(partes) == config.PARTICIONES_TOPIC, f"el topic tiene {len(partes)} particiones"
    assert sum(fines.values()) == n
    print(f"OK: {n} mensajes clasificados ({aciertos} bien), {len(v)} ventana(s), modelos versionados, "
          f"particiones usadas: {sum(1 for x in fines.values() if x)} de {len(partes)}")


if __name__ == "__main__":
    accion = sys.argv[1]
    if accion == "preparar":
        preparar()
    elif accion == "esperar":
        esperar()
    elif accion == "publicar":
        publicar(int(sys.argv[2]))
    elif accion == "verificar":
        verificar(int(sys.argv[2]))
    else:
        raise SystemExit("uso: humo_streaming.py preparar | esperar | publicar N | verificar N")
