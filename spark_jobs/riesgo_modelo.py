import time
from datetime import datetime
from functools import reduce

import numpy as np
import pandas as pd
from pymongo import GEOSPHERE, MongoClient
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window
from pyspark.sql import functions as F

from comun import RESENAS, guardar_json, sesion
from app import config
from app.riesgo import FEATURES, competidores_cercanos, etiqueta_riesgo, features_de_estado, resumen_historia
from app.versiones import escribir_version

T = datetime.fromisoformat(config.FECHA_T)
spark = sesion("lifemap-riesgo")
db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
t0 = time.time()

# los 17.000 negocios caben en memoria: viajan a los workers una sola vez
info = {d["_id"]: d for d in db[config.COL_NEGOCIOS].find({}, {"name": 1, "categoria_principal": 1, "geohash": 1, "loc": 1,
                                                              "is_open": 1, "vida": 1})}
info_b = spark.sparkContext.broadcast(info)

# ---------- variables por negocio, SOLO con resenas hasta T ----------
hist = (spark.read.parquet(RESENAS).select("business_id", "stars", "date")
        .filter(F.col("date") <= F.lit(config.FECHA_T).cast("timestamp"))
        .rdd.map(lambda r: (r["business_id"], (r["date"], int(r["stars"])))).groupByKey())


def construir(particion):
    """En cada worker: estado y variables de cada negocio; la densidad de competidores sale de $geoNear en mongo."""
    col = MongoClient(config.MONGO_URI)[config.MONGO_DB][config.COL_NEGOCIOS]
    for bid, filas in particion:
        n = info_b.value.get(bid)
        est = resumen_historia(list(filas), T)  # lo posterior a T se ignora aqui adentro
        if n is None or est is None:
            continue
        f = features_de_estado(est, T)
        lon, lat = n["loc"]["coordinates"]
        f["competidores"] = competidores_cercanos(col, lon, lat, n["categoria_principal"], bid, T, config.RADIO_COMPETIDORES_M)
        yield (bid, f, est)


filas = hist.mapPartitions(construir).collect()
print("negocios con historia hasta T:", len(filas))

registros, poblacion = [], []
for bid, f, est in filas:
    v = info[bid]["vida"]
    # la etiqueta SI mira despues de T (es lo que se quiere predecir); las variables no
    y = etiqueta_riesgo(info[bid]["is_open"], v["primera_resena"], v["ultima_resena"], T, config.HORIZONTE_MESES)
    if y is not None:
        registros.append({"business_id": bid, **f, "label": float(y)})
    if f["dias_desde_ultima"] <= 365:  # activos a T: son los que se puntuan como linea base
        poblacion.append((bid, f, est))
datos = pd.DataFrame(registros)
n_pos, n_tot = int(datos["label"].sum()), len(datos)
print(f"entrenamiento: {n_tot} negocios, {n_pos} cierres ({n_pos / n_tot * 100:.2f} %)")

# ---------- particion: 85 % desarrollo (validacion cruzada) y 15 % prueba, estratificada ----------
# (un negocio es una sola fila, asi que no hay fuga entre conjuntos). Con tan pocos cierres (342) una sola
# validacion es muy inestable, por eso el modelo y el umbral se eligen con validacion cruzada de 5 pliegues y la
# prueba se usa UNA sola vez, al final, con el modelo elegido y el umbral ya fijado.
df = spark.createDataFrame(datos)
trozos_dev, trozos_test = [], []
for clase in (0.0, 1.0):
    a, b = df.filter(F.col("label") == clase).randomSplit([0.85, 0.15], seed=42)
    trozos_dev.append(a), trozos_test.append(b)
dev, test = (reduce(lambda x, y: x.union(y), t).cache() for t in (trozos_dev, trozos_test))
tam = {"desarrollo": dev.count(), "prueba": test.count()}
pos = {"desarrollo": dev.filter(F.col("label") == 1).count(), "prueba": test.filter(F.col("label") == 1).count()}
print("tamaños:", tam, "cierres:", pos)

# peso por clase: sin esto un modelo que dice siempre "no cierra" acierta 96 % y no sirve de nada
w_pos = tam["desarrollo"] / (2 * pos["desarrollo"])
w_neg = tam["desarrollo"] / (2 * (tam["desarrollo"] - pos["desarrollo"]))
peso = F.when(F.col("label") == 1, F.lit(w_pos)).otherwise(F.lit(w_neg))
dev = dev.withColumn("peso", peso)
# pliegue de cada negocio: dentro de cada clase se ordena al azar y se reparte en 5 (asi cada pliegue tiene cierres)
orden = Window.partitionBy("label").orderBy(F.rand(7))
dev = dev.withColumn("pliegue", (F.row_number().over(orden) - 1) % 5).cache()

ensamblar = VectorAssembler(inputCols=FEATURES, outputCol="raw")
candidatos = {
    "regresion_logistica": lambda: Pipeline(stages=[ensamblar, StandardScaler(inputCol="raw", outputCol="features"),
                                                    LogisticRegression(weightCol="peso", maxIter=200, regParam=0.01)]),
    "gbt": lambda: Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol="features"),
                                    GBTClassifier(weightCol="peso", maxIter=60, maxDepth=3, stepSize=0.1, seed=42)]),
    "random_forest": lambda: Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol="features"),
                                              RandomForestClassifier(weightCol="peso", numTrees=150, maxDepth=6, seed=42)]),
}
roc, pr = (BinaryClassificationEvaluator(metricName=m, rawPredictionCol="puntaje") for m in ("areaUnderROC", "areaUnderPR"))


def con_prob(modelo, d):
    p = modelo.transform(d).withColumn("prob", vector_to_array("probability")[1])
    return p, pd.DataFrame(p.select("label", "prob").collect(), columns=["label", "prob"])


def auc(y, prob):
    """AUC-ROC y AUC-PR de un conjunto de predicciones (con el mismo evaluador de spark)."""
    d = spark.createDataFrame(pd.DataFrame({"label": y, "puntaje": prob}))
    return roc.evaluate(d), pr.evaluate(d)


def metricas_en(y, prob, umbral):
    pred = prob >= umbral
    tp, fp = int(((y == 1) & pred).sum()), int(((y == 0) & pred).sum())
    fn, tn = int(((y == 1) & ~pred).sum()), int(((y == 0) & ~pred).sum())
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    return {"umbral": round(float(umbral), 3), "precision": round(p, 4), "recall": round(r, 4),
            "f1": round(2 * p * r / (p + r), 4) if p + r else 0.0, "tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "marcados_pct": round((tp + fp) / len(y) * 100, 2)}


# ---------- validacion cruzada de 5 pliegues: AUC por pliegue y predicciones fuera de muestra ----------
resultado, fuera = {}, {}
for nombre, hacer in candidatos.items():
    rocs, prs, partes = [], [], []
    for k in range(5):
        m = hacer().fit(dev.filter(F.col("pliegue") != k))
        _, p = con_prob(m, dev.filter(F.col("pliegue") == k))
        a, b = auc(p["label"].to_numpy(), p["prob"].to_numpy())
        rocs.append(a), prs.append(b), partes.append(p)
    fuera[nombre] = pd.concat(partes)
    resultado[nombre] = {"auc_roc_cv": round(float(np.mean(rocs)), 4), "auc_roc_cv_desv": round(float(np.std(rocs)), 4),
                         "auc_pr_cv": round(float(np.mean(prs)), 4), "auc_pr_cv_desv": round(float(np.std(prs)), 4)}
    print(nombre, resultado[nombre])

# el modelo se elige con la validacion cruzada (AUC-PR, la metrica que importa con tan pocos cierres)
mejor = max(resultado, key=lambda k: resultado[k]["auc_pr_cv"])
# umbral: el que maximiza F1 sobre las predicciones fuera de muestra del modelo elegido
yv, pv = fuera[mejor]["label"].to_numpy(), fuera[mejor]["prob"].to_numpy()
umbral = float(max(np.linspace(0.02, 0.98, 97), key=lambda t: (metricas_en(yv, pv, t)["f1"], metricas_en(yv, pv, t)["recall"])))
en_cv = metricas_en(yv, pv, umbral)

# modelo final: se reentrena con todo el desarrollo y se mide UNA vez en la prueba, con el umbral ya fijado
modelo = candidatos[mejor]().fit(dev)
p_test, pdt = con_prob(modelo, test)
yt, pt = pdt["label"].to_numpy(), pdt["prob"].to_numpy()
en_test = metricas_en(yt, pt, umbral)
roc_t, pr_t = auc(yt, pt)
resultado[mejor].update({"auc_roc_test": round(roc_t, 4), "auc_pr_test": round(pr_t, 4)})
print("mejor:", mejor, "| umbral", round(umbral, 3), "| en validacion cruzada:", en_cv, "| en prueba:", en_test)

ultima = modelo.stages[-1]
valores = ultima.coefficients if mejor == "regresion_logistica" else ultima.featureImportances
importancia = dict(zip(FEATURES, [round(float(c), 4) for c in valores]))

# ---------- se guarda el modelo versionado ----------
modelo.write().overwrite().save(config.RUTA_MODELO_RIESGO)
version = escribir_version(config.RUTA_MODELO_RIESGO, {
    "modelo": mejor, "fecha_t": config.FECHA_T, "features": FEATURES, "umbral": round(float(umbral), 4),
    "auc_pr_test": resultado[mejor]["auc_pr_test"], "auc_roc_test": resultado[mejor]["auc_roc_test"],
    "entrenado_en": time.strftime("%Y-%m-%d %H:%M:%S")})
print("version del modelo de riesgo:", version["version"])

# ---------- linea base: todos los negocios activos a T con su riesgo, para consultar (/risk/near) y para que el streaming los actualice ----------
base = spark.createDataFrame(pd.DataFrame([{"business_id": b, **f} for b, f, _ in poblacion]))
puntos = modelo.transform(base).withColumn("prob", vector_to_array("probability")[1]).select("business_id", "prob").collect()
prob = {r["business_id"]: float(r["prob"]) for r in puntos}
docs = []
for bid, f, est in poblacion:
    n = info[bid]
    docs.append({"_id": bid, "business_id": bid, "name": n["name"], "categoria_principal": n["categoria_principal"],
                 "geohash": n["geohash"], "loc": n["loc"], "prob": prob[bid], "umbral": float(umbral),
                 "alerta": prob[bid] >= umbral, "fecha_ref": T, "features": f, "estado": est, "version": version["version"]})
db[config.COL_RIESGO].drop()
db[config.COL_RIESGO].insert_many(docs)
db[config.COL_RIESGO].create_index([("loc", GEOSPHERE)])
db[config.COL_RIESGO].create_index("prob")

salida = {
    "fecha_t": config.FECHA_T, "horizonte_meses": config.HORIZONTE_MESES, "features": FEATURES,
    "negocios_con_historia": len(filas), "negocios_activos_a_T": len(poblacion), "entrenamiento": {"negocios": n_tot, "cierres": n_pos,
                                                                                                    "cierres_pct": round(n_pos / n_tot * 100, 2)},
    "particion": {"tamanos": tam, "cierres": pos, "pliegues": 5}, "pesos_de_clase": {"cierra": round(w_pos, 2), "no_cierra": round(w_neg, 2)},
    "modelos": resultado, "mejor_modelo": mejor, "umbral": round(float(umbral), 4), "en_validacion_cruzada": en_cv, "en_prueba": en_test,
    "auc_pr_base_azar": round(pos["prueba"] / tam["prueba"], 4), "importancia": importancia, "version": version["version"],
    "alertas_base": int(sum(d["alerta"] for d in docs)), "segundos": round(time.time() - t0, 1),
}
guardar_json("riesgo.json", salida)
db[config.COL_METRICAS].replace_one({"_id": "riesgo"}, {"_id": "riesgo", **salida}, upsert=True)
print("alertas en la linea base:", salida["alertas_base"], "de", len(docs), "negocios activos")
spark.stop()
