import os
import time

from pymongo import MongoClient
from pyspark import StorageLevel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StopWordsRemover
from pyspark.sql import functions as F

from comun import DB, RESENAS, URI, guardar_json, sesion
from app import config

# fraccion del entrenamiento que se usa (1.0 = todo); se baja si la maquina se queda corta de memoria
FRACCION = float(os.getenv("FRACCION_ENTRENO", "1.0"))
NUM_FEATURES = 2 ** 15

spark = sesion("lifemap-sentimiento")
t0 = time.time()

df = (spark.read.parquet(RESENAS).select("business_id", "text", "sentimiento")
      .withColumn("label", F.when(F.col("sentimiento") == "negativo", 0.0)
                           .when(F.col("sentimiento") == "neutral", 1.0).otherwise(2.0)))

# particion por negocio: el hash del business_id decide el lado, asi un mismo negocio no queda en los dos conjuntos
lado = F.pmod(F.hash("business_id"), F.lit(10))
train = df.filter(lado < 8)
test = df.filter(lado >= 8)
if FRACCION < 1.0:
    train = train.sample(FRACCION, seed=42)

# comprobacion de que no hay fuga: ningun negocio aparece en los dos lados
negocios_train = train.select("business_id").distinct()
negocios_test = test.select("business_id").distinct()
cruce = negocios_train.join(negocios_test, "business_id").count()
print("negocios en train:", negocios_train.count(), "en test:", negocios_test.count(), "en los dos:", cruce)
assert cruce == 0, "un negocio quedo en train y test"

# mismo pipeline de SentimentStream: limpieza, tokenizacion, stopwords en ingles, TF-IDF
# (\\W+ parte el texto en cualquier cosa que no sea letra o numero; se pasa a minusculas antes)
etapas = [
    RegexTokenizer(inputCol="text", outputCol="palabras", pattern="\\W+", minTokenLength=2, toLowercase=True),
    StopWordsRemover(inputCol="palabras", outputCol="limpias"),  # lista de stopwords en ingles por defecto
    HashingTF(inputCol="limpias", outputCol="tf", numFeatures=NUM_FEATURES),
    IDF(inputCol="tf", outputCol="features", minDocFreq=5),
]
features = Pipeline(stages=etapas).fit(train)
train_f = features.transform(train).select("label", "features").persist(StorageLevel.MEMORY_AND_DISK)
test_f = features.transform(test).select("label", "features").persist(StorageLevel.MEMORY_AND_DISK)
n_train, n_test = train_f.count(), test_f.count()
print("resenas train:", n_train, "test:", n_test)

CLASES = config.ETIQUETAS


def metricas(pred):
    """Precision, recall y F1 por clase, calculados a mano desde la matriz de confusion."""
    filas = pred.groupBy("label", "prediction").count().collect()
    matriz = [[0] * 3 for _ in range(3)]  # matriz[real][predicho]
    for f in filas:
        matriz[int(f["label"])][int(f["prediction"])] = f["count"]
    total = sum(sum(m) for m in matriz)
    por_clase = {}
    for i, nombre in enumerate(CLASES):
        tp = matriz[i][i]
        predichos = sum(matriz[r][i] for r in range(3))
        reales = sum(matriz[i])
        p = tp / predichos if predichos else 0.0
        r = tp / reales if reales else 0.0
        f1 = 2 * p * r / (p + r) if p + r else 0.0
        por_clase[nombre] = {"precision": round(p, 4), "recall": round(r, 4), "f1": round(f1, 4), "soporte": reales}
    return {"accuracy": round(sum(matriz[i][i] for i in range(3)) / total, 4),
            "f1_macro": round(sum(c["f1"] for c in por_clase.values()) / 3, 4),
            "por_clase": por_clase, "matriz_confusion": matriz}


resultados, modelos = {}, {}
candidatos = {
    "naive_bayes": NaiveBayes(modelType="multinomial", smoothing=1.0, featuresCol="features", labelCol="label"),
    "regresion_logistica": LogisticRegression(maxIter=20, regParam=1e-4, featuresCol="features", labelCol="label"),
}
for nombre, clasif in candidatos.items():
    t = time.time()
    m = clasif.fit(train_f)
    seg_fit = time.time() - t
    r = metricas(m.transform(test_f))
    r["segundos_entrenamiento"] = round(seg_fit, 1)
    resultados[nombre] = r
    modelos[nombre] = m
    print(f"{nombre}: accuracy {r['accuracy']}, f1 macro {r['f1_macro']}, {r['segundos_entrenamiento']} s")
    for c, v in r["por_clase"].items():
        print("  ", c, v)

# el desbalance: que tan bien le iria a un modelo que siempre dice "positivo"
dist = {r["sentimiento"]: r["count"] for r in test.groupBy("sentimiento").count().collect()}
dist_train = {r["sentimiento"]: r["count"] for r in train.groupBy("sentimiento").count().collect()}
mayoritaria = max(dist, key=dist.get)
base = round(dist[mayoritaria] / n_test, 4)

mejor = max(resultados, key=lambda k: resultados[k]["f1_macro"])
# se guarda un PipelineModel completo (tokenizer, stopwords, tf-idf y clasificador) para la entrega 2 y para /predict
for nombre, m in modelos.items():
    PipelineModel(stages=features.stages + [m]).write().overwrite().save(config.RUTA_MODELO + "_" + nombre)
PipelineModel(stages=features.stages + [modelos[mejor]]).write().overwrite().save(config.RUTA_MODELO)

salida = {
    "particion": "por negocio (hash de business_id: 80 % train, 20 % test)",
    "fraccion_entrenamiento": FRACCION, "resenas_train": n_train, "resenas_test": n_test,
    "negocios_en_los_dos_lados": cruce,
    "distribucion_train": dist_train, "distribucion_test": dist,
    "clase_mayoritaria": mayoritaria, "accuracy_clase_mayoritaria": base,
    "modelos": resultados, "mejor_modelo": mejor, "ruta_modelo": config.RUTA_MODELO,
    "num_features": NUM_FEATURES, "segundos": round(time.time() - t0, 1),
}
guardar_json("sentimiento.json", salida)
MongoClient(URI)[DB][config.COL_METRICAS].replace_one({"_id": "sentimiento"}, {"_id": "sentimiento", **salida}, upsert=True)
print("mejor modelo:", mejor, "| accuracy de decir siempre", mayoritaria + ":", base)
spark.stop()
