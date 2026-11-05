import os
import threading

import requests
from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from app import config, consultas
from app.consultas import ParametroInvalido

app = Flask(__name__)
_cliente = None
_modelo = None
_lock = threading.Lock()


def db():
    global _cliente
    if _cliente is None:
        _cliente = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=3000)
    return _cliente[config.MONGO_DB]


@app.errorhandler(ParametroInvalido)
def parametro_invalido(e):
    return jsonify(error=str(e)), 400


# mongo rechaza geometrias mal formadas: se devuelve 400 en vez de un error 500
@app.errorhandler(OperationFailure)
def error_mongo(e):
    return jsonify(error=f"mongo rechazo la consulta: {e.details.get('errmsg', str(e))}"), 400


@app.get("/")
def indice():
    return jsonify(proyecto="LifeMap", area=config.AREA_NOMBRE, familia=config.FAMILIA, commit=config.GIT_COMMIT,
                   endpoints=["/health", "/businesses/near", "/businesses/within", "/zones/summary",
                              "/stats/closure", "/predict"])


# liveness barato: lo usa el healthcheck del compose (no depende de los otros servicios)
@app.get("/ping")
def ping():
    return jsonify(status="ok")


def _estado_http(url, validar):
    """Llama a un servicio por http y devuelve su estado; si no responde queda como caido."""
    try:
        r = requests.get(url, timeout=3)
        return validar(r)
    except Exception as e:
        return {"estado": "caido", "detalle": str(e)[:120]}


@app.get("/health")
def health():
    servicios = {}
    try:
        db().command("ping")
        servicios["mongo"] = {"estado": "ok", "negocios": db()[config.COL_NEGOCIOS].estimated_document_count(),
                              "resenas": db()[config.COL_RESENAS].estimated_document_count()}
    except Exception as e:
        servicios["mongo"] = {"estado": "caido", "detalle": str(e)[:120]}

    def spark(r):
        j = r.json()
        vivos = sum(1 for w in j.get("workers", []) if w.get("state") == "ALIVE")
        return {"estado": "ok" if j.get("status") == "ALIVE" and vivos else "caido", "workers_vivos": vivos}

    def dask(r):
        return {"estado": "ok" if r.status_code == 200 and r.text.strip() == "ok" else "caido"}

    servicios["spark"] = _estado_http(config.SPARK_MASTER_URL + "/json/", spark)
    servicios["dask"] = _estado_http(config.DASK_SCHEDULER_URL + "/health", dask)
    servicios["api"] = {"estado": "ok"}
    servicios["modelo_sentimiento"] = {"estado": "ok" if os.path.isdir(config.RUTA_MODELO) else "sin_entrenar"}
    ok = all(s["estado"] == "ok" for k, s in servicios.items() if k != "modelo_sentimiento")
    # 503 si algun servicio esta caido: los sistemas de monitoreo lo detectan sin leer el cuerpo
    return jsonify(status="ok" if ok else "degradado", commit=config.GIT_COMMIT, servicios=servicios), (200 if ok else 503)


@app.get("/businesses/near")
def cercanos():
    lat, lon = consultas.validar_punto(request.args.get("lat"), request.args.get("lon"))
    radio = consultas.validar_radio(request.args.get("radio"))
    categoria = consultas.validar_categoria(request.args.get("categoria"))
    limite = consultas.validar_limite(request.args.get("limit"))
    docs = consultas.cercanos(db()[config.COL_NEGOCIOS], lat, lon, radio, categoria, limite)
    return jsonify(consultas.coleccion_fc(docs))


@app.post("/businesses/within")
def dentro():
    # silent=True: si el cuerpo no es json devuelve None y abajo se responde 400
    cuerpo = request.get_json(silent=True)
    if cuerpo is None:
        raise ParametroInvalido("el cuerpo debe ser un JSON con el poligono GeoJSON")
    geom = consultas.validar_poligono(cuerpo)
    categoria = consultas.validar_categoria(request.args.get("categoria"))
    limite = consultas.validar_limite(request.args.get("limit"))
    docs = consultas.dentro_poligono(db()[config.COL_NEGOCIOS], geom, categoria, limite)
    return jsonify(consultas.coleccion_fc(docs))


@app.get("/zones/summary")
def zona():
    lat, lon = consultas.validar_punto(request.args.get("lat"), request.args.get("lon"))
    radio = consultas.validar_radio(request.args.get("radio"))
    categoria = consultas.validar_categoria(request.args.get("categoria"))
    min_dias = consultas.validar_min_dias(request.args.get("min_dias"))
    resumen = consultas.resumen_zona(db()[config.COL_NEGOCIOS], lat, lon, radio, categoria, min_dias)
    return jsonify(centro={"lat": lat, "lon": lon}, radio_m=radio, categoria=categoria, **resumen)


@app.get("/stats/closure")
def cierres():
    por = request.args.get("por", "categoria")
    if por not in ("categoria", "celda"):
        raise ParametroInvalido("por debe ser categoria o celda")
    min_dias = consultas.validar_min_dias(request.args.get("min_dias"))
    limite = consultas.validar_limite(request.args.get("limit"), defecto=20)
    filas = consultas.tasas_guardadas(db(), por, min_dias, limite)
    return jsonify(por=por, min_dias=min_dias, count=len(filas), resultados=filas)


def _cargar_modelo():
    """Carga el modelo de spark guardado por el job de sentimiento (una sola vez, en modo local)."""
    global _modelo
    with _lock:
        if _modelo is None:
            from pyspark.ml import PipelineModel
            from pyspark.sql import SparkSession
            spark = (SparkSession.builder.master("local[1]").appName("lifemap-api")
                     .config("spark.driver.memory", "512m").config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "1").getOrCreate())
            spark.sparkContext.setLogLevel("ERROR")
            _modelo = (spark, PipelineModel.load(config.RUTA_MODELO))
    return _modelo


@app.post("/predict")
def predict():
    cuerpo = request.get_json(silent=True)
    texto = cuerpo.get("text") if isinstance(cuerpo, dict) else None
    if not isinstance(texto, str) or not texto.strip():
        raise ParametroInvalido('el cuerpo debe ser un JSON con el campo "text" (texto no vacio)')
    if len(texto) > 5000:
        raise ParametroInvalido("text no puede pasar de 5000 caracteres")
    if not os.path.isdir(config.RUTA_MODELO):
        return jsonify(error="el modelo de sentimiento todavia no esta entrenado (corre spark_jobs/sentimiento.py)"), 503
    spark, modelo = _cargar_modelo()
    fila = modelo.transform(spark.createDataFrame([(texto,)], ["text"])).select("prediction", "probability").first()
    prob = [round(float(p), 4) for p in fila["probability"]]
    return jsonify(sentimiento=config.ETIQUETAS[int(fila["prediction"])],
                   probabilidades=dict(zip(config.ETIQUETAS, prob)))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
