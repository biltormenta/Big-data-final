import os
import socket
import threading
from datetime import datetime

import requests
from flask import Flask, jsonify, render_template, request
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


# mapa interactivo (leaflet): usa los mismos endpoints de abajo, desde el navegador
@app.get("/mapa")
def mapa():
    return render_template("mapa.html")


@app.get("/")
def indice():
    return jsonify(proyecto="LifeMap", area=config.AREA_NOMBRE, familia=config.FAMILIA, commit=config.GIT_COMMIT,
                   mapa="/mapa", endpoints=["/health", "/businesses/near", "/businesses/within", "/zones/summary",
                              "/stats/closure", "/predict", "/live/hotspots", "/alerts", "/survival/<categoria>", "/risk/near",
                              "/live/evolucion", "/live/estado"], dashboard="/dashboard")


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


def _estado_kafka():
    """Kafka responde si el puerto del broker acepta conexiones."""
    host, _, puerto = config.KAFKA_SERVIDOR.partition(":")
    try:
        socket.create_connection((host, int(puerto or 9092)), timeout=2).close()
        return {"estado": "ok", "servidor": config.KAFKA_SERVIDOR}
    except OSError as e:
        return {"estado": "caido", "servidor": config.KAFKA_SERVIDOR, "detalle": str(e)[:80]}


def _estado_consumidor():
    """El job de streaming deja un latido en mongo cada 5 s; si el ultimo es viejo, esta detenido."""
    d = db()[config.COL_METRICAS].find_one({"_id": "streaming"})
    if d is None:
        return {"estado": "sin_iniciar"}
    edad = (datetime.utcnow() - d["actualizado_en"]).total_seconds()
    if edad > 60:
        return {"estado": "detenido", "ultimo_latido_hace_s": round(edad)}
    return {"estado": "ok", "retraso_mensajes": d.get("retraso"), "resenas_procesadas": d.get("resenas_procesadas"),
            "ultima_fecha_evento": d["ultima_fecha_evento"].isoformat() if d.get("ultima_fecha_evento") else None,
            "filas_por_seg": d.get("filas_por_seg")}


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
    servicios["kafka"] = _estado_kafka()
    servicios["consumidor"] = _estado_consumidor()
    servicios["api"] = {"estado": "ok"}
    servicios["modelo_sentimiento"] = {"estado": "ok" if os.path.isdir(config.RUTA_MODELO) else "sin_entrenar"}
    # el consumidor (streaming) es opcional: detenido no tumba el estado general, pero se informa con su retraso
    ok = all(s["estado"] == "ok" for k, s in servicios.items() if k not in ("modelo_sentimiento", "consumidor"))
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


# ---------------- entrega 2: streaming, supervivencia y riesgo ----------------
@app.get("/live/hotspots")
def live_hotspots():
    """Celdas de una ventana de 30 dias cuyo % de resenas negativas subio mas del umbral (hotspots)."""
    inicio = consultas.validar_fecha(request.args.get("ventana"))
    zona = consultas.punto_opcional(request.args)
    todas = request.args.get("todas") == "1"
    limite = consultas.validar_limite(request.args.get("limit"), defecto=500)
    inicio, docs = consultas.hotspots(db(), inicio, zona, todas, limite)
    if inicio is None:
        return jsonify(type="FeatureCollection", count=0, ventana=None, features=[])
    feats = [consultas.ventana_a_feature(d) for d in docs]
    return jsonify(type="FeatureCollection", count=len(feats), ventana=inicio.isoformat(), todas=todas, features=feats)


@app.route("/alerts", methods=["GET", "POST"])
def alertas():
    """Alertas de riesgo de cierre generadas por el streaming: por radio (GET) o dentro de un poligono (POST)."""
    min_prob = consultas.validar_prob(request.args.get("min_prob"))
    limite = consultas.validar_limite(request.args.get("limit"), defecto=100)
    zona = poligono = None
    if request.method == "POST":
        cuerpo = request.get_json(silent=True)
        if cuerpo is None:
            raise ParametroInvalido("el cuerpo debe ser un JSON con el poligono GeoJSON")
        poligono = consultas.validar_poligono(cuerpo)
    else:
        zona = consultas.punto_opcional(request.args)
    docs = consultas.alertas(db(), zona, poligono, min_prob, request.args.get("todas") != "1", limite)
    return jsonify(consultas.coleccion_fc(docs))


@app.get("/survival")
def supervivencia_lista():
    """Que curvas hay (categorias y zonas) y cuales se pueden interpretar."""
    campos = {"_id": 0, "tipo": 1, "clave": 1, "negocios": 1, "cierres": 1, "mediana_dias": 1, "s_1a": 1, "s_3a": 1, "s_5a": 1, "pequeno": 1}
    docs = list(db()[config.COL_SUPERVIVENCIA].find({}, campos).sort("negocios", -1))
    return jsonify(count=len(docs), grupos=docs)


@app.get("/survival/<categoria>")
def supervivencia(categoria):
    """Curva de Kaplan-Meier de una categoria principal (TODAS = todos los negocios) o, con ?zona=<geohash>, de una zona."""
    zona = request.args.get("zona")
    clave = f"zona|{zona}" if zona else f"categoria|{categoria}"
    d = db()[config.COL_SUPERVIVENCIA].find_one({"_id": clave}, {"_id": 0, "geom": 0})
    if d is None:
        return jsonify(error=f"no hay curva para {clave} (grupo con menos de 30 negocios o nombre inexistente)"), 404
    if d["pequeno"]:
        d["aviso"] = "grupo pequeño: pocos negocios o pocos cierres; la curva no se interpreta"
    return jsonify(d)


@app.get("/risk/near")
def riesgo_cerca():
    """Negocios activos cerca de un punto con su probabilidad de cierre (la mas alta primero)."""
    lat, lon = consultas.validar_punto(request.args.get("lat"), request.args.get("lon"))
    radio = consultas.validar_radio(request.args.get("radio"))
    min_prob = consultas.validar_prob(request.args.get("min_prob"))
    limite = consultas.validar_limite(request.args.get("limit"), defecto=50)
    docs = consultas.riesgo_cercano(db(), lat, lon, radio, min_prob, limite)
    return jsonify(consultas.coleccion_fc(docs))


@app.get("/live/evolucion")
def live_evolucion():
    return jsonify(ventanas=consultas.a_json(consultas.evolucion(db())))


@app.get("/live/estado")
def live_estado():
    d = db()[config.COL_METRICAS].find_one({"_id": "streaming"}, {"_id": 0})
    modelo = db()[config.COL_METRICAS].find_one({"_id": "modelo_cargado"}, {"_id": 0})
    return jsonify(streaming=consultas.a_json(d), modelos=consultas.a_json(modelo), consumidor=_estado_consumidor(),
                   alertas_activas=db()[config.COL_ALERTAS].count_documents({"activa": True}),
                   resenas_clasificadas=db()[config.COL_RESENAS_STREAM].estimated_document_count())


@app.get("/dashboard")
def dashboard():
    return render_template("dashboard.html")


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
