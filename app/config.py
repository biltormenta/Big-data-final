import os

# todo sale de variables de entorno (las define docker-compose.yml); los valores por defecto son
# los nombres de servicio de la red de compose, por eso "mongo" funciona como hostname adentro
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "lifemap")
DASK_SCHEDULER = os.getenv("DASK_SCHEDULER", "tcp://dask-scheduler:8786")
DATOS_DIR = os.getenv("DATOS_DIR", "/data")
ZIP_LOCAL_DIR = os.getenv("ZIP_LOCAL_DIR", "/zip")

# para el /health de la api
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "http://spark-master:8080")
DASK_SCHEDULER_URL = os.getenv("DASK_SCHEDULER_URL", "http://dask-scheduler:8787")
GIT_COMMIT = os.getenv("GIT_COMMIT", "local")

KAGGLE_DATASET = "yelp-dataset/yelp-dataset"

# grupo del equipo: area metropolitana de Philadelphia (radio de 80 km desde el centro) + familia Restaurants
# es el unico grupo con mas de 1.000.000 de resenas incluso por area y familia a la vez
AREA_NOMBRE = "Philadelphia (PA)"
AREA_CENTRO = (39.9526, -75.1652)  # lat, lon
AREA_RADIO_KM = 80.0
FAMILIA = "Restaurants"
MIN_RESENAS_MUESTRA = 1_000_000

# colecciones de mongo
COL_NEGOCIOS = "negocios"
COL_RESENAS = "resenas"
COL_TASAS_CATEGORIA = "tasas_categoria"
COL_TASAS_CELDA = "tasas_celda"
COL_METRICAS = "metricas"

TAM_LOTE_MONGO = int(os.getenv("TAM_LOTE_MONGO", "10000"))
BLOQUE_RESENAS_MB = int(os.getenv("BLOQUE_RESENAS_MB", "32"))

# celdas de geohash de precision 5 (~5 km de lado): en el area salen unos cientos de celdas
GEOHASH_PRECISION = 5

# minimo de dias-negocio para comparar tasas entre grupos. Con la tasa global (~4,8 % anual) hacen falta
# unos 76.000 dias-negocio para esperar 10 cierres; se pide 100.000 (~274 negocios-año, ~13 cierres esperados)
MIN_DIAS_NEGOCIO = int(os.getenv("MIN_DIAS_NEGOCIO", "100000"))

# modelo de sentimiento guardado aqui (volumen compartido entre spark y la api)
RUTA_MODELO = os.path.join(DATOS_DIR, "modelos", "sentimiento")
ETIQUETAS = ["negativo", "neutral", "positivo"]

# ---------- entrega 2 ----------
# fecha de corte T: 12 meses antes de la ultima resena del dataset (2022-01-19). Lo posterior a T se reserva
# para el streaming y no se usa para entrenar nada (ni el sentimiento ni el riesgo)
FECHA_T = os.getenv("FECHA_T", "2021-01-19")
HORIZONTE_MESES = 12

KAFKA_SERVIDOR = os.getenv("KAFKA_SERVIDOR", "kafka:9092")
TOPIC_RESENAS = os.getenv("TOPIC_RESENAS", "reviews")
PARTICIONES_TOPIC = 3
# reloj acelerado del productor: cuantos segundos reales dura un mes de resenas
SEG_POR_MES = float(os.getenv("SEG_POR_MES", "10"))

# ventanas de 30 dias con watermark de 7 dias sobre la fecha de la resena (tiempo del evento)
VENTANA_DIAS = 30
WATERMARK_DIAS = 7
MIN_RESENAS_VENTANA = 30  # una ventana con menos resenas no cuenta para decidir hotspots

# supervivencia: un negocio solo entra al dataset desde su 5.a resena (Yelp pide al menos 5), eso es la entrada tardia
RESENAS_PARA_ENTRAR = 5
N_MIN_EN_RIESGO = 30  # la curva se corta donde quedan menos negocios en riesgo que esto
N_MIN_GRUPO = 100     # grupos con menos negocios o menos de 10 cierres no se interpretan
EVENTOS_MIN_GRUPO = 10

RUTA_MODELO_RIESGO = os.path.join(DATOS_DIR, "modelos", "riesgo")
RUTA_CHECKPOINTS = os.path.join(DATOS_DIR, "checkpoints")
RADIO_COMPETIDORES_M = 500

COL_RESENAS_STREAM = "resenas_stream"
COL_VENTANAS = "ventanas"
COL_ALERTAS = "alerts"
COL_RIESGO = "riesgo_actual"
COL_SUPERVIVENCIA = "supervivencia"
