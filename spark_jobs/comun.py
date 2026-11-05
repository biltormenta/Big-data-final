import json
import os

from pyspark.sql import SparkSession

URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
DB = os.getenv("MONGO_DB", "lifemap")
DATOS = os.getenv("DATOS_DIR", "/data")
RESENAS = os.path.join(DATOS, "clean", "resenas")
SALIDA = os.path.join(DATOS, "resultados")


def sesion(nombre):
    """Sesion de spark con el conector de mongo (el .jar queda en la imagen, ver docker/spark.Dockerfile)."""
    spark = (SparkSession.builder.appName(nombre)
             .config("spark.mongodb.read.connection.uri", URI)
             .config("spark.mongodb.write.connection.uri", URI)
             .config("spark.sql.shuffle.partitions", "16")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def leer_mongo(spark, coleccion):
    return spark.read.format("mongodb").option("database", DB).option("collection", coleccion).load()


def guardar_mongo(df, coleccion):
    # overwrite: repetir el job no duplica resultados
    df.write.format("mongodb").mode("overwrite").option("database", DB).option("collection", coleccion).save()


def guardar_json(nombre, datos):
    os.makedirs(SALIDA, exist_ok=True)
    with open(os.path.join(SALIDA, nombre), "w") as f:
        json.dump(datos, f, indent=2, default=str)


def info_negocios(spark):
    """Un registro por negocio desde mongo (conector): categoria, ciudad, celda y si cerro. Son ~17.000 filas."""
    from pyspark.sql import functions as F
    return (leer_mongo(spark, "negocios")
            .select(F.col("_id").alias("business_id"), "categoria_principal", "city", "geohash",
                    F.col("vida.evento").alias("evento"), F.col("vida.dias_negocio").alias("dias")))


def resenas_con_tiempo(spark):
    """Resenas desde el parquet limpio con la fecha en segundos: solo se leen las columnas que hacen falta."""
    from pyspark.sql import functions as F
    return (spark.read.parquet(RESENAS).select("business_id", "sentimiento", F.unix_timestamp("date").alias("t")))
