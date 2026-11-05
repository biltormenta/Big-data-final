# spark 3.5 con java 17 y python 3.10; esta misma imagen la usan el maestro, el worker y la api
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
# mongo spark connector 10.x y sus dependencias, bajados al construir la imagen
# (asi los jobs no necesitan internet cuando se ejecutan, por ejemplo en la sustentacion)
ARG MVN=https://repo1.maven.org/maven2/org/mongodb
RUN cd /opt/spark/jars \
 && curl -fsSLO ${MVN}/spark/mongo-spark-connector_2.12/10.4.0/mongo-spark-connector_2.12-10.4.0.jar \
 && curl -fsSLO ${MVN}/mongodb-driver-sync/5.1.4/mongodb-driver-sync-5.1.4.jar \
 && curl -fsSLO ${MVN}/mongodb-driver-core/5.1.4/mongodb-driver-core-5.1.4.jar \
 && curl -fsSLO ${MVN}/bson/5.1.4/bson-5.1.4.jar \
 && curl -fsSLO ${MVN}/bson-record-codec/5.1.4/bson-record-codec-5.1.4.jar

# librerias de python: parquet, mongo, la api flask y pytest (la api corre en esta misma imagen para poder cargar el modelo)
RUN pip install --no-cache-dir "numpy<2" pandas==2.2.3 pyarrow==18.1.0 scipy==1.13.1 \
    pymongo==4.10.1 requests==2.32.3 flask==3.1.0 gunicorn==23.0.0 pytest==8.3.4

WORKDIR /opt
# codigo dentro de la imagen (nada de volumenes de codigo, asi jenkins puede desplegar desde el repo)
COPY app /opt/app
COPY spark_jobs /opt/jobs
COPY tests /opt/tests
RUN mkdir -p /data /ivy && chmod -R 777 /data /ivy
# la api importa pyspark (para /predict) sin pasar por spark-submit, por eso se agrega a PYTHONPATH
ENV PYTHONPATH=/opt:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ARG GIT_COMMIT=local
ENV GIT_COMMIT=${GIT_COMMIT}
