#!/usr/bin/env bash
# prueba de humo del streaming: publica 100 mensajes en kafka y comprueba que quedan clasificados en mongo.
# La corre Jenkins en la pila de pruebas (contenedor pruebas-streaming). Si algo falla sale con error y NO se despliega.
set -euo pipefail
cd /opt
export TOPIC_RESENAS=reviews_test MODO_PRUEBA=1 FECHA_T="${FECHA_T:-2021-01-19}" PYTHONPATH=/opt:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

python3 tests/humo_streaming.py preparar

# el job de streaming de verdad, en modo local (sin cluster) y con el topic de prueba
/opt/spark/bin/spark-submit --master local[2] --driver-memory 1g --conf spark.ui.enabled=false \
  /opt/jobs/streaming_resenas.py > /tmp/streaming.log 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null || true' EXIT

# si el job se cae, se muestra su log en vez de quedarse esperando
python3 tests/humo_streaming.py esperar || { echo "--- log del streaming:"; tail -40 /tmp/streaming.log; exit 1; }
python3 tests/humo_streaming.py publicar 100
python3 tests/humo_streaming.py verificar 100 || { echo "--- log del streaming:"; tail -40 /tmp/streaming.log; exit 1; }
