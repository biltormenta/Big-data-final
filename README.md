# LifeMap · Entregas 1 y 2

Plataforma batch con el Yelp Open Dataset: ingesta y limpieza con Dask, negocios como GeoJSON en MongoDB, sentimiento de las reseñas con Spark MLlib, tasas de cierre anualizadas con MapReduce (`reduceByKey`), balanceo de carga y una API Flask con consultas geoespaciales. Todo corre con un solo `docker-compose.yml` y lo despliega Jenkins.

Curso Big Data · Institución Universitaria de Envigado · 2026-2 · David Mesa y Eber Urrego.

## Grupo elegido

| | |
|---|---|
| Área metropolitana | Philadelphia (PA): negocios a menos de 80 km del centro (39,9526, -75,1652) |
| Familia de categorías | Restaurants |
| Muestra | 16.944 negocios y 1.320.594 reseñas (el enunciado pide al menos 1.000.000 de reseñas) |

La ingesta verifica con un `assert` que la muestra tenga al menos 1.000.000 de reseñas.

## URL del sistema desplegado

> **URL pública:** https://reshape-varied-overlap.ngrok-free.dev  (`/health`, `/businesses/near`, ...). Túnel de ngrok con dominio fijo; ver la sección "Despliegue".

Opciones documentadas en la sección "Despliegue". `GET /health` informa el estado de MongoDB, Spark, Dask y la API, y el hash del commit desplegado.

## Arquitectura

```
Yelp (Kaggle) ─► Dask (scheduler + 2 workers) ─► Parquet limpio ─► MongoDB (negocios GeoJSON 2dsphere + reseñas)
                                                      │                    │
                                                      ▼                    ▼
                                         Spark (maestro + worker, conector de MongoDB)
                                         · sentimiento (Naive Bayes y regresión logística)
                                         · tasas de cierre (RDD reduceByKey) y balanceo
                                                                           │
                                          Flask (API) ◄────────────────────┘        Jenkins ─► despliega todo
```

Servicios del Compose: `mongo`, `dask-scheduler`, `dask-worker` (2 réplicas), `spark-master`, `spark-worker`, `api`, `jenkins`. Los jobs de una sola vez (`ingesta`, `pruebas-api`) y el túnel están en *profiles* y no arrancan con `up`.

## Cómo reconstruirlo desde cero

Requisitos: Docker Desktop (o Docker Engine + Compose v2) con **al menos 6,5 GB de RAM asignada**, 15 GB de disco libre e internet (la primera vez se baja el dataset de Yelp, 4,4 GB, desde Kaggle).

```bash
git clone https://github.com/biltormenta/Big-data-final.git lifemap && cd lifemap
cp .env.example .env            # y cambiar JENKINS_ADMIN_PASSWORD; las credenciales de Kaggle son opcionales (el dataset es público)
docker compose up -d --build --wait mongo dask-scheduler dask-worker spark-master spark-worker api

# 1. ingesta: descarga de Kaggle (kagglehub) + limpieza particionada con Dask + carga por lotes a MongoDB
docker compose --profile jobs run --rm ingesta

# 2. jobs de Spark (cada uno tarda unos minutos)
for job in sentimiento tasas_mapreduce balanceo; do
  docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --conf spark.driver.host=spark-master --driver-memory 1g --executor-memory 1800m /opt/jobs/$job.py
done
```

En Windows basta con doble clic en `ejecutar_todo.bat` (o `.\ejecutar_todo.ps1`), que hace todo lo anterior, corre las pruebas y genera el informe en Word. Si ya tienes `yelp-dataset.zip`, define `YELP_ZIP_DIR` en el `.env` con la carpeta donde está y no se vuelve a bajar.

Credenciales: nada de secretos en el repositorio. `.env` está en `.gitignore`; el token de Kaggle (si se usa) llega a Jenkins como credencial (`kaggle-username` y `kaggle-key`, creadas por JCasC desde variables de entorno).

## Endpoints de la API (`http://localhost:5000`)

| Endpoint | Qué hace | Parámetros |
|---|---|---|
| `GET /health` | Estado de MongoDB, Spark, Dask y API, y hash del commit | |
| `GET /businesses/near` | Negocios cerca de un punto (`$near`, del más cercano al más lejano) | `lat`, `lon`, `radio` (m), `categoria`, `limit` |
| `POST /businesses/within` | Negocios dentro de un polígono GeoJSON (`$geoWithin`) en el cuerpo | `categoria`, `limit` |
| `GET /zones/summary` | Sentimiento medio, % de reseñas negativas y tasa de cierre alrededor de un punto (`$geoNear`) | `lat`, `lon`, `radio`, `categoria`, `min_dias` |
| `GET /stats/closure` | Tasas de cierre por categoría o por celda de geohash, con mínimo de días-negocio | `por` (`categoria`/`celda`), `min_dias`, `limit` |
| `POST /predict` | Sentimiento de un texto nuevo con el modelo guardado | cuerpo `{"text": "..."}` |

Ejemplos (centro de Philadelphia):

```bash
curl "localhost:5000/businesses/near?lat=39.9526&lon=-75.1652&radio=500&categoria=Pizza&limit=5"
curl "localhost:5000/zones/summary?lat=39.9526&lon=-75.1652&radio=1000"
curl "localhost:5000/stats/closure?por=categoria&min_dias=100000&limit=10"
curl -X POST localhost:5000/predict -H "Content-Type: application/json" -d '{"text": "the pasta was amazing"}'
curl -X POST localhost:5000/businesses/within -H "Content-Type: application/json" \
  -d '{"type":"Polygon","coordinates":[[[-75.18,39.94],[-75.14,39.94],[-75.14,39.96],[-75.18,39.96],[-75.18,39.94]]]}'
```

Todos validan sus entradas (código 400 con el motivo) y ninguno tiene valores fijos en el código.

## Qué hace cada pieza

| Pieza | Archivo | Resumen |
|---|---|---|
| Descarga | `app/descarga.py` | Usa el ZIP local si existe; si no, baja `yelp-dataset/yelp-dataset` con la API de Kaggle (kagglehub) |
| Limpieza | `app/limpieza.py` | Reglas de negocios y reseñas, cada una con su conteo |
| Ingesta | `app/ingesta.py` | Lectura por bloques de bytes (16 MB negocios, 32 MB reseñas), uno por tarea de Dask; vida de cada negocio; carga por lotes de 10.000 |
| Tasa | `app/tasas.py` | `tasa = cierres ÷ (días-negocio ÷ 365) × 100` y vida del negocio (evento y censura) |
| Sentimiento | `spark_jobs/sentimiento.py` | Tokenizer, stopwords en inglés, HashingTF/IDF, Naive Bayes y regresión logística; partición por negocio |
| MapReduce | `spark_jobs/tasas_mapreduce.py` | `reduceByKey` por categoría y por geohash, validado contra `groupBy`, con mínimo de días-negocio |
| Balanceo | `spark_jobs/balanceo.py` | 2, 8, 32 y 200 particiones con repeticiones, y filas por partición con `spark_partition_id()` |
| API | `app/api.py`, `app/consultas.py` | Endpoints y validación |
| CI/CD | `Jenkinsfile`, `docker/jenkins/` | Checkout, build, pytest, pila de pruebas, pruebas contra la API y despliegue |

Decisiones que conviene conocer:

- **Vida de un negocio:** de su primera a su última reseña (mínimo 1 día). Es una aproximación: el dataset no trae fechas de apertura ni de cierre.
- **Evento y censura:** `is_open = 0` es un cierre observado; `is_open = 1` está censurado (solo se sabe que seguía abierto en la fecha de corte, la última reseña del dataset).
- **Mínimo de días-negocio:** 100.000 (≈ 274 negocios-año). Con la tasa global (≈ 4,8 % anual) se esperan unos 13 cierres; con menos, la tasa de un grupo se mueve demasiado por azar. Es un parámetro (`min_dias`) de la API.
- **Celdas:** geohash de precisión 5 (≈ 5 km de lado), guardadas con su geometría de polígono y su propio índice 2dsphere.
- **Partición del modelo:** `pmod(hash(business_id), 10) < 8` para entrenamiento y el resto para prueba: un negocio nunca queda en los dos lados (se comprueba con un conteo).

## Pruebas

```bash
docker run --rm lifemap-app:latest pytest tests/unit -q      # fórmula de la tasa, limpieza, geohash, validaciones
docker compose -p lifemap-ci -f docker-compose.yml -f docker-compose.ci.yml up -d --wait mongo api
docker compose -p lifemap-ci -f docker-compose.yml -f docker-compose.ci.yml run --rm pruebas-api   # contra la API
```

## Jenkins

`docker compose up -d --build --wait jenkins` y abrir `http://localhost:8080` (usuario `admin`, contraseña `JENKINS_ADMIN_PASSWORD` del `.env`). El job `lifemap` se crea solo con JCasC y lee `REPO_URL` (rama `main`, `Jenkinsfile` de la raíz).

Etapas: Checkout → construir imágenes → pytest → levantar pila de pruebas → pruebas contra la API → desplegar → (opcional) cargar datos. Si una etapa falla, las siguientes no corren y no se despliega.

- **Disparo:** webhook de GitHub (`githubPush`) y, si Jenkins no es alcanzable desde internet, `pollSCM` cada 2 minutos (`H/2 * * * *`), ya configurado en el job.
- **Docker dentro de Jenkins:** el contenedor monta el socket de Docker (`/var/run/docker.sock`) y trae el cliente de Docker y Compose.
- **Hash del commit:** el pipeline lo exporta como `GIT_COMMIT` y compose lo pasa a la imagen; `/health` lo devuelve.

## Despliegue (URL pública)

El enunciado exige correr en un servidor distinto al portátil de desarrollo, con una URL pública estable desde 48 horas antes de la entrega. Aquí el servidor es un equipo (el PC de escritorio de uno de los integrantes) que corre el Compose y se expone con un túnel de ngrok con **dominio fijo**:

1. Crear una cuenta gratis en https://ngrok.com y, en el panel, ir a *Domains* y crear el dominio gratuito (queda algo como `nombre-aleatorio.ngrok-free.dev`).
2. Copiar el *Authtoken* (panel > *Your Authtoken*).
3. En el `.env` del servidor poner `NGROK_AUTHTOKEN=...` y `NGROK_DOMAIN=nombre-aleatorio.ngrok-free.dev` (el `.env` no se sube a GitHub).
4. `docker compose --profile ngrok up -d ngrok` y comprobar `https://<dominio>/health`.
5. Escribir esa URL arriba, en "URL pública", y en el informe (`python informe/generar_informe.py --url https://<dominio>`).

El contenedor `ngrok` tiene `restart: unless-stopped`, así que vuelve solo si Docker se reinicia; el equipo debe quedar encendido y sin suspensión desde 48 horas antes hasta el final de la sustentación. Alternativa sin cuenta: `docker compose --profile tunel up -d tunel` (túnel rápido de Cloudflare), pero su URL **cambia** en cada reinicio, así que no sirve para la ventana de 48 horas. **Opción robusta:** una máquina virtual en la nube (AWS Academy, t3.large de 8 GB). `despliegue/vm_instalar.sh` instala Docker, levanta todo (incluido Jenkins y el mismo túnel de ngrok) y carga los datos; la guía paso a paso está en `despliegue/GUIA_AWS_ACADEMY.md`.

Repositorio: https://github.com/biltormenta/Big-data-final

## Limitaciones

- La vida de un negocio es una aproximación (primera y última reseña, no apertura y cierre).
- Los negocios que ya existían antes de su primera reseña tienen entrada tardía; esta entrega la documenta pero no la modela (se usará en las curvas de supervivencia de la Entrega 2).
- Los tiempos del balanceo dependen de la máquina (aquí: un worker de Spark con 2 núcleos); sirven para comparar configuraciones entre sí, no como medida absoluta.

Fuente de datos: [Yelp Open Dataset](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset), usado según sus términos para fines académicos.

---

# Entrega 2: streaming, supervivencia y alerta temprana de cierre

Se monta sobre la Entrega 1 (mismo `docker-compose.yml`, misma base MongoDB, misma API y mismo Jenkins). Agrega Kafka en modo KRaft, un job de Spark Structured Streaming, curvas de supervivencia, un modelo de riesgo de cierre y un panel en vivo.

## Flujo

```
productor (reloj acelerado) ─► Kafka: topic reviews (3 particiones, clave business_id)
                                   │
                        Spark Structured Streaming (streaming_resenas.py)
                        · carga los modelos versionados (sentimiento y riesgo) sin reentrenar
                        ├─ consulta A: clasifica cada reseña ─► resenas_stream  +  riesgo de los negocios afectados ─► alerts
                        └─ consulta B: ventanas de 30 días por celda de geohash (watermark 7 días) ─► ventanas (hotspots)
                                   │
                          MongoDB ─► API (/live/hotspots, /alerts, /survival, /risk/near) ─► /dashboard
```

**Corte T = 2021-01-19** (12 meses antes de la última reseña del dataset, 2022-01-19). Todo lo posterior a T se reserva para el streaming: el modelo de sentimiento se reentrena una vez solo con reseñas hasta T y el de riesgo usa variables calculadas hasta T. Ambos modelos se guardan con una **huella** (`version.json`); el streaming comprueba que lo que carga es exactamente lo versionado.

## Cómo correrlo

Requiere la Entrega 1 ya cargada (`ejecutar_todo.ps1`). Después, en Windows, doble clic en `ejecutar_entrega2.bat` (o `.\ejecutar_entrega2.ps1`; con `-SegPorMes 3` el productor va más rápido). Hace todo en orden:

```bash
docker compose up -d --build --wait mongo kafka dask-scheduler dask-worker spark-master spark-worker api
for job in sentimiento umbral_hotspot supervivencia_job riesgo_modelo; do        # entrenamiento (unos 10 min en total)
  docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077     --conf spark.driver.host=spark-master --driver-memory 1g --executor-memory 1800m /opt/jobs/$job.py
done
docker compose --profile streaming up -d streaming        # el job de streaming (queda corriendo)
SEG_POR_MES=10 docker compose --profile jobs run --rm productor    # publica 12 meses de reseñas en 2 minutos
```

Mientras corre el productor, abrir `http://localhost:5000/dashboard`: mapa de hotspots, alertas activas, curvas de supervivencia y evolución del % negativo, actualizándose cada 5 segundos.

## Endpoints nuevos

| Endpoint | Qué hace | Parámetros |
|---|---|---|
| `GET /live/hotspots` | Celdas de una ventana de 30 días en hotspot, con su polígono | `ventana` (fecha de inicio o `ultima`), `lat`, `lon`, `radio`, `todas`, `limit` |
| `GET /alerts` · `POST /alerts` | Alertas activas por radio, o dentro de un polígono GeoJSON (POST) | `lat`, `lon`, `radio`, `min_prob`, `todas`, `limit` |
| `GET /survival/<categoria>` | Curva de Kaplan-Meier con número en riesgo (`TODAS` = todos los negocios) | `zona` (geohash) para la curva de una zona |
| `GET /risk/near` | Negocios cerca de un punto con su probabilidad de cierre | `lat`, `lon`, `radio`, `min_prob`, `limit` |
| `GET /health` | Ahora incluye Kafka y el retraso del consumidor (mensajes sin procesar) | |
| `GET /dashboard` | Panel en vivo con las cuatro visualizaciones | |

Extras: `GET /survival` (lista de curvas), `GET /live/evolucion` y `GET /live/estado`. Para Power BI ver `Guia_PowerBI_LifeMap.docx` (en 1_ENTREGABLES) (consultas listas para pegar).

## Decisiones

| Tema | Decisión |
|---|---|
| Productor | Publica solo lo posterior a T, en orden de fecha, con clave `business_id`; `SEG_POR_MES` = segundos reales por mes de reseñas (10 por defecto) |
| Ventanas | 30 días, watermark de 7 días sobre `date` (tiempo del evento), alineadas para que una empiece en T; se guardan con el polígono de su celda |
| Hotspot | El % de reseñas negativas sube ≥ **13,9 puntos** frente a la ventana anterior, con ≥ 30 reseñas en las dos. El umbral es el percentil 95 del cambio normal entre ventanas antes de T (`umbral_hotspot.py`) |
| Supervivencia | Kaplan-Meier propio (`app/supervivencia.py`): censura (negocio abierto) y entrada tardía (Yelp exige ≥ 5 reseñas: un negocio es observable desde su 5.ª); solo se usa donde hay ≥ 30 en riesgo; grupos < 100 negocios o < 10 cierres se marcan «pequeños» |
| Riesgo | Variables hasta T (sentimiento, tendencia del % negativo a 6 meses, estrellas, n.º de reseñas, edad, días desde la última, actividad 6 m, cambio de estrellas, competidores a 500 m con `$geoNear`); etiqueta a 12 meses; validación cruzada de 5 pliegues para elegir modelo y umbral y prueba usada una sola vez; métricas AUC-ROC, AUC-PR y recall |
| Alertas | `foreachBatch`: cada micro-lote actualiza el estado de los negocios afectados (sin releer su historia), los puntúa y deja en `alerts` los que superan el umbral |

## Pruebas y Jenkins

- Unitarias (`tests/unit`): Kaplan-Meier contra casos calculados a mano (censura y entrada tardía), variables de riesgo sin fuga de lo posterior a T, estado incremental = recálculo completo, regla de hotspot, versionado de modelos.
- Contra la API (`tests/smoke/test_api.py`): los endpoints nuevos con datos sintéticos.
- **Prueba de humo del flujo** (`tests/humo_streaming.sh`): publica 100 mensajes en un topic de prueba y verifica que quedan clasificados en MongoDB, agregados en ventanas con geometría y con el riesgo actualizado, que el modelo cargado tiene la huella de su `version.json` y que el topic tiene 3 particiones. Corre en la pila de pruebas de Jenkins (etapa «Prueba del flujo de streaming y del modelo versionado»); si falla, no se despliega.

## Limitaciones

La vida de un negocio sigue siendo aproximada; el modelo de riesgo es modesto (pocos cierres en la prueba); las alertas dependen de la velocidad de reproducción; el `.pbix` de Power BI se arma con la guía (no se puede generar por código).
