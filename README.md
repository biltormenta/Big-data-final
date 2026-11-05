# LifeMap · Entrega 1

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
