# ejecutar_todo.ps1: levanta los servicios y corre toda la entrega 1 en orden.
# Uso (desde esta carpeta, o con doble clic en ejecutar_todo.bat):
#   .\ejecutar_todo.ps1                  corre todo (la primera vez baja el dataset de Yelp, 4,4 GB, si no esta en YELP_ZIP_DIR)
#   .\ejecutar_todo.ps1 -SinIngesta      salta la descarga/ingesta (cuando mongo ya tiene los datos) y corre solo spark
#   .\ejecutar_todo.ps1 -SoloInforme     solo regenera el Word con los resultados que ya existen
#   .\ejecutar_todo.ps1 -Detener         apaga los servicios (los datos se conservan en volumenes)
param(
    [switch]$SinIngesta,
    [switch]$SoloInforme,
    [switch]$Detener
)

$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot

function Paso($t) { Write-Host "`n==== $t" -ForegroundColor Cyan }
function Fallo($t) { Write-Host "ERROR: $t" -ForegroundColor Red; exit 1 }

if ($Detener) { docker compose down; exit 0 }

if (-not $SoloInforme) {
    Paso "0. Docker y servicios (Mongo, Dask, Spark, API)"
    cmd /c "docker info >nul 2>&1"
    if ($LASTEXITCODE -ne 0) { Fallo "Docker Desktop no esta corriendo: abrelo, espera a que diga 'running' y vuelve a ejecutar" }
    docker compose up -d --build --wait mongo dask-scheduler dask-worker spark-master spark-worker api
    if ($LASTEXITCODE -ne 0) { Fallo "no se pudieron levantar los servicios (mira: docker compose logs)" }

    Paso "1. Pruebas unitarias (pytest) dentro de la imagen"
    docker run --rm lifemap-app:latest pytest tests/unit -q
    if ($LASTEXITCODE -ne 0) { Fallo "fallaron las pruebas unitarias" }

    if (-not $SinIngesta) {
        Paso "2. Ingesta: descarga de Yelp, limpieza con Dask y carga a MongoDB (tarda)"
        docker compose --profile jobs run --rm ingesta
        if ($LASTEXITCODE -ne 0) { Fallo "fallo la ingesta" }
    }

    # los jobs de spark escriben mucho log; aqui solo se muestran los resultados del script
    function Spark($script) {
        docker compose exec -T spark-master bash -c "set -o pipefail; /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-master --driver-memory 1g --executor-memory 1800m /opt/jobs/$script 2>&1 | grep -vE '^[0-9]{2}/[0-9]{2}/[0-9]{2}|WARN|^\s+at |NativeCodeLoader|Setting default log|To adjust logging'"
        if ($LASTEXITCODE -ne 0) { Fallo "fallo $script" }
    }

    Paso "3. Sentimiento: Naive Bayes y regresion logistica (particion por negocio)"
    Spark "sentimiento.py"

    Paso "4. MapReduce: tasas de cierre con reduceByKey por categoria y por celda de geohash"
    Spark "tasas_mapreduce.py"

    Paso "5. Balanceo de carga: 2, 8, 32 y 200 particiones y sesgo"
    Spark "balanceo.py"

    Paso "6. Pruebas contra la API (pila aparte, no toca los datos)"
    docker compose -p lifemap-ci -f docker-compose.yml -f docker-compose.ci.yml up -d --wait mongo api
    docker compose -p lifemap-ci -f docker-compose.yml -f docker-compose.ci.yml run --rm pruebas-api
    $rc = $LASTEXITCODE
    docker compose -p lifemap-ci -f docker-compose.yml -f docker-compose.ci.yml down -v --remove-orphans
    if ($rc -ne 0) { Fallo "fallaron las pruebas contra la API" }

    Paso "7. Copiando resultados a la carpeta 'resultados'"
    New-Item -ItemType Directory -Force resultados | Out-Null
    docker compose cp spark-master:/data/resultados/. resultados/
}

Paso "8. Informe en Word"
python informe\generar_informe.py
if ($LASTEXITCODE -ne 0) { Write-Host "No se pudo generar el Word: instala python-docx y matplotlib (pip install python-docx matplotlib) y ejecuta: python informe\generar_informe.py" -ForegroundColor Yellow }

Write-Host "`nListo. Resultados en 'resultados' e informe en informe\Informe_LifeMap_Entrega1.docx" -ForegroundColor Green
Write-Host "API: http://localhost:5000/health   Spark: http://localhost:8081   Dask: http://localhost:8787"
