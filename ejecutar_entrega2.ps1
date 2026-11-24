# ejecutar_entrega2.ps1: corre la entrega 2 en orden (necesita la entrega 1 ya cargada: ejecutar_todo.ps1).
# Uso (desde esta carpeta, o con doble clic en ejecutar_entrega2.bat):
#   .\ejecutar_entrega2.ps1                  entrena (sentimiento con corte T, umbral, supervivencia, riesgo), arranca el streaming y publica
#   .\ejecutar_entrega2.ps1 -SegPorMes 3     acelera el productor (un mes de resenas cada 3 s en vez de cada 10)
#   .\ejecutar_entrega2.ps1 -SinEntrenar     salta el entrenamiento (los modelos ya estan guardados) y solo reinicia el flujo
#   .\ejecutar_entrega2.ps1 -SoloInforme     solo regenera el Word con los resultados que ya existen
#   .\ejecutar_entrega2.ps1 -Detener         apaga el streaming y los servicios
param(
    [double]$SegPorMes = 10,
    [switch]$SinEntrenar,
    [switch]$SoloInforme,
    [switch]$Detener
)

$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot

function Paso($t) { Write-Host "`n==== $t" -ForegroundColor Cyan }
function Fallo($t) { Write-Host "ERROR: $t" -ForegroundColor Red; exit 1 }

if ($Detener) { docker compose --profile streaming down; exit 0 }

if (-not $SoloInforme) {
    Paso "0. Docker y servicios (Mongo, Kafka, Dask, Spark, API)"
    cmd /c "docker info >nul 2>&1"
    if ($LASTEXITCODE -ne 0) { Fallo "Docker Desktop no esta corriendo: abrelo, espera a que diga 'running' y vuelve a ejecutar" }
    docker compose up -d --build --wait mongo kafka dask-scheduler dask-worker spark-master spark-worker api
    if ($LASTEXITCODE -ne 0) { Fallo "no se pudieron levantar los servicios (mira: docker compose logs)" }
    docker compose --profile streaming stop streaming 2>$null | Out-Null

    Paso "1. Pruebas unitarias (pytest) dentro de la imagen"
    docker run --rm lifemap-app:latest pytest tests/unit -q
    if ($LASTEXITCODE -ne 0) { Fallo "fallaron las pruebas unitarias" }

    function Spark($script) {
        docker compose exec -T spark-master bash -c "set -o pipefail; /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-master --driver-memory 1g --executor-memory 1800m /opt/jobs/$script 2>&1 | grep -vE '^[0-9]{2}/[0-9]{2}/[0-9]{2}|WARN|^\s+at |NativeCodeLoader|Setting default log|To adjust logging'"
        if ($LASTEXITCODE -ne 0) { Fallo "fallo $script" }
    }

    if (-not $SinEntrenar) {
        Paso "2. Sentimiento con corte T (el modelo no ve nada posterior a T) y version del modelo"
        Spark "sentimiento.py"
        Paso "3. Umbral de hotspot, justificado con la historia anterior a T"
        Spark "umbral_hotspot.py"
        Paso "4. Supervivencia (Kaplan-Meier con censura y entrada tardia) por categoria y por zona"
        Spark "supervivencia_job.py"
        Paso "5. Modelo de riesgo de cierre (variables hasta T, etiqueta a 12 meses) y linea base"
        Spark "riesgo_modelo.py"
    }

    Paso "6. Streaming: se limpian los datos del flujo anterior y arranca el job (Kafka -> Spark -> MongoDB)"
    docker compose exec -T mongo mongosh --quiet lifemap --eval "db.resenas_stream.drop(); db.ventanas.drop(); db.alerts.drop(); db.metricas.deleteOne({_id:'streaming'}); db.riesgo_actual.updateMany({}, {`$unset: {actualizado_en: 1}})" | Out-Null
    docker compose run --rm --no-deps -T --entrypoint bash spark-master -c "rm -rf /data/checkpoints" 2>$null | Out-Null
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic reviews 2>$null | Out-Null
    docker compose --profile streaming up -d --build streaming
    if ($LASTEXITCODE -ne 0) { Fallo "no arranco el streaming" }
    Write-Host "Esperando a que el streaming este activo (carga los dos modelos)..."
    for ($i = 0; $i -lt 60; $i++) {
        $estado = docker compose exec -T mongo mongosh --quiet lifemap --eval "print(db.metricas.countDocuments({_id:'streaming', activo:true}))" 2>$null
        if ($estado -match "1") { break }
        Start-Sleep 5
    }
    Write-Host "Panel en vivo:  http://localhost:5000/dashboard   (abrelo ahora para ver como se actualiza)" -ForegroundColor Green

    Paso "7. Productor con reloj acelerado ($SegPorMes s reales por mes de resenas)"
    $env:SEG_POR_MES = "$SegPorMes"
    docker compose --profile jobs run --rm productor
    if ($LASTEXITCODE -ne 0) { Fallo "fallo el productor" }
    Write-Host "Esperando a que el streaming termine de procesar lo publicado..."
    Start-Sleep 25

    Paso "8. Pruebas contra la API y prueba del flujo (pila aparte, no toca los datos)"
    $ci = "-p lifemap-ci -f docker-compose.yml -f docker-compose.ci.yml"
    Invoke-Expression "docker compose $ci up -d --wait mongo kafka api"
    Invoke-Expression "docker compose $ci run --rm pruebas-api"
    $rc1 = $LASTEXITCODE
    Invoke-Expression "docker compose $ci --profile tests run --rm pruebas-streaming"
    $rc2 = $LASTEXITCODE
    Invoke-Expression "docker compose $ci down -v --remove-orphans"
    if ($rc1 -ne 0 -or $rc2 -ne 0) { Fallo "fallaron las pruebas de la pila de pruebas" }

    Paso "9. Copiando resultados a la carpeta 'resultados'"
    New-Item -ItemType Directory -Force resultados | Out-Null
    docker compose cp spark-master:/data/resultados/. resultados/
}

Paso "10. Informe en Word"
python informe\generar_informe_e2.py
if ($LASTEXITCODE -ne 0) { Write-Host "No se pudo generar el Word: instala python-docx y matplotlib y ejecuta: python informe\generar_informe_e2.py" -ForegroundColor Yellow }

Write-Host "`nListo. Informe en informe\Informe_LifeMap_Entrega2.docx" -ForegroundColor Green
Write-Host "Panel: http://localhost:5000/dashboard   Mapa: http://localhost:5000/mapa   Salud: http://localhost:5000/health"
