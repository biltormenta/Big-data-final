// pipeline declarativo: cada stage es una etapa; si una falla, las siguientes NO se ejecutan
pipeline {
    // corre en el propio jenkins (que tiene el cliente de docker instalado)
    agent any

    // casilla "Build with Parameters": con RECARGAR_DATOS se repite toda la carga (tarda mucho)
    parameters {
        booleanParam(name: 'RECARGAR_DATOS', defaultValue: false,
                     description: 'Descarga de Kaggle + ingesta con Dask + sentimiento, tasas y balanceo con Spark')
    }

    options {
        // dos builds al mismo tiempo se pisarian los contenedores
        disableConcurrentBuilds()
    }

    environment {
        // pila de pruebas aislada: otro nombre de proyecto y sin puertos fijos, para no chocar con el despliegue real
        CI_COMPOSE = 'docker compose -p lifemap-ci -f docker-compose.yml -f docker-compose.ci.yml'
    }

    stages {
        // 1. descarga el codigo del repositorio configurado en el job
        stage('Checkout') {
            steps {
                checkout scm
                // el hash del commit queda en /health de la api (compose lo pasa como argumento de construccion)
                script { env.GIT_COMMIT = sh(script: 'git rev-parse HEAD', returnStdout: true).trim() }
            }
        }

        // 2. construye las imagenes con el codigo recien descargado
        stage('Construir imagenes') {
            steps { sh 'docker compose build dask-scheduler spark-master' }
        }

        // 3. pytest dentro de las imagenes: la formula de la tasa de cierre, la limpieza, geohash y validaciones
        stage('Pruebas unitarias (pytest)') {
            steps {
                sh 'docker run --rm lifemap-app:latest pytest tests/unit -v'
                sh 'docker run --rm --entrypoint python3 lifemap-spark:latest -m pytest /opt/tests/unit -q'
            }
        }

        // 4. levanta solo mongo y la api en la pila de pruebas (--wait espera a que esten sanos)
        stage('Levantar pila de pruebas') {
            steps { sh '${CI_COMPOSE} up -d --wait mongo api' }
        }

        // 5. pruebas contra la api: llaman de verdad a los endpoints y verifican las respuestas
        stage('Pruebas contra la API') {
            steps { sh '${CI_COMPOSE} run --rm pruebas-api' }
        }

        // si cualquier etapa de arriba falla el pipeline se corta aqui y no se despliega nada
        // 6. despliegue real: reconstruye y reinicia los servicios con el codigo nuevo (los datos viven en volumenes)
        stage('Desplegar') {
            steps {
                sh 'docker compose up -d --build --wait mongo dask-scheduler dask-worker spark-master spark-worker api'
            }
        }

        // 7. opcional: descarga, limpia, carga a mongo y corre los jobs de spark.
        // withCredentials entrega el token de kaggle (guardado en jenkins) como variables de entorno,
        // sin que aparezca en el codigo ni en el repositorio.
        stage('Cargar datos') {
            when { expression { params.RECARGAR_DATOS } }
            steps {
                withCredentials([string(credentialsId: 'kaggle-username', variable: 'KAGGLE_USERNAME'),
                                 string(credentialsId: 'kaggle-key', variable: 'KAGGLE_KEY')]) {
                    sh 'docker compose --profile jobs run --rm -e KAGGLE_USERNAME -e KAGGLE_KEY ingesta'
                }
                sh '''for job in sentimiento tasas_mapreduce balanceo; do
                        docker compose exec -T spark-master /opt/spark/bin/spark-submit \
                          --master spark://spark-master:7077 --conf spark.driver.host=spark-master \
                          --driver-memory 1g --executor-memory 1800m /opt/jobs/$job.py
                      done'''
            }
        }
    }

    post {
        // pase lo que pase se apaga y borra la pila de pruebas (el "|| true" evita marcar error por esto)
        always { sh '${CI_COMPOSE} down -v --remove-orphans || true' }
        failure { echo 'Fallo una etapa, no se desplego nada nuevo.' }
    }
}
