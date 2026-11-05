#!/usr/bin/env bash
# vm_instalar.sh: prepara una VM Ubuntu y deja LifeMap corriendo con su URL publica (ngrok) y Jenkins.
# Uso, desde la carpeta del repo clonado en la VM:   bash despliegue/vm_instalar.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# 1. swap de 4 GB: una VM de 8 GB queda justa con spark + mongo + dask + jenkins
if ! swapon --show | grep -q .; then
  sudo fallocate -l 4G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab > /dev/null
fi

# 2. docker (y que arranque solo cada vez que la VM se enciende)
if ! command -v docker > /dev/null; then
  curl -fsSL https://get.docker.com | sudo sh
fi
sudo systemctl enable --now docker
D="sudo docker"  # con sudo para no depender de cerrar y abrir sesion

# 3. .env: se pide una sola vez (el .env no esta en el repo y no se sube a ningun lado)
if [ ! -f .env ]; then
  read -rp "URL del repo (REPO_URL): " repo
  read -rp "Dominio de ngrok (NGROK_DOMAIN, sin https://): " dom
  read -rsp "Authtoken de ngrok (NGROK_AUTHTOKEN, no se ve al escribir): " tok; echo
  {
    echo "JENKINS_ADMIN_PASSWORD=$(openssl rand -hex 8)"
    echo "REPO_URL=$repo"
    echo "KAGGLE_USERNAME="
    echo "KAGGLE_KEY="
    echo "NGROK_AUTHTOKEN=$tok"
    echo "NGROK_DOMAIN=$dom"
  } > .env
  chmod 600 .env
fi
# el hash del commit que se despliega queda en /health
sed -i '/^GIT_COMMIT=/d' .env
echo "GIT_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo local)" >> .env

# 4. servicios (la primera vez construye las imagenes: unos 10 minutos)
$D compose up -d --build --wait mongo dask-scheduler dask-worker spark-master spark-worker api
$D compose --profile ngrok up -d ngrok   # la URL publica ya responde (con 0 datos hasta que termine la carga)

# 5. datos y jobs de Spark, en segundo plano (en una VM de 2 nucleos tardan alrededor de 1 hora)
cat > carga.sh <<'EOF'
set -e
sudo docker compose --profile jobs run --rm ingesta
for job in sentimiento tasas_mapreduce balanceo; do
  sudo docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --conf spark.driver.host=spark-master --driver-memory 1g --executor-memory 1800m /opt/jobs/$job.py
done
sudo docker compose up -d --build --wait jenkins   # Jenkins al final: su primer build redespliega y mataria una carga en curso
echo CARGA_TERMINADA
EOF
nohup bash carga.sh > carga.log 2>&1 &
echo "Listo: la carga sigue en segundo plano. Para ver el avance:  tail -f carga.log   (termina con CARGA_TERMINADA)"
echo "Contraseña de Jenkins (usuario admin): $(grep JENKINS_ADMIN_PASSWORD .env | cut -d= -f2)"
