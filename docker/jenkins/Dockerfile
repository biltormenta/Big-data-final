# Jenkins oficial (versión estable LTS con Java 17)
FROM jenkins/jenkins:lts-jdk17

USER root
# cliente de docker + compose, el demonio es el del host (se monta el socket)
# Jenkins no trae Docker: esto instala solo el comando "docker" para que el pipeline pueda usarlo
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
 && install -m 0755 -d /etc/apt/keyrings \
 && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
 && echo "deb [signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" > /etc/apt/sources.list.d/docker.list \
 && apt-get update && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin \
 && rm -rf /var/lib/apt/lists/*

# Plugins que necesita el pipeline (git, GitHub, Pipeline, credenciales y JCasC)
COPY plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli --plugin-file /usr/share/jenkins/ref/plugins.txt

# JCasC (Configuration as Code): Jenkins se configura solo al arrancar leyendo casc.yaml,
# sin tener que hacer clics en la interfaz. Se desactiva el asistente inicial.
COPY casc.yaml /usr/share/jenkins/casc.yaml
ENV CASC_JENKINS_CONFIG=/usr/share/jenkins/casc.yaml
ENV JAVA_OPTS="-Djenkins.install.runSetupWizard=false -Xmx512m"
