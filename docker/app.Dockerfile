# imagen para el scheduler y los workers de dask, la ingesta y las pruebas unitarias
FROM python:3.11-slim

WORKDIR /srv
# primero solo requirements: docker reusa esta capa mientras no cambien las librerias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY tests ./tests
RUN mkdir -p /data && chmod 777 /data
ENV PYTHONPATH=/srv
