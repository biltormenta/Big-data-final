import json
from collections import Counter

import pandas as pd
import pyarrow as pa

from app import config
from app.geo import distancia_km, geohash_codificar
from app.tasas import etiqueta_sentimiento, parse_fecha

COLS_NEGOCIO = ["business_id", "name", "city", "state", "latitude", "longitude", "stars", "review_count",
                "is_open", "categories"]

# categorias demasiado generales para servir de "categoria principal"
GENERICAS = {"Restaurants", "Food"}

# meta de dask: columnas y tipos de lo que devuelve procesar_resenas
META_RESENAS = pd.DataFrame({
    "review_id": pd.Series(dtype="object"),
    "business_id": pd.Series(dtype="object"),
    "stars": pd.Series(dtype="int64"),
    "sentimiento": pd.Series(dtype="object"),
    "date": pd.Series(dtype="datetime64[ns]"),
    "text": pd.Series(dtype="object"),
})


# esquema fijo del parquet: asi los bloques vacios no cambian los tipos al leerlos con spark
ESQUEMA_RESENAS = pa.schema([("review_id", pa.string()), ("business_id", pa.string()), ("stars", pa.int64()),
                             ("sentimiento", pa.string()), ("date", pa.timestamp("us")), ("text", pa.string())])


def leer_bloque(ruta, inicio, fin):
    """Lineas de un archivo json-lines entre dos posiciones en bytes (cada bloque lo lee un worker de dask)."""
    with open(ruta, "rb") as f:
        if inicio > 0:
            # la linea que cruza el limite es del bloque anterior; se salta hasta el siguiente salto de linea
            f.seek(inicio - 1)
            f.readline()
        pos = f.tell()
        while pos < fin:
            linea = f.readline()
            if not linea:
                break
            pos += len(linea)
            if linea.strip():
                yield linea


def rangos(tam_archivo, tam_bloque):
    return [(i, min(i + tam_bloque, tam_archivo)) for i in range(0, tam_archivo, tam_bloque)]


def limpiar_negocios(df):
    """Reglas de limpieza de negocios. Devuelve el df limpio y cuantos se descartaron por cada regla."""
    c = {"leidos": len(df)}
    # 1. coordenadas nulas: sin posicion no hay punto GeoJSON
    df = df.dropna(subset=["latitude", "longitude"])
    c["coordenadas_nulas"] = c["leidos"] - len(df)
    # 2. fuera de rango: mongo rechaza geometrias invalidas con el indice 2dsphere
    n = len(df)
    df = df[df["latitude"].between(-90, 90) & df["longitude"].between(-180, 180)]
    c["fuera_de_rango"] = n - len(df)
    # 3. sin categorias: no se puede asignar familia ni categoria principal
    n = len(df)
    df = df[df["categories"].fillna("").str.strip() != ""]
    c["sin_categorias"] = n - len(df)
    # 4. duplicados por business_id
    n = len(df)
    df = df.drop_duplicates(subset="business_id")
    c["duplicados"] = n - len(df)
    return df.reset_index(drop=True), c


def procesar_negocios(ruta, inicio, fin):
    filas = [json.loads(l) for l in leer_bloque(ruta, inicio, fin)]
    df = pd.DataFrame(filas, columns=COLS_NEGOCIO)
    return limpiar_negocios(df)


def categoria_principal(categorias, frecuencias=None):
    """Categoria principal: la mas frecuente en el area entre las del negocio, sin contar las muy generales.

    El orden de la lista de yelp no significa nada (una panaderia puede traer "Bubble Tea" primero),
    por eso se elige la categoria mas comun entre las que tiene el negocio. Sin frecuencias, la primera.
    """
    lista = [x.strip() for x in str(categorias).split(",") if x.strip() and x.strip() not in GENERICAS]
    if not lista:
        return config.FAMILIA
    if frecuencias is None:
        return lista[0]
    return max(lista, key=lambda c: frecuencias[c])  # en empate gana la que viene primero


def seleccionar_area_familia(df):
    """Se queda con los negocios del area (radio desde el centro) y de la familia. Devuelve df y conteos."""
    c = {"limpios": len(df)}
    lat0, lon0 = config.AREA_CENTRO
    dist = distancia_km(df["latitude"].to_numpy(), df["longitude"].to_numpy(), lat0, lon0)
    df = df[dist <= config.AREA_RADIO_KM]
    c["en_el_area"] = len(df)
    es_familia = df["categories"].apply(lambda s: config.FAMILIA in [x.strip() for x in s.split(",")])
    df = df[es_familia].copy()
    c["en_area_y_familia"] = len(df)
    frecuencias = Counter(c.strip() for cats in df["categories"] for c in set(cats.split(",")))
    df["categoria_principal"] = df["categories"].apply(lambda cats: categoria_principal(cats, frecuencias))
    df["geohash"] = [geohash_codificar(la, lo, config.GEOHASH_PRECISION)
                     for la, lo in zip(df["latitude"], df["longitude"])]
    return df.reset_index(drop=True), c


def procesar_lineas(lineas, ids_validos, ids_area):
    """Aplica las reglas de limpieza a las resenas de un bloque. Devuelve el df limpio y los conteos."""
    st = {"leidas": 0, "sin_negocio": 0, "fuera_area": 0, "sin_texto": 0, "invalida": 0, "duplicada": 0,
          "fecha_max": ""}
    filas, vistos = [], set()
    for linea in lineas:
        r = json.loads(linea)
        st["leidas"] += 1
        fecha = str(r.get("date") or "")[:19]
        f = parse_fecha(fecha)
        if f is not None and fecha > st["fecha_max"]:
            st["fecha_max"] = fecha  # el formato permite comparar como texto; sirve para la fecha de corte global
        bid = r.get("business_id")
        # 1. resena de un negocio que no paso la limpieza (o que ni existe en business.json)
        if bid not in ids_validos:
            st["sin_negocio"] += 1
            continue
        # 2. no es del area o de la familia elegidas (seleccion de la muestra)
        if bid not in ids_area:
            st["fuera_area"] += 1
            continue
        # 3. sin texto: no sirve para el modelo de sentimiento
        texto = r.get("text")
        if texto is None or not str(texto).strip():
            st["sin_texto"] += 1
            continue
        # 4. estrellas fuera de 1-5 o fecha ilegible: no hay etiqueta o no hay linea de tiempo
        estrellas = r.get("stars")
        try:
            estrellas = int(float(estrellas))
        except (TypeError, ValueError):
            estrellas = None
        if f is None or estrellas is None or not 1 <= estrellas <= 5:
            st["invalida"] += 1
            continue
        # 5. duplicados por review_id
        if r["review_id"] in vistos:
            st["duplicada"] += 1
            continue
        vistos.add(r["review_id"])
        filas.append((r["review_id"], bid, estrellas, etiqueta_sentimiento(estrellas), f, texto))
    df = pd.DataFrame(filas, columns=list(META_RESENAS.columns))
    df["date"] = pd.to_datetime(df["date"])
    st["validas"] = len(df)
    return df, st


def procesar_resenas(ruta, inicio, fin, ids_validos, ids_area):
    return procesar_lineas(leer_bloque(ruta, inicio, fin), ids_validos, ids_area)
