import json
import os
import shutil
import time

import dask
import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from distributed import Client
from pymongo import ASCENDING, GEOSPHERE, MongoClient

from app import config
from app.descarga import obtener_archivos
from app.limpieza import (ESQUEMA_RESENAS, procesar_negocios, procesar_resenas, rangos,
                          seleccionar_area_familia)
from app.tasas import vida_negocio

MB = 1024 * 1024


def tarea_resenas(i, ruta, inicio, fin, ids_validos, ids_area, destino):
    """Corre en un worker de dask: limpia un bloque de review.json y lo guarda como parquet."""
    df, st = procesar_resenas(ruta, inicio, fin, ids_validos, ids_area)
    tabla = pa.Table.from_pandas(df, schema=ESQUEMA_RESENAS, preserve_index=False)
    pq.write_table(tabla, os.path.join(destino, f"part-{i:05d}.parquet"))
    return st


def cargar_archivo(ruta, uri, db, coleccion, tam_lote):
    """Corre en un worker: lee un parquet y lo inserta en mongo por lotes."""
    df = pq.read_table(ruta).to_pandas()
    col = MongoClient(uri)[db][coleccion]
    for i in range(0, len(df), tam_lote):
        docs = [{"_id": r.review_id, "business_id": r.business_id, "stars": int(r.stars),
                 "sentimiento": r.sentimiento, "date": r.date.to_pydatetime(), "text": r.text}
                for r in df.iloc[i:i + tam_lote].itertuples(index=False)]
        col.insert_many(docs, ordered=False)  # ordered=False: si uno falla, mongo sigue con los demas
    return len(df)


def documento_negocio(r):
    """Un negocio como documento GeoJSON, con su vida y el resumen de sus resenas."""
    vida = vida_negocio(r.primera.to_pydatetime(), r.ultima.to_pydatetime(), r.is_open)
    n = int(r.n)
    return {
        "_id": r.business_id,
        "name": r.name, "city": r.city, "state": r.state,
        "stars": float(r.stars), "review_count": int(r.review_count), "is_open": int(r.is_open),
        "categories": [x.strip() for x in r.categories.split(",")],
        "categoria_principal": r.categoria_principal,
        "geohash": r.geohash,
        # geojson pide [longitud, latitud]
        "loc": {"type": "Point", "coordinates": [float(r.longitude), float(r.latitude)]},
        "vida": vida,
        "resenas": {"n": n, "suma_sentimiento": int(r.suma_sent), "negativas": int(r.negativas),
                    "sentimiento_medio": round(int(r.suma_sent) / n, 4),
                    "pct_negativas": round(int(r.negativas) / n * 100, 2)},
    }


def main():
    t0 = time.time()
    ruta_neg, ruta_res = obtener_archivos()
    client = Client(config.DASK_SCHEDULER)
    print("dask:", client.dashboard_link, "workers:", len(client.scheduler_info()["workers"]))

    # ---------- negocios: lectura por bloques de 16 MB, uno por tarea ----------
    tareas = [dask.delayed(procesar_negocios)(ruta_neg, a, b) for a, b in rangos(os.path.getsize(ruta_neg), 16 * MB)]
    partes = dask.compute(*tareas)
    negocios = pd.concat([p[0] for p in partes], ignore_index=True)
    c_neg = {k: sum(p[1][k] for p in partes) for k in partes[0][1]}
    # un mismo negocio podria caer en dos bloques: se vuelve a quitar duplicados con todo junto
    n = len(negocios)
    negocios = negocios.drop_duplicates(subset="business_id").reset_index(drop=True)
    c_neg["duplicados"] += n - len(negocios)
    c_neg["limpios"] = len(negocios)
    area, c_sel = seleccionar_area_familia(negocios)
    c_neg.update(c_sel)
    print("negocios:", c_neg)

    # ---------- resenas: bloques de 32 MB, cada worker limpia y escribe su parquet ----------
    destino = os.path.join(config.DATOS_DIR, "clean", "resenas")
    shutil.rmtree(destino, ignore_errors=True)
    os.makedirs(destino)
    conj = client.scatter({"validos": set(negocios["business_id"]), "area": set(area["business_id"])}, broadcast=True)
    bloques = rangos(os.path.getsize(ruta_res), config.BLOQUE_RESENAS_MB * MB)
    print(f"resenas: {len(bloques)} bloques")
    futuros = [client.submit(tarea_resenas, i, ruta_res, a, b, conj["validos"], conj["area"], destino, pure=False)
               for i, (a, b) in enumerate(bloques)]
    stats = client.gather(futuros)
    c_res = {k: sum(s[k] for s in stats) for k in ("leidas", "sin_negocio", "fuera_area", "sin_texto",
                                                   "invalida", "duplicada", "validas")}
    corte = max(s["fecha_max"] for s in stats)  # fecha de corte = ultima resena del dataset completo

    # un review_id repetido entre bloques distintos no lo ve la regla de cada bloque: se revisa aqui
    r = dd.read_parquet(destino)
    total, unicos = len(r), int(r["review_id"].nunique().compute())
    if total != unicos:
        print("hay duplicados entre bloques, se eliminan:", total - unicos)
        sin_dup = destino + "_sin_dup"
        shutil.rmtree(sin_dup, ignore_errors=True)
        r.drop_duplicates(subset="review_id").to_parquet(sin_dup, write_index=False, coerce_timestamps="us",
                                                         allow_truncated_timestamps=True)
        shutil.rmtree(destino)
        os.rename(sin_dup, destino)
        c_res["duplicada"] += total - unicos
        c_res["validas"] = unicos
        r = dd.read_parquet(destino)
    print("resenas:", c_res)

    # ---------- vida de cada negocio: primera y ultima resena, numero de resenas, sentimiento ----------
    r = r[["business_id", "stars", "date"]]
    r["sent"] = (r["stars"] >= 4).astype("int64") - (r["stars"] <= 2).astype("int64")
    r["neg"] = (r["stars"] <= 2).astype("int64")
    agg = r.groupby("business_id").agg({"date": ["min", "max", "count"], "sent": ["sum"], "neg": ["sum"]}).compute()
    agg.columns = ["primera", "ultima", "n", "suma_sent", "negativas"]
    antes = len(area)
    area = area.merge(agg, left_on="business_id", right_index=True, how="inner")
    c_neg["sin_resenas_tras_limpieza"] = antes - len(area)
    c_neg["finales"] = len(area)
    dist = dd.read_parquet(destino, columns=["sentimiento"])["sentimiento"].value_counts().compute().to_dict()

    # ---------- carga a mongo ----------
    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
    for col in (config.COL_NEGOCIOS, config.COL_RESENAS):
        db[col].drop()  # repetir la ingesta no duplica datos
    docs = [documento_negocio(x) for x in area.itertuples(index=False)]
    for i in range(0, len(docs), 5000):
        db[config.COL_NEGOCIOS].insert_many(docs[i:i + 5000], ordered=False)
    archivos = sorted(os.path.join(destino, f) for f in os.listdir(destino) if f.endswith(".parquet"))
    cargadas = client.gather(client.map(cargar_archivo, archivos, uri=config.MONGO_URI, db=config.MONGO_DB,
                                        coleccion=config.COL_RESENAS, tam_lote=config.TAM_LOTE_MONGO, pure=False))
    # los indices van al final: cargar con el 2dsphere ya creado es mucho mas lento
    db[config.COL_NEGOCIOS].create_index([("loc", GEOSPHERE)])
    for campo in ("categoria_principal", "geohash", "is_open"):
        db[config.COL_NEGOCIOS].create_index(campo)
    db[config.COL_RESENAS].create_index("business_id")
    db[config.COL_RESENAS].create_index("date")

    en_mongo = {"negocios": db[config.COL_NEGOCIOS].count_documents({}),
                "resenas": db[config.COL_RESENAS].count_documents({})}
    resumen = {
        "area": config.AREA_NOMBRE, "familia": config.FAMILIA, "fecha_corte": corte[:10],
        "negocios": c_neg, "resenas": c_res, "distribucion_sentimiento": dist,
        "cargados_en_mongo": en_mongo, "segundos": round(time.time() - t0, 1),
    }
    # verificaciones: mongo debe tener lo que se envio, y se debe cumplir el minimo del enunciado
    assert en_mongo["resenas"] == sum(cargadas) == c_res["validas"], f"mongo {en_mongo} vs enviado {sum(cargadas)}"
    assert en_mongo["negocios"] == len(area)
    assert en_mongo["resenas"] >= config.MIN_RESENAS_MUESTRA, "el enunciado pide al menos un millon de resenas"
    db[config.COL_METRICAS].replace_one({"_id": "ingesta"}, {"_id": "ingesta", **resumen}, upsert=True)
    os.makedirs(os.path.join(config.DATOS_DIR, "resultados"), exist_ok=True)
    with open(os.path.join(config.DATOS_DIR, "resultados", "ingesta.json"), "w") as f:
        json.dump(resumen, f, indent=2, default=str)
    print(json.dumps(resumen, indent=2, default=str))


if __name__ == "__main__":
    main()
