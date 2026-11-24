"""Resume lo que dejo el streaming en mongo (para el informe): corre con python3 normal, no necesita spark."""
import json
import os
from collections import Counter

from pymongo import MongoClient

from app import config
from app.consultas import a_json, evolucion

db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
SALIDA = os.path.join(config.DATOS_DIR, "resultados")
os.makedirs(SALIDA, exist_ok=True)

stream = list(db[config.COL_RESENAS_STREAM].find({}, {"stars": 1, "sentimiento": 1, "probabilidad": 1, "date": 1, "procesado_en": 1}))
reservadas = db[config.COL_RESENAS].count_documents({"date": {"$gt": __import__("datetime").datetime.fromisoformat(config.FECHA_T)}})
real = lambda s: "negativo" if s <= 2 else "neutral" if s == 3 else "positivo"
acierto = sum(1 for d in stream if d["sentimiento"] == real(d["stars"]))
ventanas = list(db[config.COL_VENTANAS].find({}, {"geom": 0}))
calientes = sorted((v for v in ventanas if v.get("hotspot")), key=lambda v: -(v.get("delta_pp") or 0))
alertas = list(db[config.COL_ALERTAS].find({}).sort("prob", -1))
activas = [a for a in alertas if a.get("activa")]
latido = db[config.COL_METRICAS].find_one({"_id": "streaming"}) or {}
prod = {}
if os.path.exists(os.path.join(SALIDA, "productor.json")):
    with open(os.path.join(SALIDA, "productor.json")) as f:
        prod = json.load(f)
fechas = [d["date"] for d in stream]

salida = {
    "fecha_t": config.FECHA_T, "reservadas_posteriores_a_T": reservadas, "clasificadas": len(stream),
    "diferencia_con_reservadas": reservadas - len(stream),
    "desde": min(fechas).strftime("%Y-%m-%d") if fechas else None, "hasta": max(fechas).strftime("%Y-%m-%d") if fechas else None,
    "distribucion_predicha": dict(Counter(d["sentimiento"] for d in stream)),
    "acierto_vs_estrellas": round(acierto / len(stream), 4) if stream else None,
    "ventanas": {"total": len(ventanas), "celdas": len({v["celda"] for v in ventanas}), "resenas_en_ventanas": sum(v["n"] for v in ventanas),
                 "hotspots": len(calientes), "celdas_con_hotspot": len({v["celda"] for v in calientes}),
                 "umbral_pp": calientes[0]["umbral_pp"] if calientes else None},
    "top_hotspots": [{"celda": v["celda"], "inicio": v["inicio"], "n": v["n"], "pct_negativas": v["pct_negativas"],
                      "delta_pp": v["delta_pp"]} for v in calientes[:8]],
    "evolucion": evolucion(db),
    "alertas": {"total_generadas": len(alertas), "activas": len(activas), "cerradas": len(alertas) - len(activas),
                "top": [{"name": a["name"], "categoria": a["categoria_principal"], "prob": round(a["prob"], 3), "fecha": a["fecha"],
                         "geohash": a["geohash"]} for a in activas[:10]]},
    "negocios_con_riesgo_actualizado": db[config.COL_RIESGO].count_documents({"actualizado_en": {"$exists": True}}),
    "productor": prod,
    "consumidor": {"lote_final": latido.get("lote"), "retraso_final": latido.get("retraso"), "sin_linea_base": latido.get("sin_linea_base"),
                   "filas_por_seg": latido.get("filas_por_seg")},
    "demora_procesamiento_s": None,
}
# cuanto tarda una resena desde que sale hasta que queda clasificada se aproxima con la hora de procesamiento mas reciente y la primera
if stream:
    t = sorted(d["procesado_en"] for d in stream)
    salida["demora_procesamiento_s"] = round((t[-1] - t[0]).total_seconds(), 1)
with open(os.path.join(SALIDA, "streaming.json"), "w") as f:
    json.dump(a_json(salida), f, indent=2, default=str)
print(f"clasificadas {len(stream)} de {reservadas} reservadas | ventanas {len(ventanas)} | hotspots {len(calientes)} | alertas activas {len(activas)}")
