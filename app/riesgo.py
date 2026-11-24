from datetime import timedelta

# variables del modelo de riesgo de cierre (todas se calculan solo con datos hasta la fecha de referencia)
FEATURES = ["sentimiento_medio", "tendencia_neg", "estrellas", "n_resenas", "edad_dias", "dias_desde_ultima",
            "actividad_6m", "delta_estrellas_6m", "competidores"]


def _pct_neg(filas):
    return sum(1 for _, s in filas if s <= 2) / len(filas) * 100 if filas else None


def resumen_historia(filas, fecha_ref):
    """Estado de un negocio a fecha_ref: lo justo para calcular sus variables y poder actualizarlas con resenas nuevas
    sin volver a leer toda su historia (el streaming lo guarda en mongo y lo va actualizando por micro-lote).

    filas: [(fecha, estrellas)]. Lo posterior a fecha_ref no existe todavia y se ignora.
    """
    filas = [(d, s) for d, s in filas if d <= fecha_ref]
    if not filas:
        return None
    return {
        "n": len(filas),
        "suma_sentimiento": sum(1 if s >= 4 else -1 if s <= 2 else 0 for _, s in filas),
        "suma_estrellas": sum(s for _, s in filas),
        "primera": min(d for d, _ in filas),
        "ultima": max(d for d, _ in filas),
        # solo el ultimo año: alcanza para la tendencia de 6 meses contra 6 meses
        "recientes": [(d, s) for d, s in filas if d > fecha_ref - timedelta(days=365)],
    }


def actualizar_estado(estado, nuevas, fecha_ref):
    """Suma resenas nuevas [(fecha, estrellas)] a un estado y descarta del año reciente lo que ya quedo viejo."""
    nuevas = [(d, s) for d, s in nuevas if d <= fecha_ref]
    est = dict(estado)
    est["n"] += len(nuevas)
    est["suma_sentimiento"] += sum(1 if s >= 4 else -1 if s <= 2 else 0 for _, s in nuevas)
    est["suma_estrellas"] += sum(s for _, s in nuevas)
    if nuevas:
        est["ultima"] = max(est["ultima"], max(d for d, _ in nuevas))
    est["recientes"] = [(d, s) for d, s in list(estado["recientes"]) + nuevas
                        if d > fecha_ref - timedelta(days=365)]
    return est


def features_de_estado(est, fecha_ref):
    """Las variables del modelo (sin competidores) a partir del estado."""
    rec = est["recientes"]
    ultimos6 = [f for f in rec if f[0] > fecha_ref - timedelta(days=182)]
    previos6 = [f for f in rec if f[0] <= fecha_ref - timedelta(days=182)]
    # tendencia: cuanto subio (o bajo) el % negativo de los ultimos 6 meses frente a los 6 anteriores; 0 si falta uno
    tendencia = _pct_neg(ultimos6) - _pct_neg(previos6) if ultimos6 and previos6 else 0.0
    estrellas = est["suma_estrellas"] / est["n"]
    return {
        "sentimiento_medio": est["suma_sentimiento"] / est["n"],  # en [-1, 1]
        "tendencia_neg": tendencia,
        "estrellas": estrellas,
        # cuantas resenas tuvo en los ultimos 6 meses (un negocio que se va apagando recibe cada vez menos)
        "actividad_6m": len(ultimos6),
        # estrellas de los ultimos 6 meses menos su promedio de siempre (0 si no hubo resenas recientes)
        "delta_estrellas_6m": sum(s for _, s in ultimos6) / len(ultimos6) - estrellas if ultimos6 else 0.0,
        "n_resenas": est["n"],
        "edad_dias": (fecha_ref - est["primera"]).days,
        "dias_desde_ultima": (fecha_ref - est["ultima"]).days,
    }


def features_de_resenas(filas, fecha_ref):
    """Variables de un negocio a partir de sus resenas [(fecha, estrellas)], mirando solo hasta fecha_ref.

    competidores no sale de las resenas: se calcula aparte con $geoNear. Devuelve None si no hay historia.
    """
    est = resumen_historia(filas, fecha_ref)
    return None if est is None else features_de_estado(est, fecha_ref)


def etiqueta_riesgo(is_open, primera, ultima, t, horizonte_meses=12):
    """1 si el negocio cerro y su ultima resena cae entre T y T + 12 meses; 0 si sigue con actividad despues
    del plazo o sigue abierto; None (se excluye) si no tiene historia antes de T o no tuvo actividad despues de T."""
    limite = t + timedelta(days=round(horizonte_meses * 30.4375))
    if primera > t or ultima <= t:
        return None
    if int(is_open) == 0 and ultima <= limite:
        return 1
    return 0


def competidores_cercanos(col, lon, lat, categoria, propio_id, hasta, radio_m=500):
    """Cuantos negocios de la misma categoria hay a menos de radio_m, con $geoNear (sin contar el propio)."""
    pipeline = [
        {"$geoNear": {"near": {"type": "Point", "coordinates": [lon, lat]}, "distanceField": "d", "maxDistance": radio_m,
                      "spherical": True, "key": "loc",
                      "query": {"categoria_principal": categoria, "_id": {"$ne": propio_id},
                                "vida.primera_resena": {"$lte": hasta}}}},
        {"$count": "n"},
    ]
    r = list(col.aggregate(pipeline))
    return r[0]["n"] if r else 0
