from datetime import datetime

from app import config
from app.tasas import tasa_cierre

LIMITE_MAX = 5000


class ParametroInvalido(ValueError):
    """Error de validacion de parametros: la api lo devuelve como http 400."""


def validar_punto(lat, lon):
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        raise ParametroInvalido("lat y lon deben ser numeros")
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise ParametroInvalido("lat debe estar entre -90 y 90 y lon entre -180 y 180")
    return lat, lon


def validar_radio(radio):
    try:
        radio = float(radio)
    except (TypeError, ValueError):
        raise ParametroInvalido("radio debe ser un numero (metros)")
    if not 0 < radio <= 100000:
        raise ParametroInvalido("radio debe estar entre 1 y 100000 metros")
    return radio


def validar_limite(limite, defecto=100):
    if limite in (None, ""):
        return defecto
    try:
        limite = int(limite)
    except (TypeError, ValueError):
        raise ParametroInvalido("limit debe ser un entero")
    if limite < 1:
        raise ParametroInvalido("limit debe ser mayor a 0")
    return min(limite, LIMITE_MAX)


def validar_categoria(categoria):
    """La categoria es opcional; si viene tiene que ser un texto corto."""
    if categoria in (None, ""):
        return None
    categoria = str(categoria).strip()
    if not 0 < len(categoria) <= 80:
        raise ParametroInvalido("categoria debe ser un texto de 1 a 80 caracteres")
    return categoria


def validar_min_dias(valor, defecto=config.MIN_DIAS_NEGOCIO):
    if valor in (None, ""):
        return defecto
    try:
        valor = float(valor)
    except (TypeError, ValueError):
        raise ParametroInvalido("min_dias debe ser un numero")
    if valor < 0:
        raise ParametroInvalido("min_dias no puede ser negativo")
    return valor


def validar_poligono(entrada):
    """Acepta un Polygon/MultiPolygon suelto o envuelto en un Feature (asi lo exporta geojson.io)."""
    geom = entrada
    if isinstance(geom, dict) and geom.get("type") == "Feature":
        geom = geom.get("geometry")
    if not isinstance(geom, dict) or geom.get("type") not in ("Polygon", "MultiPolygon"):
        raise ParametroInvalido("se espera un GeoJSON de tipo Polygon o MultiPolygon")
    coords = geom.get("coordinates")
    if not isinstance(coords, list) or not coords:
        raise ParametroInvalido("el poligono no tiene coordenadas")
    anillos = coords if geom["type"] == "Polygon" else [a for p in coords for a in p]
    for anillo in anillos:
        # un anillo cerrado necesita minimo 4 posiciones y la ultima igual a la primera
        if not isinstance(anillo, list) or len(anillo) < 4 or anillo[0] != anillo[-1]:
            raise ParametroInvalido("cada anillo necesita minimo 4 posiciones y cerrar en el mismo punto inicial")
        for pos in anillo:
            # en geojson cada posicion es [lon, lat]; validar_punto recibe (lat, lon)
            validar_punto(pos[1], pos[0])
    return geom


def a_json(valor):
    """Pasa las fechas de mongo (incluso las anidadas) a texto para poder devolverlas como json."""
    if isinstance(valor, datetime):
        return valor.isoformat()
    if isinstance(valor, dict):
        return {k: a_json(v) for k, v in valor.items()}
    if isinstance(valor, list):
        return [a_json(v) for v in valor]
    return valor


def _feature(doc):
    d = dict(doc)
    loc = d.pop("loc")
    d["business_id"] = d.pop("_id", None)
    return {"type": "Feature", "geometry": loc, "properties": a_json(d)}


def coleccion_fc(docs):
    feats = [_feature(d) for d in docs]
    return {"type": "FeatureCollection", "count": len(feats), "features": feats}


def _filtro_categoria(categoria):
    return {"categories": categoria} if categoria else {}


def cercanos(col, lat, lon, radio_m, categoria, limite):
    """Negocios dentro de un radio, del mas cercano al mas lejano ($near usa el indice 2dsphere)."""
    filtro = {"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [lon, lat]},
                                "$maxDistance": radio_m}}}
    filtro.update(_filtro_categoria(categoria))
    return list(col.find(filtro).limit(limite))


def dentro_poligono(col, geom, categoria, limite):
    """Negocios que caen dentro de un poligono ($geoWithin)."""
    filtro = {"loc": {"$geoWithin": {"$geometry": geom}}}
    filtro.update(_filtro_categoria(categoria))
    return list(col.find(filtro).limit(limite))


def resumen_zona(col, lat, lon, radio_m, categoria, min_dias):
    """Sentimiento medio y tasa de cierre de los negocios alrededor de un punto ($geoNear + $group)."""
    pipeline = [
        # $geoNear tiene que ir de primero; "query" filtra por categoria antes de agrupar
        {"$geoNear": {"near": {"type": "Point", "coordinates": [lon, lat]}, "distanceField": "dist_m",
                      "maxDistance": radio_m, "spherical": True, "key": "loc",
                      "query": _filtro_categoria(categoria)}},
        {"$group": {"_id": None, "negocios": {"$sum": 1},
                    "resenas": {"$sum": "$resenas.n"}, "suma_sentimiento": {"$sum": "$resenas.suma_sentimiento"},
                    "negativas": {"$sum": "$resenas.negativas"},
                    "cierres": {"$sum": "$vida.evento"}, "dias_negocio": {"$sum": "$vida.dias_negocio"},
                    "dist_media_m": {"$avg": "$dist_m"}}},
    ]
    filas = list(col.aggregate(pipeline))
    if not filas:
        return {"negocios": 0, "resenas": 0, "sentimiento_medio": None, "pct_negativas": None, "cierres": 0,
                "dias_negocio": 0, "tasa_cierre_pct": None, "confiable": False, "min_dias": min_dias}
    f = filas[0]
    return {
        "negocios": f["negocios"], "resenas": f["resenas"],
        # sentimiento medio en [-1, 1]: negativo = -1, neutral = 0, positivo = +1
        "sentimiento_medio": round(f["suma_sentimiento"] / f["resenas"], 4) if f["resenas"] else None,
        "pct_negativas": round(f["negativas"] / f["resenas"] * 100, 2) if f["resenas"] else None,
        "cierres": f["cierres"], "dias_negocio": f["dias_negocio"],
        "tasa_cierre_pct": round(tasa_cierre(f["cierres"], f["dias_negocio"]), 3),
        # con pocos dias-negocio la tasa no es comparable: se avisa en vez de esconder el dato
        "confiable": f["dias_negocio"] >= min_dias, "min_dias": min_dias,
        "dist_media_m": round(f["dist_media_m"], 1),
    }


def tasas_guardadas(db, por, min_dias, limite):
    """Tasas de cierre ya calculadas por spark (reduceByKey), con el minimo de dias-negocio como filtro."""
    coleccion = config.COL_TASAS_CATEGORIA if por == "categoria" else config.COL_TASAS_CELDA
    filtro = {"dias_negocio": {"$gte": min_dias}}
    return list(db[coleccion].find(filtro, {"_id": 0}).sort("tasa_cierre_pct", -1).limit(limite))
