import math

_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def distancia_km(lat1, lon1, lat2, lon2):
    """Distancia haversine en km; sirve con numeros sueltos o con series de pandas/numpy."""
    import numpy as np
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dphi = p2 - p1
    dlam = np.radians(lon2) - np.radians(lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlam / 2) ** 2
    return 6371.0088 * 2 * np.arcsin(np.sqrt(a))


def geohash_codificar(lat, lon, precision=5):
    """Codifica lat/lon como geohash (bisecciona longitud y latitud alternando, 5 bits por caracter)."""
    lat_min, lat_max, lon_min, lon_max = -90.0, 90.0, -180.0, 180.0
    resultado, bits, valor, par = [], 0, 0, True
    while len(resultado) < precision:
        if par:
            medio = (lon_min + lon_max) / 2
            if lon >= medio:
                valor = valor * 2 + 1
                lon_min = medio
            else:
                valor = valor * 2
                lon_max = medio
        else:
            medio = (lat_min + lat_max) / 2
            if lat >= medio:
                valor = valor * 2 + 1
                lat_min = medio
            else:
                valor = valor * 2
                lat_max = medio
        par = not par
        bits += 1
        if bits == 5:
            resultado.append(_BASE32[valor])
            bits, valor = 0, 0
    return "".join(resultado)


def geohash_caja(gh):
    """Devuelve (lat_min, lat_max, lon_min, lon_max) de la celda."""
    lat_min, lat_max, lon_min, lon_max = -90.0, 90.0, -180.0, 180.0
    par = True
    for c in gh:
        v = _BASE32.index(c)
        for i in range(4, -1, -1):
            bit = (v >> i) & 1
            if par:
                medio = (lon_min + lon_max) / 2
                if bit:
                    lon_min = medio
                else:
                    lon_max = medio
            else:
                medio = (lat_min + lat_max) / 2
                if bit:
                    lat_min = medio
                else:
                    lat_max = medio
            par = not par
    return lat_min, lat_max, lon_min, lon_max


def geohash_poligono(gh):
    """La celda como polygon GeoJSON (anillo cerrado, orden [lon, lat])."""
    la0, la1, lo0, lo1 = geohash_caja(gh)
    return {"type": "Polygon", "coordinates": [[[lo0, la0], [lo1, la0], [lo1, la1], [lo0, la1], [lo0, la0]]]}


def geohash_centro(gh):
    la0, la1, lo0, lo1 = geohash_caja(gh)
    return (la0 + la1) / 2, (lo0 + lo1) / 2
