import math

import numpy as np


def kaplan_meier(duraciones, eventos, entradas=None, n_min=30, z=1.96):
    """Curva de Kaplan-Meier con censura y entrada tardia.

    duraciones: dias de vida observada; eventos: 1 = cierre, 0 = censurado (sigue abierto); entradas: dias a los
    que cada negocio empieza a ser observable (entrada tardia). Un negocio esta en riesgo en el dia t si
    entrada < t <= duracion, asi que los que entran tarde no cuentan antes de entrar.

    Solo se usan los dias donde hay al menos n_min negocios en riesgo: al principio entran pocos (la curva daria
    saltos enormes con 1 o 2 negocios) y al final quedan pocos. Es una curva condicional al dia t_inicio.
    """
    d = np.asarray(duraciones, dtype=float)
    e = np.asarray(eventos, dtype=int)
    a = np.zeros_like(d) if entradas is None else np.asarray(entradas, dtype=float)
    puntos, s, g, t_inicio = [], 1.0, 0.0, None
    for t in np.unique(d[e == 1]):
        n = int(((a < t) & (d >= t)).sum())
        if n < n_min:
            if t_inicio is not None:
                break  # la curva ya empezo y el grupo en riesgo se achico demasiado
            continue
        if t_inicio is None:
            t_inicio = float(t)
        m = int(((d == t) & (e == 1)).sum())
        s *= 1 - m / n
        if n > m:
            g += m / (n * (n - m))  # suma de Greenwood para el intervalo
        if 0 < s < 1:
            se = math.sqrt(g) / abs(math.log(s))  # intervalo log-log: se mantiene dentro de [0, 1]
            lo, hi = s ** math.exp(z * se), s ** math.exp(-z * se)
        else:
            lo = hi = s
        puntos.append({"t": float(t), "S": s, "ic_inf": lo, "ic_sup": hi, "en_riesgo": n, "cierres": m})
    mediana = next((p["t"] for p in puntos if p["S"] <= 0.5), None)
    return {"curva": puntos, "t_inicio": t_inicio, "t_fin": puntos[-1]["t"] if puntos else None,
            "mediana_dias": mediana, "negocios": int(len(d)), "cierres": int(e.sum())}


def supervivencia_en(curva, dia):
    """S(dia) leyendo la curva (el ultimo punto con t <= dia); None si el dia queda fuera de la parte confiable."""
    if not curva or dia < curva[0]["t"] or dia > curva[-1]["t"]:
        return None
    previos = [p for p in curva if p["t"] <= dia]
    return previos[-1]["S"]


def reducir(curva, max_puntos=80):
    """Menos puntos para guardar en Mongo y dibujar: se queda con puntos repartidos parejo, siempre el primero y el ultimo."""
    if len(curva) <= max_puntos:
        return curva
    idx = sorted({int(i * (len(curva) - 1) / (max_puntos - 1)) for i in range(max_puntos)})
    return [curva[i] for i in idx]
