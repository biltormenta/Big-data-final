def es_hotspot(pct, pct_previo, n, n_previo, umbral_pp, min_n):
    """Una celda es hotspot cuando su % de resenas negativas sube frente a la ventana anterior en al menos
    umbral_pp puntos porcentuales, y las dos ventanas tienen suficientes resenas para que la diferencia valga."""
    if pct_previo is None or n < min_n or n_previo < min_n:
        return False
    return (pct - pct_previo) >= umbral_pp
