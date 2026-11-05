from datetime import datetime

# misma formula de la AFR del ejercicio de discos, con dias-negocio en lugar de dias-disco
DIAS_ANIO = 360


def tasa_cierre(cierres, dias_negocio):
    """Tasa de cierre anualizada (%) = cierres / (dias-negocio / 365) * 100."""
    if not dias_negocio or dias_negocio <= 0:
        return None
    return cierres / (dias_negocio / DIAS_ANIO) * 100


def ic_poisson(cierres, dias_negocio, nivel=0.95):
    """Intervalo exacto (Garwood) de la tasa, tratando los cierres como un conteo de Poisson."""
    from scipy.stats import chi2
    if not dias_negocio or dias_negocio <= 0:
        return None, None
    a = 1 - nivel
    inf = 0.0 if cierres == 0 else chi2.ppf(a / 2, 2 * cierres) / 2
    sup = chi2.ppf(1 - a / 2, 2 * (cierres + 1)) / 2
    anios = dias_negocio / DIAS_ANIO
    return inf / anios * 100, sup / anios * 100


def vida_negocio(primera, ultima, is_open):
    """Vida de un negocio a partir de sus fechas de primera/ultima resena y de is_open.

    evento = 1 cuando is_open = 0 (cierre observado); censura cuando is_open = 1 (solo se sabe que seguia abierto).
    """
    dias = max((ultima - primera).days, 1)  # minimo 1 dia para que todo negocio aporte exposicion
    cerrado = int(is_open) == 0
    return {
        "primera_resena": primera,
        "ultima_resena": ultima,
        "dias_negocio": dias,
        "evento": 1 if cerrado else 0,
        "censurado": not cerrado,
    }


def parse_fecha(texto):
    """Fecha de una resena de yelp ('2018-07-07 22:09:11'); devuelve None si no se puede leer."""
    try:
        return datetime.strptime(str(texto)[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def etiqueta_sentimiento(estrellas):
    """1 y 2 estrellas = negativo, 3 = neutral, 4 y 5 = positivo."""
    if estrellas <= 2:
        return "negativo"
    if estrellas == 3:
        return "neutral"
    return "positivo"
