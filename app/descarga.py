import glob
import os
import time
import zipfile

from app import config

ARCHIVOS = ("yelp_academic_dataset_business.json", "yelp_academic_dataset_review.json")


def _buscar(carpeta):
    """Devuelve (business, review) si los dos json ya estan en la carpeta, si no None."""
    rutas = []
    for nombre in ARCHIVOS:
        hallados = glob.glob(os.path.join(carpeta, "**", nombre), recursive=True)
        if not hallados:
            return None
        rutas.append(hallados[0])
    return tuple(rutas)


def _extraer_zip(zip_path, destino):
    os.makedirs(destino, exist_ok=True)
    with zipfile.ZipFile(zip_path) as z:
        for nombre in ARCHIVOS:
            salida = os.path.join(destino, nombre)
            if os.path.exists(salida) and os.path.getsize(salida) == z.getinfo(nombre).file_size:
                continue
            print("extrayendo", nombre)
            z.extract(nombre, destino)


def obtener_archivos():
    """Deja business.json y review.json listos en el volumen de datos y devuelve sus rutas."""
    destino = os.path.join(config.DATOS_DIR, "yelp")
    ya = _buscar(destino)
    if ya:
        print("yelp ya esta en", destino)
        return ya

    # 1. si el zip ya esta en el pc (carpeta /zip) no se baja de nuevo
    for z in glob.glob(os.path.join(config.ZIP_LOCAL_DIR, "*.zip")):
        try:
            with zipfile.ZipFile(z) as zf:
                if ARCHIVOS[1] in zf.namelist():
                    _extraer_zip(z, destino)
                    return _buscar(destino)
        except zipfile.BadZipFile:
            continue

    # 2. si no, se baja de kaggle con su api (kagglehub); el dataset es publico
    os.environ.setdefault("KAGGLEHUB_CACHE", os.path.join(config.DATOS_DIR, "kaggle"))
    # docker-compose pasa las credenciales vacias cuando no existen, y kagglehub fallaria con usuario en blanco
    for var in ("KAGGLE_USERNAME", "KAGGLE_KEY"):
        if not os.environ.get(var):
            os.environ.pop(var, None)
    import kagglehub
    # la conexion con kaggle se corta a veces a mitad de un archivo de 4,4 GB; kagglehub retoma el archivo
    # parcial, asi que reintentar no empieza de cero
    for intento in range(1, 8):
        try:
            ruta = kagglehub.dataset_download(config.KAGGLE_DATASET)
            break
        except Exception as e:
            print(f"descarga interrumpida (intento {intento}/7): {str(e)[:120]}")
            if intento == 7:
                raise
            time.sleep(5)
    print("dataset en", ruta)
    hallados = _buscar(ruta)
    if not hallados:
        raise FileNotFoundError(f"no encontre los json de yelp en {ruta}")
    return hallados
