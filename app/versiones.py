import hashlib
import json
import os


def huella_directorio(ruta):
    """Hash del contenido de un modelo guardado (todos sus archivos, en orden). Cambia si cambia el modelo."""
    h = hashlib.sha256()
    for carpeta, _, archivos in sorted(os.walk(ruta)):
        for nombre in sorted(archivos):
            if nombre == "version.json" or nombre.endswith(".crc") or nombre == "_SUCCESS":
                continue
            h.update(os.path.relpath(os.path.join(carpeta, nombre), ruta).replace("\\", "/").encode())
            with open(os.path.join(carpeta, nombre), "rb") as f:
                for bloque in iter(lambda: f.read(1 << 20), b""):
                    h.update(bloque)
    return h.hexdigest()


def escribir_version(ruta, meta):
    """Deja version.json junto al modelo: la huella de lo guardado + los datos de cuando y como se entreno."""
    huella = huella_directorio(ruta)
    datos = {"version": huella[:10], "huella": huella, **meta}
    with open(os.path.join(ruta, "version.json"), "w") as f:
        json.dump(datos, f, indent=2, default=str)
    return datos


def leer_version(ruta):
    with open(os.path.join(ruta, "version.json")) as f:
        return json.load(f)


def verificar_version(ruta):
    """Lo que se va a cargar tiene que ser exactamente lo que se versiono; si no, se corta antes de cargar."""
    v = leer_version(ruta)
    actual = huella_directorio(ruta)
    if actual != v["huella"]:
        raise RuntimeError(f"el modelo en {ruta} no coincide con su version.json ({v['version']}): fue modificado")
    return v
