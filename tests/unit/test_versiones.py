import os

import pytest

from app.versiones import escribir_version, huella_directorio, leer_version, verificar_version


def modelo_falso(tmp_path):
    (tmp_path / "metadata").mkdir()
    (tmp_path / "metadata" / "part-0").write_text("modelo v1")
    (tmp_path / "stages").mkdir()
    (tmp_path / "stages" / "coef").write_text("0.1 0.2")
    return str(tmp_path)


def test_version_coincide_con_lo_guardado(tmp_path):
    ruta = modelo_falso(tmp_path)
    v = escribir_version(ruta, {"fecha_t": "2021-01-19"})
    assert leer_version(ruta)["version"] == v["version"] == huella_directorio(ruta)[:10]
    assert verificar_version(ruta)["fecha_t"] == "2021-01-19"


# si alguien cambia el modelo sin versionarlo, cargarlo falla en vez de usar algo distinto a lo versionado
def test_un_modelo_modificado_no_se_carga(tmp_path):
    ruta = modelo_falso(tmp_path)
    escribir_version(ruta, {})
    with open(os.path.join(ruta, "stages", "coef"), "w") as f:
        f.write("9.9 9.9")
    with pytest.raises(RuntimeError):
        verificar_version(ruta)
