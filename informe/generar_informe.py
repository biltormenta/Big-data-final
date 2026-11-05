"""Genera el informe tecnico de la entrega 1 (maximo 6 paginas) con las cifras de resultados/*.json.

Uso:  python informe/generar_informe.py [--url https://la-url-desplegada]
Todo numero del texto sale de los json que dejan los jobs; nada esta escrito a mano.
"""
import argparse
import json
import os
import sys

import docx
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor

AQUI = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(AQUI, "..", "resultados")
SALIDA = os.path.join(AQUI, "Informe_LifeMap_Entrega1.docx")

ap = argparse.ArgumentParser()
ap.add_argument("--url", default="pendiente de definir (ver README, seccion Despliegue)")
args = ap.parse_args()


def J(nombre):
    with open(os.path.join(RES, nombre), encoding="utf8") as f:
        return json.load(f)


try:
    ING, SEN, TAS, BAL = J("ingesta.json"), J("sentimiento.json"), J("tasas.json"), J("balanceo.json")
except FileNotFoundError as e:
    sys.exit(f"falta {e.filename}: corre antes los jobs (ejecutar_todo.ps1) para tener los resultados")


def n(x):
    return f"{int(round(x)):,}".replace(",", ".")


def d(x, k=2):
    return f"{x:,.{k}f}".replace(",", "X").replace(".", ",").replace("X", ".")


AZUL, NARANJA, VERDE, GRIS = "#2878D6", "#EC6B36", "#1AB07A", "#555555"
plt.rcParams.update({"font.size": 8, "axes.spines.top": False, "axes.spines.right": False})

# ---------------------------------------------------------------- figuras
# 1. tasas de cierre por categoria (solo las que pasan el minimo de dias-negocio), con intervalo
top = TAS["categorias"]["mayor_tasa"][:10]
fig, ax = plt.subplots(figsize=(6.6, 2.5))
y = range(len(top))[::-1]
tasas = [t["tasa_cierre_pct"] for t in top]
err = [[t["tasa_cierre_pct"] - t["ic95_inf"] for t in top], [t["ic95_sup"] - t["tasa_cierre_pct"] for t in top]]
ax.barh(list(y), tasas, color=AZUL, xerr=err, error_kw={"ecolor": GRIS, "capsize": 2, "lw": 1})
ax.set_yticks(list(y))
ax.set_yticklabels([t["categoria"] for t in top])
ax.axvline(TAS["global"]["tasa_cierre_pct"], color=NARANJA, ls="--", label=f"global {d(TAS['global']['tasa_cierre_pct'])} %")
ax.set_xlabel("tasa de cierre anualizada (%), con intervalo de confianza al 95 %")
ax.legend(frameon=False, loc="lower right")
fig.tight_layout()
fig.savefig(os.path.join(RES, "fig_tasas.png"), dpi=200)
plt.close(fig)

# 2. balanceo: tiempos por numero de particiones y filas por particion
tiempos = BAL["tiempos"]
ps = [str(p) for p in (2, 8, 32, 200)]
fig, (a1, a2) = plt.subplots(1, 2, figsize=(6.6, 2.3))
a1.bar(ps, [tiempos[p]["media_s"] for p in ps], yerr=[tiempos[p]["desv_s"] for p in ps], color=AZUL,
       error_kw={"ecolor": GRIS, "capsize": 3, "lw": 1})
a1.set_xlabel("particiones")
a1.set_ylabel(f"segundos (media de {BAL['repeticiones']})")
a1.set_title("Tiempo de la operación", fontsize=8)
sg = BAL["sesgo"]
a2.bar(range(200), [max(v, 0.8) for v in sg["categoria"]["ordenadas"]], color=NARANJA, label="con clave (categoría)")
a2.plot(range(200), [max(v, 0.8) for v in sg["sin_clave"]["ordenadas"]], color=VERDE, lw=2, label="sin clave")
a2.set_yscale("log")
a2.set_xlabel("partición (de mayor a menor, de 200)")
a2.set_ylabel("filas (escala log)")
a2.set_title("Filas por partición", fontsize=8)
a2.legend(frameon=False, fontsize=7)
fig.tight_layout()
fig.savefig(os.path.join(RES, "fig_balanceo.png"), dpi=200)
plt.close(fig)

# ---------------------------------------------------------------- documento
doc = docx.Document()
sec = doc.sections[0]
sec.left_margin = sec.right_margin = Inches(0.8)
sec.top_margin = sec.bottom_margin = Inches(0.65)
doc.styles["Normal"].font.name = "Calibri"
doc.styles["Normal"].font.size = Pt(9.5)
doc.styles["Normal"].paragraph_format.space_after = Pt(3)
for nombre, tam in (("Heading 1", 12), ("Heading 2", 10)):
    st = doc.styles[nombre]
    st.font.name = "Calibri"
    st.font.size = Pt(tam)
    st.font.color.rgb = RGBColor(0x1F, 0x3A, 0x5F)
    st.paragraph_format.space_before = Pt(7)
    st.paragraph_format.space_after = Pt(2)
    st.paragraph_format.keep_with_next = True


def h1(x):
    doc.add_heading(x, 1)


def p(x, negrita=False, cursiva=False):
    par = doc.add_paragraph()
    r = par.add_run(x)
    r.bold, r.italic = negrita, cursiva
    return par


def v(x, titulo=None):
    par = doc.add_paragraph(style="List Bullet")
    if titulo:
        par.add_run(titulo + " ").bold = True
    par.add_run(x)
    par.paragraph_format.space_after = Pt(1)


def sombra(celda, color):
    pr = celda._tc.get_or_add_tcPr()
    sh = OxmlElement("w:shd")
    sh.set(qn("w:val"), "clear")
    sh.set(qn("w:color"), "auto")
    sh.set(qn("w:fill"), color)
    pr.append(sh)


def tabla(cab, filas, anchos, tam=8.5):
    tb = doc.add_table(rows=1, cols=len(cab))
    tb.style = "Table Grid"
    tb.alignment = WD_TABLE_ALIGNMENT.CENTER
    for i, c in enumerate(cab):
        ce = tb.rows[0].cells[i]
        ce.text = ""
        r = ce.paragraphs[0].add_run(c)
        r.bold = True
        r.font.size = Pt(tam)
        sombra(ce, "DCE6F2")
    for f in filas:
        cs = tb.add_row().cells
        for i, x in enumerate(f):
            cs[i].text = ""
            cs[i].paragraphs[0].add_run(str(x)).font.size = Pt(tam)
    for k, f in enumerate(tb.rows):
        for i, w in enumerate(anchos):
            f.cells[i].width = Inches(w)
        for ce in f.cells:
            ce.paragraphs[0].paragraph_format.space_after = Pt(0)
            ce.paragraphs[0].paragraph_format.keep_with_next = k < len(tb.rows) - 1
    doc.add_paragraph().paragraph_format.space_after = Pt(0)


def figura(nombre, ancho, pie):
    doc.add_picture(os.path.join(RES, nombre), width=Inches(ancho))
    doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
    doc.paragraphs[-1].paragraph_format.keep_with_next = True
    c = p(pie, cursiva=True)
    c.alignment = WD_ALIGN_PARAGRAPH.CENTER
    for r in c.runs:
        r.font.size = Pt(8)


# ---- portada
par = doc.add_paragraph()
par.alignment = WD_ALIGN_PARAGRAPH.CENTER
r = par.add_run("LifeMap · Entrega 1: plataforma batch, sentimiento y tasas de cierre")
r.bold = True
r.font.size = Pt(15)
par = doc.add_paragraph()
par.alignment = WD_ALIGN_PARAGRAPH.CENTER
par.add_run(f"Big Data · Institución Universitaria de Envigado · 2026-2 · David Mesa y Eber Urrego\n"
            f"Grupo: {ING['area']} + {ING['familia']} · URL del sistema: {args.url}")

# ---- 1. arquitectura
h1("1. Arquitectura")
p("Todo corre en un solo docker-compose.yml. Los datos pasan por tres etapas: Dask limpia el Yelp Open Dataset y lo carga en "
  "MongoDB, Spark (con el conector de MongoDB) entrena el modelo de sentimiento y calcula las tasas con MapReduce, y Flask "
  "expone consultas geoespaciales. Jenkins construye, prueba y despliega cada cambio que llega a main. "
  "Todos los servicios tienen healthcheck y se hablan por el nombre de su servicio en la red de Compose; docker compose up -d --build --wait solo termina cuando todos están sanos.")
tabla(["Servicio", "Función", "Detalle"], [
    ["mongo", "Negocios (GeoJSON, 2dsphere), reseñas y resultados", "MongoDB 7, caché de 0,6 GB"],
    ["dask-scheduler / dask-worker", "Ingesta y limpieza particionada", "1 scheduler y 2 workers (dashboard :8787)"],
    ["spark-master / spark-worker", "Sentimiento, MapReduce y balanceo", "Spark 3.5.3, 1 worker de 2 núcleos, conector de MongoDB 10.4"],
    ["api", "Consultas geoespaciales y /predict", "Flask con gunicorn; carga el modelo de Spark"],
    ["jenkins", "Integración y despliegue continuos", "JCasC, socket de Docker, webhook o pollSCM cada 2 min"],
], [1.7, 2.5, 2.7])

# ---- 2. ingesta y limpieza
h1("2. Ingesta y limpieza con Dask")
neg, res = ING["negocios"], ING["resenas"]
p(f"La descarga es automática: el pipeline usa el ZIP local si existe y, si no, baja yelp-dataset/yelp-dataset con la API de Kaggle "
  f"(kagglehub). La lectura es particionada: review.json (5,3 GB) se divide en bloques de bytes de 32 MB alineados a salto de línea "
  f"(business.json, en bloques de 16 MB) y cada bloque es una tarea de Dask; el worker lo limpia, lo clasifica y escribe su Parquet. "
  f"La ingesta completa tardó {d(ING['segundos'], 0)} s con 2 workers. La muestra se define por área "
  f"(radio de 80 km desde el centro de Philadelphia) y familia ({ING['familia']}).")


def pasos(inicio, reglas):
    filas, quedan = [], inicio
    for nombre, porque, descartados in reglas:
        quedan -= descartados
        filas.append([nombre, porque, n(descartados), n(quedan)])
    return filas


tabla(["Negocios", "Por qué", "Descartados", "Quedan"],
      [["Leídos (business.json)", "", "", n(neg["leidos"])]] + pasos(neg["leidos"], [
          ("Coordenadas nulas", "sin posición no hay punto GeoJSON", neg["coordenadas_nulas"]),
          ("Fuera de rango", "MongoDB rechaza geometrías inválidas con 2dsphere", neg["fuera_de_rango"]),
          ("Sin categorías", "no se puede asignar familia ni categoría", neg["sin_categorias"]),
          ("Duplicados", "un negocio por business_id", neg["duplicados"]),
          ("Fuera del área", "a más de 80 km del centro", neg["limpios"] - neg["en_el_area"]),
          ("Fuera de la familia", "no tiene la categoría Restaurants", neg["en_el_area"] - neg["en_area_y_familia"]),
          ("Sin reseñas tras limpiar", "no se puede calcular su vida", neg["sin_resenas_tras_limpieza"])]),
      [1.6, 3.1, 1.0, 1.0])
tabla(["Reseñas", "Por qué", "Descartadas", "Quedan"],
      [["Leídas (review.json)", "", "", n(res["leidas"])]] + pasos(res["leidas"], [
          ("De negocio no válido", "su negocio no pasó la limpieza", res["sin_negocio"]),
          ("Fuera del área o familia", "no pertenecen a la muestra", res["fuera_area"]),
          ("Sin texto", "no sirven para el modelo de sentimiento", res["sin_texto"]),
          ("Estrellas o fecha inválidas", "sin etiqueta o sin línea de tiempo", res["invalida"]),
          ("Duplicadas", "una reseña por review_id", res["duplicada"])]),
      [1.6, 3.1, 1.0, 1.0])
p(f"El dataset de Yelp viene muy ordenado: varias reglas no descartan nada (coordenadas nulas, fuera de rango y duplicados en negocios; "
  f"sin texto, estrellas o fechas inválidas y duplicados en reseñas), pero se dejan y se cuentan porque son las que protegen la carga si el archivo cambia. "
  f"La unión reseña–negocio se hace por business_id: en MongoDB quedan {n(ING['cargados_en_mongo']['negocios'])} negocios como GeoJSON "
  f"con índice 2dsphere y {n(ING['cargados_en_mongo']['resenas'])} reseñas que referencian su negocio, cargadas por lotes de 10.000 desde los workers.")

# ---- 3. vida de los negocios
h1("3. Vida de cada negocio")
g = TAS["global"]
p(f"Cada documento de negocio guarda su vida: primera y última reseña, número de reseñas, días-negocio (de la primera a la última reseña, "
  f"mínimo 1 día), evento (is_open = 0, cierre observado) y bandera de censura (is_open = 1). La fecha de corte es la última reseña del dataset completo "
  f"({ING['fecha_corte']}). De {n(g['negocios'])} negocios, {n(g['cierres'])} cerraron y {n(g['negocios'] - g['cierres'])} están censurados; "
  f"suman {n(g['dias_negocio'])} días-negocio.")
p("Limitación: es una aproximación. El dataset no trae fechas de apertura ni de cierre, así que la primera reseña es posterior a la apertura "
  "(subestima la vida y la exposición al riesgo) y la última reseña suele ser anterior al cierre real (un negocio puede cerrar meses después de su "
  "última reseña). Los negocios que ya existían antes de su primera reseña tienen entrada tardía; esta entrega los trata desde su primera reseña "
  "y la entrada tardía se modelará en las curvas de supervivencia de la Entrega 2.")

ej = J("ejemplo_negocio.json")
doc_ej = ej["documento"]
resumido = {k: doc_ej[k] for k in ("_id", "name", "city", "categoria_principal", "geohash", "is_open", "loc", "vida", "resenas")}
texto_ej = json.dumps(resumido, ensure_ascii=False, indent=1, default=str)
tb = doc.add_table(rows=1, cols=1)
tb.style = "Table Grid"
tb.alignment = WD_TABLE_ALIGNMENT.CENTER
ce = tb.rows[0].cells[0]
ce.text = ""
sombra(ce, "F3F3F3")
rr = ce.paragraphs[0].add_run(texto_ej)
rr.font.name = "Consolas"
rr.font.size = Pt(7)
ce.paragraphs[0].paragraph_format.space_after = Pt(0)
p("Documento de la colección negocios (un negocio que cerró). Índices: " + ", ".join(ej["indices"]) + ".", cursiva=True)

# ---- 4. sentimiento
h1("4. Modelo de sentimiento (Spark MLlib)")
dtr, dte = SEN["distribucion_train"], SEN["distribucion_test"]
tot_tr, tot_te = sum(dtr.values()), sum(dte.values())
p(f"La etiqueta sale de las estrellas (1–2 negativo, 3 neutral, 4–5 positivo) y no está balanceada: positivo {d(dtr['positivo'] / tot_tr * 100, 1)} %, "
  f"negativo {d(dtr['negativo'] / tot_tr * 100, 1)} % y neutral {d(dtr['neutral'] / tot_tr * 100, 1)} % en entrenamiento. "
  f"El pipeline es el de SentimentStream (RegexTokenizer, stopwords en inglés, HashingTF de {n(SEN['num_features'])} atributos e IDF), con la diferencia de que "
  f"aquí el texto es real. La partición es por negocio (hash del business_id: 80 % entrenamiento, 20 % prueba): {n(SEN['resenas_train'])} reseñas de entrenamiento y "
  f"{n(SEN['resenas_test'])} de prueba, y un conteo confirma que {SEN['negocios_en_los_dos_lados']} negocios quedan en los dos lados.")
filas = []
for nombre, etiqueta in (("naive_bayes", "Naive Bayes"), ("regresion_logistica", "Regresión logística")):
    m = SEN["modelos"][nombre]
    for c in ("negativo", "neutral", "positivo"):
        pc = m["por_clase"][c]
        filas.append([etiqueta if c == "negativo" else "", c, d(pc["precision"], 3), d(pc["recall"], 3), d(pc["f1"], 3),
                      f"{d(m['f1_macro'], 3)} / {d(m['accuracy'], 3)}" if c == "negativo" else ""])
tabla(["Modelo", "Clase", "Precision", "Recall", "F1", "F1 macro / accuracy"], filas, [1.5, 1.0, 1.0, 1.0, 1.0, 1.4])
mejor = SEN["mejor_modelo"]
mm = SEN["modelos"][mejor]
f1s = {c: v["f1"] for c, v in mm["por_clase"].items()}
peor = min(f1s, key=f1s.get)
cm = mm["matriz_confusion"]
fila_peor = cm[["negativo", "neutral", "positivo"].index(peor)]
p(f"Un clasificador que siempre dijera «{SEN['clase_mayoritaria']}» acertaría el {d(SEN['accuracy_clase_mayoritaria'] * 100, 1)} % de las reseñas de prueba, "
  f"así que la accuracy sola engaña: se compara con el F1 macro, que pesa igual a las tres clases. El mejor modelo es "
  f"{'Naive Bayes' if mejor == 'naive_bayes' else 'la regresión logística'} (F1 macro {d(mm['f1_macro'], 3)}) y se guarda como PipelineModel para la Entrega 2. "
  f"La clase más difícil es «{peor}» (F1 {d(f1s[peor], 3)}): de {n(sum(fila_peor))} reseñas reales de esa clase, "
  f"{n(fila_peor[0])} se predicen negativas, {n(fila_peor[1])} neutrales y {n(fila_peor[2])} positivas. Es esperable: un 3 es una opinión mixta que comparte "
  f"vocabulario con las dos clases vecinas.")

clases = ["negativo", "neutral", "positivo"]
tabla(["Real / predicho","negativo", "neutral", "positivo", "Recall"],
      [[c, n(cm[i][0]), n(cm[i][1]), n(cm[i][2]), d(mm["por_clase"][c]["recall"], 3)] for i, c in enumerate(clases)],
      [1.4, 1.1, 1.1, 1.1, 1.1])
p("Tabla: matriz de confusión del mejor modelo sobre el conjunto de prueba (filas = clase real).", cursiva=True)

# ---- 5. mapreduce y tasas
h1("5. MapReduce y tasa de cierre")
cat, cel = TAS["categorias"], TAS["celdas"]
p(f"Con RDD, de las reseñas se emite (business_id, (t, t)) y reduceByKey obtiene la primera y última reseña; luego se emite (clave, (cierre, días_negocio, 1)) "
  f"y un segundo reduceByKey suma por categoría principal y por celda de geohash (precisión 5, ≈ 5 km). Resultado validado contra un groupBy de DataFrames: "
  f"coincide en categorías ({'sí' if TAS['coincide_con_groupby']['categoria'] else 'NO'}) y en celdas ({'sí' if TAS['coincide_con_groupby']['celda'] else 'NO'}). "
  f"La tasa usa la fórmula anualizada, tasa (%) = cierres ÷ (días-negocio ÷ 365) × 100, no la proporción de cerrados "
  f"({d(g['cierres'] / g['negocios'] * 100, 1)} % de los negocios están cerrados, pero la tasa anual global es {d(g['tasa_cierre_pct'])} %). "
  f"La categoría principal es la más frecuente en el área entre las del negocio, sin contar Restaurants ni Food.")
p(f"Mínimo de días-negocio: {n(TAS['min_dias_negocio'])} (≈ {n(TAS['min_dias_negocio'] / 365)} negocios-año). Con la tasa global se esperan "
  f"{d(TAS['min_dias_negocio'] / 365 * g['tasa_cierre_pct'] / 100, 0)} cierres en ese tamaño; con menos, un solo cierre más o menos mueve demasiado la tasa. "
  f"Lo cumplen {n(cat['confiables'])} de {n(cat['total'])} categorías y {n(cel['confiables'])} de {n(cel['total'])} celdas; el resto se guarda marcado como no confiable. "
  f"Se reporta el intervalo exacto de Poisson, que se ensancha cuando hay pocos cierres.")
figura("fig_tasas.png", 6.0, "Figura 1. Categorías con mayor tasa de cierre entre las que cumplen el mínimo de días-negocio.")
mt = cat["mayor_tasa"][0]
nt = cat["menor_tasa"][0]
sobre = sum(1 for t in top if t["ic95_inf"] > g["tasa_cierre_pct"])
p(f"La mayor tasa confiable es «{mt['categoria']}» con {d(mt['tasa_cierre_pct'])} % (IC {d(mt['ic95_inf'])}–{d(mt['ic95_sup'])}); la menor, «{nt['categoria']}» con "
  f"{d(nt['tasa_cierre_pct'])} %. En {sobre} de las {len(top)} categorías del gráfico el intervalo queda por completo por encima de la tasa global, pero hay que leerlo con cuidado: "
  f"se eligieron las más altas entre {n(cat['confiables'])}, y al escoger el extremo de muchos grupos parte de esa ventaja es azar. Los intervalos entre categorías vecinas se traslapan, "
  f"así que el orden exacto del ranking no es concluyente.")

ctop = cel["mayor_tasa"][:5]
tabla(["Celda (geohash)", "Negocios", "Cierres", "Tasa anual (IC 95 %)", "% reseñas negativas"],
      [[t["celda"], n(t["negocios"]), n(t["cierres"]), f"{d(t['tasa_cierre_pct'])} ({d(t['ic95_inf'])}–{d(t['ic95_sup'])})",
        d(t["pct_negativas"], 1)] for t in ctop], [1.3, 1.0, 1.0, 2.0, 1.5])
negs = cel["mayor_pct_negativas"]
cruce = {t["celda"] for t in ctop} & {t["celda"] for t in negs}
p(f"Tabla: las 5 celdas con mayor tasa de cierre que cumplen el mínimo. Las celdas con más reseñas negativas (con al menos 5.000 reseñas) son "
  f"{', '.join(t['celda'] + ' (' + d(t['pct_negativas'], 1) + ' %)' for t in negs[:3])}; "
  f"{'coinciden con ' + ', '.join(sorted(cruce)) if cruce else 'ninguna está entre las 5 de mayor tasa de cierre'}. "
  f"Cada celda se guarda en tasas_celda con su polígono GeoJSON y su propio índice 2dsphere.", cursiva=True)

# ---- 6. balanceo
h1("6. Balanceo de carga")
filas = [[k, d(tiempos[k]["media_s"]), d(tiempos[k]["desv_s"]), ", ".join(d(x, 1) for x in tiempos[k]["medidas_s"])] for k in ps]
p(f"Se midió la operación completa (de las reseñas a la tasa por categoría, con dos reduceByKey) con 2, 8, 32 y 200 particiones, {BAL['repeticiones']} repeticiones cada una y una corrida "
  f"de calentamiento que no cuenta. El clúster tiene {BAL['nucleos']} núcleos.")
tabla(["Particiones", "Media (s)", "Desv. (s)", "Medidas (s)"], filas, [1.2, 1.2, 1.2, 3.2])
figura("fig_balanceo.png", 6.2, "Figura 2. Izquierda: tiempo por número de particiones. Derecha: filas por partición al repartir por categoría o sin clave.")
sc, sci, ssc = sg["categoria"], sg["ciudad"], sg["sin_clave"]
orden = sorted(ps, key=lambda k: tiempos[k]["media_s"])
mejor_p, segundo_p = orden[0], orden[1]
empate = tiempos[segundo_p]["media_s"] - tiempos[mejor_p]["media_s"] <= max(tiempos[mejor_p]["desv_s"], tiempos[segundo_p]["desv_s"])
sc, sci, ssc = sg["categoria"], sg["ciudad"], sg["sin_clave"]
if empate:
    p(f"Con {mejor_p} y {segundo_p} particiones el tiempo es prácticamente el mismo ({d(tiempos[mejor_p]['media_s'])} s y {d(tiempos[segundo_p]['media_s'])} s: la diferencia es menor "
      f"que la dispersión entre repeticiones). El clúster tiene {BAL['nucleos']} núcleos, así que con pocas particiones ya están todos ocupados. Con 32 el tiempo sube a "
      f"{d(tiempos['32']['media_s'])} s y con 200 a {d(tiempos['200']['media_s'])} s, además con mucha variación: cada tarea tiene un costo fijo (planificar, serializar, "
      f"arrancar Python) y con tantas tareas diminutas ese costo domina el trabajo útil.")
else:
    p(f"Con {mejor_p} particiones el tiempo fue el menor ({d(tiempos[mejor_p]['media_s'])} s) y con 200 el mayor ({d(tiempos['200']['media_s'])} s): cada tarea tiene un costo fijo "
      f"y con tantas tareas diminutas ese costo domina el trabajo útil.")
sesgo_lento = abs(sc["tiempo_s"] / ssc["tiempo_s"] - 1) > 0.25
p(f"El sesgo se ve contando filas con spark_partition_id(): repartiendo por categoría, {sc['vacias']} de 200 particiones quedan vacías y la mayor tiene "
  f"{n(sc['max'])} filas ({d(sc['max_sobre_media'], 1)} veces la media); por ciudad, {sci['vacias']} vacías y {d(sci['max_sobre_media'], 1)} veces la media; sin clave, "
  f"{ssc['vacias']} vacías y {d(ssc['max_sobre_media'], 1)} veces la media (todas con unas {n(ssc['media'])} filas). Es lo esperable: todas las filas de una misma categoría "
  f"caen en una sola partición y unas pocas categorías concentran casi todas las reseñas. La misma suma tardó {d(sc['tiempo_s'])} s con clave de categoría, "
  f"{d(sci['tiempo_s'])} s con clave de ciudad y {d(ssc['tiempo_s'])} s sin clave. "
  + ("En esta máquina el reparto desigual sí cambió el tiempo." if sesgo_lento else
     "Es decir, el sesgo se ve en el conteo pero no en el tiempo: con solo 2 núcleos y una operación tan liviana no hay un cuello de botella medible; "
     "en un clúster con más núcleos la partición más cargada sería la que marque el tiempo total, algo que aquí no se midió."))

# ---- 7. api y ci/cd
h1("7. API, Jenkins y despliegue")
tabla(["Endpoint", "Operador MongoDB", "Parámetros"], [
    ["GET /health", "ping", "estado de MongoDB, Spark, Dask y API, y hash del commit"],
    ["GET /businesses/near", "$near", "lat, lon, radio (m), categoria, limit"],
    ["POST /businesses/within", "$geoWithin", "polígono GeoJSON en el cuerpo; categoria, limit"],
    ["GET /zones/summary", "$geoNear + $group", "lat, lon, radio, categoria, min_dias: sentimiento medio, % negativas y tasa de cierre"],
    ["GET /stats/closure", "consulta con filtro", "por (categoria o celda), min_dias, limit"],
    ["POST /predict", "modelo de Spark", "text: sentimiento y probabilidades"],
], [1.7, 1.5, 3.7])
p("Todos validan sus entradas (HTTP 400 con el motivo) y reciben sus valores por parámetro. El pipeline de Jenkins tiene siete etapas: checkout, construcción de imágenes, "
  "pytest (incluye una prueba de la fórmula de la tasa de cierre), pila de pruebas aislada, pruebas contra la API, despliegue y carga opcional de datos; si una prueba falla no se despliega. "
  "El token de Kaggle se guarda como credencial de Jenkins (JCasC) y no hay secretos en el repositorio. Se dispara por webhook de GitHub o, si Jenkins no es alcanzable desde internet, "
  "con pollSCM cada 2 minutos.")
p(f"Estado del despliegue: {args.url}.", negrita=True)

doc.save(SALIDA)
print("guardado", SALIDA)
