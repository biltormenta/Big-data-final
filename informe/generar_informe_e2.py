"""Genera el informe tecnico final (maximo 10 paginas) con las cifras de resultados/*.json.

Uso:  python informe/generar_informe_e2.py [--url https://la-url-desplegada]
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
SALIDA = os.path.join(AQUI, "Informe_LifeMap_Entrega2.docx")

ap = argparse.ArgumentParser()
ap.add_argument("--url", default="pendiente de definir (ver README, seccion Despliegue)")
args = ap.parse_args()


def J(nombre):
    with open(os.path.join(RES, nombre), encoding="utf8") as f:
        return json.load(f)


try:
    SEN, RIE, SUP, HOT, STR, PRO = (J(x) for x in ("sentimiento.json", "riesgo.json", "supervivencia.json", "umbral_hotspot.json",
                                                   "streaming.json", "productor.json"))
except FileNotFoundError as e:
    sys.exit(f"falta {e.filename}: corre antes ejecutar_entrega2.ps1 para tener los resultados")


def n(x):
    return f"{int(round(x)):,}".replace(",", ".")


def d(x, k=2):
    return f"{x:,.{k}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def pct(x, k=1):
    return d(x * 100, k)


AZUL, NARANJA, VERDE, MORADO, ROJO, GRIS = "#2878D6", "#EC6B36", "#1AB07A", "#8E44AD", "#D7301F", "#555555"
plt.rcParams.update({"font.size": 8, "axes.spines.top": False, "axes.spines.right": False})

# ---------------------------------------------------------------- figuras
# 1. supervivencia: curva global con su intervalo y las dos categorias de menor y mayor supervivencia a 3 años
fig, ax = plt.subplots(figsize=(6.6, 2.7))
g = SUP["curva_global"]
t = [p["t"] / 365 for p in g]
ax.fill_between(t, [p["ic_inf"] for p in g], [p["ic_sup"] for p in g], color=GRIS, alpha=0.18, step="post")
ax.step(t, [p["S"] for p in g], where="post", color=GRIS, lw=2.2, label="todos los negocios")
for (cat, curva), color in zip(SUP["curvas_categoria"].items(), (ROJO, NARANJA, VERDE, AZUL)):
    ax.step([p["t"] / 365 for p in curva], [p["S"] for p in curva], where="post", color=color, lw=1.4, label=cat)
ax.set_xlabel("años desde la primera reseña")
ax.set_ylabel("probabilidad de seguir abierto")
ax.set_ylim(0.2, 1.0)
ax.legend(frameon=False, fontsize=7, ncol=3, loc="lower left")
fig.tight_layout()
fig.savefig(os.path.join(RES, "fig_e2_supervivencia.png"), dpi=200)
plt.close(fig)

# 2. streaming: % negativo por ventana de 30 dias y celdas hotspot
ev = STR["evolucion"]
fig, ax = plt.subplots(figsize=(6.6, 2.4))
x = [e["inicio"][:10] for e in ev]
ax.bar(x, [e["hotspots"] for e in ev], color=AZUL, alpha=0.45, label="celdas hotspot")
ax.set_ylabel("celdas hotspot")
ax.tick_params(axis="x", rotation=45)
ax2 = ax.twinx()
ax2.spines["right"].set_visible(True)
ax2.plot(x, [e["pct_negativas"] for e in ev], color=ROJO, marker="o", ms=3, label="% reseñas negativas")
ax2.set_ylabel("% negativas (predichas)")
ax2.set_ylim(0, max(e["pct_negativas"] for e in ev) * 1.3)
fig.legend(frameon=False, fontsize=7, loc="upper left", bbox_to_anchor=(0.09, 0.97), ncol=2)
fig.tight_layout()
fig.savefig(os.path.join(RES, "fig_e2_streaming.png"), dpi=200)
plt.close(fig)

# 3. riesgo: AUC-PR por modelo (validacion cruzada) frente al azar, e importancia de las variables del modelo elegido
fig, (a1, a2) = plt.subplots(1, 2, figsize=(6.6, 2.5))
nombres = {"regresion_logistica": "reg. logística", "gbt": "GBT", "random_forest": "random forest"}
ms = list(RIE["modelos"])
a1.bar([nombres[m] for m in ms], [RIE["modelos"][m]["auc_pr_cv"] for m in ms], yerr=[RIE["modelos"][m]["auc_pr_cv_desv"] for m in ms],
       color=[AZUL if m == RIE["mejor_modelo"] else "#9db8d9" for m in ms], error_kw={"ecolor": GRIS, "capsize": 3, "lw": 1})
azar = RIE["entrenamiento"]["cierres_pct"] / 100
a1.axhline(azar, color=ROJO, ls="--", label=f"azar ({d(azar, 3)})")
a1.set_ylabel("AUC-PR (validación cruzada)")
a1.legend(frameon=False, fontsize=7)
imp = sorted(RIE["importancia"].items(), key=lambda kv: abs(kv[1]))
a2.barh([k for k, _ in imp], [v for _, v in imp], color=VERDE)
a2.set_xlabel("importancia en el modelo elegido")
fig.tight_layout()
fig.savefig(os.path.join(RES, "fig_e2_riesgo.png"), dpi=200)
plt.close(fig)

# ---------------------------------------------------------------- documento
doc = docx.Document()
sec = doc.sections[0]
sec.left_margin = sec.right_margin = Inches(0.8)
sec.top_margin = sec.bottom_margin = Inches(0.65)
doc.styles["Normal"].font.name = "Calibri"
doc.styles["Normal"].font.size = Pt(9.5)
doc.styles["Normal"].paragraph_format.space_after = Pt(3)
for nombre, tam in (("Heading 1", 12),):
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
r = par.add_run("LifeMap · Entrega 2: streaming, supervivencia y alerta temprana de cierre")
r.bold = True
r.font.size = Pt(15)
par = doc.add_paragraph()
par.alignment = WD_ALIGN_PARAGRAPH.CENTER
par.add_run("Big Data · Institución Universitaria de Envigado · 2026-2 · David Mesa y Eber Urrego\n"
            f"Grupo: Philadelphia (PA) + Restaurants · URL del sistema: {args.url}")

# ---- 1. arquitectura
h1("1. Arquitectura completa")
p("La Entrega 2 se monta sobre la plataforma batch de la Entrega 1 sin reemplazarla: el mismo docker-compose.yml, la misma base MongoDB, la misma API y el mismo "
  "pipeline de Jenkins. Se agregan Kafka (modo KRaft) y un job de Spark Structured Streaming, y todo lo anterior sigue funcionando.")
tabla(["Pieza", "Función", "Detalle"], [
    ["kafka", "Bus de mensajes", "Kafka 3.7 en KRaft; topic reviews con 3 particiones y clave business_id"],
    ["productor (app/productor.py)", "Reproduce el futuro con reloj acelerado", "Publica en orden de fecha solo lo posterior a T; segundos por mes configurables"],
    ["streaming (spark_jobs/streaming_resenas.py)", "Inferencia, ventanas y alertas", "Carga los modelos versionados; dos consultas: filas clasificadas + riesgo, y ventanas por celda"],
    ["mongo", "Estado compartido", "resenas_stream, ventanas, alerts, riesgo_actual, supervivencia (con 2dsphere donde hay geometría)"],
    ["api + dashboard", "Consultas y panel en vivo", "4 endpoints nuevos, /health con Kafka y retraso del consumidor, /dashboard con 4 visualizaciones"],
    ["jenkins", "Prueba y despliega", "Agrega la prueba de humo del flujo y la del modelo versionado antes de desplegar"],
], [1.9, 1.8, 3.1])

# ---- 2. streaming
h1("2. Decisiones de streaming")
pre = STR["productor"]
v(f"T = {STR['fecha_t']} (12 meses antes de la última reseña del dataset). Las {n(STR['reservadas_posteriores_a_T'])} reseñas posteriores a T se reservan: "
  f"ni el modelo de sentimiento (reentrenado una vez, solo con reseñas hasta T: F1 macro {d(SEN['modelos'][SEN['mejor_modelo']]['f1_macro'], 3)}) "
  f"ni el de riesgo las vieron. Ambos modelos quedan versionados con una huella (hash) de sus archivos.", "Corte T y reserva.")
v(f"Publica las reseñas posteriores a T en orden de fecha, con clave business_id (las de un negocio van siempre a la misma partición) y velocidad por parámetro: "
  f"con {d(pre['seg_por_mes'], 0)} s por mes de reseñas, los 12 meses se reprodujeron en {d(pre['segundos'], 0)} s.", "Productor.")
v(f"Carga el modelo guardado (nunca lo reentrena) y, antes de cargarlo, comprueba que su huella coincide con la versionada. Cada mensaje queda en MongoDB con su predicción, "
  f"su probabilidad y la marca de procesamiento. Resultado: {n(STR['clasificadas'])} de {n(STR['reservadas_posteriores_a_T'])} reseñas clasificadas, retraso final "
  f"{n(STR['consumidor']['retraso_final'] or 0)} mensajes y un ritmo de unos {n(STR['clasificadas'] / pre['segundos'])} mensajes por segundo. "
  f"La predicción coincide con la etiqueta de las estrellas en el {pct(STR['acierto_vs_estrellas'])} % de las reseñas.", "Inferencia.")
v(f"Por celda de geohash, ventanas de 30 días con watermark de 7 días sobre el campo date (tiempo del evento, no de ingesta), alineadas para que una empiece en T. "
  f"Salen {n(STR['ventanas']['total'])} ventanas en {n(STR['ventanas']['celdas'])} celdas, cuyas reseñas suman {n(STR['ventanas']['resenas_en_ventanas'])}: "
  f"no se perdió ninguna. Cada ventana se guarda con el polígono de su celda y un índice 2dsphere.", "Ventanas.")

tabla(["Justificación del umbral de hotspot", "Valor"], [
    ["Pares de ventanas consecutivas de una celda con ≥ 30 reseñas, antes de T", n(HOT["pares_de_ventanas"])],
    ["Cambio normal del % negativo entre dos ventanas (media ± desviación)", f"{d(HOT['delta_medio_pp'], 2)} ± {d(HOT['delta_desv_pp'], 1)} puntos"],
    ["Percentil 90 / percentil 95 de ese cambio", f"{d(HOT['delta_p90_pp'], 1)} / {d(HOT['umbral_pp'], 1)} puntos"],
    ["Umbral elegido (percentil 95)", f"{d(HOT['umbral_pp'], 1)} puntos porcentuales"],
], [4.9, 1.9])
p(f"Regla: una celda es hotspot cuando su % de reseñas negativas sube al menos {d(HOT['umbral_pp'], 1)} puntos frente a la ventana anterior y las dos ventanas tienen "
  f"30 reseñas o más. El umbral sale de la historia anterior a T: es un salto que casi nunca ocurre por azar (cerca del 5 % de los cambios normales). "
  f"En el flujo hubo {n(STR['ventanas']['hotspots'])} ventanas hotspot en {n(STR['ventanas']['celdas_con_hotspot'])} celdas; las mayores fueron "
  + "; ".join(f"{h['celda']} ({h['inicio'][:7]}, {d(h['pct_negativas'], 0)} % negativas, +{d(h['delta_pp'], 1)} puntos)" for h in STR["top_hotspots"][:3]) + ".")
figura("fig_e2_streaming.png", 6.0, "Figura 1. % de reseñas negativas (predichas) por ventana de 30 días y número de celdas hotspot durante la reproducción.")

# ---- 3. supervivencia
h1("3. Supervivencia (Kaplan-Meier)")
gl = SUP["global"]
et = SUP["entrada_tardia_dias"]
p(f"Se estima con Kaplan-Meier propio (con pruebas contra casos calculados a mano). Eje de tiempo: días desde la primera reseña. Censura: un negocio abierto (is_open = 1) sale del "
  f"grupo en riesgo en su última reseña pero no cuenta como cierre ({n(gl['negocios'] - gl['cierres'])} de {n(gl['negocios'])} están censurados). "
  f"Entrada tardía: Yelp solo incluye negocios con al menos {SUP['K_resenas_para_entrar']} reseñas, así que un negocio solo es observable desde su reseña número "
  f"{SUP['K_resenas_para_entrar']}; los que cerraron antes ni aparecen. Ese tiempo (media {n(et['media'])} días, mediana {n(et['mediana'])}) no cuenta como «en riesgo»: "
  f"un negocio está en riesgo en el día t si su entrada < t ≤ su duración. Ignorarla sobrestima la supervivencia. "
  f"La curva se usa solo donde hay al menos {SUP['n_min_en_riesgo']} negocios en riesgo y los grupos con menos de {SUP['n_min_grupo']} negocios o menos de 10 cierres se marcan "
  f"«pequeños» y no se interpretan.")
p(f"Todos los negocios: probabilidad de seguir abierto {pct(gl['s_1a'])} % al año, {pct(gl['s_3a'])} % a los 3 años y {pct(gl['s_5a'])} % a los 5; mediana de vida "
  f"{n(gl['mediana_dias'])} días (unos {d(gl['mediana_dias'] / 365, 1)} años), dentro de la parte confiable de la curva (días {n(gl['t_inicio'])} a {n(gl['t_fin'])}).")
filas = []
for etiqueta, lista in (("menor", SUP["categorias"]["menor_supervivencia_3a"][:3]), ("mayor", SUP["categorias"]["mayor_supervivencia_3a"][:3])):
    for c in lista:
        filas.append([c["clave"], n(c["negocios"]), n(c["cierres"]), pct(c["s_3a"]), n(c["mediana_dias"]) if c["mediana_dias"] else "no se alcanza",
                      f"{etiqueta} supervivencia"])
tabla(["Categoría", "Negocios", "Cierres", "Seguir abierto a 3 años (%)", "Mediana (días)", "Grupo"], filas, [1.4, 0.8, 0.8, 1.5, 1.2, 1.2])
zn = SUP["zonas"]
p(f"Por zona: cada celda de geohash es un polígono y sus negocios se piden a MongoDB con $geoWithin (da exactamente los mismos negocios que el geohash en "
  f"{n(zn['geoWithin_igual_a_geohash'])} de {n(zn['celdas'])} celdas). Se ajustan {n(zn['ajustadas'])} zonas con 30 negocios o más y solo {n(zn['interpretables'])} son interpretables; "
  f"las otras {n(zn['pequenas'])} son pequeñas. Entre las interpretables, la supervivencia a 3 años va de {pct(zn['menor_supervivencia_3a'][0]['s_3a'])} % "
  f"(celda {zn['menor_supervivencia_3a'][0]['clave']}, {zn['menor_supervivencia_3a'][0]['negocios']} negocios) a {pct(zn['mayor_supervivencia_3a'][0]['s_3a'])} % "
  f"(celda {zn['mayor_supervivencia_3a'][0]['clave']}, {zn['mayor_supervivencia_3a'][0]['negocios']} negocios). Con unos 100 negocios por celda estas diferencias tienen bastante "
  f"incertidumbre: se leen como zonas a vigilar, no como un ranking exacto.")
figura("fig_e2_supervivencia.png", 5.9, "Figura 2. Curvas de Kaplan-Meier: todos los negocios (con intervalo al 95 %) y las dos categorías de menor y de mayor supervivencia a 3 años.")

# ---- 4. modelo de riesgo
h1("4. Modelo de riesgo de cierre")
en, pt = RIE["entrenamiento"], RIE["en_prueba"]
mj = RIE["modelos"][RIE["mejor_modelo"]]
nom = {"regresion_logistica": "regresión logística", "gbt": "GBT (árboles con gradiente)", "random_forest": "random forest"}
p(f"Es el patrón de KafkaMed aplicado a negocios. Variables por negocio, calculadas solo con reseñas hasta T (hay una prueba que comprueba que una reseña posterior no cambia nada): "
  f"sentimiento medio, tendencia del % negativo (últimos 6 meses menos los 6 anteriores), estrellas, número de reseñas, edad, días desde la última reseña, actividad de los últimos 6 meses, "
  f"cambio de estrellas reciente y densidad de competidores de la misma categoría a 500 m (con $geoNear). Etiqueta: 1 si el negocio cerró y su última reseña cae entre T y T + 12 meses; "
  f"0 si sigue abierto o con actividad después del plazo; se excluyen los que no tienen historia antes de T o actividad después. Quedan {n(en['negocios'])} negocios con {n(en['cierres'])} "
  f"cierres ({d(en['cierres_pct'], 2)} %): un problema muy desbalanceado.")
p(f"Partición: 85 % para desarrollo y 15 % para prueba, estratificada. El modelo y el umbral se eligen con validación cruzada de 5 pliegues (con {n(RIE['particion']['cierres']['desarrollo'])} cierres "
  f"una sola validación es muy inestable) y la prueba se usa una sola vez al final. Se pesan las clases ({d(RIE['pesos_de_clase']['cierra'], 1)} para cierres y {d(RIE['pesos_de_clase']['no_cierra'], 2)} para el resto), porque un modelo que "
  f"siempre dijera «no cierra» acertaría el {d(100 - en['cierres_pct'], 1)} % y no serviría.")
filas = [[nom[m], f"{d(RIE['modelos'][m]['auc_roc_cv'], 3)} ± {d(RIE['modelos'][m]['auc_roc_cv_desv'], 3)}", f"{d(RIE['modelos'][m]['auc_pr_cv'], 3)} ± {d(RIE['modelos'][m]['auc_pr_cv_desv'], 3)}",
          "elegido" if m == RIE["mejor_modelo"] else ""] for m in RIE["modelos"]]
tabla(["Modelo", "AUC-ROC (validación cruzada)", "AUC-PR (validación cruzada)", ""], filas, [2.0, 2.0, 2.0, 0.8])
p(f"Se eligió {nom[RIE['mejor_modelo']]} y un umbral de {d(RIE['umbral'], 2)} (el que maximiza F1 en las predicciones fuera de muestra). En la prueba, una sola vez: AUC-ROC {d(mj['auc_roc_test'], 3)}, "
  f"AUC-PR {d(mj['auc_pr_test'], 3)} (el azar daría {d(RIE['auc_pr_base_azar'], 3)}), precision {pct(pt['precision'])} %, recall {pct(pt['recall'])} % y F1 {d(pt['f1'], 3)}, marcando el "
  f"{d(pt['marcados_pct'], 1)} % de los negocios: de {n(pt['tp'] + pt['fn'])} cierres reales detecta {n(pt['tp'])} y genera {n(pt['fp'])} falsas alarmas. "
  f"Interpretación: con tan pocos cierres la accuracy no dice nada y el AUC-PR es la métrica honesta; el modelo encuentra los cierres unas {d(pt['precision'] / RIE['auc_pr_base_azar'], 1)} veces mejor que el azar, "
  f"pero la señal es débil (AUC-ROC de {d(mj['auc_roc_test'], 2)}): predecir un cierre solo con reseñas y ubicación es difícil. Sirve para priorizar a quién mirar, no para decidir. "
  f"La prueba tiene solo {n(RIE['particion']['cierres']['prueba'])} cierres, así que estas cifras tienen un margen amplio.")
figura("fig_e2_riesgo.png", 5.9, "Figura 3. Izquierda: AUC-PR por modelo en validación cruzada frente al azar. Derecha: importancia de las variables en el modelo elegido.")

# ---- 5. alertas
h1("5. Alertas en streaming")
al = STR["alertas"]
p(f"En cada micro-lote (foreachBatch) se actualiza el estado de los negocios que recibieron reseñas (conteos, suma de sentimiento y el último año de reseñas, sin releer su historia), "
  f"se recalculan sus variables, se puntúan con el modelo de riesgo y se guardan en la colección alerts, con su geometría, probabilidad y fecha, los que superan el umbral. "
  f"Si un negocio en alerta baja del umbral, la alerta se cierra. En la reproducción se actualizó el riesgo de {n(STR['negocios_con_riesgo_actualizado'])} negocios y se generaron "
  f"{n(al['total_generadas'])} alertas: {n(al['activas'])} siguen activas y {n(al['cerradas'])} se cerraron. Las reseñas de negocios sin línea base (no activos a T) no se puntúan.")
if al["top"]:
    tabla(["Negocio", "Categoría", "Riesgo", "Fecha de la alerta"], [[a["name"], a["categoria"], f"{pct(a['prob'], 0)} %", a["fecha"][:10]] for a in al["top"][:5]], [2.6, 1.7, 0.9, 1.6])

# ---- 6. api, dashboard, jenkins
h1("6. API, panel y despliegue continuo")
tabla(["Endpoint", "Qué devuelve", "Parámetros"], [
    ["GET /live/hotspots", "Celdas de una ventana de 30 días en hotspot, con su polígono", "ventana (fecha o «ultima»), lat, lon, radio, todas, limit"],
    ["GET /alerts · POST /alerts", "Alertas activas por radio o dentro de un polígono", "lat, lon, radio, min_prob, limit; polígono GeoJSON en el cuerpo"],
    ["GET /survival/<categoria>", "Curva de Kaplan-Meier con número en riesgo (TODAS = todos)", "zona (geohash) para la curva de una zona"],
    ["GET /risk/near", "Negocios cerca de un punto con su probabilidad de cierre", "lat, lon, radio, min_prob, limit"],
    ["GET /health", "Estado de cada servicio, incluidos Kafka y el retraso del consumidor", "—"],
], [1.7, 2.9, 2.3])
p("El panel (/dashboard) tiene las cuatro visualizaciones pedidas: mapa de hotspots, tabla de alertas activas, curvas de supervivencia y evolución del % negativo, y se actualiza cada 5 segundos "
  "mientras el productor publica. Para Power BI se entrega una guía con las consultas (Web.Contents) de cada visual sobre estos mismos endpoints; el archivo .pbix se arma conectándolas a la URL desplegada.")
p("Jenkins agrega dos pruebas antes de desplegar, y una falla detiene el despliegue: (1) prueba de humo del flujo, que publica 100 mensajes en un topic de prueba y verifica que aparecen "
  "clasificados en MongoDB, agregados en ventanas con su geometría y con el riesgo actualizado, y que el topic tiene 3 particiones; (2) prueba del modelo versionado, que confirma que la huella del "
  "modelo que cargó el streaming es la de su version.json (y una prueba unitaria confirma que un modelo modificado no se carga). Además hay pruebas unitarias del Kaplan-Meier (censura y entrada tardía), "
  "del riesgo (sin fuga de datos posteriores a T y estado incremental igual a recálculo completo) y de la regla de hotspot.")

# ---- 7. reflexion
h1("7. Qué se reutilizó")
v("el pipeline de limpieza, tokenización, stopwords y TF-IDF con Naive Bayes y regresión logística se entrenó en la Entrega 1 con texto real y aquí se carga ya entrenado para clasificar en vivo; "
  "lo nuevo es el corte T para no ver el futuro y el versionado por huella.", "SentimentStream:")
v("el productor, el consumidor con inferencia en streaming y la alerta temprana con un modelo evaluado con AUC-PR por el desbalance; cambia el objeto (cierre de negocio en lugar de falla cardíaca) "
  "y se agrega la actualización incremental de variables por micro-lote.", "KafkaMed:")
v("MongoDB con GeoJSON e índice 2dsphere, $near, $geoWithin y $geoNear, Docker Compose, Jenkins con despliegue automático y descarga desde Kaggle; ahora también las celdas y ventanas con su geometría.", "Trabajo geoespacial:")
v("reduceByKey y la tasa anualizada (Entrega 1), la censura y la entrada tardía, las curvas de Kaplan-Meier restringidas a donde hay suficientes en riesgo, y las ventanas con watermark.", "Ejercicio de discos:")

# ---- 8. limitaciones
h1("8. Limitaciones")
v("la vida de un negocio sigue siendo aproximada (primera y última reseña); la entrada tardía modela que Yelp exige 5 reseñas, pero no la edad real antes de la primera reseña.")
v(f"el modelo de riesgo es modesto y su prueba tiene {n(RIE['particion']['cierres']['prueba'])} cierres; sirve para priorizar, no para decidir, y con solo reseñas y ubicación hay un techo bajo.")
v("las alertas dependen de la velocidad de reproducción: el «ahora» de cada micro-lote es la fecha más reciente que contiene, así que con una reproducción muy rápida los lotes abarcan meses y salen más alertas que a la velocidad del enunciado.")
v("el sentimiento usa la clasificación del modelo (no las estrellas) para las ventanas, así que hereda su error, sobre todo en la clase neutral.")
v("el panel en vivo es web; el .pbix de Power BI no se genera automáticamente y debe armarse con la guía.")
doc.save(SALIDA)
print("guardado", SALIDA)
