import json
import os
import time
from datetime import datetime

from pymongo import MongoClient

from app import config

DIAS_MES = 30.4375


def segundo_real(fecha, t, seg_por_mes):
    """A los cuantos segundos de arrancar el productor toca publicar una resena (reloj acelerado).

    Con seg_por_mes = 10 un mes de resenas dura 10 segundos reales: una resena de T + 3 meses sale a los 30 s.
    """
    meses = (fecha - t).total_seconds() / (DIAS_MES * 86400)
    return meses * seg_por_mes


def mensaje(doc):
    """El json que viaja por kafka: la clave es el business_id (asi las resenas de un negocio van a la misma particion)."""
    valor = {"review_id": doc["_id"], "business_id": doc["business_id"], "stars": int(doc["stars"]),
             "text": doc["text"], "date": doc["date"].strftime("%Y-%m-%d %H:%M:%S")}
    return doc["business_id"], valor


def asegurar_topic(servidor, topic, particiones=config.PARTICIONES_TOPIC):
    """Crea el topic con varias particiones si no existe (con una sola no se ve el reparto por clave)."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    admin = KafkaAdminClient(bootstrap_servers=servidor)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=particiones, replication_factor=1)])
        print(f"topic {topic} creado con {particiones} particiones")
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def crear_productor(servidor):
    from kafka import KafkaProducer
    return KafkaProducer(bootstrap_servers=servidor, key_serializer=lambda k: k.encode(),
                         value_serializer=lambda v: json.dumps(v).encode(), linger_ms=20, acks=1)


def publicar(productor, topic, docs):
    """Publica una lista de documentos de resena ya ordenados por fecha (lo usan las pruebas)."""
    for doc in docs:
        k, v = mensaje(doc)
        productor.send(topic, key=k, value=v)
    productor.flush()
    return len(docs)


def main():
    t = datetime.fromisoformat(config.FECHA_T)
    servidor, topic = config.KAFKA_SERVIDOR, config.TOPIC_RESENAS
    maximo = int(os.getenv("MAX_MENSAJES", "0"))  # 0 = todas
    asegurar_topic(servidor, topic)
    productor = crear_productor(servidor)
    col = MongoClient(config.MONGO_URI)[config.MONGO_DB][config.COL_RESENAS]
    total = col.count_documents({"date": {"$gt": t}})
    print(f"reservadas (posteriores a {t.date()}): {total} resenas | {config.SEG_POR_MES:g} s reales por mes de resenas")

    # solo lo posterior a T, en orden de fecha: es lo que se reservo y el modelo nunca vio
    cursor = col.find({"date": {"$gt": t}}).sort("date", 1)
    inicio, enviados, ultimo_print, ult_fecha = time.time(), 0, 0, t
    for doc in cursor:
        espera = segundo_real(doc["date"], t, config.SEG_POR_MES) - (time.time() - inicio)
        if espera > 0.02:
            time.sleep(espera)
        k, v = mensaje(doc)
        productor.send(topic, key=k, value=v)
        enviados, ult_fecha = enviados + 1, doc["date"]
        if time.time() - ultimo_print > 5:
            ultimo_print = time.time()
            print(f"  {enviados}/{total} publicadas | fecha virtual {ult_fecha.date()} | {time.time() - inicio:.0f} s")
        if maximo and enviados >= maximo:
            break
    productor.flush()
    resumen = {"publicadas": enviados, "total_reservadas": total, "seg_por_mes": config.SEG_POR_MES, "topic": topic,
               "hasta_fecha": ult_fecha.strftime("%Y-%m-%d"), "segundos": round(time.time() - inicio, 1),
               "particiones": config.PARTICIONES_TOPIC}
    os.makedirs(os.path.join(config.DATOS_DIR, "resultados"), exist_ok=True)
    with open(os.path.join(config.DATOS_DIR, "resultados", "productor.json"), "w") as f:
        json.dump(resumen, f, indent=2)
    print("listo:", resumen)


if __name__ == "__main__":
    main()
