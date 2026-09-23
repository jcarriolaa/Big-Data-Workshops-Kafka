#!/usr/bin/env python3
"""Lee los eventos de venta de tu topic y, si se lo pides, los guarda en disco.

    python3 apps/consumidor.py --grupo G1 --carne 20241234
    python3 apps/consumidor.py --grupo G1 --carne 20241234 --guardar
    python3 apps/consumidor.py --grupo G1 --carne 20241234 --grupo-lectura otro

El GRUPO DE LECTURA (group_id) es lo que hace que varios consumidores se
repartan el trabajo: abre dos o tres terminales con el mismo y veras como Kafka
le da a cada uno unas particiones distintas. Si cambias el group_id, empiezas de
cero y recibes todo otra vez.

Tu carne va dentro del group_id a proposito: es la prueba, en tus capturas y en
tu video, de que el cluster que muestras es el tuyo.
"""
import argparse
import json
import pathlib
import sys

from kafka import KafkaConsumer

BROKERS = ["localhost:9092", "localhost:9093", "localhost:9094"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grupo", required=True, choices=["G1", "G2", "G3", "G4"])
    ap.add_argument("--carne", required=True, help="tu numero de carne")
    ap.add_argument("--grupo-lectura", default="principal",
                    help="cambialo para volver a leer todo desde el principio")
    ap.add_argument("--guardar", action="store_true",
                    help="ademas de mostrarlos, escribelos en salida/")
    ap.add_argument("--maximo", type=int, default=0,
                    help="para solo despues de leer N mensajes (0 = sin limite)")
    ap.add_argument("--topic", help="por defecto, ventas-<grupo>")
    args = ap.parse_args()

    topic = args.topic or f"ventas-{args.grupo}"
    group_id = f"{args.carne}-{args.grupo_lectura}"

    consumidor = KafkaConsumer(
        topic,
        bootstrap_servers=BROKERS,
        group_id=group_id,
        client_id=f"consumidor-{args.carne}",
        # earliest = si este grupo es nuevo, empieza por el mensaje mas viejo.
        # latest = solo lo que llegue de ahora en adelante.
        auto_offset_reset="earliest",
    )

    salida = None
    if args.guardar:
        carpeta = pathlib.Path("salida")
        carpeta.mkdir(exist_ok=True)
        ruta = carpeta / f"{topic}.jsonl"
        # Con --maximo empezamos de cero: asi el archivo tiene exactamente los
        # mensajes de esta corrida y el conteo es reproducible.
        salida = ruta.open("w" if args.maximo else "a", encoding="utf-8")
        print(f"Guardando ademas en {ruta}")

    print(f"Leyendo {topic} como grupo '{group_id}'. Ctrl+C para parar.\n")
    leidos = 0
    try:
        for msg in consumidor:
            leidos += 1
            # Todo lo que sale de Kafka son bytes: hay que convertirlo.
            clave = msg.key.decode("utf-8") if msg.key else "(sin clave)"
            try:
                v = json.loads(msg.value.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                v = repr(msg.value)
            # Un topic puede traer mensajes que no siguen el formato que
            # esperamos (por ejemplo los que escribiste a mano en el Paso 3).
            # No nos caemos por eso: los mostramos tal cual.
            if isinstance(v, dict) and "producto" in v:
                detalle = f"{v['producto']:10s} x{v['cantidad']}"
            else:
                detalle = f"(otro formato) {v}"
            print(f"  particion {msg.partition}  offset {msg.offset:6d}  "
                  f"{clave:20s}  {detalle}", flush=True)
            if salida:
                salida.write(json.dumps(v, ensure_ascii=False) + "\n")
                salida.flush()   # sin esto no verias crecer el archivo en vivo
            if args.maximo and leidos >= args.maximo:
                print(f"\nLlegue a {args.maximo} mensajes; paro aqui.")
                break
    except KeyboardInterrupt:
        print("\nParado con Ctrl+C.")
    finally:
        if salida:
            salida.close()
        consumidor.close()
        print(f"Total leidos por este consumidor: {leidos}")


if __name__ == "__main__":
    main()
