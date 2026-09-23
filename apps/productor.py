#!/usr/bin/env python3
"""Publica eventos de venta en Kafka, uno por uno, como si fueran ventas reales.

Cada grupo tiene su propia semilla, asi que los numeros de tu stream no son los
mismos que los de otro grupo.

    python3 apps/productor.py --grupo G1                 # 2000 eventos y termina
    python3 apps/productor.py --grupo G1 --eventos 0     # no para nunca (Ctrl+C)
    python3 apps/productor.py --grupo G1 --pausa 0.05    # mas rapido

La CLAVE del mensaje es la tienda. Kafka usa la clave para decidir la particion,
asi que todas las ventas de una misma tienda caen siempre en la misma y llegan
en orden.
"""
import argparse
import json
import random
import time

from kafka import KafkaProducer

# Los tres brokers. En realidad basta con uno: Kafka le cuenta al cliente donde
# estan los demas. Ponemos los tres por si el primero esta apagado.
BROKERS = ["localhost:9092", "localhost:9093", "localhost:9094"]

SEMILLAS = {"G1": 101, "G2": 202, "G3": 303, "G4": 404}

TIENDAS = {
    "Guatemala":   ["tienda-gt-centro", "tienda-gt-zona10", "tienda-gt-mixco"],
    "El Salvador": ["tienda-sv-sansal", "tienda-sv-santaana"],
    "Honduras":    ["tienda-hn-tegus", "tienda-hn-sps"],
    "Costa Rica":  ["tienda-cr-sanjose"],
}
PRODUCTOS = [
    ("Cafe", 45.0), ("Chocolate", 38.0), ("Miel", 72.0), ("Bocina", 830.0),
    ("Cafetera", 735.0), ("Licuadora", 520.0), ("Zapatos", 410.0),
    ("Pantalon", 295.0), ("Balon", 160.0), ("Bicicleta", 3700.0),
]
CANALES = ["tienda", "web", "app"]


def generar_eventos(grupo, cuantos):
    """Va soltando los eventos de un grupo, siempre los mismos y en el mismo orden.

    La semilla depende del grupo, asi que cada uno tiene sus propios numeros y
    ningun resultado sirve para otro grupo.
    """
    rnd = random.Random(SEMILLAS[grupo])

    # Cada grupo reparte sus ventas de forma distinta entre paises y canales,
    # asi que el pais y el canal ganadores cambian de un grupo a otro.
    paises = list(TIENDAS)
    peso_pais = [rnd.randint(1, 10) for _ in paises]
    peso_canal = [rnd.randint(1, 5) for _ in CANALES]

    n = 0
    while cuantos == 0 or n < cuantos:
        pais = rnd.choices(paises, weights=peso_pais)[0]
        tienda = rnd.choice(TIENDAS[pais])
        producto, precio = rnd.choice(PRODUCTOS)
        n += 1
        yield {
            "id_venta": n,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "pais": pais,
            "tienda": tienda,
            "producto": producto,
            "cantidad": rnd.randint(1, 5),
            "precio_unitario": precio,
            "canal": rnd.choices(CANALES, weights=peso_canal)[0],
        }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grupo", required=True, choices=sorted(SEMILLAS))
    ap.add_argument("--eventos", type=int, default=2000, help="0 = sin fin")
    ap.add_argument("--pausa", type=float, default=0.2, help="segundos entre eventos")
    ap.add_argument("--topic", help="por defecto, ventas-<grupo>")
    args = ap.parse_args()

    topic = args.topic or f"ventas-{args.grupo}"

    productor = KafkaProducer(
        bootstrap_servers=BROKERS,
        # acks dice CUANDO damos por buena una escritura:
        #   "0"   -> no esperamos nada (rapido, se pueden perder mensajes)
        #   "1"   -> basta con que responda el lider
        #   "all" -> tienen que confirmar todas las copias al dia
        # Con "all", el broker respeta el min.insync.replicas del topic. Esa
        # pareja es la que decide, POR ESCRITURA, si prefieres no perder datos
        # o seguir disponible. Pruebalo en el Paso 10.
        acks="all",
    )

    print(f"Publicando en {topic}. Ctrl+C para parar.\n")
    enviados = 0
    try:
        for evento in generar_eventos(args.grupo, args.eventos):
            tienda = evento["tienda"]

            # La clave (la tienda) decide la particion. Misma tienda, misma
            # particion, siempre.
            futuro = productor.send(
                topic,
                key=tienda.encode("utf-8"),
                value=json.dumps(evento).encode("utf-8"),
            )
            try:
                md = futuro.get(timeout=10)
                enviados += 1
                print(f"  [{enviados:5d}] {tienda:20s} -> particion {md.partition}  offset {md.offset}", flush=True)
            except Exception as e:
                # Cuando tumbes brokers vas a caer aqui. Eso es el experimento.
                print(f"  [!] NO se pudo enviar: {type(e).__name__}: {e}", flush=True)

            time.sleep(args.pausa)
    except KeyboardInterrupt:
        print("\nParado con Ctrl+C.")
    finally:
        productor.flush()
        productor.close()
        print(f"Total enviados: {enviados}")


if __name__ == "__main__":
    main()
