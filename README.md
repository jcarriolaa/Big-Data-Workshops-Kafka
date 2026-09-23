# Taller de Apache Kafka

Curso *Big Data y su Relación con Generative AI*, UFM. Clase 16.

## Introducción

En el taller de Hadoop se procesaron 75 000 ventas **en lote**. En un sistema real las ventas llegan
**una por una, en tiempo real**. Kafka es la plataforma que las recibe, las reparte entre varios
servidores y las conserva aunque uno de ellos falle. Lo usan Uber, Netflix y LinkedIn (donde
nació) para mover millones de eventos por segundo.

## Objetivo

Responder, con evidencia generada en tu propio clúster:

- ¿Por qué los eventos de una misma tienda llegan siempre en orden, pero los de tiendas distintas no?
- Si apago un servidor mientras entran datos, ¿se pierde algo?
- ¿Por qué a veces Kafka se niega a aceptar datos, y por qué eso es una decisión y no una falla?

## Qué vas a construir

```
  tu máquina                             Docker
                         ┌─────────────────────────────────────┐
  productor.py  ───────► │  controller                         │
                         │  kafka1     kafka2     kafka3       │
  consumidor.py ◄─────── │  interfaz web     Kafka Connect ────┼──► archivo
                         └─────────────────────────────────────┘
```

- **controller**: lleva el inventario del clúster (qué topics hay y quién manda en cada partición).
  No guarda mensajes. Es el NameNode del taller pasado.
- **kafka1, kafka2, kafka3**: los **brokers**, que guardan los mensajes repartidos y copiados. Son
  los DataNodes.
- **productor / consumidor**: programas en Python que escriben y leen. Nunca se hablan entre sí,
  solo con Kafka.

## Contenido

| Parte | Modalidad | Tiempo estimado | Contenido |
|---|---|---|---|
| **Paso 0** | Prerrequisito | 30 min | Docker, Python y la imagen de Kafka |
| **[Parte 1 — Guiada](parte-1-guiada.md)** | En clase | 80 min | Levantar Kafka, crear un topic con particiones, producir y consumir mensajes con Python. Todo el código se proporciona |
| **[Parte 2 — Tarea](parte-2-tarea.md)** | Tarea | 10–12 h | Interfaz web, clúster de tres brokers, escenarios de falla y volcado a disco con Kafka Connect. Tres pasos son de investigación |
| **[Entrega](ENTREGA.md)** | Tarea | 2–3 h | Bitácora, video y Pull Request |

La Parte 1 no se califica y es prerrequisito de la Parte 2.

**Dudas:** en clase. Aprovecha ese tiempo.

## Qué entregas y cuánto vale

Todo es **individual**. El detalle de cada punto está en [`ENTREGA.md`](ENTREGA.md).

| Qué entregas | % |
|---|---|
| **Bitácora** | **50** |
| · 6 capturas de tu clúster (evidencias E1 a E6) | 14 |
| · 6 preguntas respondidas con tus palabras y tus números | 24 |
| · Mini-reto: una pregunta de negocio sobre los datos de tu grupo | 12 |
| **Video de 5 a 8 minutos** mostrando tu clúster funcionando y fallando | **50** |
| **Total** | **100** |

También se entregan el `compose.yml`, los dos archivos de Kafka Connect, `mini-reto.py` y
`check.txt`. No tienen porcentaje propio: respaldan la autoría del trabajo.

**Fecha de entrega: lunes 28 de septiembre, 4:00 PM** (hora de Guatemala), por Pull Request. Hasta
el viernes 2 de octubre a las 4:00 PM se acepta tarde, y vale la mitad; después, no se recibe.

## Paso 0: Prerrequisitos

1. **Docker Desktop** (<https://www.docker.com/products/docker-desktop/>), con al menos **6 GB de
   RAM** asignados en *Settings → Resources*. El taller completo usa unos 2.5 GB.
2. **Fork del repo.** Un *fork* es tu copia del repo en tu cuenta de GitHub; desde ahí se abre el Pull
   Request de la entrega. Haz **Fork** de <https://github.com/jcarriolaa/Big-Data-Workshops-Kafka> y
   clona **tu** fork.

   ```bash
   git clone https://github.com/TU-USUARIO/Big-Data-Workshops-Kafka.git
   cd Big-Data-Workshops-Kafka
   ```

3. **Python 3.10 o más nuevo**, y la librería de Kafka:

   ```bash
   pip3 install -r apps/requirements.txt
   ```

4. **La imagen de Kafka** (unos 450 MB):

   ```bash
   docker pull apache/kafka:4.3.1
   ```

Todos los comandos del taller se corren **desde la carpeta del repo**.

### ✅ Checkpoint

```bash
bash scripts/check.sh prereq
```

```
✅ docker compose v2 disponible
✅ Docker esta corriendo
✅ Docker tiene 15 GB de RAM
✅ imagen apache/kafka:4.3.1 descargada
✅ Python instalado como 'python3' (3.10.9)
✅ la libreria kafka-python esta instalada

Todo en orden.
```

La RAM reportada debe ser de 6 GB o más.

Siguiente: **[Parte 1](parte-1-guiada.md)**.
