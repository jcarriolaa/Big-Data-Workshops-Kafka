# Parte 1 — Guiada: un broker, un topic, productor y consumidor

Sesión guiada en clase (80 minutos). Todo el código se proporciona. No se califica; la Parte 2
continúa sobre el clúster que queda al final.

Cada paso tiene la misma estructura: objetivo, código, **✅ Checkpoint** con la salida esperada y
**❌ Si falla**.

En los comandos, `G1` es el grupo (`G1` a `G4`) y `20241234` el carné: sustitúyelos por los tuyos.

**Regla de oro.** Hay dos direcciones para hablar con Kafka:

| Desde dónde | Dirección |
|---|---|
| Un comando **dentro** de un contenedor (`docker exec ...`) | `kafka1:19092` |
| Programas de Python **en tu máquina** | `localhost:9092` |

La razón se explica en el Paso 6 de la Parte 2.

---

## Paso 1: Levanta Kafka

Kafka necesita dos piezas: el **controller**, que lleva el inventario, y un **broker**, que guarda
los mensajes. El archivo `compose.yml` ya define las dos (`controller` y `kafka1`), con cada línea
comentada.

```bash
docker compose up -d
```

`docker compose` lee `compose.yml` y levanta un **contenedor** por servicio: una copia aislada de la
imagen `apache/kafka:4.3.1`. Los datos viven en **volúmenes**
(`kafka1-data`), que sobreviven a `docker compose down`; solo `down -v` los borra.

### ✅ Checkpoint

```bash
docker compose ps --format 'table {{.Name}}\t{{.Status}}'
```

```
NAME         STATUS
controller   Up 30 seconds
kafka1       Up 30 seconds
```

### ❌ Si falla

| Síntoma | Arreglo |
|---|---|
| `no configuration file provided` | No estás en la carpeta del repo. Con `ls` tienes que ver `compose.yml`. |
| `port is already allocated` | Otro proceso usa el puerto 9092. |
| El contenedor se detiene solo | `docker compose logs kafka1`: el último error indica la causa. |

---

## Paso 2: Crea tu topic con tres particiones

Un **topic** es un canal de mensajes con nombre. Se parte en **particiones**: pedazos que se pueden
leer y escribir en paralelo, como los bloques de HDFS.

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:19092 \
  --create --topic tiendas-G1 --partitions 3 --replication-factor 1
```

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:19092 --describe --topic tiendas-G1
```

```
Topic: tiendas-G1	TopicId: cDKX2wUZTf-GQMxlvV0H0A	PartitionCount: 3	ReplicationFactor: 1	Configs: min.insync.replicas=1
	Topic: tiendas-G1	Partition: 0	Leader: 11	Replicas: 11	Isr: 11	Elr: 	LastKnownElr: 
	Topic: tiendas-G1	Partition: 1	Leader: 11	Replicas: 11	Isr: 11	Elr: 	LastKnownElr: 
	Topic: tiendas-G1	Partition: 2	Leader: 11	Replicas: 11	Isr: 11	Elr: 	LastKnownElr: 
```

Tres particiones (0, 1 y 2), y las tres viven en el broker `11` (`kafka1`), que es su **Leader**:
el que atiende lecturas y escrituras. `ReplicationFactor: 1` quiere decir que hay **una sola copia**
de cada partición; con un solo broker no puede haber más. `TopicId` es aleatorio.

### ✅ Checkpoint

```bash
bash scripts/check.sh broker
```

```
✅ Contenedor controller corriendo
✅ Contenedor kafka1 corriendo
✅ kafka1 responde a kafka-topics
✅ Existe tu topic tiendas-G1

Todo en orden.
```

### ❌ Si falla

| Síntoma | Arreglo |
|---|---|
| `Topic 'tiendas-G1' already exists` | El topic ya existe; continúa con el paso siguiente. |

---

## Paso 3: Escribe y lee a mano

En dos terminales, desde la carpeta del repo. En la primera, un **productor** de consola (escribe):

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:19092 --topic tiendas-G1
```

Escribe `Hola Kafka`, Enter, y `Segundo mensaje`, Enter. Cada línea tras el `>` es un mensaje.

En la segunda, un **consumidor** (lee):

```bash
docker exec kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:19092 --topic tiendas-G1 --from-beginning
```

```
The consumer rebalance protocol (KIP-848) is production-ready! Set group.protocol=consumer to try it out. See https://kafka.apache.org/documentation/#consumer_rebalance_protocol
Hola Kafka
Segundo mensaje
```

La primera línea es un aviso informativo de Kafka.

Una línea nueva en el productor aparece de inmediato en el consumidor: eso es **streaming**.
Detén ambos con `Ctrl+C`.

Dos conceptos que se usan en todo el taller:

- `--from-beginning` lee desde el mensaje más viejo. **Kafka guarda los mensajes aunque ya se hayan
  leído**; esa es la gran diferencia con una cola tradicional.
- Dentro de cada partición los mensajes se numeran 0, 1, 2... Ese número es el **offset**. "Por dónde
  va un lector" es un offset.

### ✅ Checkpoint

Los mensajes del productor aparecen en el consumidor.

### ❌ Si falla

| Síntoma | Arreglo |
|---|---|
| El consumidor no muestra nada | Falta `--from-beginning`: sin él solo se leen mensajes nuevos. |
| No aparece el `>` | Falta `-it` en `docker exec -it`. |

---

## Paso 4: Productor y consumidor en Python

`apps/productor.py` genera ventas como lo haría un punto de venta. La parte clave:

```python
futuro = productor.send(
    topic,
    key=tienda.encode("utf-8"),
    value=json.dumps(evento).encode("utf-8"),
)
```

Cada mensaje lleva una **clave** (la tienda) y un **valor** (la venta en JSON). Kafka elige la
partición a partir de la clave, así que **una misma tienda cae siempre en la misma partición**.

```bash
python3 apps/productor.py --grupo G1 --topic tiendas-G1 --eventos 30
```

```
Publicando en tiendas-G1. Ctrl+C para parar.

  [    1] tienda-gt-centro     -> particion 0  offset 2
  [    2] tienda-gt-zona10     -> particion 1  offset 0
  [    3] tienda-gt-mixco      -> particion 0  offset 3
  [    4] tienda-gt-mixco      -> particion 0  offset 4
  [    5] tienda-sv-santaana   -> particion 2  offset 0
  [    6] tienda-hn-sps        -> particion 1  offset 1
  ...
Total enviados: 30
```

Cada tienda cae siempre en la misma partición. Los datos varían por grupo; el patrón no.

**Por qué importa.** Kafka garantiza el orden **dentro de una partición**, no en todo el topic. Como
cada tienda vive en una sola partición, sus ventas llegan en orden. Las de dos tiendas distintas se
pueden intercalar, porque son independientes.

Consumo:

```bash
python3 apps/consumidor.py --grupo G1 --carne 20241234 --topic tiendas-G1 --grupo-lectura clase
```

```
Leyendo tiendas-G1 como grupo '20241234-clase'. Ctrl+C para parar.

  particion 1  offset      0  tienda-gt-zona10      Chocolate  x3
  particion 1  offset      1  tienda-hn-sps         Cafetera   x4
  ...
  particion 0  offset      0  (sin clave)           (otro formato) b'Hola Kafka'
  particion 0  offset      1  (sin clave)           (otro formato) b'Segundo mensaje'
  particion 0  offset      2  tienda-gt-centro      Cafetera   x4
```

El consumidor lee partición por partición, y el orden entre particiones varía entre corridas: es la
garantía descrita arriba. Las líneas `(otro formato)` son los mensajes del Paso 3, sin clave y sin
JSON, y ocupan los primeros offsets de la partición donde cayeron. Con el consumidor abierto, una
nueva corrida del productor aparece en tiempo real. Detén ambos con `Ctrl+C`.

### ✅ Checkpoint

El productor termina con `Total enviados: 30` y el consumidor muestra las ventas con su partición.

### ❌ Si falla

| Síntoma | Arreglo |
|---|---|
| `No module named 'kafka'` | `pip3 install -r apps/requirements.txt` |
| `NoBrokersAvailable` | Kafka no está corriendo: `docker compose ps`. |
| `UnknownTopicOrPartitionError` | El nombre del topic no coincide con el del Paso 2. |

---

## Estado al terminar

Un broker, un topic con tres particiones, un productor y un consumidor que no se conocen entre sí.
Sin tolerancia a fallas: si `kafka1` falla, falla todo.

Siguiente: **[Parte 2](parte-2-tarea.md)**, sobre este mismo clúster.
