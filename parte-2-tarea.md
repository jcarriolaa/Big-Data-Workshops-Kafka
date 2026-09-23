# Parte 2 — Tarea: un clúster tolerante a fallas

Continúa sobre el clúster de la [Parte 1](parte-1-guiada.md). Tres pasos son de investigación: la
interfaz web (Paso 5), los brokers nuevos (Paso 6) y Kafka Connect (Paso 11.2). Para ellos se indica
el estado final esperado y la documentación, no el código. Los demás pasos incluyen sus comandos.

Las **seis evidencias** (E1 a E6) están marcadas con **📸** en el paso correspondiente. El detalle
de la entrega está en [`ENTREGA.md`](ENTREGA.md).

---

## Paso 5: Una interfaz web

Kafka no incluye interfaz web. Hay varias de terceros que se agregan al `compose.yml` como un
servicio más. **Elige una e intégrala**:

| Imagen | Documentación |
|---|---|
| `kafbat/kafka-ui` | <https://ui.docs.kafbat.io/> |
| `redpandadata/console` | <https://docs.redpanda.com/current/console/> |
| `obsidiandynamics/kafdrop` | <https://github.com/obsidiandynamics/kafdrop> |

Estado esperado:

- La interfaz es un servicio de tu `compose.yml`, con `profiles: ["ui"]`. Un **perfil** deja
  el servicio apagado hasta que lo pides por nombre.
- Se conecta al clúster **desde dentro de Docker** (regla de oro de la Parte 1).
- Se abre en el navegador en `http://localhost:8080`.
- La imagen lleva una versión fija, nunca `latest`.

```bash
docker compose --profile ui up -d
```

### ✅ Checkpoint

La interfaz muestra **1 broker** y el topic `tiendas-G#` con sus mensajes.

### ❌ Si falla

| Síntoma | Arreglo |
|---|---|
| La página no carga | La interfaz tarda unos 30 segundos en arrancar; luego, `docker compose logs <servicio>`. |
| Carga pero no encuentra el clúster | Se configuró `localhost`: dentro de Docker, `localhost` es el propio contenedor de la interfaz. |
| `Additional property ... is not allowed` | Indentación del YAML: tu servicio va al mismo nivel que `kafka1`. |

---

## Paso 6: De uno a tres brokers

Agrega `kafka2` y `kafka3` al `compose.yml`, en el espacio marcado, sin modificar lo existente. El
punto de partida es el bloque de `kafka1`. Estado esperado:

| | `kafka2` | `kafka3` |
|---|---|---|
| `KAFKA_NODE_ID` | `12` | `13` |
| Puerto publicado | `9093` | `9094` |
| `container_name` y `hostname` | `kafka2` | `kafka3` |
| Perfil | `["cluster"]` | `["cluster"]` |
| Volumen | el suyo propio | el suyo propio |

Pistas: el puerto aparece en `ports`, en
`KAFKA_LISTENERS` y en `KAFKA_ADVERTISED_LISTENERS`; el volumen, en el servicio y en la lista del
final. `CLUSTER_ID` y `KAFKA_CONTROLLER_QUORUM_VOTERS` **no** cambian: son el mismo clúster. La
referencia de cada variable está en <https://kafka.apache.org/documentation/#brokerconfigs>.

El 9093 del controller es interno a su contenedor y no se publica: no entra en conflicto con el de
`kafka2`.

```bash
docker compose --profile ui --profile cluster up -d
```

`controller`, `kafka1` y la interfaz aparecen como *Running*: agregar brokers no reinicia los
servicios existentes, y los datos de `tiendas-G#` se conservan.

**Por qué hay dos direcciones.** Un programa que se conecta a Kafka pregunta primero "¿dónde están
todos?", y el broker le contesta con su `KAFKA_ADVERTISED_LISTENERS`. Luego el programa se conecta a
**esas** direcciones. Por eso cada broker anuncia dos: `kafka2:19092` para quien está dentro de
Docker y `localhost:9093` para tu máquina. Un `docker exec` que pregunta por `localhost` recibe
direcciones que dentro del contenedor no existen, y se cuelga.

### ✅ Checkpoint

```bash
docker exec kafka1 /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka1:19092 | grep -o "id: [0-9]*" | sort -u
```

```
id: 11
id: 12
id: 13
```

Esta salida es la primera mitad de la evidencia E1.

### ❌ Si falla

| Síntoma | Arreglo |
|---|---|
| Solo aparecen 1 o 2 ids | Un broker no arrancó: `docker compose logs kafka2`. |
| Un broker arranca y se apaga | Casi siempre es un `KAFKA_NODE_ID` repetido o un puerto que no coincide en las tres líneas. |
| `port is already allocated` | Dos brokers publican el mismo puerto. |
| Docker se queda sin memoria | Asigna al menos 6 GB de RAM a Docker. La Parte 2 requiere los tres brokers. |

---

## Paso 7: Tu topic con copias

El topic principal, `ventas-G#`, con **3 particiones y 3 copias** de cada una:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:19092 \
  --create --topic ventas-G1 --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2
```

`min.insync.replicas=2` dice cuántas copias tienen que estar al día para aceptar una escritura. Es
la pieza clave del Paso 10.

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:19092 --describe --topic ventas-G1
```

```
Topic: ventas-G1	TopicId: aLsovR-7TLKyAP0UwXonwg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: ventas-G1	Partition: 0	Leader: 11	Replicas: 11,12,13	Isr: 11,12,13	Elr: 	LastKnownElr: 
	Topic: ventas-G1	Partition: 1	Leader: 12	Replicas: 12,13,11	Isr: 12,13,11	Elr: 	LastKnownElr: 
	Topic: ventas-G1	Partition: 2	Leader: 13	Replicas: 13,11,12	Isr: 13,11,12	Elr: 	LastKnownElr: 
```

Lectura del `--describe`, por partición:

- **Leader**: el broker que la atiende. Kafka reparte los liderazgos.
- **Replicas**: los tres brokers que guardan una copia.
- **Isr** (*in-sync replicas*): las copias **al día** en este momento. Se reduce cuando falla un
  broker.
- **Elr**: copias que salieron del `Isr` pero tienen todo lo confirmado. Vacío mientras todo esté
  sano.

La asignación de líderes puede variar.

La diferencia entre controller y broker, en disco:

```bash
docker exec kafka1 ls /var/lib/kafka/data | grep -v __consumer_offsets
docker exec controller ls /var/lib/kafka/data
```

```
kafka1                              controller
------------------------------      ------------------------
__cluster_metadata-0                __cluster_metadata-0
bootstrap.checkpoint                bootstrap.checkpoint
cleaner-offset-checkpoint           meta.properties
log-start-offset-checkpoint
meta.properties
recovery-point-offset-checkpoint
replication-offset-checkpoint
tiendas-G1-0
tiendas-G1-1
tiendas-G1-2
ventas-G1-0
ventas-G1-1
ventas-G1-2
```

Los dos guardan `__cluster_metadata`, el registro de qué existe: el controller lo escribe y los
brokers lo copian. **Solo el broker tiene una carpeta por partición** (`tiendas-G1-0`, `ventas-G1-0`...):
ahí se almacenan los mensajes. El `grep -v` omite las 50 carpetas de `__consumer_offsets`, el topic interno
donde Kafka anota por dónde va cada grupo de lectura.

### 📸 E1

Los tres ids del Paso 6 **y** este `--describe` (`E1-tres-brokers.png`, o `E1a-...` y `E1b-...`).

### ✅ Checkpoint

```bash
bash scripts/check.sh cluster
```

```
✅ Contenedor controller corriendo
✅ Contenedor kafka1 corriendo
✅ Contenedor kafka2 corriendo
✅ Contenedor kafka3 corriendo
✅ Brokers registrados en el cluster: 3
✅ Existe tu topic de ventas: ventas-G1
✅ Particiones del topic: 3
✅ Copias de cada particion: 3
✅ min.insync.replicas=2 configurado

Todo en orden.
```

### Para pensar

Tu topic `tiendas-G#` de la Parte 1 se creó con un solo broker. Ahora que hay tres, ¿Kafka le
agregó copias solo?

<details>
<summary>Respuesta</summary>

No: `--describe --topic tiendas-G1` sigue diciendo `ReplicationFactor: 1`. La replicación se decide
al crear el topic y no cambia sola.
</details>

### ❌ Si falla

| Síntoma | Arreglo |
|---|---|
| `Replication factor: 3 larger than available brokers` | No están los tres brokers activos (Paso 6). |
| `Isr` con menos de 3 | Un broker recién iniciado aún se sincroniza; repetir en 30 segundos. |

---

## Paso 8: El stream y los grupos de lectura

Con `--eventos 0`, el productor publica de forma continua hasta `Ctrl+C`:

```bash
python3 apps/productor.py --grupo G1 --eventos 0
```

Sin `--topic`, los programas usan `ventas-G#`.

En otra terminal, un consumidor:

```bash
python3 apps/consumidor.py --grupo G1 --carne 20241234
```

```
Leyendo ventas-G1 como grupo '20241234-principal'. Ctrl+C para parar.

  particion 0  offset      0  tienda-gt-centro      Cafetera   x4
  particion 0  offset      1  tienda-gt-mixco       Licuadora  x2
  particion 0  offset      2  tienda-gt-mixco       Zapatos    x4
```

Tu consumidor se presenta ante Kafka con un **`group.id`**, aquí `20241234-principal`, que lleva tu
carné. Un **grupo de lectura** es un equipo de consumidores con el mismo `group.id` que se reparten
las particiones: cada partición la lee **uno solo** del equipo.

Agrega un **segundo**, un **tercero** y un **cuarto** consumidor con el mismo comando. Cada vez que
entra uno, Kafka redistribuye las particiones (**rebalanceo**).

El reparto, en números:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:19092 --describe --group 20241234-principal
```

```
GROUP              TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
20241234-principal ventas-G1  0          62              73              11   consumidor-20241234-4a2a159e-...
20241234-principal ventas-G1  1          53              64              11   consumidor-20241234-74dea164-...
20241234-principal ventas-G1  2          10              11              1    consumidor-20241234-a7169eb3-...
```

(Se omiten las columnas `HOST` y `CLIENT-ID`.) Con cuatro consumidores aparecen tres `CONSUMER-ID`:
con tres particiones, el cuarto queda sin asignación.

**LAG** es cuántos mensajes le faltan por leer al grupo. Es la métrica principal del streaming: si crece
de forma sostenida, los consumidores no dan abasto.

Un consumidor con **otro** grupo:

```bash
python3 apps/consumidor.py --grupo G1 --carne 20241234 --grupo-lectura auditoria
```

Lee **todo desde el principio**, independientemente del otro grupo: cada grupo mantiene sus propios
offsets.

### 📸 E2

Tu interfaz web con el topic `ventas-G#` abierto y sus mensajes (`E2-interfaz-web.png`).

### 📸 E3

La tabla de `kafka-consumer-groups.sh --describe` con **dos o más `CONSUMER-ID` distintos** y la
columna `LAG` (`E3-consumer-group.png`).

### ✅ Checkpoint

```bash
bash scripts/check.sh stream
```

```
✅ Mensajes publicados en ventas-G1: 191
✅ Grupos de lectura registrados: 4

Todo en orden.
```

Los valores dependen de cada clúster. Los grupos incluyen `clase` (Parte 1), `principal` y
`auditoria`. Puede aparecer también un `console-consumer-...` del Paso 3, que Kafka elimina al poco
tiempo por no tener offsets guardados. Se listan con:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:19092 --list
```

### ❌ Si falla

| Síntoma | Arreglo |
|---|---|
| El segundo consumidor no recibe nada | Rebalanceo en curso; tarda unos segundos. |
| Todos reciben todos los mensajes | Tienen `--grupo-lectura` distinto: son grupos separados. |
| `KafkaTimeoutError` desde el primer mensaje | Los tres brokers no están activos, o sus puertos no son 9092, 9093 y 9094. |

---

## Paso 9: Falla de un broker

Con **el productor y un consumidor corriendo**, detén un broker:

```bash
docker compose stop kafka2
```

El productor y el consumidor **continúan**. Estado del topic:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:19092 --describe --topic ventas-G1
```

```
Topic: ventas-G1	TopicId: aLsovR-7TLKyAP0UwXonwg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: ventas-G1	Partition: 0	Leader: 11	Replicas: 11,12,13	Isr: 11,13	Elr: 	LastKnownElr: 
	Topic: ventas-G1	Partition: 1	Leader: 13	Replicas: 12,13,11	Isr: 13,11	Elr: 	LastKnownElr: 
	Topic: ventas-G1	Partition: 2	Leader: 13	Replicas: 13,11,12	Isr: 13,11	Elr: 	LastKnownElr: 
```

1. **El `Isr` bajó de 3 a 2**: el broker 12 está apagado.
2. **La partición que lideraba el 12 cambió de líder** (aquí la 1, ahora del 13), de forma
   automática y en segundos.

Quedan 2 copias al día, que es el mínimo que pide `min.insync.replicas=2`; por eso las escrituras
continúan.

### 📸 E4

**Con `kafka2` detenido**: el `--describe` con el `Isr` en dos **y** el productor sin errores
(`E4-broker-caido.png`, o `E4a-...` y `E4b-...`).

### ✅ Checkpoint

Con `kafka2` apagado, el `Isr` tiene dos brokers y el productor no reporta errores.

### ❌ Si falla

| Síntoma | Arreglo |
|---|---|
| El productor falla al detener un broker | El topic no tiene 3 copias (Paso 7). |
| Con `kafka1` detenido falla todo | El experimento se hace con `kafka2` o `kafka3`. El topic interno de offsets de los grupos tiene una sola copia, en `kafka1`. |
| El consumidor se detiene al parar `kafka2` | Ocurre si el clúster se recreó con `down -v` teniendo los tres brokers activos: el topic interno de offsets queda repartido entre ellos. Para corregirlo, recrear el clúster siguiendo el orden original: la Parte 1 completa con un solo broker y, después, el Paso 6. |

### Antes del Paso 10

Reinicia `kafka2`; en unos 20 segundos el `Isr` vuelve a tener tres:

```bash
docker compose --profile cluster start kafka2
```

(Para detener un servicio basta su nombre; para iniciarlo se indica su perfil.)

---

## Paso 10: Escrituras rechazadas

Dos escenarios; ambos forman la evidencia E5. El productor del Paso 8 sigue corriendo.

### 10.1 Sin copias

Un topic de tres particiones **sin copias**, y dos brokers detenidos:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:19092 \
  --create --topic sin-copias --partitions 3 --replication-factor 1

docker compose stop kafka2 kafka3

docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:19092 --describe --topic sin-copias
```

```
Topic: sin-copias	TopicId: nVBfE1ifSjuJz79p7aCzPA	PartitionCount: 3	ReplicationFactor: 1	Configs: min.insync.replicas=1
	Topic: sin-copias	Partition: 0	Leader: 11	Replicas: 11	Isr: 11	Elr: 	LastKnownElr: 
	Topic: sin-copias	Partition: 1	Leader: none	Replicas: 12	Isr: 	Elr: 12	LastKnownElr: 12
	Topic: sin-copias	Partition: 2	Leader: none	Replicas: 13	Isr: 	Elr: 13	LastKnownElr: 13
```

**`Leader: none`**: esas particiones estaban en los brokers detenidos y no tienen copia.

### 10.2 Con copias, pero no suficientes

El productor del Paso 8 empieza a fallar al quedar un solo broker activo:

```
  [  307] tienda-sv-sansal     -> particion 0  offset 159
  [  308] tienda-hn-tegus      -> particion 1  offset 123
  [!] NO se pudo enviar: KafkaTimeoutError: KafkaTimeoutError: Timeout after waiting for 10 secs.
  [!] NO se pudo enviar: KafkaTimeoutError: KafkaTimeoutError: Timeout after waiting for 10 secs.
```

Cada intento espera 10 segundos antes de fallar. Tras dos o tres fallos, detenlo con `Ctrl+C`.

`kafka1` sigue activo, pero Kafka **rechaza la escritura**. La causa está en el log del broker:

```bash
docker compose logs kafka1 | grep NotEnoughReplicas | tail -2
```

```
kafka1  | org.apache.kafka.common.errors.NotEnoughReplicasException: The size of the current ISR : 1 is insufficient to satisfy the min.isr requirement of 2 for partition ventas-G1-0, live replica(s) broker.id are : Set(11)
```

El cliente solo recibe un *timeout*; la causa se registra en el servidor.

### `acks` y el teorema CAP

`acks` es cuánto espera el productor antes de dar un mensaje por guardado:

| `acks` | Espera | Riesgo |
|---|---|---|
| `0` | Nada | Se pueden perder mensajes sin aviso |
| `1` | Que conteste el líder | Si el líder cae antes de copiarlo, se pierde |
| `all` | Que confirmen todas las copias del `Isr` | Más lento, pero lo aceptado está respaldado |

`apps/productor.py` usa `acks="all"`. Y `min.insync.replicas` **solo se
aplica con `acks="all"`**: son una pareja.

Con una sola copia al día, Kafka tenía dos opciones: **aceptar** y seguir disponible (la **A** de
CAP), o **rechazar** para no guardar nada sin respaldo (la **C**). Rechazó por configuración:
`acks="all"` y `min.insync.replicas=2`. Con cualquiera de los dos cambiado, el mismo clúster, ante la
misma falla, acepta.

Esa es la tesis del curso: **CP o AP no es una propiedad del sistema, es una decisión por
operación.** El mismo Kafka puede ser CP para un topic de pagos y AP para uno de clics.

### 📸 E5

El `--describe` con `Leader: none` **y** el productor fallando (`E5-escritura-rechazada.png`, o
`E5a-...` y `E5b-...`).

Después, reinicia los brokers y borra el topic de prueba:

```bash
docker compose --profile cluster start kafka2 kafka3
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:19092 --delete --topic sin-copias
```

### ✅ Checkpoint

`sin-copias` muestra `Leader: none` y el productor falla con dos brokers detenidos.

### ❌ Si falla

| Síntoma | Arreglo |
|---|---|
| El productor sigue funcionando con dos brokers detenidos | El topic no tiene `min.insync.replicas=2` (Paso 7). |
| `grep NotEnoughReplicas` no muestra nada | `docker compose logs kafka1 --since 5m \| grep -i insync` |

---

## Paso 11: Volcado a disco

El destino final de un stream se llama **sink**. Dos formas de implementarlo.

### 11.1 Con un programa propio

```bash
python3 apps/consumidor.py --grupo G1 --carne 20241234 --grupo-lectura archivo --guardar
```

Usa su propio grupo de lectura (`archivo`): lee el topic completo sin competir por particiones con
los consumidores del Paso 8. Con el productor publicando
(`python3 apps/productor.py --grupo G1 --eventos 0`), `salida/ventas-G1.jsonl` crece con un evento
JSON por línea.

### 11.2 Sin programar: Kafka Connect

**Kafka Connect** mueve datos entre Kafka y otros sistemas con **conectores** existentes: se
configura, no se programa. La imagen `apache/kafka` lo incluye, junto con el conector
`FileStreamSink`, que escribe un topic en un archivo.

```bash
docker exec kafka1 ls /opt/kafka/bin | grep connect
docker exec kafka1 ls /opt/kafka/libs | grep connect-file
```

Documentación:

- Cómo funciona y cómo se arranca en modo *standalone* (un solo proceso): <https://kafka.apache.org/documentation/#connect>
- Propiedades del worker: <https://kafka.apache.org/documentation/#connectconfigs>

Estado esperado:

| Qué | Nombre |
|---|---|
| El servicio en tu `compose.yml` | `connect`, con `profiles: ["connect"]` |
| Sus dos archivos de configuración | `connect/connect-standalone.properties` y `connect/connect-file-sink.properties` |
| El archivo que escribe | `connect/ventas.txt`, con los eventos de `ventas-G#` |

```bash
docker compose --profile connect up -d connect
```

Puntos a resolver:

1. **Servicio**: Connect corre como servicio del compose, con la misma imagen de Kafka.
2. **Conexión**: se ejecuta dentro de Docker, así que usa la dirección interna de los brokers
   (regla de oro).
3. **Formato**: el productor envía JSON plano, y el *converter* que Connect usa por defecto espera
   otro formato.
4. **Conector**: en Kafka 4 el JAR de `FileStreamSink` está en la imagen pero no se carga por
   defecto.

**Cuándo conviene cada uno.** Connect, para mover datos de A a B sin código que mantener. Un programa
propio, cuando hay que transformar, filtrar o decidir algo en el camino.

### 📸 E6

Connect corriendo (`docker compose ps` o sus logs) **y** el `tail` de `connect/ventas.txt` con
eventos (`E6-connect.png`).

### ✅ Checkpoint

`connect/ventas.txt` existe y crece mientras el productor publica.

### ❌ Si falla

| Síntoma | Arreglo |
|---|---|
| `Failed to find any class that implements Connector` | Punto 4: Connect no encuentra el JAR del conector. |
| `JsonConverter with schemas.enable requires "schema" and "payload"` | Punto 3: el converter no corresponde al JSON del productor. |
| El contenedor se detiene al arrancar | `docker compose logs connect`; suele ser una ruta incorrecta en los `.properties`. |
| El archivo no aparece | Revisa `docker compose logs connect` y que el conector apunte a `ventas-G#`. |

---

## Paso 12: Limpieza

> **Nota:** este paso se ejecuta **después de entregar la bitácora y el video, y de recibir la
> calificación**. Borra el clúster y sus datos de forma definitiva.

```bash
docker compose --profile ui --profile cluster --profile connect down -v
```

`down` elimina los contenedores y `-v`, los volúmenes. Se indican los tres perfiles porque Compose
no actúa sobre servicios de perfiles no nombrados.

No usar `docker stop $(docker ps -q)`: detiene también contenedores de otros proyectos.

### ✅ Checkpoint

`docker compose ps -a` no lista nada.
