# Qué entregas y cómo se califica

Taller de Kafka · Curso *Big Data y su Relación con Generative AI*, UFM.

**Fecha de entrega: lunes 28 de septiembre, 4:00 PM** (hora de Guatemala), por Pull Request.

Todo es **individual**. En tu grupo comparten el topic y los datos, pero cada quien levanta su
clúster, toma sus capturas, graba su video y escribe sus respuestas.

Las capturas se toman durante la Parte 2. En los comandos, sustituye `G1`, `20241234` y
`juan-perez` por tu grupo, carné y nombre.

---

## Tu carpeta de entrega

Todo va en `entregas/G#/<carné>-<nombre>-<apellido>/`, en minúsculas y sin tildes ni ñ. Por ejemplo,
`entregas/G1/20241234-juan-perez/`:

| Archivo | Qué es |
|---|---|
| `bitacora.md` | Tu bitácora, copiada de [`bitacora-plantilla.md`](bitacora-plantilla.md) y llena |
| `capturas/E1-tres-brokers.png` … `capturas/E6-connect.png` | Las seis capturas |
| `video-demo.mp4` | Tu video |
| `mini-reto.py` | Tu programa del mini-reto |
| `compose.yml` | Tu compose, con los tres brokers, la interfaz web y Kafka Connect |
| `connect/connect-standalone.properties` y `connect/connect-file-sink.properties` | Tu configuración de Connect |
| `check.txt` | La prueba de que tu clúster estuvo completo |

El `compose.yml`, los `.properties` y el `check.txt` no tienen porcentaje propio: respaldan la autoría.
Su contenido se evalúa a través de las capturas y el video.

---

## 1. Bitácora (50 %)

En tu bitácora, borra las líneas que empiezan con `>` (son instrucciones), pero **no** los enunciados
en negrita de las preguntas: la validación los usa para encontrar tus respuestas.

### 1.1 Las evidencias (14 %)

Capturas de **tu** máquina donde se vea **el comando y su salida** (E2 es del navegador). Debajo de
cada una, en la bitácora, una o dos frases diciendo qué se ve.

| # | % | Archivo | Qué debe verse | Paso |
|---|---|---|---|---|
| E1 | **2** | `E1-tres-brokers.png` | Los tres ids de broker **y** el `--describe` de `ventas-G#` (o dos capturas, `E1a` y `E1b`) | 6 y 7 |
| E2 | **1** | `E2-interfaz-web.png` | Tu interfaz web con `ventas-G#` y sus mensajes | 8 |
| E3 | **2** | `E3-consumer-group.png` | `kafka-consumer-groups.sh --describe` con dos o más `CONSUMER-ID` y la columna `LAG` | 8 |
| E4 | **3** | `E4-broker-caido.png` | El `Isr` en dos **y** tu productor sin errores | 9 |
| E5 | **3** | `E5-escritura-rechazada.png` | `Leader: none` **y** tu productor fallando | 10 |
| E6 | **3** | `E6-connect.png` | Connect corriendo **y** `connect/ventas.txt` con eventos | 11 |

Cada imagen en `.png` o `.jpg`, de 1 MB como máximo, con al menos 800 px de ancho para que se lea,
y distinta de las demás. Para reducir el peso: capturar solo la ventana o usar `.jpg`. Si no caben en una pantalla, E1, E4, E5 y E6 se pueden partir en dos (`E1a` y `E1b`,
etc.). Las evidencias que piden dos cosas ("esto **y** aquello") valen la mitad si solo se ve una.

### 1.2 Las 6 preguntas (24 %, 4 cada una)

De dos a cinco líneas (mínimo 15 palabras), **con tus palabras y con tus números**.

| % | Cómo es la respuesta |
|---|---|
| **4** | Correcta, con tus palabras, citando algo de tu pantalla: tus ids, tu ISR, tu partición |
| **3** | Correcta y con tus palabras, pero sin ningún dato tuyo |
| **2** | Parcialmente correcta, incompleta o con conceptos mezclados |
| **1** | Copiada de internet o de la guía |
| **0** | En blanco |

1. ¿Qué guarda el **controller** y qué guarda cada **broker**? Justifícalo con lo que viste, y
   compáralo con el NameNode y los DataNodes del taller anterior.
2. Con tu `--describe`: ¿cuántas particiones tiene tu topic, quién es **líder** de cada una y qué
   significa que el **Isr** tenga tres entradas?
3. Produjiste con la tienda como **clave**. ¿Por qué una misma tienda cae siempre en la misma
   partición y qué garantía de orden te da eso? ¿Qué perderías si produjeras sin clave?
4. Corriste dos, tres y cuatro consumidores en el mismo grupo de lectura. ¿Qué pasó con el cuarto y
   por qué? ¿Qué cambió con un `--grupo-lectura` distinto?
5. Al detener brokers, en un caso el productor siguió y en otro falló. Explica por qué, y relaciona
   `acks` y `min.insync.replicas` con la **C** y la **A** del teorema CAP.
6. Sacaste los datos a un archivo con tu consumidor y con Kafka Connect. ¿Qué tuviste que resolver
   para que Connect funcionara, y en qué caso real preferirías cada forma?

### 1.3 El mini-reto de tu grupo (12 %)

| Grupo | Pregunta |
|---|---|
| **G1** | ¿Qué país concentra más ventas en dinero (`cantidad × precio_unitario`)? Total de cada país, de mayor a menor. |
| **G2** | ¿Cuáles son los 5 productos con más unidades vendidas? ¿Cuántas vendió el primero? |
| **G3** | ¿Qué tienda vendió más en dinero, y cuánto? Total por tienda. |
| **G4** | Tratando cada evento como una venta, ¿cuál es el monto promedio (`cantidad × precio_unitario`) por canal, qué canal tiene el más alto y cuántas ventas tuvo cada canal? |

Va después del video (orden de cierre en la sección 3): el primer comando borra el topic. Antes,
detén el productor y Connect (`docker compose stop connect`).

Se trabaja sobre **exactamente 2000 eventos**, para que el resultado sea comparable:

```bash
# 1. Borra el topic y créalo de nuevo
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 \
  --delete --topic ventas-G1

# Espera a que ya no aparezca en la lista:
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --list

docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 \
  --create --topic ventas-G1 --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2

# 2. Publica 2000 eventos, sin pausa
python3 apps/productor.py --grupo G1 --eventos 2000 --pausa 0

# 3. Guárdalos: el consumidor para solo al llegar a 2000
python3 apps/consumidor.py --grupo G1 --carne 20241234 --grupo-lectura reto \
  --guardar --maximo 2000
```

Con `--maximo`, el consumidor escribe `salida/ventas-G1.jsonl` desde cero: el archivo del Paso 11.1
se sobrescribe. `wc -l salida/ventas-G1.jsonl` debe dar 2000. Para repetir el paso 3 se usa otro grupo
(`--grupo-lectura reto2`): el grupo `reto` ya consumió todo y no recibiría mensajes.

El programa lee el archivo, un evento JSON por línea:

```python
import json
for linea in open("salida/ventas-G1.jsonl"):
    evento = json.loads(linea)      # evento["pais"], evento["cantidad"], etc.
```

En la bitácora van **el programa, el resultado (con la línea del `wc -l`) y una frase
interpretándolo**. Programa correcto 6 · resultado correcto 4 · interpretación 2.

El resultado numérico es el mismo dentro de un grupo. El programa y la interpretación son
individuales.

---

## 2. Video (50 %)

Entre **5 y 8 minutos**, con narración, en `video-demo.mp4`, de **50 MB** como máximo. Si dura más
de 8, se califican los primeros 8; si dura menos de 5, se califica sobre lo que muestre.

### El guion, en este orden

1. **Quién eres** (20 s). Nombre, carné y grupo, con `hostname` y `date` en la terminal.
2. **Tu clúster** (45 s). `docker compose ps` con `controller`, los tres brokers y tu interfaz, y el
   `--describe` de tu topic explicando `Leader`, `Replicas` e `Isr`.
3. **El stream** (1 min). Productor y consumidor a la vez. Una misma tienda cae siempre en la misma
   partición. Levanta un segundo consumidor del mismo grupo y enseña el reparto con
   `kafka-consumer-groups.sh --describe`, donde se ve tu carné en el `group.id`.
4. **Tolerancia a fallas** (1–1.5 min). Con todo corriendo, `docker compose stop kafka2`. Corre el
   `--describe` de tu topic: el `Isr` bajó a dos y el productor sigue.
5. **Escrituras rechazadas** (1.5–2 min). La falla del Paso 10, un broker a la vez: sin reiniciar
   `kafka2`, `docker compose stop kafka3`; el productor que ya corría empieza a fallar. Corre otra vez
   el `--describe` y explica con **tus números** —copias en el `Isr`, `min.insync.replicas`,
   `acks`— por qué Kafka rechaza. Detén el productor tras dos o tres fallos.
6. **Sink** (1 min). Reinicia los brokers (`docker compose --profile cluster start kafka2 kafka3`) y
   Connect si no está activo (`docker compose --profile connect up -d connect`); tras 20 segundos,
   relanza el productor y muestra Connect escribiendo `connect/ventas.txt` en tiempo real. Una
   frase: por qué aquí no hace falta código.

El topic `sin-copias` no va en el video: se queda en tu captura E5.

**Los puntos 4 y 5 van en una sola toma, sin cortes.** Repetir un comando dentro de la toma está
permitido; editar o cortar, no. El resto se puede grabar por partes.

### Cómo se califica

| Qué | % |
|---|---|
| Tu clúster y el stream funcionando (puntos 2 y 3) | 12 |
| Los dos escenarios de falla en una sola toma, leyendo el `--describe` en pantalla | 15 |
| Entorno propio: nombre y carné, `hostname` y `date`, y tu carné en el `group.id` | 6 |
| Kafka Connect escribiendo el archivo, y por qué un conector en vez de código | 12 |
| Explicar por qué `acks="all"` con `min.insync.replicas=2` hace que Kafka rechace escrituras | 5 |

Cada criterio se califica completo o no se califica.

### Si pesa más de 50 MB

Grabar en 720p. Para comprimir:

```bash
ffmpeg -i entrada.mov -vf scale=1280:-2 -r 15 -c:v libx264 -crf 30 \
  -c:a aac -b:a 64k -movflags +faststart video-demo.mp4
```

---

## 3. Cómo se entrega

**Orden de cierre** (todo requiere el clúster activo):

1. Las seis capturas (durante la Parte 2).
2. El video, grabado y revisado.
3. El mini-reto (sección 1.3), que borra tu topic.
4. El `check.txt` (abajo), con el clúster todavía arriba.
5. Tu carpeta, la validación y el PR (abajo).
6. Limpieza (Paso 12 de la [Parte 2](parte-2-tarea.md)): solo después de recibir la calificación.

Desde el clon de tu fork (Paso 0):

**1. Conecta el repo del curso y crea tu rama** (una sola vez):

```bash
git remote add upstream https://github.com/jcarriolaa/Big-Data-Workshops-Kafka.git
git checkout main
git pull upstream main
git checkout -b entrega-20241234-juan-perez
```

`git pull upstream main` trae las correcciones publicadas en el repo del curso.

**2. Arma tu carpeta:**

```bash
mkdir -p entregas/G1/20241234-juan-perez/capturas
cp bitacora-plantilla.md entregas/G1/20241234-juan-perez/bitacora.md
cp compose.yml entregas/G1/20241234-juan-perez/
mkdir -p entregas/G1/20241234-juan-perez/connect
cp connect/*.properties entregas/G1/20241234-juan-perez/connect/

# Con el clúster arriba:
bash scripts/check.sh cluster >  entregas/G1/20241234-juan-perez/check.txt
bash scripts/check.sh stream  >> entregas/G1/20241234-juan-perez/check.txt
```

Después se agregan las capturas, el video y `mini-reto.py`, y se completa la bitácora.

**3. Valida en tu máquina:**

```bash
python3 scripts/validar_entrega.py entregas/G1/20241234-juan-perez
```

Resultado esperado: **0 ❌**. Cada ❌ indica qué falta. Antes del primer commit, la verificación
de que solo se modifica tu carpeta («T2 solo tu carpeta») aparece como ⚠️.

**4. Sube y abre el Pull Request:**

```bash
git add entregas/G1/20241234-juan-perez
git commit -m "Entrega taller Kafka - Juan Perez"
git push -u origin entrega-20241234-juan-perez
```

Sube **solo tu carpeta** (`git add` de tu carpeta, no `git add -A`). En GitHub, haz clic en **Compare &
pull request**, con destino `jcarriolaa/Big-Data-Workshops-Kafka`, rama `main`, y título
`Entrega G1 - Juan Perez`. **No hagas merge**: el PR es solo el medio de entrega.

La validación automática se publica en el PR. Cada push posterior la vuelve a ejecutar. La
validación no asigna la nota: un ❌ no es un cero, pero lo que falte no suma.

**5. Registra la URL del PR en MIU** (<https://miu.ufm.edu/>). La entrega es el PR; el registro en
MIU es informativo.

### ❌ Si falla

| Síntoma | Arreglo |
|---|---|
| `remote: Permission denied` | El push va al repo del curso. `git remote -v`: `origin` debe ser **tu** fork. |
| `this exceeds GitHub's file size limit` | El video excede el límite. Comprimir (sección 2). |
| El PR muestra archivos que no tocaste | `git fetch upstream && git merge upstream/main && git push` |

---

## Reglas

- **Hora.** Abrir el PR, hacer un commit o hacer un push a partir de las 4:00 PM del lunes vuelve
  tardía la entrega completa: se califica y se multiplica por 0.5. Hasta el viernes 2 de
  octubre a las 4:00 PM se acepta tarde; después, no se recibe.
- **Trabajo propio.** Entregas con la misma captura, el mismo video o la misma interpretación del
  mini-reto se revisan caso por caso. Resultados numéricos iguales dentro de un grupo son esperados.
- **Problemas no resueltos** se documentan en la sección 4 de la bitácora. No sustituyen lo que
  falte.
