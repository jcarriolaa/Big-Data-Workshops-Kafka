# Bitácora del taller de Kafka

- **Nombre:**
- **Carné:**
- **Grupo:** G#
- **Enlace a tu video:** `video-demo.mp4` (en esta misma carpeta)

> Copia este archivo como `bitacora.md` dentro de tu carpeta de entrega y llénalo.
> Las capturas van en `capturas/` y se enlazan desde aquí.
> Borra estas líneas de ayuda (las que empiezan con `>`) antes de entregar.

## 1. Evidencias

> Cada captura debe verse completa, con el comando y su salida. Debajo de cada una,
> escribe **una o dos frases** con lo que muestra.

### E1. Los tres brokers y el detalle de tu topic (Pasos 6 y 7)

![E1](capturas/E1-tres-brokers.png)

> Si guardaste una captura como .jpg o la partiste en dos (E1a y E1b, por ejemplo), cambia su
> enlace para que diga el nombre exacto de tu archivo. Vale para E1, E4, E5 y E6.

Lo que muestra:

### E2. La interfaz web con tu topic de ventas (Paso 8)

![E2](capturas/E2-interfaz-web.png)

Lo que muestra:

### E3. Varios consumidores repartiéndose las particiones (Paso 8)

![E3](capturas/E3-consumer-group.png)

Lo que muestra:

### E4. Falla de un broker (Paso 9)

![E4](capturas/E4-broker-caido.png)

Lo que muestra:

### E5. Escrituras rechazadas (Paso 10)

![E5](capturas/E5-escritura-rechazada.png)

Lo que muestra:

### E6. Kafka Connect escribiendo el archivo (Paso 11)

![E6](capturas/E6-connect.png)

Lo que muestra:

## 2. Preguntas

> De 2 a 5 líneas por respuesta (mínimo 15 palabras), con tus palabras y con lo que viste en tu pantalla.
> Escribe debajo de cada pregunta como texto normal, sin `>` al inicio.
> Cita tus propios números: los de tu `--describe`, tus particiones, tus tiendas.

**1. ¿Qué guarda el controller y qué guarda cada broker? Justifícalo con lo que viste, y compáralo con el NameNode y los DataNodes del taller anterior.**

**2. Con tu `--describe`: ¿cuántas particiones tiene tu topic, quién es líder de cada una y qué significa que el Isr tenga tres entradas?**

**3. Produjiste con la tienda como clave. ¿Por qué una misma tienda cae siempre en la misma partición y qué garantía de orden te da eso? ¿Qué perderías si produjeras sin clave?**

**4. Corriste dos, tres y cuatro consumidores en el mismo grupo de lectura. ¿Qué pasó con el cuarto y por qué? ¿Qué cambió con un `--grupo-lectura` distinto?**

**5. Al detener brokers, en un caso el productor siguió y en otro falló. Explica por qué, y relaciona `acks` y `min.insync.replicas` con la C y la A del teorema CAP.**

**6. Sacaste los datos a un archivo con tu consumidor y con Kafka Connect. ¿Qué tuviste que resolver para que Connect funcionara, y en qué caso real preferirías cada forma?**

## 3. Mini-reto de tu grupo

**Pregunta asignada a tu grupo:**

**Tu programa** (también va como `mini-reto.py`):

```python
```

**Resultado de tu programa** — la salida tal como la imprimió, y también la línea del `wc -l`:

```
```

**Interpretación en una frase:**

## 4. Problemas encontrados (opcional)

> Opcional: qué falló, qué se intentó y cómo se resolvió.
