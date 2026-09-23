#!/usr/bin/env python3
"""Revisa que tu entrega esté completa antes de que el catedrático la califique.

Se corre solo en cada Pull Request, y también lo puedes correr tú:

    python3 scripts/validar_entrega.py entregas/G1/20241234-juan-perez

Solo revisa lo mecánico: que estén los archivos, que las capturas pesen poco, que el video dure lo
que debe, que no hayas dejado las líneas de ayuda de la plantilla. NO califica: la calidad de tus
respuestas y de tu video la evalúa el catedrático.

Termina con código 1 si hay algún ❌.
"""
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent

MAX_IMG = 1_000_000            # 1 MB por captura (1 millon de bytes)
MAX_VIDEO = 50 * 1024 * 1024   # 50 MB de video
VIDEO_MIN_S, VIDEO_MAX_S = 5 * 60, 8 * 60

CAPTURAS = ["E1", "E2", "E3", "E4", "E5", "E6"]
ARCHIVOS = ["bitacora.md", "check.txt", "mini-reto.py", "video-demo.mp4", "compose.yml",
            "connect/connect-standalone.properties", "connect/connect-file-sink.properties"]
PREGUNTAS = 6
MIN_PALABRAS = 15

# Frases de la plantilla que hay que borrar antes de entregar
AYUDAS = ["Copia este archivo como", "Borra estas líneas de ayuda",
          "Cada captura debe verse completa", "De 2 a 5 líneas por respuesta",
          "Opcional: qué falló", "Si guardaste una captura como",
          "Escribe debajo de cada pregunta"]

# Lo que tiene que decir check.txt (sale de los ok() de scripts/check.sh)
CADENAS_CHECK = ["Brokers registrados en el cluster: 3", "Mensajes publicados en"]

resultados = []


def ok(n, d=""):   resultados.append(("✅", n, d))
def warn(n, d=""): resultados.append(("⚠️", n, d))
def fail(n, d=""): resultados.append(("❌", n, d))


def git(*args):
    try:
        return subprocess.check_output(["git", "-C", str(RAIZ), *args], text=True,
                                       stderr=subprocess.DEVNULL)
    except Exception:
        return ""


def cabecera_video(ruta):
    """Los bytes donde un .mp4 guarda sus metadatos (el átomo 'moov').

    Unos grabadores lo ponen al principio del archivo y otros al final, así
    que leemos los primeros y los últimos megabytes.
    """
    with ruta.open("rb") as f:
        inicio = f.read(4_000_000)
        tam = ruta.stat().st_size
        if tam <= 8_000_000:
            return inicio + f.read()
        f.seek(tam - 4_000_000)
        return inicio + f.read()


def duracion_video(ruta):
    """Duración en segundos de un .mp4, leyendo sus metadatos. Sin dependencias.

    Un MP4 guarda la duración en el átomo 'mvhd'. Lo buscamos directamente en
    vez de exigir ffprobe, que no está en todas las máquinas ni en los runners
    de GitHub.
    """
    try:
        datos = cabecera_video(ruta)
        i = datos.find(b"mvhd")
        if i < 0:
            return None
        version = datos[i + 4]
        if version == 1:          # tiempos de 64 bits
            escala = int.from_bytes(datos[i + 24:i + 28], "big")
            unidades = int.from_bytes(datos[i + 28:i + 36], "big")
        else:                     # tiempos de 32 bits
            escala = int.from_bytes(datos[i + 16:i + 20], "big")
            unidades = int.from_bytes(datos[i + 20:i + 24], "big")
        return unidades / escala if escala else None
    except Exception:
        return None


def medidas_imagen(ruta):
    """Ancho y alto de un PNG o JPG, leyendo su cabecera."""
    try:
        d = ruta.open("rb").read(200_000)
        if d.startswith(b"\x89PNG\r\n\x1a\n"):
            return int.from_bytes(d[16:20], "big"), int.from_bytes(d[20:24], "big")
        if d.startswith(b"\xff\xd8"):
            i = 2
            while i < len(d) - 9:
                if d[i] != 0xFF:
                    i += 1
                    continue
                marca = d[i + 1]
                if marca in (0xC0, 0xC1, 0xC2, 0xC3):
                    return (int.from_bytes(d[i + 7:i + 9], "big"),
                            int.from_bytes(d[i + 5:i + 7], "big"))
                i += 2 + int.from_bytes(d[i + 2:i + 4], "big")
    except Exception:
        pass
    return None, None


def video_tiene_audio(ruta):
    """True si el mp4 declara una pista de sonido ('soun' en sus metadatos).

    None si no encontramos los metadatos: en ese caso no afirmamos nada.
    """
    try:
        datos = cabecera_video(ruta)
        if b"moov" not in datos:
            return None
        return b"soun" in datos
    except Exception:
        return None


def main():
    carpeta = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else detectar()
    if not carpeta:
        print("No supe qué carpeta revisar. Uso: python3 scripts/validar_entrega.py entregas/G1/<tu-carpeta>")
        return 2

    dir_ = RAIZ / carpeta

    # --- T1: la carpeta se llama como debe
    m = re.fullmatch(r"entregas/(G[1-4])/(\d+)-([a-z][a-z0-9-]*[a-z0-9])", carpeta)
    if m:
        grupo = m.group(1)
        ok("T1 carpeta", carpeta)
    else:
        grupo = None
        fail("T1 carpeta", f"{carpeta} no sigue entregas/G#/<carné>-<nombre>-<apellido>, "
                                "todo en minúsculas y sin tildes ni ñ")

    if not dir_.is_dir():
        fail("T1 carpeta", f"no existe {carpeta}")
        return terminar(carpeta)

    # --- T2: no tocaste nada fuera de tu carpeta
    base = os.environ.get("BASE_REF") or base_local()
    cambios = [l for l in git("diff", "--name-only", f"{base}...HEAD").splitlines() if l.strip()]
    fuera = [c for c in cambios if not c.startswith(carpeta + "/")]
    if not cambios:
        warn("T2 solo tu carpeta", "no pude comparar con main")
    elif fuera:
        fail("T2 solo tu carpeta", "tocas archivos fuera: " + ", ".join(fuera[:3]))
    else:
        ok("T2 solo tu carpeta")

    # --- T3: la rama se llama como debe
    rama = (os.environ.get("RAMA") or git("rev-parse", "--abbrev-ref", "HEAD")).strip()
    if rama and rama not in ("HEAD", "main"):
        if re.fullmatch(r"entrega-\d+-[a-z][a-z0-9-]*[a-z0-9]", rama):
            ok("T3 rama", rama)
        else:
            warn("T3 rama", f"{rama}: se esperaba entrega-<carné>-<nombre>-<apellido> en minúsculas")

    # --- T4: están todos los archivos
    faltan = [a for a in ARCHIVOS if not (dir_ / a).exists()]
    if faltan:
        fail("T4 archivos", "faltan: " + ", ".join(faltan))
    else:
        ok("T4 archivos", f"los {len(ARCHIVOS)} archivos están")

    # --- T5: las capturas
    caps = dir_ / "capturas"
    for e in CAPTURAS:
        encontradas = sorted(caps.glob(f"{e}*")) if caps.is_dir() else []
        if not encontradas:
            fail(f"T5 captura {e}", "no está")
            continue
        for img in encontradas:
            cab = img.open("rb").read(8)
            if not (cab.startswith(b"\x89PNG\r\n\x1a\n") or cab.startswith(b"\xff\xd8\xff")):
                fail(f"T5 captura {e}", f"{img.name} no es PNG ni JPG")
            elif img.stat().st_size > MAX_IMG:
                fail(f"T5 captura {e}", f"{img.name} pesa {img.stat().st_size / 1_000_000:.2f} MB y el máximo es 1 MB")
            else:
                an, al = medidas_imagen(img)
                if an and an < 800:
                    warn(f"T5 captura {e}", f"{img.name} mide {an}x{al} px y el mínimo son 800 de ancho: se va a ver ilegible")
                else:
                    medida = f", {an}x{al} px" if an else ""
                    ok(f"T5 captura {e}", f"{img.name} ({img.stat().st_size / 1000:.0f} KB{medida})")

    # --- T5b: no vale subir la misma imagen para varias evidencias
    if caps.is_dir():
        vistos = {}
        repes = []
        for img in sorted(caps.glob("*")):
            if not img.is_file():
                continue
            h = hashlib.sha256(img.read_bytes()).hexdigest()
            if h in vistos:
                repes.append(f"{img.name} = {vistos[h]}")
            else:
                vistos[h] = img.name
        if repes:
            fail("T5b capturas repetidas", "subiste la misma imagen para varias evidencias: "
                                           + "; ".join(repes[:3]))
        elif vistos:
            ok("T5b capturas repetidas", "las capturas son distintas entre sí")

    # --- T6: el video
    video = dir_ / "video-demo.mp4"
    if video.exists():
        mb = video.stat().st_size / 1024 / 1024
        if video.stat().st_size > MAX_VIDEO:
            fail("T6 video", f"pesa {mb:.0f} MB y el máximo es 50 MB. Comprímelo (ver ENTREGA.md)")
        else:
            ok("T6 video", f"{mb:.1f} MB")
        seg = duracion_video(video)
        if seg is None:
            warn("T6 duración", "no pude medirla: ¿el archivo es un .mp4 de verdad?")
        elif seg < VIDEO_MIN_S:
            warn("T6 duración", f"dura {seg/60:.1f} min; se piden 5 como mínimo. Se califica igual, pero revisa que tenga todo el guion")
        elif seg > VIDEO_MAX_S:
            warn("T6 duración", f"dura {seg/60:.1f} min; se piden 8 como máximo")
        else:
            ok("T6 duración", f"{seg/60:.1f} min")
        audio = video_tiene_audio(video)
        if audio is False:
            fail("T6 audio", "el video no trae pista de sonido y tienes que explicar en voz alta")
        elif audio:
            ok("T6 audio", "tiene pista de sonido")

    # --- T7: check.txt
    chk = dir_ / "check.txt"
    if chk.exists():
        txt = chk.read_text(errors="replace")
        if "❌" in txt:
            fail("T7 check.txt", "tu check.txt trae ❌: el entorno no estaba completo")
        else:
            faltantes = [c for c in CADENAS_CHECK if c not in txt]
            if faltantes:
                warn("T7 check.txt", "no encuentro: " + "; ".join(faltantes))
            else:
                ok("T7 check.txt", "cluster y stream en verde")

    # --- T8/T9: la bitácora
    bit_f = dir_ / "bitacora.md"
    if not bit_f.exists():
        fail("T8 bitácora", "no existe bitacora.md")
        return terminar(carpeta)

    bit = bit_f.read_text(errors="replace")

    ay = [a for a in AYUDAS if a in bit]
    if ay:
        fail("T8 plantilla", f"quedaron {len(ay)} líneas de ayuda sin borrar "
                                 "(las que empiezan con >)")
    else:
        ok("T8 plantilla", "sin líneas de ayuda")

    for i in range(1, PREGUNTAS + 1):
        mm = re.search(rf"\*\*{i}\.\s.*?\*\*(.*?)(?=\n\*\*{i+1}\.\s|\n## |\Z)", bit, re.S)
        t = (mm.group(1) if mm else "").strip()
        t = "\n".join(l for l in t.splitlines() if not l.strip().startswith(">")).strip()
        n = len(t.split())
        if n < MIN_PALABRAS:
            fail(f"T8 pregunta {i}", f"{n} palabras: está vacía o incompleta")
        else:
            ok(f"T8 pregunta {i}", f"{n} palabras")

    sec = re.search(r"## 3\..*?(?=\n## |\Z)", bit, re.S)
    sec = sec.group(0) if sec else ""
    prog = re.search(r"```\w*\s*\n(.*?)```", sec, re.S)
    # Tras el rotulo "Resultado" puede venir texto suelto (el wc -l, por ejemplo)
    # antes del bloque de codigo. Lo que importa es que el bloque este.
    res = re.search(r"\*\*Resultado(?:(?!```)[\s\S])*?```\w*\s*\n(.*?)```", sec, re.S)
    # Sin \s* antes del grupo: si la línea queda vacía, no se cuela el siguiente título.
    inter = re.search(r"Interpretaci[oó]n[^\n:]*:\**[ \t]*(.*(?:\n(?!\s*$|#).*)*)", sec)
    partes = [("programa", prog and len(prog.group(1).strip()) > 20),
              ("resultado", res and len(res.group(1).strip()) > 10),
              ("interpretación", inter and len(inter.group(1).strip()) > 15)]
    faltan = [p for p, v in partes if not v]
    if faltan:
        fail("T9 mini-reto", "falta: " + ", ".join(faltan))
    else:
        ok("T9 mini-reto", "programa, resultado e interpretación presentes")

    # --- T9b: el compose que escribiste tiene tres brokers
    comp = dir_ / "compose.yml"
    if comp.exists():
        c = comp.read_text(errors="replace")
        # Contamos los brokers sin depender del formato del YAML: partimos por
        # cada servicio (una linea con menos sangria que termina en ":") y dentro
        # de cada bloque buscamos su rol y su id, con o sin comillas.
        bloques = re.split(r"\n(?=\s{1,4}[A-Za-z0-9_.-]+:\s*(?:#.*)?$)", c, flags=re.M)
        ids = []
        for bloque in bloques:
            if not re.search(r"KAFKA_PROCESS_ROLES[:=]\s*[\"']?broker[\"']?\s*(?:#.*)?$",
                             bloque, re.M):
                continue
            m_id = re.search(r"KAFKA_NODE_ID[:=]\s*[\"']?(\d+)", bloque)
            if m_id:
                ids.append(m_id.group(1))

        # Paso 5 y Paso 11: la interfaz web y Connect los agrega el estudiante.
        servicios_kafka = len(re.findall(r"image:\s*apache/kafka", c))
        imagenes = [i.strip("\"'") for i in re.findall(r"image:\s*(\S+)", c)]
        otras_imagenes = [i for i in imagenes if not i.startswith("apache/kafka")]
        if otras_imagenes:
            ok("T9c interfaz", f"agregaste {otras_imagenes[0]}")
        else:
            fail("T9c interfaz", "no encuentro ninguna interfaz web en tu compose (Paso 5)")
        if re.search(r"connect-(standalone|distributed)\.sh|^\s*connect:", c, re.M):
            ok("T9d connect", "Kafka Connect configurado")
        else:
            fail("T9d connect", "no encuentro Kafka Connect en tu compose (Paso 11.2)")
        con_latest = [i for i in imagenes if ":" not in i or i.endswith(":latest")]
        if con_latest:
            warn("T9e versiones", "imágenes sin versión fija: " + ", ".join(con_latest))

        if len(ids) < 3:
            fail("T9b compose", f"solo encuentro {len(ids)} brokers y el taller pide 3 (Paso 6)")
        elif len(set(ids)) != len(ids):
            fail("T9b compose", f"hay brokers con el mismo KAFKA_NODE_ID: {ids}")
        else:
            ok("T9b compose", f"{len(ids)} brokers con ids {sorted(ids, key=int)}")

    # --- T11: las imágenes que enlaza la bitácora existen de verdad
    # Los comentarios HTML no cuentan: pueden traer ejemplos de enlaces.
    # Tampoco las líneas de ayuda con ">" que el estudiante olvidó borrar.
    sin_comentarios = re.sub(r"<!--.*?-->", "", bit, flags=re.S)
    sin_comentarios = "\n".join(l for l in sin_comentarios.splitlines()
                                 if not l.lstrip().startswith(">"))
    enlaces = re.findall(r"!\[[^\]]*\]\(([^)]+)\)", sin_comentarios)
    rotos = [e for e in enlaces if not (dir_ / e).exists()]
    if enlaces and rotos:
        fail("T11 enlaces", "la bitácora enlaza imágenes que no están: " + ", ".join(rotos[:3]))
    elif enlaces:
        ok("T11 enlaces", f"las {len(enlaces)} imágenes enlazadas existen")

    # --- T12: respuestas distintas entre sí (no la misma pegada cinco veces)
    respuestas = []
    for i in range(1, PREGUNTAS + 1):
        mm = re.search(rf"\*\*{i}\.\s.*?\*\*(.*?)(?=\n\*\*{i+1}\.\s|\n## |\Z)", bit, re.S)
        respuestas.append(" ".join((mm.group(1) if mm else "").split()).lower())
    llenas = [r for r in respuestas if r]
    repetidas = len(llenas) - len(set(llenas))
    if repetidas:
        fail("T12 respuestas", f"{repetidas + 1} de tus respuestas son iguales entre sí")
    elif llenas:
        ok("T12 respuestas", f"las {len(llenas)} son distintas")

    # --- T13: tu carné aparece en la bitácora
    if m:
        carne = m.group(2)
        if carne in bit:
            ok("T13 carné", f"{carne} aparece en la bitácora")
        else:
            warn("T13 carné", f"tu carpeta dice {carne} pero ese carné no aparece en la bitácora")

    # --- T14: el programa del mini-reto compila
    mini = dir_ / "mini-reto.py"
    if mini.exists():
        try:
            compile(mini.read_text(errors="replace"), "mini-reto.py", "exec")
            ok("T14 mini-reto.py", "es Python válido")
        except SyntaxError as e:
            fail("T14 mini-reto.py", f"tiene un error de sintaxis en la línea {e.lineno}")

    # --- T10: usaste el topic de tu grupo
    extra = (dir_ / "mini-reto.py").read_text(errors="replace") if (dir_ / "mini-reto.py").exists() else ""
    grupos = sorted(set(re.findall(r"ventas[-_](G[1-4])", bit + extra)))
    if grupo and grupos and grupos != [grupo]:
        warn("T10 topic", f"mencionas {grupos} y tu grupo es {grupo}")
    elif grupos:
        ok("T10 topic", f"usas ventas-{grupos[0]}")

    return terminar(carpeta)


def base_local():
    """Contra qué comparar en la máquina del estudiante: el repo del curso si
    lo agregó como upstream (su fork puede ir atrasado), si no su origin."""
    return "upstream/main" if git("rev-parse", "--verify", "-q", "upstream/main").strip() else "origin/main"


def detectar():
    """Si no me dan la carpeta, la deduzco de los archivos que cambió el PR."""
    base = os.environ.get("BASE_REF") or base_local()
    for linea in git("diff", "--name-only", f"{base}...HEAD").splitlines():
        m = re.match(r"(entregas/G[1-4]/[^/]+)/", linea)
        if m:
            return m.group(1)
    return None


def terminar(carpeta="?"):
    fails = sum(1 for e, _, _ in resultados if e == "❌")
    warns = sum(1 for e, _, _ in resultados if e == "⚠️")
    lineas = [f"## Validación de `{carpeta}`", "",
              f"**{len(resultados) - fails - warns} ✅ · {warns} ⚠️ · {fails} ❌**", "",
              "| | Verificación | Detalle |", "|---|---|---|"]
    lineas += [f"| {e} | {n} | {d} |" for e, n, d in resultados]
    lineas += ["", "Los ⚠️ no bloquean. Los ❌ conviene corregirlos, pero **corregir después de la "
                   "hora de entrega hace tardía toda la entrega**: revisa esto antes de subir, con "
                   "`python3 scripts/validar_entrega.py entregas/G#/<tu-carpeta>`. "
                   "El detalle de la entrega está en "
                   "[ENTREGA.md](https://github.com/jcarriolaa/Big-Data-Workshops-Kafka/blob/main/ENTREGA.md)."]
    salida = "\n".join(lineas)
    print(salida)
    for var in ("GITHUB_STEP_SUMMARY",):
        ruta = os.environ.get(var)
        if ruta:
            with open(ruta, "a") as f:
                f.write(salida + "\n")
    archivo = os.environ.get("SALIDA_MD")
    if archivo:
        Path(archivo).write_text(salida + "\n")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
