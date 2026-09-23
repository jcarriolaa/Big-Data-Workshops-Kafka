#!/usr/bin/env bash
# Revisa que tu entorno este como debe estar antes de seguir.
#
#   bash scripts/check.sh prereq    # antes de la clase: Docker, Python, imagenes
#   bash scripts/check.sh broker    # despues del Paso 2: un broker con tu topic tiendas-G#
#   bash scripts/check.sh cluster   # despues del Paso 7: tres brokers y tu topic replicado
#   bash scripts/check.sh stream    # despues del Paso 8: tu grupo de lectura funcionando
set -uo pipefail

# En Windows (Git Bash) las rutas que empiezan con / se convierten en rutas de
# Windows antes de llegar a Docker. Esto lo evita para todo el script.
export MSYS_NO_PATHCONV=1

FALLOS=0
ok()   { echo "✅ $1"; }
warn() { echo "⚠️  $1"; }
fail() { echo "❌ $1"; FALLOS=$((FALLOS + 1)); }

K="/opt/kafka/bin"

corriendo() { docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null | grep -q true; }
kcmd()      { docker exec kafka1 $K/"$@" 2>/dev/null; }

case "${1:-prereq}" in

  prereq)
    docker compose version >/dev/null 2>&1 \
      && ok "docker compose v2 disponible" \
      || fail "no encuentro 'docker compose'. Instala Docker Desktop y abrelo."

    docker info >/dev/null 2>&1 \
      && ok "Docker esta corriendo" \
      || fail "Docker no responde. Abre Docker Desktop y espera a que diga 'running'."

    MEM=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)
    [ -z "$MEM" ] && MEM=0
    if [ "$MEM" -ge 6000000000 ]; then
      ok "Docker tiene $((MEM / 1000000000)) GB de RAM"
    else
      fail "Docker tiene menos de 6 GB. Subelo en Settings > Resources > Memory."
    fi

    docker image inspect "apache/kafka:4.3.1" >/dev/null 2>&1 \
      && ok "imagen apache/kafka:4.3.1 descargada" \
      || fail "falta la imagen. Ejecuta: docker pull apache/kafka:4.3.1"

    # En Windows el instalador crea python.exe y py.exe, no python3.
    PY=""
    for cand in python3 python py; do
      command -v "$cand" >/dev/null 2>&1 || continue
      "$cand" -c "import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)" 2>/dev/null \
        && { PY="$cand"; break; }
    done
    if [ -n "$PY" ]; then
      ok "Python instalado como '$PY' ($($PY -V 2>&1 | cut -d' ' -f2))"
      "$PY" -c "import kafka" 2>/dev/null \
        && ok "la libreria kafka-python esta instalada" \
        || fail "falta kafka-python. Ejecuta: $PY -m pip install -r apps/requirements.txt"
    else
      fail "no encuentro Python 3.10 o mas nuevo. Instalalo desde python.org."
    fi
    ;;

  broker)
    corriendo controller && ok "Contenedor controller corriendo" || fail "controller apagado. Revisa: docker compose logs controller"
    corriendo kafka1     && ok "Contenedor kafka1 corriendo"     || fail "kafka1 apagado. Revisa: docker compose logs kafka1"

    kcmd kafka-topics.sh --bootstrap-server kafka1:19092 --list >/dev/null \
      && ok "kafka1 responde a kafka-topics" \
      || fail "kafka1 no responde. Espera 30 segundos mas y vuelve a intentar."

    T1=$(kcmd kafka-topics.sh --bootstrap-server kafka1:19092 --list | grep -m1 "^tiendas-G[1-4]$")
    [ -n "${T1:-}" ] \
      && ok "Existe tu topic $T1" \
      || fail "todavia no existe tu topic tiendas-G# (lo creas en el Paso 2)"
    ;;

  cluster)
    for c in controller kafka1 kafka2 kafka3; do
      corriendo "$c" && ok "Contenedor $c corriendo" || fail "$c apagado. Ejecuta: docker compose --profile cluster start $c"
    done

    N=$(kcmd kafka-broker-api-versions.sh --bootstrap-server kafka1:19092 | grep -c "id: ")
    if [ "${N:-0}" -eq 3 ]; then
      ok "Brokers registrados en el cluster: 3"
    else
      fail "Brokers registrados: ${N:-0}, deberian ser 3. Revisa: docker compose ps"
    fi

    TOPIC=$(kcmd kafka-topics.sh --bootstrap-server kafka1:19092 --list | grep -m1 "^ventas-G[1-4]$")
    if [ -n "${TOPIC:-}" ]; then
      ok "Existe tu topic de ventas: $TOPIC"
      DESC=$(kcmd kafka-topics.sh --bootstrap-server kafka1:19092 --describe --topic "$TOPIC")
      echo "$DESC" | grep -q "PartitionCount: 3" \
        && ok "Particiones del topic: 3" \
        || fail "el topic no tiene 3 particiones (Paso 7)."
      echo "$DESC" | grep -q "ReplicationFactor: 3" \
        && ok "Copias de cada particion: 3" \
        || fail "el topic no tiene replicacion 3 (Paso 7)."
      echo "$DESC" | grep -q "min.insync.replicas=2" \
        && ok "min.insync.replicas=2 configurado" \
        || fail "falta --config min.insync.replicas=2 en el topic (Paso 7)."
    else
      fail "no existe ningun topic ventas-G#. Crealo como dice el Paso 7."
    fi
    ;;

  stream)
    TOPIC=$(kcmd kafka-topics.sh --bootstrap-server kafka1:19092 --list | grep -m1 "^ventas-G[1-4]$")
    if [ -z "${TOPIC:-}" ]; then
      fail "no existe tu topic de ventas. Empieza por el Paso 7."
    else
      MSJS=$(kcmd kafka-get-offsets.sh --bootstrap-server kafka1:19092 --topic "$TOPIC" \
             | awk -F: '{s += $3} END {print s+0}')
      if [ "${MSJS:-0}" -gt 0 ]; then
        ok "Mensajes publicados en $TOPIC: $MSJS"
      else
        fail "$TOPIC esta vacio. Corre el productor (Paso 8)."
      fi
    fi

    GRUPOS=$(kcmd kafka-consumer-groups.sh --bootstrap-server kafka1:19092 --list | grep -c .)
    if [ "${GRUPOS:-0}" -gt 0 ]; then
      ok "Grupos de lectura registrados: $GRUPOS"
    else
      fail "no hay ningun grupo de lectura. Corre el consumidor (Paso 8)."
    fi
    ;;

  *)
    echo "Uso: bash scripts/check.sh [prereq|broker|cluster|stream]"
    exit 2
    ;;
esac

echo
if [ "$FALLOS" -eq 0 ]; then
  echo "Todo en orden."
else
  echo "$FALLOS problema(s). Mira la seccion '❌ Si falla' del paso correspondiente."
fi
exit "$FALLOS"
