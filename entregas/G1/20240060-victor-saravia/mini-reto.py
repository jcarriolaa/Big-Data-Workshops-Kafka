from collections import defaultdict
from decimal import Decimal
import json
from pathlib import Path

ruta = Path("salida/ventas-G1.jsonl")
totales = defaultdict(Decimal)
conteo = 0

with ruta.open(encoding="utf-8") as archivo:
    for linea in archivo:
        venta = json.loads(linea)
        monto = Decimal(str(venta["cantidad"])) * Decimal(str(venta["precio_unitario"]))
        totales[venta["pais"]] += monto
        conteo += 1

if conteo != 2000:
    raise SystemExit(f"Se esperaban 2000 eventos; encontré {conteo}")

for pais, total in sorted(totales.items(), key=lambda par: par[1], reverse=True):
    print(f"{pais}: Q{total:,.2f}")
