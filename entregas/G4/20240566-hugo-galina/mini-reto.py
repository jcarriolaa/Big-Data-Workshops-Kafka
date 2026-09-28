import json
from collections import defaultdict

total = defaultdict(float)   # suma de montos por canal
ventas = defaultdict(int)    # número de ventas por canal

for linea in open("salida/ventas-G4.jsonl", encoding="utf-8"):
    e = json.loads(linea)
    monto = e["cantidad"] * e["precio_unitario"]
    total[e["canal"]] += monto
    ventas[e["canal"]] += 1

promedios = {c: total[c] / ventas[c] for c in ventas}

for canal, prom in sorted(promedios.items(), key=lambda x: x[1], reverse=True):
    print(f"{canal:10} promedio {prom:,.2f}   ventas {ventas[canal]}")

print("Canal con el promedio más alto:", max(promedios, key=promedios.get))