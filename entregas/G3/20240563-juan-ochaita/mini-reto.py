import json
from collections import defaultdict

totales = defaultdict(float)

with open("salida/ventas-G3.jsonl", encoding="utf-8") as archivo:
    for linea in archivo:
        evento = json.loads(linea)
        totales[evento["tienda"]] += evento["cantidad"] * evento["precio_unitario"]

tienda, total = max(totales.items(), key=lambda elemento: elemento[1])
print(f"{tienda}: Q{total:.2f}")