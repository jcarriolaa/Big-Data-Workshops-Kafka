import json
from collections import Counter

unidades = Counter()
with open("salida/ventas-G2.jsonl", encoding="utf-8") as archivo:
    for linea in archivo:
        evento = json.loads(linea)
        unidades[evento["producto"]] += evento["cantidad"]

for producto, total in unidades.most_common(5):
    print(producto, total)
