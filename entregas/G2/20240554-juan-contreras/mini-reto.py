import json

unidades = {}

with open("salida/ventas-G2.jsonl", "r", encoding="utf-8") as archivo:
    for linea in archivo:
        evento = json.loads(linea)
        producto = evento["producto"]
        cantidad = evento["cantidad"]

        unidades[producto] = unidades.get(producto, 0) + cantidad

top_5 = sorted(unidades.items(), key=lambda x: x[1], reverse=True)[:5]

print("Top 5 productos por unidades vendidas:")
for posicion, (producto, total) in enumerate(top_5, start=1):
    print(f"{posicion}. {producto}: {total} unidades")