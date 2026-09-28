import json

totales = {}

with open("salida/ventas-G1.jsonl", "r") as archivo:
    for linea in archivo:
        venta = json.loads(linea)

        pais = venta["pais"]
        total_venta = venta["cantidad"] * venta["precio_unitario"]

        if pais not in totales:
            totales[pais] = 0

        totales[pais] += total_venta

ordenados = sorted(totales.items(), key=lambda x: x[1], reverse=True)

print("Total de ventas por pais:")
for pais, total in ordenados:
    print(f"{pais}: {total:.2f}")

print(f"\nPais con mayor monto de ventas: {ordenados[0][0]}")
print(f"Total: {ordenados[0][1]:.2f}")
