"""Mini-reto G3: que tienda vendio mas en dinero, y cuanto. Total por tienda.

Lee salida/ventas-G3.jsonl (un evento JSON por linea, 2000 en total) y suma
cantidad x precio_unitario por tienda.
"""
import json
from collections import defaultdict

total_por_tienda = defaultdict(float)
ventas_por_tienda = defaultdict(int)
eventos = 0

with open("salida/ventas-G3.jsonl", encoding="utf-8") as f:
    for linea in f:
        evento = json.loads(linea)
        monto = evento["cantidad"] * evento["precio_unitario"]
        total_por_tienda[evento["tienda"]] += monto
        ventas_por_tienda[evento["tienda"]] += 1
        eventos += 1

print(f"Eventos leidos: {eventos}\n")
print(f"{'Tienda':22s} {'Ventas':>7s} {'Total':>14s}")
ranking = sorted(total_por_tienda.items(), key=lambda t: t[1], reverse=True)
for tienda, total in ranking:
    print(f"{tienda:22s} {ventas_por_tienda[tienda]:7d} {total:14,.2f}")

ganadora, monto = ranking[0]
print(f"\nLa tienda que mas vendio en dinero es {ganadora}, con {monto:,.2f}.")
