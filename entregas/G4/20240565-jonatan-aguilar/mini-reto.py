#!/usr/bin/env python3
"""Mini-reto Grupo G4: Analisis de ventas por canal.

Pregunta asignada:
Tratando cada evento como una venta, ¿cual es el monto promedio
(cantidad x precio_unitario) por canal, que canal tiene el mas alto
y cuantas ventas tuvo cada canal?
"""
import json
from collections import defaultdict
from pathlib import Path


def main():
    # Ruta del archivo de eventos consumidos
    archivo = Path("salida/ventas-G4.jsonl")
    if not archivo.exists():
        archivo = Path("../../salida/ventas-G4.jsonl")

    ventas_por_canal = defaultdict(int)
    monto_por_canal = defaultdict(float)

    total_eventos = 0
    with open(archivo, "r", encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue
            evento = json.loads(linea)
            canal = evento["canal"]
            monto = evento["cantidad"] * evento["precio_unitario"]

            ventas_por_canal[canal] += 1
            monto_por_canal[canal] += monto
            total_eventos += 1

    print(f"Total de ventas procesadas: {total_eventos}")
    print("\nResultados por canal:")
    print(f"{'Canal':<10} | {'Ventas':<8} | {'Monto Total ($)':<16} | {'Monto Promedio ($)':<18}")
    print("-" * 60)

    canal_mayor_promedio = None
    max_promedio = -1.0

    for canal in sorted(ventas_por_canal.keys()):
        ventas = ventas_por_canal[canal]
        total = monto_por_canal[canal]
        promedio = total / ventas
        print(f"{canal:<10} | {ventas:<8d} | {total:<16.2f} | {promedio:<18.2f}")

        if promedio > max_promedio:
            max_promedio = promedio
            canal_mayor_promedio = canal

    print("-" * 60)
    print(f"Canal con el monto promedio mas alto: {canal_mayor_promedio} (${max_promedio:.2f})")


if __name__ == "__main__":
    main()
