#!/usr/bin/env python3
"""Mini-reto G1: que pais concentra mas ventas en dinero.

Lee salida/ventas-G1.jsonl (un evento JSON por linea) y suma
cantidad * precio_unitario por pais.
"""
import json

ARCHIVO = "salida/ventas-G1.jsonl"


def main():
    total_por_pais = {}
    ventas_por_pais = {}

    with open(ARCHIVO, encoding="utf-8") as f:
        for linea in f:
            evento = json.loads(linea)
            pais = evento["pais"]
            monto = evento["cantidad"] * evento["precio_unitario"]
            # setdefault evita tener que inicializar cada pais a mano
            total_por_pais[pais] = total_por_pais.get(pais, 0) + monto
            ventas_por_pais[pais] = ventas_por_pais.get(pais, 0) + 1

    # De mayor a menor, que es como lo pide el enunciado.
    orden = sorted(total_por_pais.items(), key=lambda par: par[1], reverse=True)

    total_general = sum(total_por_pais.values())
    eventos = sum(ventas_por_pais.values())

    print(f"Ventas en dinero por pais ({eventos} eventos)\n")
    print(f"{'Pais':<15} {'Total':>14} {'Ventas':>8} {'% del total':>12}")
    print("-" * 52)
    for pais, total in orden:
        pct = total / total_general * 100
        print(f"{pais:<15} {total:>14,.2f} {ventas_por_pais[pais]:>8} {pct:>11.1f}%")
    print("-" * 52)
    print(f"{'TOTAL':<15} {total_general:>14,.2f} {eventos:>8}")

    ganador, monto_ganador = orden[0]
    print(f"\nEl pais que mas vende en dinero es {ganador}, "
          f"con {monto_ganador:,.2f} de {total_general:,.2f} en total "
          f"({monto_ganador / total_general * 100:.1f}%).")


if __name__ == "__main__":
    main()
