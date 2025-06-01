#!/usr/bin/env python
import sys

# CSV: row_id,timestamp,employee_id,greenhouse_id,action

for line in sys.stdin:
    # Saltamos la cabecera del archivo o lineas vacias
    if line.startswith("row_id") or not line.strip():
        continue
    # Dividimos la linea por comas para obtener cada campo
    parts = line.strip().split(",")

    # Validamos que tenga al menos 5 columnas
    if len(parts) < 5:
        continue

    # TODO: obtener los datos relevantes del csv y armar la clave y el valor para el output
    # utilizar mapper.py como referencia. Deberia contar con una clave que agrupe empleados y fecha y el valor seria la accion