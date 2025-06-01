#!/usr/bin/env python
import sys

# CSV: timestamp, greenhouse_id, sensor_id, temperature_C, humidity_%, co2_ppm

for line in sys.stdin:
    # Saltamos la cabecera del archivo o lineas vacias
    if line.startswith("timestamp") or not line.strip():
        continue
    # Dividimos la linea por comas para obtener cada campo
    parts = line.strip().split(",")

    # Validamos que tenga al menos 4 columnas: greenhouse_id y temperatura son esenciales
    if len(parts) < 4:
        continue

    # Asignamos los campos relevantes
    ts = parts[0]                 # Primer campo: timestamp
    greenhouse_id = parts[1]      # Segundo campo: ID del invernadero
    temperature = parts[3]        # Cuarto campo: temperatura en grados Celsius

    # Emitimos en formato tabulado: clave = greenhouse_id, valor = temper_
    day = ts.split("T")[0]
    print "%s_%s\t%s" % (greenhouse_id, day, temperature)