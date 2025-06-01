#!/usr/bin/env python
import sys

current_key = None
in_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        # Nos llega una clave y un valor, el valor asumimos es IN o OUT dependiendo si es un ingreso o una salida  
        key, action = line.split("\t")
    except:
        continue

    # TODO: Implementar logica para contar cuantas de las acciones para ese empleado son del tipo IN

