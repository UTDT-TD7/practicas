#!/usr/bin/env python
import sys

current_key = None
total_temp = 0.0
count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        key, temp = line.split("\t")
        temp = float(temp)
    except:
        continue

    if current_key != key:
        if current_key is not None:
            avg = total_temp / count if count > 0 else 0.0
            print "%s\t%.2f" % (current_key, avg)
        current_key = key
        total_temp = temp
        count = 1
    else:
        total_temp += temp
        count += 1

# Emitir la ultima clave
if current_key is not None:
    avg = total_temp / count if count > 0 else 0.0
    print "%s\t%.2f" % (current_key, avg)
