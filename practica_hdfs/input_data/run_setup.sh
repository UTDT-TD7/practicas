#!/bin/bash

# Crea el directorio de input si no existe
hdfs dfs -mkdir -p /sensores/input

# Sube el archivo CSV a HDFS con blocksize de 2MB
hdfs dfs -Ddfs.blocksize=2m -put -f /shared_input/datos_invernaderos.csv /sensores/input
hdfs dfs -Ddfs.blocksize=2m -put -f /shared_input/movimientos_empleados.csv /sensores/input

# Borra el output anterior si existe
hdfs dfs -rm -r -skipTrash /sensores/output/temp_por_sensor
hdfs dfs -rm -r -skipTrash /sensores/output/ingresos_empleados

# Da permisos de ejecución a los scripts
chmod +x /shared_input/mapper.py
chmod +x /shared_input/reducer.py

chmod +x /shared_input/mapper_empleados.py
chmod +x /shared_input/reducer_empleados.py

echo "✅ Listo para ejecutar el trabajo con Hadoop Streaming."

