# Práctica Redis - Sistema de Conteo de Números

## Enunciado

Vamos a crear dos workers en Python. El primero va a mandar mensajes a una cola de Redis cada 3 segundos. El segundo va a leer esos mensajes.

El mensaje a enviar va a ser un número aleatorio entre 1 y 100.

Al leerlo, el consumidor va a incrementar un contador en Redis dependiendo si el valor recibido es par o impar.

Además, va a guardar en un HASH de Redis la cantidad de veces que aparece cada número.

**BONUS**: Modificar el contador para que se reinicie cada día automáticamente.

## Estructura del Proyecto

```
practica_redis/
├── app/
│   ├── worker_sender.py     # Emisor de números aleatorios
│   └── worker_consumer.py   # Consumidor y contador
├── docker-compose.yml       # Configuración de servicios
└── README.md               # Este archivo
```

## Estructura en Redis

- **Cola**: `number_queue` - Almacena los números generados
- **Contadores**:
  - `cant_par` - Contador de números pares
  - `cant_impar` - Contador de números impares
- **Hash**: `conteo_numeros` - Almacena la frecuencia de aparición de cada número


## Pasos para correr el ejercicio

### 1. Levantar los containers

```bash
docker compose up -d
```

### 2. Ver lo que tenemos en Redis

Accede a Redis Commander en:
```
http://localhost:8081
```

### 3. Correr los workers en dos consolas diferentes

**En la primera consola** (Consumidor):
```bash
docker exec -it practica_redis-python-1 bash
python worker_consumer.py
```

**En la segunda consola** (Emisor):
```bash
docker exec -it practica_redis-python-1 bash
python worker_sender.py
```

## Ejemplo de Salida

### Worker Sender:
```
Iniciando el emisor...
Número enviado: 42
Número enviado: 17
Número enviado: 89
```

### Worker Consumer:
```
Iniciando el consumidor...
Número 42 es par. Contador pares: 1
El número 42 ha aparecido 1 veces
Número 17 es impar. Contador impares: 1
El número 17 ha aparecido 1 veces
Número 89 es impar. Contador impares: 2
El número 89 ha aparecido 1 veces
```

## Servicios Incluidos

- **Redis**: Base de datos en memoria (puerto 6379)
- **Redis Commander**: Interfaz web para Redis (puerto 8081)
- **Python**: Contenedor con los scripts de trabajo

## Detener el Sistema

Para detener todos los servicios:
```bash
docker compose down
```

Para detener y eliminar los datos:
```bash
docker compose down -v
```
