import redis
import time

# Conectar a Redis
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

# Nombre de la cola
QUEUE_NAME = 'number_queue'

# Nombres de los contadores
CONTADOR_PAR = 'cant_par'
CONTADOR_IMPAR = 'cant_impar'
# Nombre del hash para contar ocurrencias de cada número
HASH_NUMEROS = 'conteo_numeros'

def process_numbers():
    while True:
        # TODO: agregar código para procesar los números de la cola de Redis
        print("No hay números en la cola")

if __name__ == "__main__":
    print("Iniciando el consumidor...")
    try:
        process_numbers()
    except KeyboardInterrupt:
        print("\nConsumidor detenido por el usuario")
