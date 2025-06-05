import redis
import random
import time

# Conectar a Redis
redis_client = redis.Redis(host='redis', port=6379, db=0)

# Nombre de la cola
QUEUE_NAME = 'number_queue'

def send_random_number():
    while True:
        # Generar número aleatorio entre 1 y 100
        # Enviar a la cola de Redis
        
        # TODO: agregar código para enviar números aleatorios a la cola de Redis

        # Esperar 3 segundos
        time.sleep(3)

if __name__ == "__main__":
    print("Iniciando el emisor...")
    try:
        send_random_number()
    except KeyboardInterrupt:
        print("\nEmisor detenido por el usuario")
