# Repositorio de prácticas

¡Hola! En este repositorio vas a encontrar el código que usamos para alguna de las prácticas de la materia TD7 - Ingeniería de Datos.

## Guía de prácticas

| Práctica                               | Carpeta    | Detalle                                                                                                                 |
|----------------------------------------|------------|-------------------------------------------------------------------------------------------------------------------------|
| (1) Despliegue de servidor dockerizado | practica_1 | Incluye un docker compose para levantar un postgres, crear las tablas de datos del simulador y cargar datos desde CSVs. |

## Simulador

A continuación enumeramos los pasos para levantar el simulador de eventos, que generará los datos:


1. Ir a la carpeta simulador y ejecutar `docker-compose up`.
2. Entrar a [Kafka UI](http://localhost:8080) y verificar que se cargan los mensajes en los tópicos.
3. Hacer un dump de datos a un archivo jsonline con:
```docker container exec -it simulador_kafka_1 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic listen_events --from-beginning --timeout-ms 10000 > listen_events.jsonl```
4. Repetir 3) para page_view_events, status_change_events, auth_events


kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic listen_events --from-beginning --timeout-ms 2000 > /tmp/stuff.jsonl