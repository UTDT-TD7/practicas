El objetivo de esta clase es ver eventos desde el WAL mediante CDC (change data capture).

Para ello utilizamos Debezium, que lee la replicación lógica de Postgres y la vuelca en un tópico de Kafka.

Links utiles:
- http://localhost:18888 para correr los notebooks en paralelo
    - `Kafka.ipynb` para ver los mensajes a medida que llegan
    - `Changes.ipynb` para generar eventos
- http://localhost:9090/ui/clusters/local/brokers para ver los tópicos de Kafka
- http://localhost:18080 para ver la UI de Debezium (no hay mucho para ver de todos modos)

# Links:
- https://www.crunchydata.com/blog/postgresl-unlogged-tables
- https://www.iamninad.com/posts/docker-compose-for-your-next-debezium-and-postgres-project/
