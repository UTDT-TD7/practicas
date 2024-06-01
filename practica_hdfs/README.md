
# Práctica de HDFS 


```
docker compose up -d 
```

* URL Namenode: http://localhost:9870/ 
* URL Resource manager: http://localhost:8088/


Job de ejemplo:
```
docker exec -it docker-3_namenode_1 /bin/bash 
yarn jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar pi 10 15
```