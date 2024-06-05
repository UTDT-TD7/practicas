mkdir minio_data

docker run \
   -p 9000:9000 \
   -p 9090:9090 \
   --user $(id -u):$(id -g) \
   --name minio \
   -e "MINIO_ROOT_USER=catedra" \
   -e "MINIO_ROOT_PASSWORD=catedrapass" \
   -v ./minio_data:/data \
   quay.io/minio/minio server /data --console-address ":9090"
