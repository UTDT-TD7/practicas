FROM python:3.11-slim
RUN pip install -U pip
RUN pip install -U psycopg2-binary pandas jupyterlab pymysql cryptography sqlalchemy faker
WORKDIR /notebooks
EXPOSE 8888
ENTRYPOINT jupyter lab --ip 0.0.0.0 --port 8888 --no-browser --allow-root
