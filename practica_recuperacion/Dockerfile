FROM python:3.11-slim
RUN pip install -U pip
RUN pip install -U \
	psycopg2-binary==2.9.9 \
	pandas==2.2.1 \
	jupyterlab==4.1.5 \
	kafka-python==2.0.2 \
	rich==13.7.1 \
	sqlalchemy==2.0.29
WORKDIR /notebooks
EXPOSE 8888
ENTRYPOINT ["jupyter"]
CMD ["lab", "--ip", "0.0.0.0", "--port", "8888", "--no-browser", "--allow-root"]
