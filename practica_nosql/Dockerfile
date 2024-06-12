FROM python:3.11-slim
RUN pip install -U pip
RUN pip install -U \
	psycopg2-binary==2.9.9 \
	redis==5.0.4 \
	pymongo==4.7.2 \
	pandas==2.2.1 \
	jupyterlab==4.1.5 \
	pgvector==0.2.5 \
	openai==1.30.3 \
	llama-index==0.10.39 \
	tqdm==4.66.4 \
	wikipedia==1.4.0 \
	sentence-transformers==3.0.1
WORKDIR /notebooks
EXPOSE 8888
ENTRYPOINT ["jupyter"]
CMD ["lab", "--ip", "0.0.0.0", "--port", "8888", "--no-browser", "--allow-root"]
