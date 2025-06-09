FROM python:3.11-slim
RUN pip install -U pip
RUN pip install -U \
	pymongo==4.7.2 \
	jupyterlab==4.1.5 \
	tqdm==4.66.4
WORKDIR /notebooks
EXPOSE 8888
ENTRYPOINT ["jupyter"]
CMD ["lab", "--ip", "0.0.0.0", "--port", "8888", "--no-browser", "--allow-root"]
