
FROM python:3.9-slim-buster

RUN apt-get update && apt-get install -y python3-dev libpq-dev gcc
# pipx & poetry
ENV PATH="/root/.local/bin:$PATH"
ENV PATH="/root/.local/pipx/venvs/poetry/bin/:$PATH"


# Setup pipx and poetry
RUN pip install pipx
RUN pipx install poetry==1.4.2
RUN poetry config virtualenvs.create false

# Copy files
COPY pyproject.toml poetry.lock /gq/
WORKDIR /gq
RUN poetry install --no-interaction --no-root
COPY . /gq
RUN poetry install --no-interaction

CMD tail -f /dev/null 
