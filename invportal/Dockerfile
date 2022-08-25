FROM python:3.8.5-slim

WORKDIR /usr/src/app

COPY requirements.txt .

COPY entrypoint.sh .
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN chmod u+x /entrypoint.sh

COPY . .

ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
