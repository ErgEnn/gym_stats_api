FROM python:3.7-alpine
RUN apk add --update gcc musl-dev libffi-dev openssl-dev
COPY requirements.txt /app/
COPY run_server.py /app/
RUN pip install -r /app/requirements.txt
ENTRYPOINT python /app/run_server.py
