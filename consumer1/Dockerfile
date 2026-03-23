FROM python:3.11-slim

WORKDIR /app

COPY consumer1/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY consumer1/consumer.py /app/consumer.py

CMD ["python", "/app/consumer.py"]
