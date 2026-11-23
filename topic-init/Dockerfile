
FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir "confluent-kafka>=2.6,<3"

COPY init_topic.py .

CMD ["python", "init_topic.py"]
