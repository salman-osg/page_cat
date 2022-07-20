FROM python:3.7.9
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY kafka_consumer.py .
CMD [ "python", "./kafka_consumer.py"]