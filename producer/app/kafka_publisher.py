import os, json, asyncio
from fastapi import FastAPI
from confluent_kafka import Producer
from mongo_connection import collection
# from models import UserRegisterModel
import time



KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "users.registered")


producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,})



async def send_to_kafka():

    orders_customers = []
    for document in collection.find({},{'_id': 0}):
        orders_customers.append(document)
        
    batch_size = 50

    for i in range(0, len(orders_customers), batch_size):
        batch = orders_customers[i:i + batch_size]

        for document in batch:
            payload = json.dumps(document).encode("utf-8") # converting to str
            producer.produce(KAFKA_TOPIC, payload) # sending to kafka
            producer.flush()
            # time.sleep(0.5)
    print('done!')


asyncio.run(send_to_kafka())


# docker compose logs -f kafka
