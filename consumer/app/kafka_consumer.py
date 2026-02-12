import json
from confluent_kafka import Consumer
                                
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe(['users.registered'])


def read_from_kafka():
    try:
        while True:
            msg = consumer.poll(1.0) # Wait 1s for a message
            if msg is None: continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            data = json.loads(msg.value())
            print(f"Received: {len(data)}")
            # yield data
    finally:
        consumer.close()


read_from_kafka()

