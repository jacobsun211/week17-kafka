import json
from confluent_kafka import Consumer
from mysql_connection import insert_to_sql

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe(['users.registered'])


async def read_from_kafka():
    try:
        print('trying')
        while True:
            data = await consumer.poll() 
            if data is None: continue
            if data.error():
                print(f"Consumer error: {data.error()}")
                continue

            data = json.loads(data.value())
            print(f"Received: {len(data)}")
            yield insert_to_sql(data)
    except Exception as e:
        print('error')
        print(e)

    finally:
        print('d')
        consumer.close()



