from kafka_publisher import send_to_kafka
import asyncio


    # send_to_kafka()

asyncio.create_task(send_to_kafka())