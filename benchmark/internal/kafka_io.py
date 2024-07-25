import calendar
import random
import time
from datetime import datetime
from json import dumps, loads
from random import randint
from time import sleep
from kafka import KafkaProducer, errors, KafkaConsumer


class Producer:
    def __init__(self):
        print("Connecting to Kafka brokers")
        for _i in range(6):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=["localhost:9092"],
                    value_serializer=lambda x: dumps(x).encode("utf-8"),
                )
                print("Connected to Kafka")
                return
            except errors.NoBrokersAvailable:
                print("Waiting for brokers to become available")
                sleep(10)
        raise RuntimeError("Failed to connect to brokers within 60 seconds")
    
    def write_datum(self):
        order_id = calendar.timegm(time.gmtime())
        order_topic = "source"

        print("producing single datum for source topic")
        # produce payment info to payment topic
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        order_id += 1
        
        # produce order info to order topic
        order_data = {
            "createTime": ts,
            "orderId": order_id,
            "category": random.choice(
                [
                    "gas_transport",
                    "grocery_pos",
                    "home",
                    "shopping_pos",
                    "kids_pets",
                    "personal_care",
                    "health_fitness",
                    "travel",
                    "misc_pos",
                    "food_dining",
                    "entertainment",
                ]
            ),
            "merchantId": randint(0, 1000),
        }
        self.producer.send(order_topic, value=order_data)


class Consumer:
    def __init__(self):
        print("Connecting to Kafka brokers")
        for _i in range(6):
            try:
                self.consumer = KafkaConsumer(
                    "sink",
                    bootstrap_servers=["localhost:9092"],
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    value_deserializer=lambda x: loads(x.decode("utf-8")),
                )
                print("Connected to Kafka")
                return
            except errors.NoBrokersAvailable:
                print("Waiting for brokers to become available")
                sleep(10)
        raise RuntimeError("Failed to connect to brokers within 60 seconds")
    
    def read_datum(self):
        print("consuming single datum from sink topic")
        for message in self.consumer:
            message = message.value
            print(f"Consumed message: {message}")
            break


def main():
    producer = Producer()
    print("Staring producer of infinite data...")
    while True:
        producer.write_datum()
        sleep(1)


if __name__ == "__main__":
    main()
