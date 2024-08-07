import calendar
import random
import time
from datetime import datetime
from json import dumps, loads
from random import randint
from time import sleep
from kafka import KafkaProducer, errors, KafkaConsumer


class Producer:
    def __init__(self, topic: str):
        self.topic = topic
        self.order_id = 1
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

    def write_data(self, items=1000000):
        print(f"producing {items} items for {self.topic} topic")
        for _ in range(items):
            self.write_datum()
        print(f"finished producing data")
    
    def write_datum(self):
        # produce payment info to payment topic
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # produce order info to order topic
        order_data = {
            "createTime": ts,
            "orderId": self.order_id,
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
        self.order_id += 1
        self.producer.send(self.topic, value=order_data)


class Consumer:
    def __init__(self):
        self.read_timestamp = None
        self.did_read = False
        print("Connecting to Kafka brokers")
        for _i in range(6):
            try:
                self.consumer = KafkaConsumer(
                    "sink",
                    # assign group_id so next test run can resume from committed offset
                    group_id="bench_consumer",
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

    def read_data(self, stoppable, poll_interval=20):
        print(f"polling sink topic every {poll_interval} seconds, returning at first empty poll")
        self.read_timestamp = time.perf_counter()
        last_data = self.consumer.poll(timeout_ms=poll_interval * 1000)
        messages = []
        for v in last_data.values():
            messages.extend(v)
        while last_data:
            self.did_read = True
            self.read_timestamp = time.perf_counter()
            last_data = self.consumer.poll(timeout_ms=poll_interval * 1000)
            for v in last_data.values():
                messages.extend(v)
        print("Consumer stopped polling, all data read:")
        for message in messages:
            print(message)
        self.consumer.commit()
        stoppable.do_stop = True


def main():
    producer = Producer()
    print("Staring producer of infinite data...")
    while True:
        producer.write_data()
        sleep(1)


if __name__ == "__main__":
    main()
