import calendar
import random
import time
from datetime import datetime
from json import dumps, loads
from random import randint
from time import sleep
from kafka import KafkaProducer, errors, KafkaConsumer, TopicPartition


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

    def write_data(self, items=1000000):
        print(f"producing {items} items for source topic")
        for _ in range(items):
            self.write_datum()
    
    def write_datum(self):
        order_id = calendar.timegm(time.gmtime())
        order_topic = "source"

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
        self.read_timestamp = None
        print("Connecting to Kafka brokers")
        for _i in range(6):
            try:
                tp = TopicPartition("sink", 0)
                self.consumer = KafkaConsumer(
                    # "sink",
                    group_id="bench_consumer",
                    bootstrap_servers=["localhost:9092"],
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    value_deserializer=lambda x: x.decode("utf-8"),
                )
                self.consumer.assign([tp])
                print("Connected to Kafka")
                self.read_old_data()
                print("Read old data")
                return
            except errors.NoBrokersAvailable:
                print("Waiting for brokers to become available")
                sleep(10)
        raise RuntimeError("Failed to connect to brokers within 60 seconds")

    def read_old_data(self):
        self.consumer.seek_to_end()
        self.consumer.commit()

    def read_data(self, stoppable, poll_interval=10):
        print("polling sink topic every {poll_interval} seconds, returning at first empty poll")
        self.read_timestamp = time.perf_counter()
        data = self.consumer.poll(timeout_ms=poll_interval * 1000)
        self.consumer.commit()
        if not data:
            stoppable.do_stop = True
            raise Exception("No data in sink topic: either there's a failure or the query filters out everything")
        while data:
            self.read_timestamp = time.perf_counter()
            data = self.consumer.poll(timeout_ms=poll_interval * 1000)
        stoppable.do_stop = True


def main():
    producer = Producer()
    print("Staring producer of infinite data...")
    while True:
        producer.write_data()
        sleep(1)


if __name__ == "__main__":
    main()
