# https://developer.confluent.io/get-started/python/#build-producer

from random import choice, randint
from confluent_kafka import Producer
from json import dumps


class Prod:
    def __init__(self, dict_generator):
        config = {
            # User-specific properties that you must set
            'bootstrap.servers': 'localhost:9092',
            # Fixed properties
            'acks': 'all'
        }
        self.producer = Producer(config)
        self.deliver_count = 0 
        self.dict_generator = dict_generator
    
    def produce(self, topic, amount=10, timeout_ms=10000):
        self.deliver_count = 0
        def delivery_callback(err, msg):
            if err:
                print('ERROR: Message failed delivery: {}'.format(err))
            else:
                self.deliver_count += 1 
        
        for i in range(amount):
            value = next(self.dict_generator)
            value = dumps(value).encode('utf-8')
            self.producer.produce(topic, value=value, key=f"ID_{i}", callback=delivery_callback)
        # Block until the messages are sent or the timeout expires
        self.producer.poll(timeout_ms)
        self.producer.flush()
        if self.deliver_count == amount:
            print(f"Confirmed delivered all {amount} events to topic {topic}")
        else:    
            print(f"Failed to deliver {amount - self.deliver_count} out of {amount} events to topic {topic}")


if __name__ == '__main__':
    producer = Prod()
    producer.produce("purchases")
