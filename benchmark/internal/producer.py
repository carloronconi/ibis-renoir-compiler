# https://developer.confluent.io/get-started/python/#build-producer

from random import choice, randint
from confluent_kafka import Producer
from json import dumps


class Prod:
    def __init__(self):
        config = {
            # User-specific properties that you must set
            'bootstrap.servers': 'localhost:9092',
            # Fixed properties
            'acks': 'all'
        }
        self.producer = Producer(config)
        self.deliver_count = 0 
    
    def produce(self, topic, amount=10, timeout_ms=10000, no_cb=False):
        def delivery_callback(err, msg):
            if err:
                print('ERROR: Message failed delivery: {}'.format(err))
            else:
                self.deliver_count += 1 
        cb = None if no_cb else delivery_callback
        
        products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']
        for i in range(amount):
            order_id = f"order_{i}"
            value = {
                "order_id": order_id,
                "product": choice(products),
                "quantity": randint(1, 100),
            }
            value = dumps(value).encode('utf-8')
            self.producer.produce(topic, value=value, key=order_id, callback=cb)
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
