from confluent_kafka import Consumer

class Cons:
    def __init__(self):
        config = {
            # User-specific properties that you must set
            'bootstrap.servers': 'localhost:9092',
            # Fixed properties
            'group.id':          'kafka-python-getting-started',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(config)
    
    def consume(self, topic, start_attempts=6, timeout=10):
        self.consumer.subscribe([topic])
        messages = []
        while True:
            msg = self.consumer.poll(timeout)
            if msg is None:
                print("No messages in consumer after {timeout} seconds at attempt {start_attempts}")
                if start_attempts == 0:
                    break
                start_attempts -= 1
                continue
            start_attempts = 0
            messages.append(msg.value().decode('utf-8'))
        self.consumer.close()
        return messages