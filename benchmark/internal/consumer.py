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
    
    def consume(self, topic, start_attempts=6, timeout=10, max_messages=None):
        print(f"Consuming messages from topic {topic}")
        self.consumer.subscribe([topic])
        messages = []
        while True:
            msg = self.consumer.poll(timeout)
            if msg is None:
                print(f"No messages in consumer after {timeout} seconds at attempt {start_attempts}")
                if start_attempts == 0:
                    break
                start_attempts -= 1
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                break
            start_attempts = 0
            messages.append(msg.value().decode('utf-8'))
            if max_messages and len(messages) >= max_messages:
                # don't close in this case
                return messages
        self.consumer.close()
        return messages