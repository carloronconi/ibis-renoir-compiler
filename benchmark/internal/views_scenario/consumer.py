import time
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
    
    def consume(self, topic, start_attempts=6, timeout=10, max_messages=None, do_close=True, stream_pipe=None):
        print(f"Consuming messages from topic {topic}")
        self.consumer.subscribe([topic])
        messages = []
        last_recv_time = None
        exception = None
        while True:
            msg = self.consumer.poll(timeout)
            if msg is None:
                if start_attempts == 0:
                    break
                # when a stream pipe variable is given, 
                # early return when an exception is detected in subprocess
                if stream_pipe and stream_pipe.poll():
                    exception = stream_pipe.recv()
                    break
                print(f"No messages in consumer after {timeout} seconds at attempt {start_attempts}")
                start_attempts -= 1
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                break
            last_recv_time = time.perf_counter()
            start_attempts = 0
            value = msg.value()
            messages.append(value.decode('utf-8') if value else "")
            if max_messages and len(messages) >= max_messages:
                # commit so we won't re-read messages
                self.consumer.commit(asynchronous=False)
                break
        if do_close:
            self.consumer.close()
        return messages, last_recv_time, exception