import ibis
import pandas as pd
from ibis import _
from benchmark.internal.producer import Prod
from benchmark.internal.consumer import Cons
from ibis.backends.risingwave import Backend as RisingwaveBackend
import random
import string
import time


# changed to lowercase only because of risingwave!
def rand_string(prefix="", len=16):
    return prefix.lower() + "".join(random.choices(string.ascii_lowercase, k=len))


def create_stream_query(source_topic, sink_topic):
    con: RisingwaveBackend = RisingwaveBackend().connect(
                    user="root",
                    host="localhost",
                    port=4566,
                    database="dev")
    
    tab_schema = ibis.schema({
            "order_id": ibis.dtype("string"),
            "product": ibis.dtype("string"),
            "quantity": ibis.dtype("int64")})
    
    table = con.create_source(
        name=source_topic,
        schema=tab_schema,
        connector_properties={"connector": "kafka",
                              "topic": source_topic,
                              "properties.bootstrap.server": "localhost:9092",
                              "scan.startup.mode": "earliest",
                              "scan.startup.timestamp.millis": "140000000"},
        data_format="PLAIN",
        encode_format="JSON"
    )
    
    # this would fail because tables in risingwave are lowercase only and their name is turned
    # to all lowercase, and when the method returns it looks
    # for the table with the name of the source topic, that has uppercase letters, and doesn't find it
    # it's a bug in the ibis-risingwave backend!
    con.create_materialized_view(source_topic + "_view", 
                                 obj=table.mutate(price=_.quantity * 2)
                                          .mutate(value=_.order_id), 
                                 overwrite=True)
    
    con.list_tables()

    con.create_sink(sink_topic,
                    sink_from=source_topic + "_view",
                    connector_properties={"connector": "kafka",
                                          "topic": sink_topic,
                                          "properties.bootstrap.server": "localhost:9092"},
                    data_format="PLAIN",
                    encode_format="JSON",
                    encode_properties={"force_append_only": "true"})


def main():
    producer_topic = rand_string("prod_topic_")
    consumer_topic = rand_string("cons_topic_")

    producer = Prod()
    consumer = Cons()

   # consumer can't subscribe to non-existing topic, so produce single message to create it
    # and discard it from consumer
    producer.produce(consumer_topic, amount=1, no_cb=True)
    result = consumer.consume(consumer_topic, max_messages=1)
    print(f"Created consumer topic and consumed message {result}")
    producer.produce(producer_topic, amount=1, no_cb=True)
    print("Created producer topic without consuming messages")

    create_stream_query(producer_topic, consumer_topic)

    start_time = time.perf_counter()
    producer.produce(producer_topic)
    result = consumer.consume(consumer_topic)
    end_time = time.perf_counter()

    if result:
        print(f"Successfully consumed {result} messages in {end_time - start_time} seconds")  
    else:
        print("Failed to consume any messages")  
    
    # TODO: stream_query_proc.kill()

if __name__ == "__main__":
    main()
