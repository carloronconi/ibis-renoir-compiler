from ibis import _
import ibis
from ibis.backends.risingwave import Backend as RisingwaveBackend
from .backend_connector import BackendConnector
from ibis import Table
from typing import Callable


class RisingwaveConnector(BackendConnector):
    def __init__(self, source_topic, sink_topic):
        self.con: RisingwaveBackend = RisingwaveBackend().connect(
                    user="root",
                    host="localhost",
                    port=4566,
                    database="dev")
        self.source_topic = source_topic
        self.sink_topic = sink_topic

    def create_table(self, schema):
        self.table = self.con.create_source(
            name=self.source_topic,
            schema=schema,
            connector_properties={"connector": "kafka",
                                  "topic": self.source_topic,
                                  "properties.bootstrap.server": "localhost:9092",
                                  "scan.startup.mode": "earliest",
                                  "scan.startup.timestamp.millis": "140000000"},
            data_format="PLAIN",
            encode_format="JSON"
        )
        
    def create_view(self, test_query: Callable[[Table], Table]):
        # this would fail because tables in risingwave are lowercase only and their name is turned
        # to all lowercase, and when the method returns it looks
        # for the table with the name of the source topic, that has uppercase letters, and doesn't find it
        # it's a bug in the ibis-risingwave backend!
        self.view = self.con.create_materialized_view(
            self.source_topic + "_view", 
            obj=test_query(self.table), 
            overwrite=True)
        
    def await_stream_query(self):
        # doesn't actually await as in risingwave it runs in the background
        self.con.create_sink(self.sink_topic,
                        sink_from=self.source_topic + "_view",
                        connector_properties={"connector": "kafka",
                                              "topic": self.sink_topic,
                                              "properties.bootstrap.server": "localhost:9092"},
                        data_format="PLAIN",
                        encode_format="JSON",
                        encode_properties={"force_append_only": "true"})
