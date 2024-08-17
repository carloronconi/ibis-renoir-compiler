from ibis import _
from ibis.backends.risingwave import Backend as RisingwaveBackend
from .backend_connector import BackendConnector
from ibis import Table, Schema
from typing import Callable


class RisingwaveConnector(BackendConnector):
    def __init__(self, source_topic_schemas: dict[str, Schema], sink_topic: str) -> None:
        self.con: RisingwaveBackend = RisingwaveBackend().connect(
                    user="root",
                    host="localhost",
                    port=4566,
                    database="dev")
        self.source_topic_schemas = source_topic_schemas
        self.sink_topic = sink_topic
        self.tables = []

    def create_tables(self):
        for topic, schema in self.source_topic_schemas.items():
            self.tables.append(self.con.create_source(
                name=topic,
                schema=schema,
                connector_properties={"connector": "kafka",
                                      "topic": topic,
                                      "properties.bootstrap.server": "localhost:9092",
                                      "scan.startup.mode": "earliest",
                                      "scan.startup.timestamp.millis": "140000000"},
                data_format="PLAIN",
                encode_format="JSON"))
        
    def create_view(self, test_query: Callable[[list[Table]], Table]):
        # this would fail because tables in risingwave are lowercase only and their name is turned
        # to all lowercase, and when the method returns it looks
        # for the table with the name of the source topic, that has uppercase letters, and doesn't find it
        # it's a bug in the ibis-risingwave backend!
        self.view = self.con.create_materialized_view(
            self.sink_topic + "_view", 
            obj=test_query(self.tables), 
            overwrite=True)
        
    def await_stream_query(self):
        # doesn't actually await as in risingwave it runs in the background
        self.con.create_sink(self.sink_topic,
                        sink_from=self.sink_topic + "_view",
                        connector_properties={"connector": "kafka",
                                              "topic": self.sink_topic,
                                              "properties.bootstrap.server": "localhost:9092"},
                        data_format="PLAIN",
                        encode_format="JSON",
                        encode_properties={"force_append_only": "true"})
