import os
import shutil
from pyspark.sql import SparkSession
from ibis import _
import ibis
import ibis.backends.pyspark
from .backend_connector import BackendConnector
from ..backend_benchmark import SparkBenchmark
from ibis import Table, Schema
from typing import Callable
import inspect

class SparkConnector(BackendConnector):
    def __init__(self, source_topic_schemas: dict[str, Schema], sink_topic: str) -> None:
        self.con: ibis.backends.pyspark.Backend = SparkBenchmark.get_backend_con()
        self.source_topic_schemas = source_topic_schemas
        self.sink_topic = sink_topic
        self.tables = []
        self.query_contains_aggregation = False

    def create_tables(self):
        for topic, schema in self.source_topic_schemas.items():
            self.tables.append(self.con.read_kafka(
                table_name=topic,
                auto_parse=True,
                schema=schema,
                # watermark is required by nexmark q6 but it still doesn't work when using it
                # watermark=ibis.watermark("date_time", ibis.interval(seconds=10)),
                options={
                    "kafka.bootstrap.servers": "localhost:9092",
                    "subscribe": topic,
                    "startingOffsets": "earliest",
                    "failOnDataLoss": "false"}))
        
    def create_view(self, test_query: Callable[[list[Table]], Table]):
        source = inspect.getsource(test_query)
        if ".aggregate(" in source:
            self.query_contains_aggregation = True
        self.view = self.con.create_view(
            self.sink_topic + "_view",
            test_query(self.tables))
        
        
    def await_stream_query(self):
        shutil.rmtree("spark_checkpoint", ignore_errors=True)
        os.makedirs("spark_checkpoint")
        if self.query_contains_aggregation:
            # in this case, wee need to set the output mode to complete
            # as it's not supported by ibis, we need this workaround to access the spark dataframe
            sql_query = self.view.compile()
            spark_df = self.con.raw_sql(sql_query)
            stream_query = (spark_df 
                .selectExpr('to_json(struct(*)) as value')
                .writeStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", self.sink_topic)
                .option("checkpointLocation", "spark_checkpoint")
                .outputMode("complete")
                .start())
        else:
            stream_query = self.con.to_kafka(
                self.view, 
                auto_format=True,
                options={"kafka.bootstrap.servers": "localhost:9092", 
                         "topic": self.sink_topic,
                         "checkpointLocation": "spark_checkpoint"}).start()
        print("Starting and awaiting stream query")
        stream_query.awaitTermination()
        print("Stream query terminated")
