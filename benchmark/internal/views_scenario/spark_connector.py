from pyspark.sql import SparkSession
from ibis import _
import ibis
import ibis.backends.pyspark
from .backend_connector import BackendConnector
from ibis import Table
from typing import Callable


class SparkConnector(BackendConnector):
    def __init__(self, source_topic, sink_topic):
        scala_version = '2.12'
        spark_version = '3.1.2'
        # ensure match above values match the correct versions in pip
        packages = [
            f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
            'org.apache.kafka:kafka-clients:3.2.1'
        ]
        session = SparkSession.builder\
            .master("spark://local:7077")\
            .appName("ibis")\
            .config("spark.jars.packages", ",".join(packages))\
            .getOrCreate()
        self.con: ibis.backends.pyspark.Backend = ibis.pyspark.connect(session, mode="streaming")
        self.source_topic = source_topic
        self.sink_topic = sink_topic

    def create_table(self, schema):
        self.table: ibis.Table = self.con.read_kafka(
            table_name=self.source_topic,
            auto_parse=True,
            schema=schema,
            options={
                "kafka.bootstrap.servers": "localhost:9092",
                "subscribe": self.source_topic,
                "startingOffsets": "earliest",
                "failOnDataLoss": "false"})
        
    def create_view(self, test_query: Callable[[Table], Table]):
        self.view = self.con.create_view(
            self.source_topic + "_view",
            test_query(self.table))
        
    def await_stream_query(self):
        stream_query = self.con.to_kafka(
            self.view, 
            options={"kafka.bootstrap.servers": "localhost:9092", 
                     "topic": self.sink_topic,
                     "checkpointLocation": "/tmp/spark_checkpoint"}).start()
        print("Starting and awaiting stream query")
        stream_query.awaitTermination()
        print("Stream query terminated")
