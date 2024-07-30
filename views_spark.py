import ibis
import ibis.backends
import ibis.backends.pyspark
from pyspark.sql import SparkSession
from ibis import _
from ibis.expr.visualize import to_graph


# before running this, cd to ./benchmark/compose-kafka and do `docker-compose up`
# if there are any errors in the Dockerfile an you need to rebuild, clean the everything
# first with `docker system prune` (it prunes everything unused so careful on server) and 
# then `docker compose build --no-cache` and again `docker compose up`

scala_version = '2.12'
spark_version = '3.1.2'
# ensure match above values match the correct versions in pip
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]
session = SparkSession.builder\
   .master("local")\
   .appName("kafka-example")\
   .config("spark.jars.packages", ",".join(packages))\
   .getOrCreate()

con: ibis.backends.pyspark.Backend = ibis.pyspark.connect(session, mode="streaming")
tab_schema = ibis.schema({"createTime": ibis.dtype("string"),
                                           "orderId": ibis.dtype("int64"),
                                           "category": ibis.dtype("string"),
                                           "merchantId": ibis.dtype("int64")})

table: ibis.Table = con.read_kafka(
                       table_name="orders",
                       auto_parse=True,
                       schema=tab_schema,
                       options={"kafka.bootstrap.servers": "localhost:9092", 
                                "subscribe": "order",
                                "startingOffsets": "latest",
                                "failOnDataLoss": "false"})
print(table)

# in the new ibis 9.0, there are no more weird `Selection` nodes for filters,
# there's a new `Filter` node instead! Changing would require re-writing the compiler
# query = (table
#          .filter(_.merchantId % 2 == 0)
#          .select(["merchantId", "category"]))
# to_graph(query).render("out/test_query")
# node = query.op()

# careful: 
# - a "value" column is required
# - create a checkpointLocation on the host of this script (not the kafka container!)
# - call .start() on .to_kafka(), docs are wrong and that actually returns a DataStreamWriter, to get a StreamingQuery you need to call .start()
view = con.create_view("ordersview", table.mutate(value=_.category))
stream_query = con.to_kafka(view, options={"kafka.bootstrap.servers": "localhost:9092", 
                            "topic": "sink",
                            "checkpointLocation": "/tmp/spark_checkpoint"}).start()

stream_query.awaitTermination()