from pyflink.table import EnvironmentSettings, TableEnvironment
from kafka import KafkaConsumer

import ibis

"""
https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/try-flink/datastream/
base apache flink tutorial: Java

https://ibis-project.org/tutorials/open-source-software/apache-flink/1_single_feature
tutorial for using apache flink with ibis

https://github.com/ibis-project/ibis-flink-tutorial
github repo hosting the tutorial to be cloned: contains jupyter notebook with more complete guide
and docker compose to create Kafka topics, generate sample data, and launch a Flink cluster in the background.

So to make the following work:
1. ensure you have a .venv (if not: bottom-right corner / add new interpreter / local / virtualenv / new and add with 
defaults)
2. let PyCharm install imports by clicking on red underlined imports
3. PyCharm installed the wrong kafka! Do: `pip uninstall kafka` and `pip install kafka-python`
4. start the docker desktop application
5. cd to the [cloned notebook](https://github.com/ibis-project/ibis-flink-tutorial) and do `docker compose up -d`
6. run this script and ensure that it prints kafka topic contents
7. do `wget -N https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-conn
ector-kafka-3.0.2-1.18.jar`
"""


def conn():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    connection = ibis.flink.connect(table_env)
    print(connection)


def ibis_flink_tutorial():
    consumer = KafkaConsumer("transaction", auto_offset_reset="earliest")
    for _, msg in zip(range(10), consumer):
        # this ensures that messages exist in the `transaction` topic before
        # proceeding
        print(msg)

    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set("parallelism.default", "1")
    connection = ibis.flink.connect(table_env)

    connection.raw_sql("ADD JAR './flink-sql-connector-kafka-3.0.2-1.18.jar'")


if __name__ == '__main__':
    ibis_flink_tutorial()
