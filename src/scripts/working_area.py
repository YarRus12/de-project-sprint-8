import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
            [
                "org.postgresql:postgresql:42.4.0",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            ]
        )

IN_TOPIC_NAME = 'student.topic.cohort5.xxxrichiexxx'

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()


def restaurant_read_stream_default(spark):
    """Дефолтный код из задания.
        Исполнение завершается с ошибкой
        ERROR DefaultSslEngineFactory: Modification time of key store could not be obtained: /usr/lib/jvm/java-1.17.0-openjdk-amd64/lib/security/cacerts
        На докер нет такого файла =(
    """
    return spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('kafka.ssl.truststore.location', '/usr/lib/jvm/java-1.17.0-openjdk-amd64/lib/security/cacerts') \
        .option('kafka.ssl.truststore.password', 'changeit') \
        .option('subscribe', IN_TOPIC_NAME) \
        .load()


def restaurant_read_stream_mine(spark):
    """Код по аналогии с кодом на уроке"""
    schema = StructType([
                StructField("restaurant_id", StringType(), nullable=True),
                StructField("adv_campaign_id", StringType(), nullable=True),
                StructField("adv_campaign_content", StringType(), nullable=True),
                StructField("adv_campaign_owner", StringType(), nullable=True),
                StructField("adv_campaign_owner_contact", StringType(), nullable=True),
                StructField("adv_campaign_datetime_start", StringType(), nullable=True), # Заменить тип
                StructField("adv_campaign_datetime_end", StringType(), nullable=True), # Заменить тип
                StructField("adv_campaign_datetime_end", StringType(), nullable=True), # Заменить тип
                StructField("datetime_created", StringType(), nullable=True) # Заменить тип
                        ])
    df = spark.readStream.format('kafka')\
                .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
                .option("subscribe", IN_TOPIC_NAME)\
                .options(**kafka_security_options)\
                .load()\
                .withColumn('data', from_json(col('value').cast(StringType()), schema))\
        
    df.printSchema()
    return df

result_df = restaurant_read_stream_mine(spark)
query = (result_df
             .writeStream
             .outputMode("append")
             .format("console")
             .option('spark.sql.streaming.forceDeleteTempCheckpointLocation', True)
             .option("truncate", False)
             .trigger(once=True)
             .start())
try:
    query.awaitTermination()
finally:
    query.stop()
