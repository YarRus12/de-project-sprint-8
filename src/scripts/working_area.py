import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, current_timestamp, round, expr, to_utc_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType

print("version 1.1.19")

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


def restaurant_read_stream(spark):
    """Код по аналогии с кодом на уроке"""
    
    schema = StructType([
                StructField("restaurant_id", StringType(), nullable=True),
                StructField("adv_campaign_id", StringType(), nullable=True),
                StructField("adv_campaign_content", StringType(), nullable=True),
                StructField("adv_campaign_owner", StringType(), nullable=True),
                StructField("adv_campaign_owner_contact", StringType(), nullable=True),
                StructField("adv_campaign_datetime_start", StringType(), nullable=True), # Заменить тип
                StructField("adv_campaign_datetime_end", StringType(), nullable=True), # Заменить тип
                StructField("datetime_created", StringType(), nullable=True) # Заменить тип
                        ])
    df = spark.readStream.format('kafka')\
                .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
                .option("subscribe", IN_TOPIC_NAME)\
                .options(**kafka_security_options)\
                .load()\
                .withColumn('data', from_json(col('value').cast(StringType()), schema))\
                .withColumn('current_timestamp_utc', unix_timestamp(current_timestamp(),'yyyy-MM-dd HH:mm:ss.SSS'))\
                .select('data.restaurant_id', 'data.adv_campaign_id','data.adv_campaign_content','data.adv_campaign_owner',\
                    'data.adv_campaign_owner_contact','data.adv_campaign_datetime_start','data.adv_campaign_datetime_end',\
                        'data.datetime_created', 'current_timestamp_utc')\
                .where("(adv_campaign_datetime_start < current_timestamp_utc) and (current_timestamp_utc < adv_campaign_datetime_end)")
    return df



def subscribers_restaurant_df(spark):
    return spark.read \
            .format('jdbc') \
            .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
            .option('driver', 'org.postgresql.Driver') \
            .option('dbtable', 'subscribers_restaurants') \
            .option('user', 'student') \
            .option('password', 'de-student') \
            .load()
    

database_data = subscribers_restaurant_df(spark)
database_data.show(truncate=False)
stream_data = restaurant_read_stream(spark)
result_data = stream_data.join(database_data, 'restaurant_id', 'inner')


query = (result_data
             .writeStream
             .outputMode("append")
             .format("console")
             .option('spark.sql.streaming.forceDeleteTempCheckpointLocation', True)
             .option("truncate", False)
             .trigger(processingTime='30 seconds')
             .start())
try:
    query.awaitTermination()
finally:
    query.stop()

