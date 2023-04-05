from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, current_timestamp, round, expr, \
    to_utc_timestamp, unix_timestamp, current_date, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType
import psycopg2

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
    [
        "org.postgresql:postgresql:42.4.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    ]
)

IN_TOPIC_NAME = 'student.topic.cohort5.xxxx'
OUT_TOPIC_NAME = 'student.topic.cohort5.xxx.out'

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required '
                              'username=\"de-student\" password=\"ltcneltyn\";',
}

spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()


def restaurant_read_stream(spark_session: SparkSession):
    """Функция принимает в себя спарк-сессию и возвращает топик из кафка"""
    schema = StructType([
        StructField("restaurant_id", StringType(), nullable=True),
        StructField("adv_campaign_id", StringType(), nullable=True),
        StructField("adv_campaign_content", StringType(), nullable=True),
        StructField("adv_campaign_owner", StringType(), nullable=True),
        StructField("adv_campaign_owner_contact", StringType(), nullable=True),
        StructField("adv_campaign_datetime_start", LongType(), nullable=True),
        StructField("adv_campaign_datetime_end", LongType(), nullable=True),
        StructField("datetime_created", LongType(), nullable=True)
    ])
    return spark_session.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option("subscribe", IN_TOPIC_NAME) \
        .options(**kafka_security_options) \
        .load() \
        .withColumn('data', from_json(col('value').cast(StringType()), schema)) \
        .withColumn('current_timestamp_utc', unix_timestamp(current_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')) \
        .withColumn('current_date', current_date()) \
        .select('data.restaurant_id', 'data.adv_campaign_id', 'data.adv_campaign_content', 'data.adv_campaign_owner',
                'data.adv_campaign_owner_contact', 'data.adv_campaign_datetime_start', 'data.adv_campaign_datetime_end',
                'data.datetime_created', 'current_date', 'current_timestamp_utc') \
        .where(
        "(adv_campaign_datetime_start < current_timestamp_utc) and (current_timestamp_utc < adv_campaign_datetime_end)")


def subscribers_restaurant_df(spark_session: SparkSession):
    """Функция принимает в себя спарк-сессию и возвращает данные из postgresql"""
    return spark_session.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'subscribers_restaurants') \
        .option('user', 'student') \
        .option('password', 'de-student') \
        .load()


def foreach_batch_function(df):
    """Функци обрабатывает каждый получаемый батч с данными и делает запись в postgresql и оправляет данные в kafka"""
    # добавляем колонку trigger_datetime_created
    df = df.withColumn('trigger_datetime_created', current_timestamp())
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.cache()
    # записываем df в PostgreSQL с полем feedback
    df.select("restaurant_id", "adv_campaign_id", "adv_campaign_content", "adv_campaign_owner",
              "adv_campaign_owner_contact", "adv_campaign_datetime_start", "adv_campaign_datetime_end",
              "datetime_created", "client_id", 'trigger_datetime_created') \
        .write.format("jdbc") \
        .option("url", f"jdbc:postgresql://{host}:{port}/{dbname}") \
        .mode("overwrite") \
        .option("driver", "org.postgresql.Driver").option("dbtable", "public.subscribers_feedback") \
        .option("user", f"{user}").option("password", f"{password}").save()
    # создаём df для отправки в Kafka. Сериализация в json.
    kafka_out = df.withColumn(
        'value',
        to_json(
            struct(
                col("restaurant_id"), col("adv_campaign_id"), col("adv_campaign_content"), col("adv_campaign_owner"),
                col("adv_campaign_owner_contact"), col("adv_campaign_datetime_start"), col("adv_campaign_datetime_end"),
                col("datetime_created"), col("client_id"), col('trigger_datetime_created')
            ))).select('value')

    kafka_out.write \
        .format("kafka") \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .options(**kafka_security_options) \
        .option("topic", OUT_TOPIC_NAME) \
        .mode("append") \
        .save()
    # очищаем память от df
    df.unpersist()


database_data = subscribers_restaurant_df(spark)
database_data.show(truncate=False)
stream_data = restaurant_read_stream(spark)
result_data = stream_data.join(database_data, 'restaurant_id', 'inner') \
    .withColumn('current_timestamp', current_timestamp()) \
    .dropDuplicates(['restaurant_id', 'client_id', 'current_timestamp']) \
    .withWatermark('current_timestamp', '5 minute')

host = 'localhost'
port = '5432'
dbname = 'de'
user = 'jovyan'
password = 'jovyan'
connect_to_postgresql = psycopg2.connect(f"host={host} port={port} dbname={dbname} user={user} password={password}")
cursor = connect_to_postgresql.cursor()
cursor.execute(open('DDL.sql', 'r').read())
connect_to_postgresql.commit()

result_data.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()
