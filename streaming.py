from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("spark-streaming").getOrCreate()

lines_df = spark.readStream.format("socket")\
    .option("host", "localhost").option("port", "9999").load()

words_df = lines_df.select(expr("explode(split(value, ' ')) as word"))
counts_df = words_df.groupBy("word").count()

word_count_query = counts_df.writeStream.format("console")\
    .outputMode("complete")\
    .option("checkpointLocation", ".checkpoint")\
    .start()

word_count_query.awaitTermination()


### nc -lk 9999 (to open socket with port 9999)
# then, spark-submit streaming.py
# in socket terminal, hit words (e.g. spark airflow flink kafka...)
