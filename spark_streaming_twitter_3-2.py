from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests


def get_sql_context_instance(spark_context):
    if "sqlContextSingletonInstance" not in globals():
        globals()["sqlContextSingletonInstance"] = SQLContext(spark_context)
    return globals()["sqlContextSingletonInstance"]


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)

        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(word=w[0], word_count=w[1]))

        # create a DF from the Row RDD
        all_words_df = sql_context.createDataFrame(row_rdd)

        # Register the dataframe as table
        all_words_df.registerTempTable("words")

        # get the top 10 words from the table using SQL and print them
        all_words_counts_df = sql_context.sql(
            "select word, word_count from words order by word_count desc limit 10"
        )
        all_words_counts_df.show()

    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)

# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 8282
dataStream = ssc.socketTextStream("localhost", 8008)

# split each tweet into words
words = dataStream.flatMap(lambda line: line.split())

# Basic point-3: Map each word (not only hashtags) to be a pair of (word,1)
words_map = words.map(lambda x: (x, 1))

# Basic point-3: generate word count over the last 30 seconds of data, every 10 secords
word_count = words_map.reduceByKeyAndWindow(
    lambda x, y: x + y, lambda x, y: x - y, 30, 10
)

# do processing for each RDD generated in each interval
word_count.foreachRDD(process_rdd)

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()