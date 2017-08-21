import locale
from datetime import datetime

from TwitterStreamingCSVModule import TwitterStreamingModule
from VectorizingModule import VectorizingModule
from pyspark.ml.feature import Tokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, window, unix_timestamp, format_string
from pyspark.sql.types import StringType, TimestampType

locale.setlocale(locale.LC_TIME, "us")

if __name__ == "__main__":
    # vectorizingModule = VectorizingModule("glove.twitter.27B.25d.txt")

    spark = SparkSession.builder.appName("SocialAnalyst").getOrCreate()
    # vectorize_spark = udf(vectorizingModule.vectorize, StringType())
    timestamper = udf(lambda x: datetime.strptime(x, '%a %b %d %H:%M:%S +0000 %Y'), TimestampType())
    streamingModule = TwitterStreamingModule(spark)

    tweets = streamingModule.run()

    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    tweets_tokenized = tokenizer.transform(tweets)
    tweets_timed = tweets_tokenized.withColumn("timestamp", timestamper("time"))
    window_tweets = tweets_timed.groupBy(
        window(tweets_timed.timestamp, "2 hours", "1 minute"),
        tweets_timed.id,
        tweets_timed.coordinates,
        tweets_timed.location,
        tweets_timed.words
    )

    # tweet_vectorized = tweets.withColumn('vector', vectorize_spark("text"))

    query = window_tweets.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
