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
    streamingModule = TwitterStreamingModule(spark)

    tweets = streamingModule.run()

    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    tweets_tokenized = tokenizer.transform(tweets)
    window_tweets = tweets_tokenized.groupBy(
        window(tweets_tokenized.time, "2 hours", "1 minute"),
        tweets_tokenized.id,
        tweets_tokenized.coordinates,
        tweets_tokenized.location,
        tweets_tokenized.words
    )

    # tweet_vectorized = tweets.withColumn('vector', vectorize_spark("text"))

    query = tweets_tokenized.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
