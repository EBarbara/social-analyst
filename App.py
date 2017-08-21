from PreprocessingModule import PreprocessingModule
from TwitterStreamingCSVModule import TwitterStreamingModule
from VectorizingModule import VectorizingModule
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, window

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SocialAnalyst").getOrCreate()
    streamingModule = TwitterStreamingModule(spark)
    preprocessingModule = PreprocessingModule(inputCol="text", outputCol="words")
    # vectorizingModule = VectorizingModule("glove.twitter.27B.25d.txt")

    tweets = streamingModule.run()

    #preprocessing
    tweets_filtered = preprocessingModule.run(tweets)

    # vectorize_spark = udf(vectorizingModule.vectorize, StringType())

    window_tweets = tweets_filtered.groupBy(
        window(tweets_filtered.time, "2 hours", "1 minute"),
        tweets_filtered.id,
        tweets_filtered.coordinates,
        tweets_filtered.location,
        tweets_filtered.words
    )

    # tweet_vectorized = tweets.withColumn('vector', vectorize_spark("text"))

    query = tweets_filtered.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
