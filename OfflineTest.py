from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, FloatType, TimestampType, StringType

if __name__ == "__main__":
    # initializations
    spark = SparkSession.builder.appName("SocialAnalyst").getOrCreate()

    schema = StructType([StructField("id", LongType(), True),
                         StructField("time", TimestampType(), True),
                         StructField("latitude", FloatType(), True),
                         StructField("longitude", FloatType(), True),
                         StructField("text", StringType(), True)])

    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
    stopword_remover = StopWordsRemover(inputCol="tokens", outputCol="words")
    hashing_tf = HashingTF(numFeatures=5000, inputCol="words", outputCol="rawFeatures")
    idf = IDF(inputCol="rawFeatures", outputCol="features")

    # streaming, preprocessing and vectorizing
    tweets = spark.read.format("csv").option("sep", ";").schema(schema).load("tweets/tweet.csv")
    tweets_tokenized = tokenizer.transform(tweets)
    tweets_filtered = stopword_remover.transform(tweets_tokenized).drop("tokens")
    tweets_hashed = hashing_tf.transform(tweets_filtered)
    idf_model = idf.fit(tweets_hashed)
    tweets_vectorized = idf_model.transform(tweets_hashed).drop("rawFeatures")
    tweets_vectorized.show()

    kmeans = KMeans(featuresCol="features", k=50, seed=1)
    k_model = kmeans.fit(tweets_vectorized)

    centers = k_model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)

    # sinking
    '''query = tweets_final.writeStream.\
        outputMode("append").\
        format("json").\
        option("path", "classified_data").\
        option("checkpointLocation", "data_checkpoint").\
        start()'''
    # query = tweets.writeStream.outputMode("append").format("console").start()
    # query.awaitTermination()
