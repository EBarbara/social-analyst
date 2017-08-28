import PreprocessingModule
# import TwitterStreamingModule
import TwitterStreamingFileModule as TwitterStreamingModule
import VectorizingModule
from pyspark import SQLContext
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    # Start k-means
    dimensions = 10
    k_means_model = StreamingKMeans(k=200).setRandomCenters(dimensions, 1.0, 1)

    # start Spark Context
    spark = SparkSession.builder.appName("SocialAnalyst").getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(spark.sparkContext, 1)

    # load word embedding model
    sqlContext = SQLContext(sc)
    model = sqlContext.read.parquet("training_data/trained_word2vec_model/data")
    vector_model = model.rdd.collectAsMap()
    # vector_model = sqlContext.read.parquet("training_data/trained_glove_model")

    # streaming and preprocessing
    tweets = TwitterStreamingModule.run(ssc)
    tweets_filtered = PreprocessingModule.run(tweets)
    tweets_vectorized = VectorizingModule.run(tweets_filtered, vector_model, dimensions)

    # Run K-Means
    tweet_vectors = tweets_vectorized.map(lambda tweet: (tweet[6]))
    tweet_labelled = tweets_vectorized.map(lambda tweet: ((tweet[0], tweet[1], tweet[2], tweet[3], tweet[4], tweet[5]),
                                                          tweet[6]))
    k_means_model.trainOn(tweet_vectors)
    tweets_clustered = k_means_model.predictOnValues(tweet_labelled)

    # Each time window get cluster results and export data to json

    ssc.start()
    ssc.awaitTermination()
