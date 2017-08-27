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
    '''centers = [
        [0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, 0.25, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, 0.0, 0.25, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.25, 0.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.25, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.25, 0.0],
        [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.25],
        [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [-0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, -0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, -0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, -0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, -0.25, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, 0.0, -0.25, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -0.25, 0.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -0.25, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -0.25, 0.0],
        [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -0.25]]
    weights = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
               1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
               1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
    k_means_model = StreamingKMeans(k=21).setInitialCenters(centers, weights)'''

    # start Spark Context
    spark = SparkSession.builder.appName("SocialAnalyst").getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(spark.sparkContext, 5)

    # load word embedding model
    sqlContext = SQLContext(sc)
    # vector_model = sqlContext.read.parquet("training_data/trained_glove_model")
    model = sqlContext.read.parquet("training_data/trained_word2vec_model/data")
    vector_model = model.rdd.collectAsMap()

    # streaming and preprocessing
    tweets = TwitterStreamingModule.run(ssc)
    tweets_filtered = PreprocessingModule.run(tweets)

    # breaking in time windows
    tweets_windowed = tweets_filtered.window(1800, 60)
    tweets_vectorized = VectorizingModule.run(tweets_windowed, vector_model, dimensions)
    tweets_vectorized.pprint()

    # Run K-Means
    '''tweet_vectors = tweets_vectorized.map(lambda tweet: (tweet[5].tolist()))
    tweet_labelled = tweets_vectorized.map(lambda tweet: ((tweet[0], tweet[1], tweet[2], tweet[3], tweet[4]),
                                                          tweet[5].tolist()))
    k_means_model.trainOn(tweet_vectors)
    tweets_clustered = k_means_model.predictOnValues(tweet_labelled)
    tweets_clustered.pprint()'''

    ssc.start()
    ssc.awaitTermination()
