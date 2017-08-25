import PreprocessingModule
# import TwitterStreamingModule
import TwitterStreamingFileModule as TwitterStreamingModule
import VectorizingModule
from pyspark import SQLContext
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.mllib.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    # Start k-means
    k_means_model = StreamingKMeans(k=10).setRandomCenters(3, 1.0, 0)

    # start Spark Context
    spark = SparkSession.builder.appName("SocialAnalyst").getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(spark.sparkContext, 5)

    # load word2vec model
    sqlContext = SQLContext(sc)
    model = sqlContext.read.parquet("training_data/trained_word2vec_model/data")
    word2vec_model = model.rdd.collectAsMap()

    # streaming and preprocessing
    tweets = TwitterStreamingModule.run(ssc)
    tweets_filtered = PreprocessingModule.run(tweets)
    tweets_vectorized = VectorizingModule.run(tweets_filtered, word2vec_model)

    # Run K-Means
    tweet_vectors = tweets_vectorized.map(lambda tweet: (Vectors.dense(tweet[5].tolist())))
    tweet_labelled = tweets_vectorized.map(lambda tweet: ((tweet[0], tweet[1], tweet[2], tweet[3], tweet[4]),
                                                          Vectors.dense(tweet[5].tolist())))
    k_means_model.trainOn(tweet_vectors)
    tweets_clustered = k_means_model.predictOnValues(tweet_labelled)
    tweets_clustered.pprint()

    ssc.start()
    ssc.awaitTermination()

