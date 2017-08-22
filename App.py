import PreprocessingModule
import TrainingModule
# import TwitterStreamingModule
import TwitterStreamingFileModule as TwitterStreamingModule
import VectorizingModule
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    # start Spark Context
    spark = SparkSession.builder.appName("SocialAnalyst").getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(spark.sparkContext, 5)

    # load word2vec model
    sqlContext = SQLContext(sc)
    model = sqlContext.read.parquet("training_data/trained_word2vec_model/data").alias("lookup")
    word2vec_model = model.rdd.collectAsMap()

    # train clusterer
    k_means_model = TrainingModule.run(sc, ssc, "training_data/training_tweets.txt", word2vec_model)

    # streaming and preprocessing
    tweets = TwitterStreamingModule.run(ssc)
    tweets_filtered = PreprocessingModule.run(tweets)
    tweets_vectorized = VectorizingModule.run(tweets_filtered, word2vec_model)
    tweets_vectorized.pprint()



    ssc.start()
    ssc.awaitTermination()

