# import TwitterStreamingModule
import TwitterStreamingFileModule as TwitterStreamingModule
from PreprocessingModule import PreprocessingModule
from VectorizingModule import VectorizingModule
from pyspark.ml.classification import NaiveBayes, NaiveBayesModel
from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # initializations
    spark = SparkSession.builder.appName("SocialAnalyst").getOrCreate()
    dimensions = 10
    folder = "tweets"
    preprocessingModule = PreprocessingModule(inputCol="text", outputCol="words")
    vectorizingModule = VectorizingModule(inputCol="words", outputCol="features", dimensions=dimensions)

    # streaming, preprocessing and vectorizing
    tweets = TwitterStreamingModule.run(spark, folder)
    tweets_filtered = preprocessingModule.run(tweets).drop("tokens")
    tweets_vectorized = vectorizingModule.run(tweets_filtered).drop("words")

    # classifying
    # nb = NaiveBayes(smoothing=1.0, modelType="multinomial", featuresCol="features")
    # nb_model = NaiveBayesModel.load("training_data/trained_naive_bayes_model")
    # tweets_classified = nb_model.transform(tweets_vectorized).drop("rawPrediction", "probability", "features")
    # tweets_final = tweets_classified.select("*").where("prediction != 0.0")

    # clustering
    kmeans = KMeans(featuresCol="features", k=50, seed=1)
    k_model = kmeans.fit(tweets_vectorized)
    # k_model.

    # sinking
    '''query = tweets_final.writeStream.\
        outputMode("append").\
        format("json").\
        option("path", "classified_data").\
        option("checkpointLocation", "data_checkpoint").\
        start()'''
    query = tweets_final.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
