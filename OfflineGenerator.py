from PreprocessingModule import PreprocessingModule
from VectorizingModule import VectorizingModule
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType, LongType


def loadTrainData(spark_session):
    schema = StructType() \
        .add("id", LongType()) \
        .add("time", TimestampType()) \
        .add("coordinates", StringType()) \
        .add("location", StringType()) \
        .add("text", StringType())

    training_tweets = spark_session.read \
        .format("csv") \
        .option("sep", ";") \
        .schema(schema) \
        .load("training_data/TrainingTweets.csv")

    return training_tweets


def trainWord2Vec(spark_session):
    trainingTweets = loadTrainData(spark_session)

    # loading modules
    preprocessingModule = PreprocessingModule(inputCol="text", outputCol="words")
    vectorizingModule = VectorizingModule(inputCol="words", outputCol="features")

    # preprocessing
    tweets_filtered = preprocessingModule.run(trainingTweets)

    # vectorizing
    vectorizingModule.train(tweets_filtered)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SocialAnalyst").getOrCreate()
    sc = spark.sparkContext
    trainWord2Vec(spark)
    spark.stop()
