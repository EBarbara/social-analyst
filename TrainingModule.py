from PreprocessingModule import PreprocessingModule
from VectorizingModule import VectorizingModule
from pyspark.ml.classification import NaiveBayes
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SocialAnalyst").getOrCreate()

    dimensions = 10
    preprocessingModule = PreprocessingModule(inputCol="text", outputCol="words")
    vectorizingModule = VectorizingModule(inputCol="words", outputCol="features", dimensions=dimensions)
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial", featuresCol="features", labelCol="label")

    model_schema = StructType([StructField("label", LongType(), True),
                               StructField("text", StringType(), True)])

    model_raw_dataframe = spark.read.format("csv")\
        .option("sep", ";")\
        .schema(model_schema)\
        .load("training_data/Training_bayes.csv")
    model_filtered = preprocessingModule.run(model_raw_dataframe)
    model_vectorized = vectorizingModule.run(model_filtered)
    model = nb.fit(model_vectorized)
    model.save("training_data/trained_naive_bayes_model")
