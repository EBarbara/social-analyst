from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType


if __name__ == "__main__":
    spark = SparkSession.builder.appName("SocialAnalyst").getOrCreate()
    sc = spark.sparkContext

    model_schema = StructType([StructField("word", StringType(), True),
                               StructField("vector", ArrayType(FloatType(), True), True)])
    print(model_schema)
    model_raw_rdd = sc.textFile("training_data/glove.twitter.27B.25d.txt")
    model_parsed_rdd = model_raw_rdd.map(lambda line: (line.split()[0], list(map(float, line.split()[1:]))))
    model_dataframe = spark.createDataFrame(model_parsed_rdd, model_schema)
    model_dataframe.write.save("training_data/trained_glove_model", format="parquet")