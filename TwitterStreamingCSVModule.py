from pyspark.sql.types import *


class TwitterStreamingModule:
    def __init__(self, spark_session):
        self.session = spark_session
        self.folder = "C:\\Users\\Estevan\\PycharmProjects\\Mining\\tweets"
        self.schema = StructType() \
            .add("id", StringType()) \
            .add("time", TimestampType()) \
            .add("coordinates", StringType()) \
            .add("location", StringType()) \
            .add("text", StringType())

    def run(self):
        return self.session.readStream\
            .option("sep", ';')\
            .schema(self.schema)\
            .csv(self.folder)


