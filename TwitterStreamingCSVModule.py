from pyspark.sql.types import StructType


class TwitterStreamingModule:
    def __init__(self, spark_session):
        self.session = spark_session
        self.folder = "C:\\Users\\Estevan\\PycharmProjects\\Mining\\tweets"
        self.schema = StructType() \
            .add("id", "string") \
            .add("time", "string") \
            .add("coordinates", "string") \
            .add("location", "string") \
            .add("text", "string")

    def run(self):
        return self.session.readStream\
            .option("sep", ";")\
            .option('includeTimestamp', 'true')\
            .schema(self.schema)\
            .csv(self.folder)


