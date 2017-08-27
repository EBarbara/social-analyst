from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType, TimestampType

schema = StructType([StructField("id", LongType(), True),
                     StructField("time", TimestampType(), True),
                     StructField("latitude", FloatType(), True),
                     StructField("longitude", FloatType(), True),
                     StructField("text", StringType(), True)])


def run(session, folder):
    return session.readStream.option('sep', ';').schema(schema).csv(folder)
