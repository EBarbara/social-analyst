import PreprocessingModule
import TwitterStreamingModule
import VectorizingModule
from pyspark.mllib.linalg import Vectors


def run(spark_context, streaming_context, training_file, word2vec_model):
    lines = spark_context.textFile(training_file)
    training_raw = lines.map(lambda line: TwitterStreamingModule.extract_json(line)) \
        .filter(lambda json: json is not None) \
        .map(lambda json: (json["id"],
                           json["created_at"],
                           TwitterStreamingModule.extract_coordinates(json["coordinates"], json["place"])[0],
                           TwitterStreamingModule.extract_coordinates(json["coordinates"], json["place"])[1],
                           json["text"])) \
        .filter(lambda tweet: tweet[2] != 0 or tweet[3] != 0)
    training_queue = [training_raw]
    training_stream = streaming_context.queueStream(training_queue)
    training_filtered = PreprocessingModule.run(training_stream)
    training_vectorized = VectorizingModule.run(training_filtered, word2vec_model)
    training_data = training_vectorized.map(lambda tweet: (Vectors.dense(tweet[5].tolist())))
    return training_data
