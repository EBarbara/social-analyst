import PreprocessingModule
import TwitterStreamingModule
import VectorizingModule
from pyspark.mllib.clustering import StreamingKMeans


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
    training_data = training_vectorized.map(lambda tweet: [tweet[2], tweet[3]] + tweet[5].tolist())
    model = StreamingKMeans(k=15, decayFactor=0.6).setRandomCenters(3, 1.0, 0)
    model.trainOn(training_data)
