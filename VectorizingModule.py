'''from pyspark.ml.feature import Word2VecModel, Word2Vec


class VectorizingModule:
    def __init__(self, inputCol, outputCol):
        self.word2Vec = Word2Vec(vectorSize=10, minCount=0, inputCol=inputCol, outputCol=outputCol)
        self.model_path = "training_data/trained_word2vec_model"

    def train(self, data_frame):
        word2vec_model = self.word2Vec.fit(data_frame)
        word2vec_model.save(self.model_path)

    def run(self, data_frame):
        word2vec_model = Word2VecModel.load(self.model_path)
        vectorized_data = word2vec_model.transform(data_frame)
        return vectorized_data
'''
import numpy


def load_word2vec():
    pass


def average_word_vector(words, model, dimensions):
    vector = numpy.zeros(dimensions)
    word_count = 0

    for word in words:
        word_vector = model.filter(model.word == word).select(model.vector)
        if word_vector.count() == 1:
            vector += word_vector.collect()[0]
            word_count += 1

    if word_count == 0:
        return vector
    return vector / float(word_count)


def run(tweet_stream, model, dimensions):
    vectorized_data = tweet_stream.map(lambda tweet: (tweet[0],
                                                      tweet[1],
                                                      tweet[2],
                                                      tweet[3],
                                                      tweet[4],
                                                      average_word_vector(tweet[5], model, dimensions)))
    return vectorized_data
