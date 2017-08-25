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


def doc2vec(words, model):
    vector = numpy.zeros(10)
    word_count = 0

    for word in words:
        if word in model:
            vector += model[word]
            word_count += 1

    if word_count == 0:
        print("Zeroed vector from words {0} {1}".format(words, vector.tolist()))
        return vector
    return vector / float(word_count)


def run(tweet_stream, model):
    vectorized_data = tweet_stream.map(lambda tweet: (tweet[0],
                                                      tweet[1],
                                                      tweet[2],
                                                      tweet[3],
                                                      tweet[4],
                                                      doc2vec(tweet[4], model)))
    return vectorized_data
