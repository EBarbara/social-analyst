from pyspark.ml.feature import Word2VecModel, Word2Vec


class VectorizingModule:
    def __init__(self, inputCol, outputCol, dimensions):
        self.word2Vec = Word2Vec(vectorSize=dimensions, minCount=0, inputCol=inputCol, outputCol=outputCol)
        self.model_path = "training_data/trained_word2vec_model"

    def train(self, data_frame):
        word2vec_model = self.word2Vec.fit(data_frame)
        word2vec_model.save(self.model_path)

    def run(self, data_frame):
        word2vec_model = Word2VecModel.load(self.model_path)
        vectorized_data = word2vec_model.transform(data_frame)
        return vectorized_data
